use chrono::Local;
use clap::{Arg, Command};
use ibm1047::Decode;
use log::{debug, error, info};
use rayon::prelude::*; // par_iter
use regex::Regex;
use std::cmp::min;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command as PCommand, Stdio};
use tempfile::tempdir;

/// Runtime parameters
struct Parameters {
    regex_patterns: Vec<Regex>,
    skip_mime: Vec<String>,
    max_file_size: u64,
    max_archive_size: u64,
}

/// Represents a data source (file with optional logical path)
#[derive(Clone, Debug)]
struct Datasource {
    data_path: PathBuf,
    logical_path: Option<String>,
}

impl Datasource {
    fn new(path: PathBuf, lpath: Option<String>) -> Self {
        Datasource {
            data_path: path,
            logical_path: lpath,
        }
    }
    fn path(&self) -> &Path {
        &self.data_path
    }
    fn lpath(&self) -> String {
        self.logical_path
            .clone()
            .unwrap_or_else(|| self.data_path.to_string_lossy().to_string())
    }
}

struct TempWorkDir {
    path: PathBuf,
}

impl TempWorkDir {
    fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for TempWorkDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn setup_logger(verbose_file: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let mut base_config = fern::Dispatch::new();

    // Console: INFO level, simple message
    base_config = base_config.chain(
        fern::Dispatch::new()
            .level(log::LevelFilter::Info)
            .format(|out, message, _record| out.finish(format_args!("{}", message)))
            .chain(std::io::stdout()),
    );

    // Optional file: DEBUG level, detailed
    if let Some(file) = verbose_file {
        base_config = base_config.chain(
            fern::Dispatch::new()
                .level(log::LevelFilter::Debug)
                .format(|out, message, record| {
                    out.finish(format_args!(
                        "{} [{}] {} - {}",
                        Local::now().format("%Y-%m-%d %H:%M:%S"),
                        record.level(),
                        record.target(),
                        message
                    ))
                })
                .chain(fern::log_file(file)?),
        );
    }
    base_config.apply()?;
    Ok(())
}

/// Report a failure
fn fail(kind: &str, ds: &Datasource, reason: &str) -> bool {
    error!("FAIL {} File: {} Reason: {}", kind, ds.lpath(), reason);
    false
}

fn content_match(ds: &Datasource, params: &Parameters, content: &str, file_type: &str) -> bool {
    for re in &params.regex_patterns {
        if let Some(m) = re.find(content) {
            info!(
                "MATCH {}\tFile: {} Offset: {} FileType: {}",
                m.as_str(),
                ds.lpath(),
                m.start(),
                file_type
            );
            return true;
        }
    }
    false
}

fn read_contents(ds: &Datasource, max_size: u64) -> std::io::Result<Vec<u8>> {
    let file = File::open(ds.path())?;
    let metadata = file.metadata()?;
    let file_size = min(metadata.len(), max_size);

    let mut buffer = Vec::with_capacity(file_size as usize);
    let mut content = file.take(file_size as u64);
    let _ = content.read_to_end(&mut buffer);
    Ok(buffer)
}

/// Check file contents for regex matches
fn check(ds: &Datasource, params: &Parameters, file_type: &str) -> bool {
    debug!("SCAN {} {}", file_type, ds.lpath());
    match read_contents(ds, params.max_file_size) {
        Ok(contents) => {
            return content_match(
                ds,
                params,
                &String::from_utf8_lossy(&contents).into_owned(),
                file_type,
            );
        }
        Err(error) => {
            return fail("READ", ds, &error.to_string());
        }
    }
}

/// Return MIME type and charset of the file
fn get_mime(ds: &Datasource) -> Option<(String, Option<String>)> {
    let output = PCommand::new("file")
        .arg("--mime")
        .arg(ds.path())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let text = String::from_utf8_lossy(&output.stdout);
    if let Some((_, rest)) = text.split_once(": ") {
        let parts: Vec<&str> = rest.trim().split("; charset=").collect();
        let mime_type = parts[0].to_string();
        let charset = if parts.len() > 1 {
            Some(parts[1].to_string())
        } else {
            None
        };
        return Some((mime_type, charset));
    }
    None
}

/// Determine if MIME type should be skipped
fn skip_mime_type(params: &Parameters, mime_type: &str) -> bool {
    params
        .skip_mime
        .iter()
        .any(|t| t == mime_type || (t.ends_with('*') && mime_type.starts_with(&t[..t.len() - 1])))
}

fn convert_and_check(ds: &Datasource, params: &Parameters, file_type: &str) -> bool {
    match read_contents(ds, params.max_file_size) {
        Ok(contents) => {
            return content_match(ds, params, &String::from_ibm1047(&contents), file_type);
        }
        Err(error) => {
            return fail("READ", ds, &error.to_string());
        }
    }
}

fn classify_file(ds: &Datasource, params: &Parameters) {
    let tmpdir = tempdir().expect("Failed to create temporary directory");
    classify_file_entry(ds, params, tmpdir.path());
}

/// Classify a file
fn classify_file_entry(ds: &Datasource, params: &Parameters, tmpdir: &Path) {
    match get_mime(ds) {
        Some((mime_type, charset)) => {
            if skip_mime_type(params, &mime_type) {
                debug!("SKIP known {}: {}", mime_type, ds.lpath());
            } else if let Some(charset) = charset
                && (charset != "binary")
            {
                if charset == "unknown-8bit" || charset == "ebcdic" {
                    debug!("EBCDIC {}: {}", mime_type, ds.lpath());
                    convert_and_check(ds, params, &mime_type);
                } else {
                    check(ds, params, &format!("{}-{}", mime_type, charset));
                }
            } else {
                let mime_subtype = mime_type
                    .split_once('/')
                    .map(|(_, s)| s)
                    .unwrap_or(&mime_type);
                match mime_subtype {
                    "octet-stream" => {
                        if !check(ds, params, &mime_type) {
                            convert_and_check(ds, params, &mime_type);
                        }
                    }
                    "gzip" | "zip" | "zstd" | "java-archive" | "x-tar" | "x-gzip" | "x-bzip2"
                    | "x-7z-compressed" | "x-rar" | "x-xz" | "x-lz4" | "x-compress" | "x-rpm" => {
                        debug!("ARCHIVE {}: {}", mime_type, ds.lpath());
                        extract_and_classify(ds, params, &mime_type, &tmpdir);
                    }
                    _ => {
                        debug!("DEFAULT {}: {}", mime_type, ds.lpath());
                        check(ds, params, mime_type.as_str());
                    }
                }
            }
        }
        None => {
            fail("MIME", ds, "Could not determine MIME type");
        }
    }
}

fn extract_and_classify(ds: &Datasource, params: &Parameters, mime_type: &String, tmpdir: &Path) {
    let archive_path = ds.path();

    if let Ok(metadata) = fs::metadata(archive_path) {
        if metadata.len() > params.max_archive_size {
            fail(
                "EXTRACT",
                ds,
                &format!("Archive is too big: {} bytes", metadata.len()),
            );
            return;
        }
    }

    let extract_dir = archive_path
        .file_name()
        .map(|basename| tmpdir.join(basename).with_extension("_extracted"))
        .expect("Work directory name");

    let archive_type = archive_path
        .extension()
        .and_then(|ext| ext.to_str())
        .or_else(|| {
            mime_type.split_once('/').map(|(_, archive_type)| {
                if let Some(stripped) = archive_type.strip_prefix("x-") {
                    stripped
                } else {
                    archive_type
                }
            })
        });
    let archive_type: &str = match archive_type {
        Some(atype) => atype,
        None => {
            fail("ARCH", ds, &format!("Unsupported archive {mime_type}"));
            return;
        }
    };
    let workdir = TempWorkDir::new(extract_dir).expect("Work directory");
    let extract_dir_str = String::from(workdir.path().to_str().unwrap());
    let archive_path_str = String::from(archive_path.to_str().unwrap());
    debug!(
        "extract_dir_str={} archive_path_str={}",
        extract_dir_str, archive_path_str
    );
    struct Decompress<C, T> {
        cmd: C,
        args: T,
        stdout: bool,
    }
    let decompress = match archive_type {
        "tar" => Decompress {
            cmd: "tar",
            args: vec!["-xf", &archive_path_str, "-C", &extract_dir_str],
            stdout: false,
        },
        "tar.gz" | "tgz" | "compress" => Decompress {
            cmd: "tar",
            args: vec!["-xzf", &archive_path_str, "-C", &extract_dir_str],
            stdout: false,
        },
        "tar.bz2" | "tbz2" => Decompress {
            cmd: "tar",
            args: vec!["-xjf", &archive_path_str, "-C", &extract_dir_str],
            stdout: false,
        },
        "7z" | "7z-compressed" => Decompress {
            cmd: "7z",
            args: vec!["x", "-p", "-y", &archive_path_str, "-o", &extract_dir_str], // -o{extract_dir}
            stdout: false,
        },
        "zip" | "jar" => Decompress {
            cmd: "unzip",
            args: vec![
                "-P",
                "dummy",
                "-o",
                "-qq",
                &archive_path_str,
                "-d",
                &extract_dir_str,
            ],
            stdout: false,
        },
        "rpm" => Decompress {
            cmd: "bsdtar",
            args: vec!["-xvf", &archive_path_str, "-C", &extract_dir_str],
            stdout: false,
        },
        "gz" | "gzip" => Decompress {
            cmd: "gunzip",
            args: vec!["-c", &archive_path_str],
            stdout: true,
        },
        "bz" | "bz2" | "bzip2" => Decompress {
            cmd: "bunzip2",
            args: vec!["-c", &archive_path_str],
            stdout: true,
        },
        "xz" => Decompress {
            cmd: "xz",
            args: vec!["-dc", &archive_path_str],
            stdout: true,
        },
        "lz4" => Decompress {
            cmd: "lz4",
            args: vec!["-d", &archive_path_str],
            stdout: true,
        },
        "zstd" => Decompress {
            cmd: "zstd",
            args: vec!["-d", &archive_path_str, "--stdout"],
            stdout: true,
        },
        other => {
            fail("ARCH", ds, &format!("Unsupported archive {other}"));
            return;
        }
    };
    {
        let mut cmd = PCommand::new(decompress.cmd);
        cmd.args(&decompress.args);
        if decompress.stdout {
            // Build output file path
            let file_name = Path::new(archive_path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("output");

            let file_name_without_ext = file_name
                .strip_suffix(&format!(".{archive_type}"))
                .unwrap_or(file_name);

            // Decompress to the file
            let output_path = PathBuf::new()
                .join(extract_dir_str)
                .join(file_name_without_ext);
            match File::create(&output_path) {
                Ok(output_file) => {
                    cmd.stdout(Stdio::from(output_file));
                }
                Err(error) => {
                    fail("ARCH", ds, &format!("{}", error));
                    return;
                }
            }
        } else {
            cmd.stdout(Stdio::null());
        }
        cmd.stderr(Stdio::null());

        if let Err(error) = cmd.status() {
            fail("ARCH", ds, &format!("{}", error));
            return;
        }
    }

    for entry in walkdir::WalkDir::new(workdir.path())
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
    {
        let basename = entry.file_name();
        let fixed = basename.to_str().unwrap().replace(" ", "_");

        classify_file_entry(
            &Datasource::new(entry.into_path(), Some(format!("{}/{}", ds.lpath(), fixed))),
            params,
            workdir.path(),
        );
    }
}

/// Parse regex file
fn load_regex_file(path: &Path) -> Vec<Regex> {
    let file = fs::File::open(path).expect("Regex file missing");
    let reader = BufReader::new(file);
    reader
        .lines()
        .filter_map(|line| {
            let l = line.ok()?.trim().to_string();
            if l.is_empty() || l.starts_with('#') {
                None
            } else {
                Some(Regex::new(&l).expect("invalid regex"))
            }
        })
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("scanner")
        .arg(Arg::new("directory").required(true))
        .arg(
            Arg::new("verbose_file")
                .short('v')
                .long("verbose")
                .help("Verbose log file"),
        )
        .arg(
            Arg::new("regex_file")
                .short('r')
                .long("regex")
                .default_value("regex.lst"),
        )
        .arg(
            Arg::new("skip_mime_file")
                .long("skip_mime")
                .default_value("skip-mime.lst"),
        )
        .arg(Arg::new("jobs").short('j').long("jobs").default_value("0"))
        .get_matches();

    let verbose_file = matches
        .get_one::<String>("verbose_file")
        .map(|s| s.as_str());
    setup_logger(verbose_file)?;

    let regex_file = matches.get_one::<String>("regex_file").unwrap();
    let regex_patterns = load_regex_file(Path::new(regex_file));
    if regex_patterns.is_empty() {
        return Err("Regex list is empty".into());
    }

    let skip_mime_file = matches.get_one::<String>("skip_mime_file").unwrap();
    let skip_mime = fs::read_to_string(skip_mime_file)
        .unwrap_or_default()
        .lines()
        .map(|s| s.to_string())
        .collect();

    let params = Parameters {
        regex_patterns,
        skip_mime,
        max_file_size: 100 * 1024 * 1024,           // 100MB
        max_archive_size: 100 * 1024 * 1024 * 1024, // 1GB
    };

    let directory = matches.get_one::<String>("directory").unwrap();

    walkdir::WalkDir::new(directory)
        .follow_links(true)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .par_bridge() // turn the sequential iterator into a parallel stream
        .map(|entry| Datasource::new(entry.into_path(), None))
        .for_each(|ds| {
            classify_file(&ds, &params);
        });

    Ok(())
}
