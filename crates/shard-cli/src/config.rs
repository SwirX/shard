use crate::cli::style::ColorChoice;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub style: StyleSection,
    #[serde(default)]
    pub download: DownloadSection,
    #[serde(default)]
    pub filetype: FiletypeSection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StyleSection {
    #[serde(default = "default_color")]
    pub color: ColorChoice,
    #[serde(rename = "prefer-eyecandy", default = "default_prefer_eyecandy")]
    pub prefer_eyecandy: bool,
}

impl Default for StyleSection {
    fn default() -> Self {
        Self {
            color: default_color(),
            prefer_eyecandy: default_prefer_eyecandy(),
        }
    }
}

fn default_color() -> ColorChoice {
    ColorChoice::Auto
}

fn default_prefer_eyecandy() -> bool {
    false
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadSection {
    #[serde(default = "default_connections")]
    pub connections: usize,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_retry_base_ms")]
    pub retry_base_ms: u64,
    #[serde(default = "default_retry_max_ms")]
    pub retry_max_ms: u64,
    #[serde(default = "default_checkpoint_ms")]
    pub checkpoint_ms: u64,
    #[serde(default = "default_resume")]
    pub resume: bool,
    #[serde(default = "default_download_dir")]
    pub download_dir: String,
}

impl Default for DownloadSection {
    fn default() -> Self {
        Self {
            connections: default_connections(),
            chunk_size: default_chunk_size(),
            max_attempts: default_max_attempts(),
            retry_base_ms: default_retry_base_ms(),
            retry_max_ms: default_retry_max_ms(),
            checkpoint_ms: default_checkpoint_ms(),
            resume: default_resume(),
            download_dir: default_download_dir(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiletypeSection {
    #[serde(default = "default_video_dir")]
    pub video: String,
    #[serde(default = "default_image_dir")]
    pub image: String,
    #[serde(default = "default_audio_dir")]
    pub audio: String,
    #[serde(default = "default_archive_dir")]
    pub archive: String,
    #[serde(default = "default_document_dir")]
    pub document: String,
    #[serde(default = "default_other_dir")]
    pub other: String,
}

impl Default for FiletypeSection {
    fn default() -> Self {
        Self {
            video: default_video_dir(),
            image: default_image_dir(),
            audio: default_audio_dir(),
            archive: default_archive_dir(),
            document: default_document_dir(),
            other: default_other_dir(),
        }
    }
}

fn default_connections() -> usize {
    8
}

fn default_chunk_size() -> u64 {
    8 * 1024 * 1024
}

fn default_max_attempts() -> u32 {
    5
}

fn default_retry_base_ms() -> u64 {
    500
}

fn default_retry_max_ms() -> u64 {
    30_000
}

fn default_checkpoint_ms() -> u64 {
    3000
}

fn default_resume() -> bool {
    true
}

fn default_download_dir() -> String {
    let base = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    let mut candidate = base.join("Downloads");
    if let Ok(dirs) = std::fs::read_to_string(base.join(".config").join("user-dirs.dirs"))
        && let Some(line) = dirs
            .lines()
            .find_map(|line| line.strip_prefix("XDG_DOWNLOAD_DIR="))
    {
        let expanded = line
            .trim()
            .trim_matches('"')
            .replace("$HOME", base.to_str().unwrap_or(""));
        if !expanded.is_empty() {
            candidate = PathBuf::from(expanded);
        }
    }
    candidate.to_string_lossy().to_string()
}

fn default_video_dir() -> String {
    "Videos".to_string()
}

fn default_image_dir() -> String {
    "Images".to_string()
}

fn default_audio_dir() -> String {
    "Audio".to_string()
}

fn default_archive_dir() -> String {
    "Archives".to_string()
}

fn default_document_dir() -> String {
    "Documents".to_string()
}

fn default_other_dir() -> String {
    "Other".to_string()
}

impl Config {
    pub fn load() -> std::io::Result<Config> {
        Self::load_from(&config_path())
    }

    fn load_from(path: &Path) -> std::io::Result<Config> {
        let Ok(body) = std::fs::read_to_string(path) else {
            return Ok(Config::default());
        };
        if body.trim().is_empty() {
            return Ok(Config::default());
        }
        toml::from_str(&body).map_err(|err| {
            std::io::Error::other(format!("invalid config {}: {err}", path.display()))
        })
    }

    pub fn init() -> std::io::Result<()> {
        Self::init_at(&config_path())
    }

    fn init_at(path: &Path) -> std::io::Result<()> {
        if path.exists() {
            return Err(std::io::Error::other(format!(
                "config already exists at {}",
                path.display()
            )));
        }
        std::fs::create_dir_all(path.parent().expect("config always has a parent directory"))?;
        shard_core::fsutil::atomic_write(path, SHARD_CONF_SAMPLE.as_bytes())
    }

    pub fn set(key: &str, value: &str) -> std::io::Result<()> {
        Self::set_at(&config_path(), key, value)
    }

    fn set_at(path: &Path, key: &str, value: &str) -> std::io::Result<()> {
        let (section, field, parsed) = parse_set_value(key, value)?;
        std::fs::create_dir_all(path.parent().expect("config always has a parent directory"))?;
        let mut doc: toml::Table = std::fs::read_to_string(path)
            .ok()
            .and_then(|body| toml::from_str(&body).ok())
            .unwrap_or_default();
        let table = doc
            .entry(section.as_str().to_string())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        let table = table
            .as_table_mut()
            .ok_or_else(|| std::io::Error::other("config section is not a table"))?;
        table.insert(field.to_string(), parsed);
        shard_core::fsutil::atomic_write(
            path,
            toml::to_string(&doc)
                .expect("re-serializing toml cannot fail")
                .as_bytes(),
        )
    }

    pub fn edit() -> std::io::Result<()> {
        let path = config_path();
        if !path.exists() {
            Self::init()?;
        }
        let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
        let status = std::process::Command::new(&editor).arg(&path).status()?;
        if !status.success() {
            return Err(std::io::Error::other(format!(
                "{editor} exited with {status}"
            )));
        }
        Ok(())
    }

    pub fn keys() -> [&'static str; 16] {
        [
            "color",
            "prefer-eyecandy",
            "connections",
            "chunk_size",
            "max_attempts",
            "retry_base_ms",
            "retry_max_ms",
            "checkpoint_ms",
            "resume",
            "download_dir",
            "filetype.video",
            "filetype.image",
            "filetype.audio",
            "filetype.archive",
            "filetype.document",
            "filetype.other",
        ]
    }
}

pub fn config_path() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .unwrap_or_default();
    base.join("shard").join("shard.conf")
}

#[derive(Clone, Copy)]
enum Section {
    Style,
    Download,
    Filetype,
}

impl Section {
    const fn as_str(self) -> &'static str {
        match self {
            Section::Style => "style",
            Section::Download => "download",
            Section::Filetype => "filetype",
        }
    }
}

fn parse_set_value<'a>(
    key: &'a str,
    value: &str,
) -> std::io::Result<(Section, &'a str, toml::Value)> {
    let (section, field): (Section, &str) = match key {
        "color" | "prefer-eyecandy" => (Section::Style, key),
        "connections" | "chunk_size" | "max_attempts" | "retry_base_ms" | "retry_max_ms"
        | "checkpoint_ms" | "resume" | "download_dir" => (Section::Download, key),
        "filetype.video" | "filetype.image" | "filetype.audio" | "filetype.archive"
        | "filetype.document" | "filetype.other" => (
            Section::Filetype,
            key.strip_prefix("filetype.").unwrap_or(key),
        ),
        _ => {
            return Err(std::io::Error::other(format!(
                "unknown key {key:?}; pick one of: {}",
                Config::keys().join(", ")
            )));
        }
    };
    let parsed = match field {
        "color" => parse_enum(
            value,
            &["auto", "always", "never"],
            "color must be auto, always, or never",
        )?,
        "prefer-eyecandy" => toml::Value::Boolean(parse_bool(value, field)?),
        "resume" => toml::Value::Boolean(parse_bool(value, field)?),
        "connections" | "chunk_size" | "max_attempts" | "retry_base_ms" | "retry_max_ms"
        | "checkpoint_ms" => toml::Value::Integer(parse_positive(value, field)?),
        _ => toml::Value::String(value.to_string()),
    };
    Ok((section, field, parsed))
}

fn parse_enum(value: &str, allowed: &[&str], hint: &str) -> std::io::Result<toml::Value> {
    if !allowed.contains(&value) {
        return Err(std::io::Error::other(hint.to_string()));
    }
    Ok(toml::Value::String(value.to_string()))
}

fn parse_bool(value: &str, field: &str) -> std::io::Result<bool> {
    match value {
        "true" | "yes" | "on" | "1" => Ok(true),
        "false" | "no" | "off" | "0" => Ok(false),
        other => Err(std::io::Error::other(format!(
            "{field} must be true or false, got {other:?}"
        ))),
    }
}

fn parse_positive(value: &str, field: &str) -> std::io::Result<i64> {
    let parsed: i64 = value
        .parse()
        .map_err(|_| std::io::Error::other(format!("{field} must be a positive integer")))?;
    if parsed <= 0 {
        return Err(std::io::Error::other(format!(
            "{field} must be a positive integer"
        )));
    }
    Ok(parsed)
}

const SHARD_CONF_SAMPLE: &str = r#"# shard.conf - at ~/.config/shard/shard.conf (or $XDG_CONFIG_HOME/shard/shard.conf)
# Every key is optional; omitted keys fall back to the built-in defaults shown here.

[style]
color = "auto"         # auto | always | never
prefer-eyecandy = false # false = TTY-safe hash/ascii glyphs; true = Nerd Font blocks

[download]
connections = 8
chunk_size = 8388608
max_attempts = 5
retry_base_ms = 500
retry_max_ms = 30000
checkpoint_ms = 3000
resume = true
download_dir = "~/Downloads" # base dir when -o is omitted; ~ is expanded

[filetype]
video = "Videos"
image = "Images"
audio = "Audio"
archive = "Archives"
document = "Documents"
other = "Other"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "shard-config-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("shard.conf")
    }

    static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    #[test]
    fn absent_file_yields_builtin_defaults() {
        let config = Config::load_from(&scratch()).unwrap();
        assert_eq!(config.style.color, ColorChoice::Auto);
        assert!(!config.style.prefer_eyecandy);
        assert_eq!(config.download.connections, 8);
        assert_eq!(config.download.chunk_size, 8 * 1024 * 1024);
        assert!(config.download.resume);
    }

    #[test]
    fn partial_files_fill_missing_keys_from_defaults() {
        let path = scratch();
        std::fs::write(&path, "[download]\nconnections = 3\n").unwrap();
        let config = Config::load_from(&path).unwrap();
        assert_eq!(config.download.connections, 3);
        assert_eq!(config.download.max_attempts, 5);
        assert_eq!(config.style.color, ColorChoice::Auto);
    }

    #[test]
    fn invalid_toml_is_reported() {
        let path = scratch();
        std::fs::write(&path, "connections = ]").unwrap();
        assert!(Config::load_from(&path).is_err());
    }

    #[test]
    fn bad_enum_value_fails_typed_set() {
        let path = scratch();
        assert!(Config::set_at(&path, "color", "violet").is_err());
        assert!(Config::set_at(&path, "connections", "twelve").is_err());
        assert!(Config::set_at(&path, "connections", "0").is_err());
        assert!(Config::set_at(&path, "resume", "true").is_ok());
        assert!(Config::set_at(&path, "resume", "no").is_ok());
    }

    #[test]
    fn set_then_load_round_trips() {
        let path = scratch();
        Config::set_at(&path, "chunk_size", "4194304").unwrap();
        Config::set_at(&path, "prefer-eyecandy", "true").unwrap();
        let config = Config::load_from(&path).unwrap();
        assert_eq!(config.download.chunk_size, 4 * 1024 * 1024);
        assert!(config.style.prefer_eyecandy);
    }

    #[test]
    fn overwriting_an_existing_key_updates_it() {
        let path = scratch();
        Config::set_at(&path, "connections", "3").unwrap();
        Config::set_at(&path, "connections", "6").unwrap();
        let config = Config::load_from(&path).unwrap();
        assert_eq!(config.download.connections, 6);
        assert_eq!(config.download.max_attempts, 5);
    }

    #[test]
    fn unknown_key_is_rejected() {
        let path = scratch();
        let message = Config::set_at(&path, "threads", "4")
            .unwrap_err()
            .to_string();
        assert!(message.contains("unknown key"), "got: {message}");
    }

    #[test]
    fn sample_document_parses() {
        let config: Config = toml::from_str(SHARD_CONF_SAMPLE).unwrap();
        assert_eq!(config.download.connections, 8);
        assert_eq!(config.style.color, ColorChoice::Auto);
        assert!(!config.style.prefer_eyecandy);
        assert_eq!(config.download.download_dir, "~/Downloads");
        assert_eq!(config.filetype.video, "Videos");
        assert_eq!(config.filetype.other, "Other");
        assert_eq!(config.filetype.document, "Documents");
    }

    #[test]
    fn download_dir_and_filetype_setting_round_trip() {
        let path = scratch();
        Config::set_at(&path, "download_dir", "/home/me/Shared").unwrap();
        Config::set_at(&path, "filetype.video", "Movies").unwrap();
        let config = Config::load_from(&path).unwrap();
        assert_eq!(config.download.download_dir, "/home/me/Shared");
        assert_eq!(config.filetype.video, "Movies");
        assert_eq!(config.filetype.image, "Images");
    }
}
