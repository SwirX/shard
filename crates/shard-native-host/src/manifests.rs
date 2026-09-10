//! Native messaging host manifests for Firefox and Chromium browsers.
//!
//! Each browser discovers hosts through a manifest JSON file in a
//! well-known directory; `install` writes those files pointing at the host
//! binary itself, so `cargo install` + one command is all that is needed.

use serde_json::json;
use std::path::{Path, PathBuf};

/// Stable host id registered in the browsers (and expected by the extension).
pub const HOST_NAME: &str = "com.shard.native_host";

/// Default extension id accepted when `--extension` is not given.
pub const DEFAULT_EXTENSION: &str = "shard-extension@swirx";

/// Home manifest directory for a browser profile platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Browser {
    Firefox,
    Chrome,
    Chromium,
}

impl Browser {
    /// Manifest directory for this browser on Linux (per-user).
    pub fn manifest_dir(self) -> PathBuf {
        match self {
            Browser::Firefox => home_dir().join(".mozilla").join("native-messaging-hosts"),
            Browser::Chrome => config_home()
                .join("google-chrome")
                .join("NativeMessagingHosts"),
            Browser::Chromium => config_home().join("chromium").join("NativeMessagingHosts"),
        }
    }
}

/// Build one browser's manifest. `host_path` is the absolute location of the
/// `shard-native-host` binary; `extensions` are the extension ids allowed to
/// talk to it (Chrome stores them as origins).
pub fn manifest(browser: Browser, host_path: &Path, extensions: &[String]) -> serde_json::Value {
    let extensions = if extensions.is_empty() {
        vec![DEFAULT_EXTENSION.to_string()]
    } else {
        extensions.to_vec()
    };
    match browser {
        Browser::Firefox => json!({
            "name": HOST_NAME,
            "description": "shard download daemon bridge",
            "path": host_path,
            "type": "stdio",
            "allowed_extensions": extensions,
        }),
        Browser::Chrome | Browser::Chromium => json!({
            "name": HOST_NAME,
            "description": "shard download daemon bridge",
            "path": host_path,
            "type": "stdio",
            "allowed_origins": extensions
                .iter()
                .map(|id| format!("chrome-extension://{id}/"))
                .collect::<Vec<_>>(),
        }),
    }
}

/// Write the manifest for `browser` next to the given host binary, returning
/// the file path written.
pub fn install(
    browser: Browser,
    host_path: &Path,
    extensions: &[String],
) -> std::io::Result<PathBuf> {
    let dir = browser.manifest_dir();
    std::fs::create_dir_all(&dir)?;
    let file = dir.join(format!("{HOST_NAME}.json"));
    let contents = serde_json::to_vec_pretty(&manifest(browser, host_path, extensions))
        .expect("manifest always serializes");
    std::fs::write(&file, contents)?;
    Ok(file)
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
}

fn config_home() -> PathBuf {
    if let Some(base) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(base)
    } else {
        home_dir().join(".config")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn firefox_manifest_lists_extension_ids() {
        let value = manifest(
            Browser::Firefox,
            Path::new("/opt/shard/shard-native-host"),
            &["ext@x".into()],
        );
        assert_eq!(value["name"], HOST_NAME);
        assert_eq!(value["type"], "stdio");
        assert_eq!(value["path"], "/opt/shard/shard-native-host");
        assert_eq!(value["allowed_extensions"], json!(["ext@x"]));
        assert!(value.get("allowed_origins").is_none());
    }

    #[test]
    fn chrome_manifest_maps_ids_to_origins() {
        let value = manifest(
            Browser::Chrome,
            Path::new("/opt/shard/shard-native-host"),
            &["ext@x".into()],
        );
        assert_eq!(
            value["allowed_origins"],
            json!(["chrome-extension://ext@x/"])
        );
        assert!(value.get("allowed_extensions").is_none());
    }

    #[test]
    fn empty_extension_list_uses_the_default() {
        let value = manifest(Browser::Firefox, Path::new("/tmp/host"), &[]);
        assert_eq!(value["allowed_extensions"], json!([DEFAULT_EXTENSION]));
    }
}
