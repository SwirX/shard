use crate::engine::error::{DownloadError, DownloadResult};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub url: String,
    pub final_url: String,
    pub filename: String,
    pub remote: RemoteSnapshot,
    pub layout: LayoutSnapshot,
    pub chunks: Vec<ChunkEntry>,
    pub integrity: IntegritySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSnapshot {
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutSnapshot {
    pub chunk_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegritySnapshot {
    #[serde(default)]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ManifestTemplate {
    pub url: String,
    pub final_url: String,
    pub filename: String,
    pub remote: RemoteSnapshot,
    pub chunk_size: u64,
}

impl ManifestTemplate {
    pub fn into_manifest(self, chunks: Vec<ChunkEntry>, sha256: Option<String>) -> Manifest {
        Manifest {
            version: MANIFEST_VERSION,
            url: self.url,
            final_url: self.final_url,
            filename: self.filename,
            remote: self.remote,
            layout: LayoutSnapshot {
                chunk_size: self.chunk_size,
            },
            chunks,
            integrity: IntegritySnapshot { sha256 },
        }
    }
}

impl Manifest {
    pub fn load(dest: &Path) -> DownloadResult<Option<Manifest>> {
        let Ok(body) = std::fs::read_to_string(sidecar_path(dest)) else {
            return Ok(None);
        };
        let manifest: Manifest = serde_json::from_str(&body)?;
        if manifest.version != MANIFEST_VERSION {
            return Err(DownloadError::InvalidArgument(format!(
                "manifest version {} is not supported",
                manifest.version
            )));
        }
        Ok(Some(manifest))
    }

    pub fn save_atomic(&self, dest: &Path) -> DownloadResult<()> {
        let body = serde_json::to_vec_pretty(self)?;
        write_json_atomic(dest, &body)
    }
}

pub fn sidecar_path(dest: &Path) -> PathBuf {
    with_suffix(dest, ".shard")
}

pub fn remove_sidecar(dest: &Path) {
    let _ = std::fs::remove_file(sidecar_path(dest));
    let _ = std::fs::remove_file(tmp_path(dest));
}

fn tmp_path(dest: &Path) -> PathBuf {
    with_suffix(dest, ".shard.tmp")
}

fn with_suffix(dest: &Path, suffix: &str) -> PathBuf {
    let mut name = dest
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    name.push(suffix);
    dest.with_file_name(name)
}

fn write_json_atomic(dest: &Path, body: &[u8]) -> DownloadResult<()> {
    let target = sidecar_path(dest);
    let tmp = tmp_path(dest);
    crate::fsutil::atomic_replace(&tmp, &target, body)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeVerdict {
    Continue,
    Refuse(String),
}

pub fn judge_resume(
    manifest: &Manifest,
    current: &RemoteSnapshot,
    local_len: u64,
) -> ResumeVerdict {
    if local_len != manifest.remote.size {
        return ResumeVerdict::Refuse(format!(
            "local file is {} bytes but the remote is {}",
            local_len, manifest.remote.size
        ));
    }
    if current.size != manifest.remote.size {
        return ResumeVerdict::Refuse(format!(
            "remote changed size from {} to {} since the download started",
            manifest.remote.size, current.size
        ));
    }
    let identity_ok = match (&manifest.remote.etag, current.etag.as_deref()) {
        (Some(from_before), Some(from_now)) => from_before == from_now,
        (Some(_), None) | (None, Some(_)) => false,
        (None, None) => match (
            &manifest.remote.last_modified,
            current.last_modified.as_deref(),
        ) {
            (Some(from_before), Some(from_now)) => from_before == from_now,
            (Some(_), None) | (None, Some(_)) => false,
            (None, None) => true,
        },
    };
    if identity_ok {
        ResumeVerdict::Continue
    } else {
        ResumeVerdict::Refuse("remote identity (etag/last-modified) changed".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> Manifest {
        let template = ManifestTemplate {
            url: "https://example.com/file.ext".into(),
            final_url: "https://cdn.example.com/file.ext".into(),
            filename: "file.ext".into(),
            remote: RemoteSnapshot {
                size: 1000,
                etag: Some("\"abc\"".into()),
                last_modified: Some("Wed, 02 Sep 2026 14:21:00 GMT".into()),
            },
            chunk_size: 256,
        };
        template.into_manifest(
            vec![
                ChunkEntry {
                    start: 0,
                    end: 255,
                    downloaded: 256,
                },
                ChunkEntry {
                    start: 256,
                    end: 511,
                    downloaded: 128,
                },
                ChunkEntry {
                    start: 512,
                    end: 767,
                    downloaded: 0,
                },
                ChunkEntry {
                    start: 768,
                    end: 999,
                    downloaded: 0,
                },
            ],
            None,
        )
    }

    #[test]
    fn manifest_round_trips_through_disk() {
        let dest = std::env::temp_dir().join("shard-manifest-roundtrip.bin");
        let manifest = sample_manifest();
        manifest.save_atomic(&dest).unwrap();
        let loaded = Manifest::load(&dest).unwrap().unwrap();
        assert_eq!(loaded.version, MANIFEST_VERSION);
        assert_eq!(loaded.chunks.len(), 4);
        assert_eq!(loaded.chunks[1].downloaded, 128);
        assert_eq!(loaded.remote.etag.as_deref(), Some("\"abc\""));
        let _ = std::fs::remove_file(sidecar_path(&dest));
    }

    #[test]
    fn missing_sidecar_loads_as_none() {
        let dest = std::env::temp_dir().join("shard-never-existed.bin");
        assert!(Manifest::load(&dest).unwrap().is_none());
    }

    #[test]
    fn identical_identity_allows_resume() {
        let manifest = sample_manifest();
        let current = RemoteSnapshot {
            size: 1000,
            etag: Some("\"abc\"".into()),
            last_modified: Some("Wed, 02 Sep 2026 14:21:00 GMT".into()),
        };
        assert_eq!(
            judge_resume(&manifest, &current, 1000),
            ResumeVerdict::Continue
        );
    }

    #[test]
    fn changed_etag_refuses_resume() {
        let manifest = sample_manifest();
        let current = RemoteSnapshot {
            size: 1000,
            etag: Some("\"different\"".into()),
            last_modified: None,
        };
        assert!(matches!(
            judge_resume(&manifest, &current, 1000),
            ResumeVerdict::Refuse(_)
        ));
    }

    #[test]
    fn changed_last_modified_refuses_when_no_etag() {
        let mut manifest = sample_manifest();
        manifest.remote.etag = None;
        let current = RemoteSnapshot {
            size: 1000,
            etag: None,
            last_modified: Some("Thu, 03 Sep 2026 09:00:00 GMT".into()),
        };
        assert!(matches!(
            judge_resume(&manifest, &current, 1000),
            ResumeVerdict::Refuse(_)
        ));
    }

    #[test]
    fn size_only_identity_allows_resume() {
        let mut manifest = sample_manifest();
        manifest.remote.etag = None;
        manifest.remote.last_modified = None;
        let current = RemoteSnapshot {
            size: 1000,
            etag: None,
            last_modified: None,
        };
        assert_eq!(
            judge_resume(&manifest, &current, 1000),
            ResumeVerdict::Continue
        );
    }

    #[test]
    fn resized_local_file_refuses_resume() {
        let manifest = sample_manifest();
        let current = RemoteSnapshot {
            size: 1000,
            etag: Some("\"abc\"".into()),
            last_modified: None,
        };
        assert!(matches!(
            judge_resume(&manifest, &current, 512),
            ResumeVerdict::Refuse(_)
        ));
    }

    #[test]
    fn resized_remote_refuses_resume() {
        let manifest = sample_manifest();
        let current = RemoteSnapshot {
            size: 2000,
            etag: Some("\"abc\"".into()),
            last_modified: None,
        };
        assert!(matches!(
            judge_resume(&manifest, &current, 1000),
            ResumeVerdict::Refuse(_)
        ));
    }
}
