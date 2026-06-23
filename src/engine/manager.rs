use crate::engine::error::DownloadResult;
use crate::engine::http::EngineHttp;
use crate::engine::metadata::{RemoteMetadata, RemoteResolver};
use crate::engine::planner::ChunkPlan;
use crate::engine::progress::ProgressEvent;
use crate::engine::verify::Sha256Hasher;
use crate::engine::worker::WorkerPool;
use crate::engine::writer::PositionalWriter;
use reqwest::Client as HttpClient;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct DownloadOptions {
    pub url: String,
    pub dest_path: std::path::PathBuf,
    pub chunk_size: u64,
    pub connections: usize,
    pub max_attempts: u32,
}

pub struct DownloadOutcome {
    pub final_url: String,
    pub size: u64,
    pub sha256: String,
    pub dest_path: std::path::PathBuf,
}

pub struct DownloadManager {
    client: HttpClient,
    resolver: RemoteResolver,
}

impl DownloadManager {
    #[allow(clippy::new_without_default)]
    pub fn new() -> DownloadResult<Self> {
        let client = EngineHttp::client()?;
        let resolver = RemoteResolver::new()?;
        Ok(Self { client, resolver })
    }

    pub async fn download(
        &self,
        options: &DownloadOptions,
        progress_tx: Option<mpsc::Sender<ProgressEvent>>,
    ) -> DownloadResult<DownloadOutcome> {
        let metadata = Arc::new(self.resolver.resolve(&options.url).await?);
        let dest_path = resolve_destination(options, &metadata);
        let plan = Arc::new(ChunkPlan::build(metadata.size, options.chunk_size));
        if let Some(tx) = &progress_tx {
            let _ = tx.try_send(ProgressEvent::Start {
                total: metadata.size,
                chunk_size: options.chunk_size,
            });
        }
        let writer = Arc::new(PositionalWriter::open(&dest_path)?);
        writer.preallocate(metadata.size)?;

        WorkerPool::new(
            self.client.clone(),
            Arc::clone(&metadata),
            Arc::clone(&plan),
            Arc::clone(&writer),
            options.connections,
            options.max_attempts,
            progress_tx,
        )
        .run()
        .await?;

        let sha256 = hash_file(&dest_path)?;
        Ok(DownloadOutcome {
            final_url: metadata.final_url.clone(),
            size: metadata.size,
            sha256,
            dest_path,
        })
    }
}

fn resolve_destination(options: &DownloadOptions, metadata: &RemoteMetadata) -> PathBuf {
    let wants_dir = options.dest_path.is_dir()
        || options
            .dest_path
            .as_os_str()
            .to_string_lossy()
            .ends_with(['/', '\\']);
    if !wants_dir {
        return options.dest_path.clone();
    }
    let dir = options.dest_path.clone();
    if !dir.exists() {
        let _ = std::fs::create_dir_all(&dir);
    }
    let name = metadata
        .content_disposition
        .as_deref()
        .and_then(parse_content_disposition_filename)
        .or_else(|| url_basename(&metadata.final_url))
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "download.bin".to_string());
    dir.join(name)
}

fn parse_content_disposition_filename(value: &str) -> Option<String> {
    value
        .split(';')
        .map(str::trim)
        .find_map(|part| {
            let rest = part.strip_prefix("filename=")?;
            let name = rest.trim().trim_matches('"');
            if name.is_empty() || name.contains('/') || name.contains('\\') {
                None
            } else {
                Some(name.to_string())
            }
        })
}

fn url_basename(final_url: &str) -> Option<String> {
    let parsed = url::Url::parse(final_url).ok()?;
    let name = parsed.path_segments()?.next_back()?.to_string();
    if name.is_empty() { None } else { Some(name) }
}

fn hash_file(path: &Path) -> DownloadResult<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256Hasher::new();
    let mut buffer = vec![0u8; 256 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finish_hex())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::test_server::{expected_sha256, deterministic_body, handler, TestServer};

    #[tokio::test]
    async fn downloads_multiple_chunks_byte_for_byte() {
        let body = deterministic_body(4 * 1024 * 1024, 12345);
        let server = TestServer::spawn(body.clone()).await;

        let dest = std::env::temp_dir().join("shard-vertical-slice.bin");
        let manager = DownloadManager::new().unwrap();
        let outcome = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 1024 * 1024,
                    connections: 8,
                    max_attempts: 3,
                },
                None,
            )
            .await
            .unwrap();

        let on_disk = std::fs::read(&dest).unwrap();
        assert_eq!(on_disk, body);
        assert_eq!(outcome.size, body.len() as u64);
        assert_eq!(outcome.sha256, expected_sha256(&body));
        let _ = std::fs::remove_file(&dest);
    }

    #[tokio::test]
    async fn directory_output_resolves_filename_from_content_disposition() {
        let body = deterministic_body(2 * 1024 * 1024, 4242);
        let features = handler::ServerFeatures {
            content_disposition: Some(r#"attachment; filename="album-art-cache.zip""#.to_string()),
            ..handler::ServerFeatures::default()
        };
        let server = TestServer::spawn_with(body.clone(), features).await;

        let dir = std::env::temp_dir().join(format!("shard-cd-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let manager = DownloadManager::new().unwrap();
        let outcome = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dir.clone(),
                    chunk_size: 512 * 1024,
                    connections: 4,
                    max_attempts: 3,
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            outcome.dest_path.file_name().unwrap().to_str().unwrap(),
            "album-art-cache.zip"
        );
        let on_disk = std::fs::read(&outcome.dest_path).unwrap();
        assert_eq!(on_disk, body);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn content_disposition_names_are_parsed_and_sanitized() {
        assert_eq!(
            parse_content_disposition_filename(r#"attachment; filename="game.iso""#),
            Some("game.iso".to_string())
        );
        assert_eq!(
            parse_content_disposition_filename("attachment; filename=simple.bin; size=5"),
            Some("simple.bin".to_string())
        );
        assert!(parse_content_disposition_filename("attachment; filename=../../etc/passwd").is_none());
        assert!(parse_content_disposition_filename("attachment").is_none());
    }

    #[test]
    fn url_fallback_names_the_last_path_segment() {
        assert_eq!(
            url_basename("https://cdn.example.com/dl/pack.tar.gz?token=abc"),
            Some("pack.tar.gz".to_string())
        );
        assert_eq!(url_basename("https://cdn.example.com/"), None);
    }

    #[test]
    fn resolve_destination_keeps_explicit_files_untouched() {
        let options = DownloadOptions {
            url: "http://x".into(),
            dest_path: PathBuf::from("/tmp/out/result.bin"),
            chunk_size: 1024,
            connections: 1,
            max_attempts: 2,
        };
        let metadata = RemoteMetadata {
            final_url: "http://x/file.bin".into(),
            size: 10,
            etag: None,
            last_modified: None,
            accepts_ranges: true,
            content_disposition: Some("attachment; filename=\"server.bin\"".into()),
        };
        assert_eq!(
            resolve_destination(&options, &metadata),
            PathBuf::from("/tmp/out/result.bin")
        );
    }
}