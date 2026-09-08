use crate::engine::checkpoint::CheckpointGuard;
use crate::engine::control::Controller;
use crate::engine::error::{DownloadError, DownloadResult};
use crate::engine::http::EngineHttp;
use crate::engine::manifest::{
    Manifest, ManifestTemplate, RemoteSnapshot, ResumeVerdict, judge_resume, remove_sidecar,
};
use crate::engine::metadata::{RemoteMetadata, RemoteResolver};
use crate::engine::planner::{Chunk, ChunkPlan};
use crate::engine::progress::ProgressEvent;
use crate::engine::retry::RetryPolicy;
use crate::engine::verify::Sha256Hasher;
use crate::engine::worker::{PoolConfig, WorkerPool};
use crate::engine::writer::{PositionalWriter, file_len};
use reqwest::Client as HttpClient;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub url: String,
    pub dest_path: std::path::PathBuf,
    pub chunk_size: u64,
    pub connections: usize,
    pub max_attempts: u32,
    pub retry_base_delay: Duration,
    pub retry_max_delay: Duration,
    pub checkpoint_interval: Duration,
    pub resume: bool,
    /// When set, the destination is resolved as `<dest_path>/<category>/<name>`
    /// with the category picked from the file extension.
    pub routing: Option<FiletypeRouting>,
}

#[derive(Debug, Clone)]
pub struct FiletypeRouting {
    /// category name -> directory leaf under the base download dir
    pub dirs: std::collections::BTreeMap<String, String>,
    /// leaf used when the extension matches no known category
    pub other: String,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            dest_path: PathBuf::new(),
            chunk_size: 8 * 1024 * 1024,
            connections: 8,
            max_attempts: 5,
            retry_base_delay: Duration::from_millis(500),
            retry_max_delay: Duration::from_secs(30),
            checkpoint_interval: Duration::from_secs(3),
            resume: true,
            routing: None,
        }
    }
}

fn retry_policy(options: &DownloadOptions) -> RetryPolicy {
    RetryPolicy::new(options.max_attempts)
        .with_delays(options.retry_base_delay, options.retry_max_delay)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeStatus {
    Completed,
    Cancelled,
}

#[derive(Debug)]
pub struct DownloadOutcome {
    pub final_url: String,
    pub size: u64,
    pub sha256: String,
    pub dest_path: std::path::PathBuf,
    pub status: OutcomeStatus,
}

pub struct DownloadHandle {
    pub controller: Controller,
    task: JoinHandle<DownloadResult<DownloadOutcome>>,
}

impl DownloadHandle {
    pub async fn wait(&mut self) -> DownloadResult<DownloadOutcome> {
        (&mut self.task)
            .await
            .map_err(|join_err| DownloadError::Io(std::io::Error::other(join_err.to_string())))?
    }

    pub async fn cancel_and_wait(&mut self) -> DownloadResult<DownloadOutcome> {
        self.controller.cancel();
        self.wait().await
    }
}

pub struct DownloadManager {
    client: HttpClient,
}

impl DownloadManager {
    #[allow(clippy::new_without_default)]
    pub fn new() -> DownloadResult<Self> {
        let client = EngineHttp::client()?;
        Ok(Self { client })
    }

    pub fn start(
        &self,
        options: &DownloadOptions,
        progress_tx: Option<mpsc::Sender<ProgressEvent>>,
    ) -> DownloadResult<DownloadHandle> {
        let controller = Controller::default();
        let client = self.client.clone();
        let resolver = RemoteResolver::with_client(client.clone());
        let options = options.clone();
        let runtime_controller = controller.clone();
        let task = tokio::spawn(async move {
            run_download(client, resolver, options, runtime_controller, progress_tx).await
        });
        Ok(DownloadHandle { controller, task })
    }

    pub async fn download(
        &self,
        options: &DownloadOptions,
        progress_tx: Option<mpsc::Sender<ProgressEvent>>,
    ) -> DownloadResult<DownloadOutcome> {
        self.start(options, progress_tx)?.wait().await
    }
}

async fn run_download(
    client: HttpClient,
    resolver: RemoteResolver,
    options: DownloadOptions,
    controller: Controller,
    progress_tx: Option<mpsc::Sender<ProgressEvent>>,
) -> DownloadResult<DownloadOutcome> {
    let remote = Arc::new(resolver.resolve(&options.url).await?);
    let dest_path = resolve_destination(&options, &remote);
    let stream_client = client.clone();

    if !remote.accepts_ranges {
        remove_sidecar(&dest_path);
        let writer = Arc::new(PositionalWriter::open(&dest_path)?);
        return run_single_stream(
            &stream_client,
            writer,
            &remote,
            &dest_path,
            &options,
            &controller,
            &progress_tx,
        )
        .await;
    }

    let resumed = prepare_resume(&options, &remote, &dest_path)?;

    let (plan, effective_chunk_size) = build_plan(&options, &remote, &dest_path, resumed.as_ref());
    if let Some(tx) = &progress_tx {
        let _ = tx.try_send(ProgressEvent::Start {
            total: remote.size,
            chunk_size: effective_chunk_size,
        });
    }

    let plan = Arc::new(plan);
    let dispatcher = Arc::new(crate::engine::dispatch::ChunkDispatcher::new(plan));
    let writer = Arc::new(open_writer(&dest_path, resumed.is_some())?);
    writer.preallocate(remote.size)?;

    let template = manifest_template(&options, &remote, &dest_path, effective_chunk_size);
    let checkpoint = CheckpointGuard::spawn(
        &dest_path,
        template,
        Arc::clone(&dispatcher),
        options.checkpoint_interval,
    );

    let pool = WorkerPool::new(
        client,
        Arc::clone(&remote),
        Arc::clone(&dispatcher),
        Arc::clone(&writer),
        controller.clone(),
        PoolConfig {
            connections: options.connections,
            retry: retry_policy(&options),
        },
        progress_tx.clone(),
    );
    let pool_result = pool.run().await;

    match pool_result {
        Ok(()) if controller.is_cancelled() => {
            checkpoint.finish().await?;
            Ok(DownloadOutcome {
                final_url: remote.final_url.clone(),
                size: remote.size,
                sha256: String::new(),
                dest_path,
                status: OutcomeStatus::Cancelled,
            })
        }
        Ok(()) => {
            let sha256 = hash_file(&dest_path)?;
            checkpoint.set_integrity_sha(sha256.clone());
            checkpoint.finish().await?;
            Ok(DownloadOutcome {
                final_url: remote.final_url.clone(),
                size: remote.size,
                sha256,
                dest_path,
                status: OutcomeStatus::Completed,
            })
        }
        Err(DownloadError::RangeUnsupported) => {
            checkpoint.finish().await?;
            remove_sidecar(&dest_path);
            let writer = Arc::new(PositionalWriter::open(&dest_path)?);
            run_single_stream(
                &stream_client,
                writer,
                &remote,
                &dest_path,
                &options,
                &controller,
                &progress_tx,
            )
            .await
        }
        Err(err) => {
            checkpoint.finish().await?;
            Err(err)
        }
    }
}

fn prepare_resume(
    options: &DownloadOptions,
    remote: &RemoteMetadata,
    dest: &Path,
) -> DownloadResult<Option<Manifest>> {
    if !options.resume {
        remove_sidecar(dest);
        return Ok(None);
    }
    let Some(manifest) = Manifest::load(dest)? else {
        return Ok(None);
    };
    let local_len = match file_len(dest) {
        Ok(len) => len,
        Err(_) => return Ok(None),
    };
    let current = RemoteSnapshot {
        size: remote.size,
        etag: remote.etag.clone(),
        last_modified: remote.last_modified.clone(),
    };
    match judge_resume(&manifest, &current, local_len) {
        ResumeVerdict::Continue => Ok(Some(manifest)),
        ResumeVerdict::Refuse(reason) => Err(DownloadError::ResumeRefused(reason)),
    }
}

fn build_plan(
    options: &DownloadOptions,
    remote: &RemoteMetadata,
    dest: &Path,
    resumed: Option<&Manifest>,
) -> (ChunkPlan, u64) {
    match resumed {
        Some(manifest) => {
            let chunks = manifest
                .chunks
                .iter()
                .map(|entry| Chunk {
                    start: entry.start,
                    end: entry.end,
                    downloaded: entry.downloaded,
                })
                .collect();
            (ChunkPlan::from_chunks(chunks), manifest.layout.chunk_size)
        }
        None => {
            remove_sidecar(dest);
            (
                ChunkPlan::build(remote.size, options.chunk_size),
                options.chunk_size,
            )
        }
    }
}

async fn run_single_stream(
    client: &HttpClient,
    writer: Arc<PositionalWriter>,
    remote: &RemoteMetadata,
    dest: &Path,
    options: &DownloadOptions,
    controller: &Controller,
    progress_tx: &Option<mpsc::Sender<ProgressEvent>>,
) -> DownloadResult<DownloadOutcome> {
    use crate::engine::manifest::ChunkEntry;
    use crate::engine::stream::{SingleStreamSpec, download_single_stream};

    let spec = SingleStreamSpec {
        url: remote.final_url.clone(),
        size: remote.size,
        policy: retry_policy(options),
    };
    match download_single_stream(client, writer, controller, &spec, progress_tx).await {
        Ok(()) => {
            let sha256 = hash_file(dest)?;
            let template = manifest_template(options, remote, dest, remote.size.max(1));
            let manifest = template.into_manifest(
                vec![ChunkEntry {
                    start: 0,
                    end: remote.size.saturating_sub(1),
                    downloaded: remote.size,
                }],
                Some(sha256.clone()),
            );
            manifest.save_atomic(dest)?;
            Ok(DownloadOutcome {
                final_url: remote.final_url.clone(),
                size: remote.size,
                sha256,
                dest_path: dest.to_path_buf(),
                status: OutcomeStatus::Completed,
            })
        }
        Err(DownloadError::Canceled) => Ok(DownloadOutcome {
            final_url: remote.final_url.clone(),
            size: remote.size,
            sha256: String::new(),
            dest_path: dest.to_path_buf(),
            status: OutcomeStatus::Cancelled,
        }),
        Err(err) => Err(err),
    }
}

fn open_writer(dest: &Path, resuming: bool) -> DownloadResult<PositionalWriter> {
    if resuming {
        PositionalWriter::open_existing(dest)
    } else {
        PositionalWriter::open(dest)
    }
}

fn manifest_template(
    options: &DownloadOptions,
    remote: &RemoteMetadata,
    dest: &Path,
    chunk_size: u64,
) -> ManifestTemplate {
    let filename = dest
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    ManifestTemplate {
        url: options.url.clone(),
        final_url: remote.final_url.clone(),
        filename,
        remote: RemoteSnapshot {
            size: remote.size,
            etag: remote.etag.clone(),
            last_modified: remote.last_modified.clone(),
        },
        chunk_size,
    }
}

fn resolve_destination(options: &DownloadOptions, metadata: &RemoteMetadata) -> PathBuf {
    if let Some(routing) = &options.routing {
        let file_name = metadata
            .content_disposition
            .as_deref()
            .and_then(parse_content_disposition_filename)
            .or_else(|| url_basename(&metadata.final_url))
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "download.bin".to_string());
        let category = filetype_category(&file_name);
        let leaf = routing
            .dirs
            .get(category)
            .map(String::as_str)
            .unwrap_or(&routing.other);
        let dir = options.dest_path.join(leaf);
        if !dir.exists() {
            let _ = std::fs::create_dir_all(&dir);
        }
        return dir.join(file_name);
    }
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

fn filetype_category(name: &str) -> &'static str {
    let extension = name
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .unwrap_or(name)
        .to_ascii_lowercase();
    const VIDEO: &[&str] = &[
        "mp4", "mkv", "avi", "mov", "webm", "m4v", "flv", "wmv", "mpg", "mpeg",
    ];
    const IMAGE: &[&str] = &[
        "jpg", "jpeg", "png", "gif", "webp", "bmp", "svg", "heic", "avif", "ico", "tiff",
    ];
    const AUDIO: &[&str] = &["mp3", "wav", "flac", "ogg", "m4a", "aac", "opus", "wma"];
    const ARCHIVE: &[&str] = &[
        "zip", "tar", "gz", "tgz", "bz2", "xz", "7z", "rar", "zst", "iso",
    ];
    const DOCUMENT: &[&str] = &[
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "md", "odt", "ods", "epub",
    ];
    if VIDEO.contains(&extension.as_str()) {
        "video"
    } else if IMAGE.contains(&extension.as_str()) {
        "image"
    } else if AUDIO.contains(&extension.as_str()) {
        "audio"
    } else if ARCHIVE.contains(&extension.as_str()) {
        "archive"
    } else if DOCUMENT.contains(&extension.as_str()) {
        "document"
    } else {
        "other"
    }
}

fn parse_content_disposition_filename(value: &str) -> Option<String> {
    value.split(';').map(str::trim).find_map(|part| {
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
    use crate::engine::manifest::{ChunkEntry, ManifestTemplate, RemoteSnapshot, sidecar_path};
    use crate::engine::test_server::handler::ServerFeatures;
    use crate::engine::test_server::{TestServer, deterministic_body, expected_sha256, handler};
    use std::os::unix::fs::FileExt;

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
                    ..Default::default()
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
                    ..Default::default()
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
        assert!(
            parse_content_disposition_filename("attachment; filename=../../etc/passwd").is_none()
        );
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
            ..Default::default()
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

    #[test]
    fn filetype_category_classifies_known_extensions() {
        assert_eq!(filetype_category("movie.mp4"), "video");
        assert_eq!(filetype_category("PIC.JPG"), "image");
        assert_eq!(filetype_category("song.flac"), "audio");
        assert_eq!(filetype_category("backup.tar.gz"), "archive");
        assert_eq!(filetype_category("resume.pdf"), "document");
        assert_eq!(filetype_category("mystery.xyz"), "other");
        assert_eq!(filetype_category("noext"), "other");
    }

    #[test]
    fn resolve_destination_routes_by_extension_under_base_dir() {
        let options = DownloadOptions {
            url: "http://x".into(),
            dest_path: PathBuf::from("/tmp/shard-route-destination"),
            chunk_size: 1024,
            connections: 1,
            max_attempts: 2,
            routing: Some(FiletypeRouting {
                dirs: [
                    ("video".to_string(), "Videos".to_string()),
                    ("document".to_string(), "Documents".to_string()),
                ]
                .into_iter()
                .collect(),
                other: "Other".to_string(),
            }),
            ..Default::default()
        };
        let metadata = RemoteMetadata {
            final_url: "http://x/movie.mp4".into(),
            size: 10,
            etag: None,
            last_modified: None,
            accepts_ranges: true,
            content_disposition: None,
        };
        assert_eq!(
            resolve_destination(&options, &metadata),
            PathBuf::from("/tmp/shard-route-destination/Videos/movie.mp4")
        );
    }

    #[tokio::test]
    async fn resume_fetches_only_chunks_missing_from_the_manifest() {
        let chunk_size = 256 * 1024;
        let body = deterministic_body(4 * chunk_size, 7777);
        let server = TestServer::spawn_with(body.clone(), ServerFeatures::default()).await;
        let dest = std::env::temp_dir().join("shard-resume-manifest.bin");

        let chunks = vec![
            ChunkEntry {
                start: 0,
                end: 262143,
                downloaded: 262144,
            },
            ChunkEntry {
                start: 262144,
                end: 524287,
                downloaded: 0,
            },
            ChunkEntry {
                start: 524288,
                end: 786431,
                downloaded: 262144,
            },
            ChunkEntry {
                start: 786432,
                end: 1048575,
                downloaded: 100_000,
            },
        ];
        let template = ManifestTemplate {
            url: server.url(),
            final_url: server.url(),
            filename: "shard-resume-manifest.bin".into(),
            remote: RemoteSnapshot {
                size: body.len() as u64,
                etag: Some(server.etag.clone()),
                last_modified: Some("Wed, 01 Jul 2026 12:00:00 GMT".into()),
            },
            chunk_size: chunk_size as u64,
        };
        template
            .into_manifest(chunks, None)
            .save_atomic(&dest)
            .unwrap();

        let file = std::fs::File::create(&dest).unwrap();
        file.set_len(body.len() as u64).unwrap();
        file.write_at(&body[0..262144], 0).unwrap();
        file.write_at(&body[524288..786432], 524288).unwrap();
        file.sync_all().unwrap();

        let manager = DownloadManager::new().unwrap();
        let outcome = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 64 * 1024,
                    connections: 4,
                    max_attempts: 3,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        let on_disk = std::fs::read(&dest).unwrap();
        assert_eq!(on_disk, body);
        assert_eq!(outcome.sha256, expected_sha256(&body));
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(
            server.request_count(),
            5,
            "expected one pending and one partial chunk fetch plus the three preflight probes"
        );
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(sidecar_path(&dest));
    }

    #[tokio::test]
    async fn identity_change_refuses_resume() {
        let body = deterministic_body(2 * 1024 * 1024, 555);
        let server = TestServer::spawn(body.clone()).await;
        let dest = std::env::temp_dir().join("shard-resume-refused.bin");

        let template = ManifestTemplate {
            url: server.url(),
            final_url: server.url(),
            filename: "shard-resume-refused.bin".into(),
            remote: RemoteSnapshot {
                size: body.len() as u64,
                etag: Some("\"fake-{}-{}\"".to_string()),
                last_modified: None,
            },
            chunk_size: 1024 * 1024,
        };
        template
            .into_manifest(
                vec![ChunkEntry {
                    start: 0,
                    end: 2097151,
                    downloaded: 0,
                }],
                None,
            )
            .save_atomic(&dest)
            .unwrap();

        let file = std::fs::File::create(&dest).unwrap();
        file.set_len(body.len() as u64).unwrap();

        let manager = DownloadManager::new().unwrap();
        let result = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 256 * 1024,
                    connections: 4,
                    max_attempts: 3,
                    ..Default::default()
                },
                None,
            )
            .await;
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(sidecar_path(&dest));
        assert!(matches!(result, Err(DownloadError::ResumeRefused(_))));
    }

    #[tokio::test]
    async fn cancel_writes_a_resumable_sidecar_and_resume_completes() {
        let body = deterministic_body(4 * 1024 * 1024, 1313);
        let server = TestServer::spawn_with(
            body.clone(),
            handler::ServerFeatures {
                delay: handler::DelayProfile::Jitter {
                    seed: 9,
                    max_delay_ms: 250,
                },
                fragment_bytes: Some(1024),
                ..Default::default()
            },
        )
        .await;
        let dest = std::env::temp_dir().join("shard-cancel-resume.bin");
        let manager = DownloadManager::new().unwrap();

        let mut handle = manager
            .start(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 512 * 1024,
                    connections: 4,
                    max_attempts: 3,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.controller.cancel();
        let outcome = handle.wait().await.unwrap();
        assert_eq!(outcome.status, OutcomeStatus::Cancelled);
        assert!(
            sidecar_path(&dest).exists(),
            "cancel must leave a resume sidecar"
        );
        assert!(sidecar_path(&dest).metadata().unwrap().len() > 0);

        let outcome = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 512 * 1024,
                    connections: 4,
                    max_attempts: 3,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.sha256, expected_sha256(&body));
        assert_eq!(std::fs::read(&dest).unwrap(), body);
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(sidecar_path(&dest));
    }

    #[tokio::test]
    async fn non_range_server_downloads_via_single_stream() {
        let body = deterministic_body(3 * 1024 * 1024, 7777);
        let server = TestServer::spawn_with(
            body.clone(),
            handler::ServerFeatures {
                ranges: handler::RangesMode::None,
                ..Default::default()
            },
        )
        .await;
        let dest = std::env::temp_dir().join("shard-no-ranges.bin");
        let manager = DownloadManager::new().unwrap();

        let outcome = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 512 * 1024,
                    connections: 4,
                    max_attempts: 3,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.size, body.len() as u64);
        assert_eq!(outcome.sha256, expected_sha256(&body));
        assert_eq!(std::fs::read(&dest).unwrap(), body);
        assert_eq!(server.request_count(), 4);
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(sidecar_path(&dest));
    }

    #[tokio::test]
    async fn paused_download_resumes_cleanly_and_completes() {
        let body = deterministic_body(2 * 1024 * 1024, 4242);
        let server = TestServer::spawn_with(
            body.clone(),
            handler::ServerFeatures {
                delay: handler::DelayProfile::Reverse { max_delay_ms: 400 },
                fragment_bytes: Some(2048),
                ..Default::default()
            },
        )
        .await;
        let dest = std::env::temp_dir().join("shard-pause-resume.bin");
        let manager = DownloadManager::new().unwrap();

        let mut handle = manager
            .start(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 512 * 1024,
                    connections: 4,
                    max_attempts: 3,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
        handle.controller.pause();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(handle.controller.is_paused());
        tokio::time::timeout(Duration::from_millis(150), handle.wait())
            .await
            .expect_err("a paused download must not report completion");
        handle.controller.resume();
        let outcome = handle.wait().await.unwrap();
        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.sha256, expected_sha256(&body));
        assert_eq!(std::fs::read(&dest).unwrap(), body);
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(sidecar_path(&dest));
    }

    #[tokio::test]
    async fn lying_server_falls_back_to_single_stream() {
        let body = deterministic_body(2 * 1024 * 1024, 31337);
        let server = TestServer::spawn_with(
            body.clone(),
            handler::ServerFeatures {
                ranges: handler::RangesMode::ProbeOnly,
                ..Default::default()
            },
        )
        .await;
        let dest = std::env::temp_dir().join("shard-lying-server.bin");
        let manager = DownloadManager::new().unwrap();

        let outcome = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 256 * 1024,
                    connections: 4,
                    max_attempts: 3,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(outcome.status, OutcomeStatus::Completed);
        assert_eq!(outcome.sha256, expected_sha256(&body));
        assert_eq!(std::fs::read(&dest).unwrap(), body);
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_file(sidecar_path(&dest));
    }
}
