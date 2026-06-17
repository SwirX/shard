use crate::engine::error::DownloadResult;
use crate::engine::http::EngineHttp;
use crate::engine::metadata::RemoteResolver;
use crate::engine::planner::ChunkPlan;
use crate::engine::progress::ProgressEvent;
use crate::engine::verify::Sha256Hasher;
use crate::engine::worker::WorkerPool;
use crate::engine::writer::PositionalWriter;
use reqwest::Client as HttpClient;
use std::path::Path;
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
        let plan = Arc::new(ChunkPlan::build(metadata.size, options.chunk_size));
        let writer = Arc::new(PositionalWriter::open(&options.dest_path)?);
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

        let sha256 = hash_file(&options.dest_path)?;
        Ok(DownloadOutcome {
            final_url: metadata.final_url.clone(),
            size: metadata.size,
            sha256,
        })
    }
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
    use crate::engine::test_server::TestServer;
    use sha2::Digest as _;

    pub fn deterministic_body(size: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        (0..size)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state & 0xff) as u8
            })
            .collect()
    }

    pub fn expected_sha256(body: &[u8]) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(body);
        let output = hasher.finalize();
        output.iter().map(|b| format!("{:02x}", b)).collect()
    }

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
}