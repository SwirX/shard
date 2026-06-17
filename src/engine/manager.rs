use crate::engine::error::{DownloadError, DownloadResult};
use crate::engine::http::EngineHttp;
use crate::engine::metadata::{RemoteMetadata, RemoteResolver};
use crate::engine::planner::ChunkPlan;
use crate::engine::progress::ProgressEvent;
use crate::engine::verify::Sha256Hasher;
use crate::engine::writer::PositionalWriter;
use futures_util::StreamExt;
use http::header::{CONTENT_RANGE, RANGE};
use reqwest::Client as HttpClient;
use std::path::Path;
use tokio::sync::mpsc;

pub struct DownloadOptions {
    pub url: String,
    pub dest_path: std::path::PathBuf,
    pub chunk_size: u64,
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
        let metadata = self.resolver.resolve(&options.url).await?;
        let plan = ChunkPlan::build(metadata.size, options.chunk_size);
        let writer = PositionalWriter::open(&options.dest_path)?;
        writer.preallocate(metadata.size)?;

        for (index, chunk) in plan.chunks.iter().enumerate() {
            self.download_chunk(&metadata, chunk, index, &writer, progress_tx.as_ref())
                .await?;
        }

        let sha256 = hash_file(&options.dest_path)?;
        Ok(DownloadOutcome {
            final_url: metadata.final_url,
            size: metadata.size,
            sha256,
        })
    }

    async fn download_chunk(
        &self,
        metadata: &RemoteMetadata,
        chunk: &crate::engine::planner::Chunk,
        index: usize,
        writer: &PositionalWriter,
        progress_tx: Option<&mpsc::Sender<ProgressEvent>>,
    ) -> DownloadResult<()> {
        let start = chunk.start;
        let end = chunk.end;
        let response = self
            .client
            .get(&metadata.final_url)
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .await?;
        if response.status() != http::StatusCode::PARTIAL_CONTENT {
            return Err(DownloadError::RangeUnsupported);
        }
        validate_content_range(response.headers(), start, metadata.size)?;

        let mut stream = response.bytes_stream();
        let mut written = 0u64;
        let mut buffer = Vec::with_capacity(64 * 1024);
        while let Some(item) = stream.next().await {
            let bytes = item?;
            buffer.extend_from_slice(&bytes);
            if buffer.len() >= 32 * 1024 {
                let flushed_len = buffer.len() as u64;
                writer
                    .write_at(std::mem::take(&mut buffer), start + written)
                    .await?;
                written += flushed_len;
            }
            if let Some(tx) = progress_tx {
                let _ = tx.send(ProgressEvent::ChunkAdvanced { index, written }).await;
            }
        }
        if !buffer.is_empty() {
            writer
                .write_at(std::mem::take(&mut buffer), start + written)
                .await?;
        }
        if let Some(tx) = progress_tx {
            let _ = tx.send(ProgressEvent::ChunkComplete { index }).await;
        }
        Ok(())
    }
}

fn validate_content_range(headers: &http::HeaderMap, expected_start: u64, total: u64) -> DownloadResult<()> {
    let value = headers
        .get(CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .ok_or(DownloadError::RangeUnsupported)?;
    let spec = value
        .strip_prefix("bytes ")
        .ok_or(DownloadError::RangeUnsupported)?;
    let (range, declared_total) = spec
        .split_once('/')
        .ok_or(DownloadError::RangeUnsupported)?;
    let (start, _) = range.split_once('-').ok_or(DownloadError::RangeUnsupported)?;
    if start.parse::<u64>() != Ok(expected_start) {
        return Err(DownloadError::RangeUnsupported);
    }
    if declared_total.parse::<u64>() != Ok(total) {
        return Err(DownloadError::RangeUnsupported);
    }
    Ok(())
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

    fn deterministic_body(size: usize, seed: u64) -> Vec<u8> {
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

    fn expected_sha256(body: &[u8]) -> String {
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