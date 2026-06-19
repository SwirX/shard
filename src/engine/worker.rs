use super::dispatch::ChunkDispatcher;
use super::error::{DownloadError, DownloadResult};
use super::metadata::RemoteMetadata;
use super::planner::ChunkPlan;
use super::progress::ProgressEvent;
use super::writer::PositionalWriter;
use futures_util::StreamExt;
use http::header::{CONTENT_RANGE, RANGE};
use reqwest::Client as HttpClient;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct WorkerPool {
    queue: Arc<ChunkDispatcher>,
    http: HttpClient,
    writer: Arc<PositionalWriter>,
    metadata: Arc<RemoteMetadata>,
    connections: usize,
    max_attempts: u32,
    progress_tx: Option<mpsc::Sender<ProgressEvent>>,
}

impl WorkerPool {
    pub fn new(
        http: HttpClient,
        metadata: Arc<RemoteMetadata>,
        plan: Arc<ChunkPlan>,
        writer: Arc<PositionalWriter>,
        connections: usize,
        max_attempts: u32,
        progress_tx: Option<mpsc::Sender<ProgressEvent>>,
    ) -> Self {
        let queue = Arc::new(ChunkDispatcher::new(plan));
        Self {
            queue,
            http,
            writer,
            metadata,
            connections,
            max_attempts,
            progress_tx,
        }
    }

    pub async fn run(self) -> DownloadResult<()> {
        let pool = Arc::new(self);
        let mut handles = Vec::with_capacity(pool.connections);
        for worker_id in 0..pool.connections {
            let worker = Arc::clone(&pool);
            handles.push(tokio::spawn(async move { worker.run_worker(worker_id).await }));
        }
        for handle in handles {
            handle
                .await
                .map_err(|join_err| DownloadError::Io(std::io::Error::other(join_err.to_string())))?;
        }
        if pool.queue.failed_count() > 0 {
            return Err(DownloadError::ChunksFailed(pool.queue.failed_count()));
        }
        Ok(())
    }

    async fn run_worker(self: &Arc<Self>, worker_id: usize) {
        loop {
            let index = match self.queue.take().await {
                Some(index) => index,
                None => break,
            };
            self.work_chunk(worker_id, index).await;
        }
    }

    async fn work_chunk(self: &Arc<Self>, worker_id: usize, index: usize) {
        if let Some(tx) = &self.progress_tx {
            let _ = tx.try_send(ProgressEvent::ChunkStarted { worker: worker_id, index });
        }
        let attempts = self.queue.attempt(index);
        self.queue.reset_progress(index);
        let chunk = self.queue.chunk(index).clone();
        match self.download_chunk(&chunk, index, worker_id).await {
            Ok(()) => self.queue.settle_complete(index),
            Err(_) if attempts < self.max_attempts as u64 => self.queue.requeue(index).await,
            Err(_) => self.queue.settle_failed(index),
        }
    }

    async fn download_chunk(
        self: &Arc<Self>,
        chunk: &super::planner::Chunk,
        index: usize,
        worker_id: usize,
    ) -> DownloadResult<()> {
        let start = chunk.start;
        let end = chunk.end;
        let response = self
            .http
            .get(&self.metadata.final_url)
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .await?;
        if response.status() != http::StatusCode::PARTIAL_CONTENT {
            return Err(DownloadError::RangeUnsupported);
        }
        validate_content_range(response.headers(), start, self.metadata.size)?;

        let mut stream = response.bytes_stream();
        let mut written = 0u64;
        let mut buffer = Vec::with_capacity(64 * 1024);
        while let Some(item) = stream.next().await {
            let bytes = item?;
            buffer.extend_from_slice(&bytes);
            if buffer.len() >= 32 * 1024 {
                let flushed_len = buffer.len() as u64;
                self.writer
                    .write_at(std::mem::take(&mut buffer), start + written)
                    .await?;
                written += flushed_len;
                self.queue.record_progress(index, written);
            }
            if let Some(tx) = &self.progress_tx {
                let _ = tx.try_send(ProgressEvent::ChunkAdvanced {
                    worker: worker_id,
                    index,
                    written,
                });
            }
        }
        if !buffer.is_empty() {
            let flushed_len = buffer.len() as u64;
            self.writer
                .write_at(std::mem::take(&mut buffer), start + written)
                .await?;
            written += flushed_len;
        }
        self.queue.record_progress(index, written);
        if let Some(tx) = &self.progress_tx {
            let _ = tx.try_send(ProgressEvent::ChunkComplete {
                worker: worker_id,
                index,
            });
        }
        Ok(())
    }
}

pub fn validate_content_range(
    headers: &http::HeaderMap,
    expected_start: u64,
    total: u64,
) -> DownloadResult<()> {
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