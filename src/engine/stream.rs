use super::control::Controller;
use super::error::DownloadResult;
use super::progress::ProgressEvent;
use super::retry::RetryPolicy;
use super::writer::PositionalWriter;
use reqwest::Client as HttpClient;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct SingleStreamSpec {
    pub url: String,
    pub size: u64,
    pub policy: RetryPolicy,
}

pub async fn download_single_stream(
    http: &HttpClient,
    writer: Arc<PositionalWriter>,
    controller: &Controller,
    spec: &SingleStreamSpec,
    progress_tx: &Option<mpsc::Sender<ProgressEvent>>,
) -> DownloadResult<()> {
    let chunk_size = spec.size.max(1);
    if let Some(tx) = progress_tx {
        let _ = tx.try_send(ProgressEvent::Start {
            total: spec.size,
            chunk_size,
        });
    }
    let mut failures = 0u32;
    loop {
        match attempt_single_stream(http, writer.clone(), controller, spec, progress_tx).await {
            Ok(()) => {
                if let Some(tx) = progress_tx {
                    let _ = tx.try_send(ProgressEvent::ChunkComplete {
                        worker: 0,
                        index: 0,
                    });
                }
                return Ok(());
            }
            Err(err) if controller.is_cancelled() => return Err(err),
            Err(_) if failures < spec.policy.max_attempts.saturating_sub(1) => {
                controller
                    .delay(spec.policy.next_delay(failures + 1))
                    .await?;
                failures += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn attempt_single_stream(
    http: &HttpClient,
    writer: Arc<PositionalWriter>,
    controller: &Controller,
    spec: &SingleStreamSpec,
    progress_tx: &Option<mpsc::Sender<ProgressEvent>>,
) -> DownloadResult<()> {
    let response = http.get(&spec.url).send().await?;
    if response.status() == http::StatusCode::PARTIAL_CONTENT
        || response.headers().contains_key(http::header::CONTENT_RANGE)
    {
        return Err(super::error::DownloadError::RangeUnsupported);
    }

    let mut stream = response.bytes_stream();
    let mut written = 0u64;
    let mut buffer = Vec::with_capacity(64 * 1024);
    while let Some(item) = controller.next_body_chunk(&mut stream).await? {
        let bytes = item?;
        buffer.extend_from_slice(&bytes);
        if buffer.len() >= 32 * 1024 {
            flush(&writer, &mut buffer, &mut written, progress_tx).await?;
        }
    }
    flush(&writer, &mut buffer, &mut written, progress_tx).await?;
    Ok(())
}

async fn flush(
    writer: &Arc<PositionalWriter>,
    buffer: &mut Vec<u8>,
    written: &mut u64,
    progress_tx: &Option<mpsc::Sender<ProgressEvent>>,
) -> DownloadResult<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let flushed_len = buffer.len() as u64;
    writer.write_at(std::mem::take(buffer), *written).await?;
    *written += flushed_len;
    if let Some(tx) = progress_tx {
        let _ = tx.try_send(ProgressEvent::ChunkAdvanced {
            worker: 0,
            index: 0,
            written: *written,
        });
    }
    Ok(())
}
