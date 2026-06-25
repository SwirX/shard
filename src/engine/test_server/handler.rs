use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Debug)]
pub enum DelayProfile {
    None,
    Reverse { max_delay_ms: u64 },
    Jitter { seed: u64, max_delay_ms: u64 },
}

#[derive(Clone)]
pub struct ServerFeatures {
    pub delay: DelayProfile,
    pub fragment_bytes: Option<usize>,
    pub drop_once: Arc<Mutex<HashMap<(u64, u64), u32>>>,
    pub drop_forever: Arc<HashSet<(u64, u64)>>,
    pub content_disposition: Option<String>,
    pub requests: Arc<std::sync::atomic::AtomicUsize>,
}

impl Default for ServerFeatures {
    fn default() -> Self {
        Self {
            delay: DelayProfile::None,
            fragment_bytes: None,
            drop_once: Arc::new(Mutex::new(HashMap::new())),
            drop_forever: Arc::new(HashSet::new()),
            content_disposition: None,
            requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

pub async fn serve(
    listener: TcpListener,
    body: Vec<u8>,
    etag: String,
    features: ServerFeatures,
) {
    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => return,
        };
        let body = body.clone();
        let etag = etag.clone();
        let features = features.clone();
        tokio::spawn(async move {
            handle_connection(&mut socket, &body, &etag, &features).await;
        });
    }
}

async fn handle_connection(
    socket: &mut TcpStream,
    body: &[u8],
    etag: &str,
    features: &ServerFeatures,
) {
    socket.set_nodelay(true).ok();
    let mut request = Vec::with_capacity(4096);
    let mut buf = [0u8; 4096];
    loop {
        let read = match socket.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };
        request.extend_from_slice(&buf[..read]);
        if request.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if request.len() > 8192 {
            return;
        }
    }
    let text = String::from_utf8_lossy(&request);
    let head = text.split("\r\n").next().unwrap_or("");
    features
        .requests
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let header = |name: &str| -> Option<String> {
        text.lines()
            .find(|line| line.to_ascii_lowercase().starts_with(&name.to_ascii_lowercase()))
            .and_then(|line| line.split_once(':').map(|(_, v)| v.trim().to_string()))
    };
    let range_text = header("range");
    let range = range_text
        .as_deref()
        .and_then(|range| parse_single_range(range, body.len()));
    let is_head = head.starts_with("HEAD");

    if should_drop(features, range) {
        return;
    }
    if is_chunk_range(range)
        && let Some(delay) = compute_delay(&features.delay, range, body.len())
    {
        tokio::time::sleep(delay).await;
    }

    let head_bytes = build_response_head(is_head, range, body.len(), etag, features.content_disposition.as_deref());
    let _ = socket.write_all(&head_bytes).await;

    if !is_head {
        let payload = match range {
            None => body,
            Some((0, 0)) => &body[0..1],
            Some((start, end)) => &body[start..=end],
        };
        write_payload(socket, payload, features.fragment_bytes).await;
    }
    let _ = socket.shutdown().await;
}

fn is_chunk_range(range: Option<(usize, usize)>) -> bool {
    matches!(range, Some((start, end)) if start != 0 || end != 0)
}

fn should_drop(features: &ServerFeatures, range: Option<(usize, usize)>) -> bool {
    let Some((start, end)) = range else {
        return false;
    };
    let key = (start as u64, end as u64);
    if features.drop_forever.contains(&key) {
        return true;
    }
    let mut remaining = features.drop_once.lock().unwrap();
    if let Some(count) = remaining.get_mut(&key)
        && *count > 0
    {
        *count -= 1;
        return true;
    }
    false
}

fn compute_delay(
    profile: &DelayProfile,
    range: Option<(usize, usize)>,
    body_len: usize,
) -> Option<Duration> {
    match profile {
        DelayProfile::None => None,
        DelayProfile::Reverse { max_delay_ms } => {
            let start = range.map(|(start, _)| start).unwrap_or(0);
            let fraction = (body_len - start) as f64 / body_len as f64;
            let millis = ((fraction * *max_delay_ms as f64) as u64).max(1);
            Some(Duration::from_millis(millis))
        }
        DelayProfile::Jitter { seed, max_delay_ms } => {
            let start = range.map(|(start, _)| start).unwrap_or(0) as u64;
            let mut state = seed ^ start;
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let millis = state % (*max_delay_ms).max(1);
            Some(Duration::from_millis(millis))
        }
    }
}

fn build_response_head(
    is_head: bool,
    range: Option<(usize, usize)>,
    body_len: usize,
    etag: &str,
    content_disposition: Option<&str>,
) -> Vec<u8> {
    let _ = is_head;
    let (status, content_length, content_range) = match range {
        None => ("200 OK", body_len.to_string(), None),
        Some((0, 0)) => ("206 Partial Content", "1".to_string(), Some("0-0".to_string())),
        Some((start, end)) => (
            "206 Partial Content",
            (end - start + 1).to_string(),
            Some(format!("{start}-{end}")),
        ),
    };
    let range_header = content_range
        .as_deref()
        .map(|spec| format!("Content-Range: bytes {spec}/{body_len}\r\n"))
        .unwrap_or_default();
    let disposition_header = content_disposition
        .map(|value| format!("Content-Disposition: {value}\r\n"))
        .unwrap_or_default();
    format!(
        "HTTP/1.1 {status}\r\nContent-Length: {content_length}\r\n{range_header}Accept-Ranges: bytes\r\nETag: {etag}\r\nLast-Modified: Wed, 01 Jul 2026 12:00:00 GMT\r\n{disposition_header}Connection: close\r\n\r\n"
    )
    .into_bytes()
}

async fn write_payload(socket: &mut TcpStream, payload: &[u8], fragment: Option<usize>) {
    match fragment {
        None => {
            let _ = socket.write_all(payload).await;
        }
        Some(size) => {
            let size = size.max(1);
            for piece in payload.chunks(size) {
                let _ = socket.write_all(piece).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

fn parse_single_range(range: &str, body_len: usize) -> Option<(usize, usize)> {
    let spec = range.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: usize = start.parse().ok()?;
    let end: usize = if end.is_empty() {
        body_len - 1
    } else {
        end.parse().ok()?
    };
    Some((start, end))
}