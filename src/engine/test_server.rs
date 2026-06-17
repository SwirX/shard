use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub struct TestServer {
    pub addr: std::net::SocketAddr,
    pub body: Vec<u8>,
    pub etag: String,
}

impl TestServer {
    pub async fn spawn(body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let etag = format!("\"fake-{}-{}\"", body.len(), body.len() + 1);
        let server = Self {
            addr,
            etag,
            body: body.clone(),
        };
        tokio::spawn(serve(listener, body, server.etag.clone()));
        server
    }

    pub fn url(&self) -> String {
        format!("http://{}/file.bin", self.addr)
    }
}

async fn serve(listener: TcpListener, body: Vec<u8>, etag: String) {
    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => return,
        };
        let body = body.clone();
        let etag = etag.clone();
        tokio::spawn(async move {
            handle_connection(&mut socket, &body, &etag).await;
        });
    }
}

async fn handle_connection(socket: &mut tokio::net::TcpStream, body: &[u8], etag: &str) {
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
    let header = |name: &str| -> Option<String> {
        text.lines()
            .find(|line| line.to_ascii_lowercase().starts_with(&name.to_ascii_lowercase()))
            .and_then(|line| line.split_once(':').map(|(_, v)| v.trim().to_string()))
    };
    let range = header("range");
    let is_head = head.starts_with("HEAD");

    let response = if is_head {
        match range {
            Some(range_text) if range_text == "bytes=0-0" => {
                format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nLast-Modified: Wed, 01 Jul 2026 12:00:00 GMT\r\nConnection: close\r\n\r\n",
                    body.len(),
                    etag
                )
                .into_bytes()
            }
            Some(range_text) => full_range_head(&range_text, body.len(), etag).into_bytes(),
            None => {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nLast-Modified: Wed, 01 Jul 2026 12:00:00 GMT\r\nConnection: close\r\n\r\n",
                    body.len(),
                    etag
                )
                .into_bytes()
            }
        }
    } else {
        match range {
            Some(range_text) if range_text == "bytes=0-0" => append_body(
                format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nLast-Modified: Wed, 01 Jul 2026 12:00:00 GMT\r\nConnection: close\r\n\r\n",
                    body.len(),
                    etag
                ),
                &body[0..1],
            ),
            Some(range_text) => {
                if let Some((start, end)) = parse_single_range(&range_text, body.len()) {
                    append_body(full_range_head(&range_text, body.len(), etag), &body[start..=end])
                } else {
                    format!(
                        "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nContent-Range: bytes */{}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes()
                }
            }
            None => append_body(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nLast-Modified: Wed, 01 Jul 2026 12:00:00 GMT\r\nConnection: close\r\n\r\n",
                    body.len(),
                    etag
                ),
                body,
            ),
        }
    };

    let _ = socket.write_all(&response).await;
    let _ = socket.shutdown().await;
}

fn full_range_head(range_text: &str, body_len: usize, etag: &str) -> String {
    match parse_single_range(range_text, body_len) {
        Some((start, end)) => format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nLast-Modified: Wed, 01 Jul 2026 12:00:00 GMT\r\nConnection: close\r\n\r\n",
            end - start + 1,
            start,
            end,
            body_len,
            etag
        ),
        None => format!(
            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nContent-Range: bytes */{}\r\nConnection: close\r\n\r\n",
            body_len
        ),
    }
}

fn append_body(headers: String, payload: &[u8]) -> Vec<u8> {
    let mut bytes = headers.into_bytes();
    bytes.extend_from_slice(payload);
    bytes
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