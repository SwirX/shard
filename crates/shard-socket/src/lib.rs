//! Unix-domain request/response server and client for the shard protocol.
//!
//! Each connection is a sequence of newline-framed JSON lines (see
//! `shard_rpc::framing`). The server owns a listener, accepts connections and
//! drives one `Handler` per connection; the client is a thin `request()`/response
//! pair used by the CLI, the GUI and the native-messaging host.

use async_trait::async_trait;
use shard_rpc::framing::{decode_frame, encode_frame};
use shard_rpc::{Request, Response};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Handle one request. Returning `None` closes the connection; returning a
    /// response keeps it open for the next line.
    async fn handle(&self, request: Request) -> Option<Response>;
}

/// Accept connections forever, running each on its own task.
pub async fn serve(listener: UnixListener, handler: Arc<dyn Handler>) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, handler).await {
                eprintln!("shard-socket: connection error: {err}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    handler: Arc<dyn Handler>,
) -> std::io::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        }
        let Ok(request) = decode_frame::<Request>(line.as_bytes()) else {
            continue;
        };
        let Some(response) = handler.handle(request).await else {
            return Ok(());
        };
        writer.write_all(&encode_frame(&response)).await?;
        writer.flush().await?;
    }
    Ok(())
}

/// A connected protocol peer.
pub struct Client {
    stream: UnixStream,
}

impl Client {
    pub async fn connect(path: &Path) -> std::io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self { stream })
    }

    /// Send one request and wait for the daemon's response.
    pub async fn request(&mut self, request: &Request) -> std::io::Result<Response> {
        self.stream.write_all(&encode_frame(request)).await?;
        self.stream.flush().await?;
        let mut line = Vec::new();
        read_line(&mut self.stream, &mut line).await?;
        decode_frame::<Response>(&line).map_err(std::io::Error::other)
    }
}

async fn read_line(stream: &mut UnixStream, out: &mut Vec<u8>) -> std::io::Result<()> {
    out.clear();
    let mut byte = [0u8; 1];
    loop {
        let read = stream.read(&mut byte).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before a full line arrived",
            ));
        }
        if byte[0] == b'\n' {
            return Ok(());
        }
        out.push(byte[0]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    struct PingHandler;

    #[async_trait]
    impl Handler for PingHandler {
        async fn handle(&self, request: Request) -> Option<Response> {
            match request {
                Request::Ping => Some(Response::Pong),
                _ => Some(Response::Error {
                    code: "unsupported".into(),
                    message: "only Ping is handled in tests".into(),
                }),
            }
        }
    }

    #[tokio::test]
    async fn ping_round_trips_over_a_unix_socket() {
        let dir = std::env::temp_dir().join("shard-socket-test-ping");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.sock");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();

        let handler: Arc<dyn Handler> = Arc::new(PingHandler);
        let serve = tokio::spawn(async move { serve(listener, handler).await });

        let mut client = Client::connect(&path).await.unwrap();
        assert_eq!(client.request(&Request::Ping).await.unwrap(), Response::Pong);

        client.stream.shutdown().await.unwrap();
        assert!(client.request(&Request::Ping).await.is_err());
        serve.abort();
        let _ = std::fs::remove_file(&path);
    }
}