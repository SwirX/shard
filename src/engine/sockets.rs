use super::control::Controller;
use crate::engine::error::DownloadResult;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

pub async fn serve(
    path: &Path,
    controller: Controller,
    mut shutdown: watch::Receiver<bool>,
) -> DownloadResult<()> {
    let _ = std::fs::remove_file(path);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let listener = UnixListener::bind(path)
        .map_err(|err| crate::engine::error::DownloadError::Io(std::io::Error::other(format!(
            "cannot bind control socket {}: {err}",
            path.display()
        ))))?;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let controller = controller.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, controller).await;
                });
            }
        }
    }
    let _ = std::fs::remove_file(path);
    Ok(())
}

async fn handle_connection(stream: UnixStream, controller: Controller) -> std::io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line).await?;
        if read == 0 {
            return Ok(());
        }
        let reply = match line.trim() {
            "pause" => {
                controller.pause();
                "ok\n"
            }
            "resume" => {
                controller.resume();
                "ok\n"
            }
            "cancel" => {
                controller.cancel();
                "ok\n"
            }
            "status" => {
                if controller.is_cancelled() {
                    "cancelled\n"
                } else if controller.is_paused() {
                    "paused\n"
                } else {
                    "running\n"
                }
            }
            other => {
                writer.write_all(format!("error unknown command {other:?}\n").as_bytes()).await?;
                continue;
            }
        };
        writer.write_all(reply.as_bytes()).await?;
        writer.flush().await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::watch;

    async fn rpc(sock: &Path, command: &str) -> String {
        let stream = UnixStream::connect(sock).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        writer.write_all(format!("{command}\n").as_bytes()).await.unwrap();
        writer.flush().await.unwrap();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        line
    }

    #[tokio::test]
    async fn commands_control_the_controller_over_the_socket() {
        let controller = Controller::default();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let dir = std::env::temp_dir().join(format!(
            "shard-socket-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("run.sock");
        let serve_sock = sock.clone();
        tokio::spawn(async move {
            let _ = serve(&serve_sock, controller.clone(), shutdown_rx).await;
        });
        for _ in 0..100 {
            if sock.exists() {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert_eq!(rpc(&sock, "status").await, "running\n");
        assert_eq!(rpc(&sock, "pause").await, "ok\n");
        assert_eq!(rpc(&sock, "status").await, "paused\n");
        assert_eq!(rpc(&sock, "resume").await, "ok\n");
        assert_eq!(rpc(&sock, "cancel").await, "ok\n");
        assert_eq!(rpc(&sock, "status").await, "cancelled\n");

        shutdown_tx.send(true).unwrap();
        let _ = std::fs::remove_file(&sock);
    }
}