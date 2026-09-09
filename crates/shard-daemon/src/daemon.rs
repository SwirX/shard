use crate::Backend;
use shard_socket::{Handler, UnixListener, serve};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;

/// Name of the control socket inside the daemon's run directory
/// (e.g. `~/.local/share/shard/run/control.sock`).
pub const SOCKET_NAME: &str = "control.sock";

/// The daemon: binds the control socket, serves the protocol and owns the
/// process lifecycle. State (manager, registry, history) lives behind the
/// [`Backend`] supplied by the host process.
pub struct Daemon {
    socket_path: PathBuf,
}

impl Daemon {
    /// Prepare the daemon bound to `run_dir` (the directory that holds the
    /// control socket and per-download sockets).
    pub fn new(run_dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(run_dir)?;
        Ok(Self {
            socket_path: run_dir.join(SOCKET_NAME),
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Serve until `shutdown` fires or the process receives SIGINT/SIGTERM.
    pub async fn run<B: Backend>(
        self,
        backend: Arc<B>,
        mut shutdown: watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let _ = std::fs::remove_file(&self.socket_path);
        let listener = UnixListener::bind(&self.socket_path)?;
        eprintln!("shard daemon: listening on {}", self.socket_path.display());

        let handler = Arc::new(HandlerBridge { backend });
        let serve_task = tokio::spawn(serve(listener, handler));

        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() {
                    eprintln!("shard daemon: shutdown channel closed");
                }
            }
            _ = tokio::signal::ctrl_c() => {}
        }

        serve_task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
        eprintln!("shard daemon: goodbye");
        Ok(())
    }
}

struct HandlerBridge<B> {
    backend: Arc<B>,
}

#[async_trait::async_trait]
impl<B: Backend> Handler for HandlerBridge<B> {
    async fn handle(&self, request: shard_rpc::Request) -> Option<shard_rpc::Response> {
        Some(self.backend.handle(request).await)
    }
}
