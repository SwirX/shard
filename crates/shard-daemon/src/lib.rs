//! The daemon service: a long-lived process that owns the download engine
//! behind the versioned protocol. The binary wiring (state persistence,
//! registry, history) lives behind the [`Backend`] trait, so the CLI, future
//! GUI and the native host all speak one language to the same daemon.

mod daemon;

pub use daemon::{Daemon, SOCKET_NAME};

use async_trait::async_trait;
use shard_rpc::{DownloadInfo, Request, Response};
use tokio::sync::broadcast;

/// Implemented by whoever owns the daemon's state (currently `shard daemon`).
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Answer one request. The daemon layer handles transport so backends only
    /// need protocol semantics.
    async fn handle(&self, request: Request) -> Response;

    /// Current download set, sent to a client the moment it starts watching.
    async fn snapshot(&self) -> Vec<DownloadInfo>;

    /// Subscribe to live download snapshots (see [`Request::Watch`]).
    async fn watch(&self) -> broadcast::Receiver<Vec<DownloadInfo>>;
}
