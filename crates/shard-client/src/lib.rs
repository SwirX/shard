//! High-level client for talking to the shard daemon's control socket.
//!
//! Built on top of `shard_socket`, this is the entry point the GUI, the
//! native-messaging host and third-party apps use instead of reaching for
//! the transport themselves.

use shard_rpc::{Request, Response, ServerEvent};
use std::path::{Path, PathBuf};

/// Name of the daemon's control socket (kept in sync with `shard-daemon`).
pub const SOCKET_NAME: &str = "control.sock";

/// A connected control socket. Cheap to clone per request; the server runs
/// one `Handler` per connection either way.
pub struct Client {
    inner: shard_socket::Client,
}

impl Client {
    /// Connect to the default control socket:
    /// `$XDG_DATA_HOME/shard/run/control.sock` (or `~/.local/share/...`).
    pub async fn connect() -> std::io::Result<Self> {
        Self::connect_to(&control_socket_path()).await
    }

    pub async fn connect_to(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            inner: shard_socket::Client::connect(path).await?,
        })
    }

    /// Send one request and read the daemon's reply.
    pub async fn request(&mut self, request: &Request) -> std::io::Result<Response> {
        self.inner.request(request).await
    }

    /// Switch to the `Watch` stream; use [`Self::next_event`] afterwards.
    pub async fn watch(&mut self) -> std::io::Result<()> {
        self.inner.watch().await
    }

    /// Read the next server-pushed snapshot (after [`Self::watch`]).
    pub async fn next_event(&mut self) -> std::io::Result<ServerEvent> {
        self.inner.next_event().await
    }
}

/// Location of the control socket the daemon serves on.
pub fn control_socket_path() -> PathBuf {
    data_dir().join("run").join(SOCKET_NAME)
}

/// XDG data directory shared by the daemon, registry and history.
pub fn data_dir() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local").join("share"))
        })
        .unwrap_or_default();
    base.join("shard")
}
