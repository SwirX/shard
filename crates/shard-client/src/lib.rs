//! High-level client for talking to the shard daemon's control socket.
//!
//! Built on top of `shard_socket`, this is the entry point the GUI, the
//! native-messaging host and third-party apps use instead of reaching for
//! the transport themselves.

use shard_rpc::{Request, Response, ServerEvent};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

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

    /// Like [`Self::connect`], but if nothing is listening a daemon is
    /// spawned (override with `$SHARD_DAEMON`) and the socket is polled
    /// until it answers or the timeout elapses.
    pub async fn connect_auto() -> std::io::Result<Self> {
        if let Ok(client) = Self::connect().await {
            return Ok(client);
        }
        spawn_daemon();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match Self::connect().await {
                Ok(client) => return Ok(client),
                Err(_) if std::time::Instant::now() >= deadline => {
                    return Err(std::io::Error::other(
                        "shard daemon did not come up within 5s",
                    ));
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
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

/// Launch `shard daemon` detached. Best effort: the caller polls the socket
/// afterwards, so a failed spawn simply ends up timing out.
fn spawn_daemon() {
    let _ = Command::new(daemon_binary())
        .arg("daemon")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

/// Where to find the daemon binary: `$SHARD_DAEMON`, then a `shard` sibling
/// of the current executable, then `shard` on `$PATH`.
fn daemon_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("SHARD_DAEMON") {
        return PathBuf::from(path);
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(sibling) = exe.parent().map(|dir| dir.join("shard"))
        && sibling.exists()
    {
        return sibling;
    }
    PathBuf::from("shard")
}
