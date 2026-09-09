use crate::config::Config;
use crate::history::{History, HistoryEntry};
use crate::registry::{Entry, EntryStatus, Registry, data_dir, iso_now, new_id};
use shard_core::engine::{
    Controller, DownloadManager, DownloadOptions, FiletypeRouting, OutcomeStatus, ProgressEvent,
};
use shard_daemon::{Backend, Daemon};
use shard_rpc::{DownloadInfo, DownloadStatus, Request, Response};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast, mpsc, watch};

/// Run the `shard daemon` subcommand: own the download engine, the registry,
/// history and the control socket until interrupted.
pub async fn run() -> anyhow::Result<()> {
    let conf = Config::load()?;
    let manager = DownloadManager::new()?;
    let backend = Arc::new(DaemonBackend::new(manager, conf)?);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let run_dir = data_dir().join("run");
    let daemon = Daemon::new(&run_dir)?;
    daemon.run(backend, shutdown_rx).await?;
    Ok(())
}

/// Shared, but-avoiding-&self-borrow state handed to background tasks.
#[derive(Clone)]
struct DaemonState {
    registry: Arc<Registry>,
    history: Arc<Mutex<History>>,
    controllers: Arc<Mutex<HashMap<String, Controller>>>,
    live_bytes: Arc<Mutex<HashMap<String, (u64, u64)>>>,
    bus: broadcast::Sender<Vec<DownloadInfo>>,
}

struct DaemonBackend {
    manager: DownloadManager,
    conf: Config,
    state: DaemonState,
}

impl DaemonBackend {
    fn new(manager: DownloadManager, conf: Config) -> anyhow::Result<Self> {
        let history = History::open()?;
        let (bus, _) = broadcast::channel(128);
        Ok(Self {
            manager,
            conf,
            state: DaemonState {
                registry: Arc::new(Registry::load()),
                history: Arc::new(Mutex::new(history)),
                controllers: Arc::new(Mutex::new(HashMap::new())),
                live_bytes: Arc::new(Mutex::new(HashMap::new())),
                bus,
            },
        })
    }

    async fn start_download(&self, url: &str, dest: Option<String>) -> Response {
        let id = new_id();
        let (dest_path, routing) = self.resolve_dest(dest);
        let options = DownloadOptions {
            url: url.to_string(),
            dest_path: dest_path.clone(),
            chunk_size: self.conf.download.chunk_size,
            connections: self.conf.download.connections,
            max_attempts: self.conf.download.max_attempts,
            retry_base_delay: Duration::from_millis(self.conf.download.retry_base_ms),
            retry_max_delay: Duration::from_millis(self.conf.download.retry_max_ms),
            checkpoint_interval: Duration::from_millis(self.conf.download.checkpoint_ms),
            resume: self.conf.download.resume,
            routing,
        };
        let (progress_tx, mut progress_rx) = mpsc::channel(1024);
        let handle = match self.manager.start(&options, Some(progress_tx)) {
            Ok(handle) => handle,
            Err(err) => {
                return Response::Error {
                    code: "start_failed".into(),
                    message: err.to_string(),
                };
            }
        };
        let controller = handle.controller.clone();
        self.state
            .controllers
            .lock()
            .await
            .insert(id.clone(), controller);

        let entry = Entry {
            id: id.clone(),
            url: url.to_string(),
            dest: dest_path.clone(),
            final_dest: None,
            status: EntryStatus::Downloading,
            pid: std::process::id(),
            started_at: iso_now(),
            updated_at: iso_now(),
        };
        let _ = self.state.registry.add(&entry);

        let state = self.state.clone();
        let forwarder_id = id.clone();
        let forwarder = state.clone();
        tokio::spawn(async move {
            let mut chunk_written: HashMap<usize, u64> = HashMap::new();
            let mut total = 0u64;
            let mut last_emit = Instant::now() - Duration::from_millis(250);
            while let Some(event) = progress_rx.recv().await {
                match event {
                    ProgressEvent::Start { total: t, .. } => total = t,
                    ProgressEvent::ChunkAdvanced { index, written, .. } => {
                        chunk_written.insert(index, written);
                    }
                    _ => {}
                }
                if last_emit.elapsed() >= Duration::from_millis(250) {
                    let done: u64 = chunk_written.values().sum();
                    emit_snapshot(&forwarder, &forwarder_id, total, done, false).await;
                    last_emit = Instant::now();
                }
            }
            let done: u64 = chunk_written.values().sum();
            emit_snapshot(&forwarder, &forwarder_id, total, done, false).await;
        });
        let completion_state = state.clone();
        tokio::spawn(async move {
            let mut handle = handle;
            let outcome = handle.wait().await;
            complete_download(&completion_state, &entry, outcome).await;
        });

        emit_snapshot(&self.state, &id, 0, 0, false).await;

        Response::Started { id }
    }

    fn resolve_dest(&self, dest: Option<String>) -> (PathBuf, Option<FiletypeRouting>) {
        match dest {
            Some(dest) => (expand_tilde(&dest), None),
            None => {
                let base = expand_tilde(&self.conf.download.download_dir);
                (
                    base.clone(),
                    Some(FiletypeRouting {
                        dirs: [
                            ("video", self.conf.filetype.video.as_str()),
                            ("image", self.conf.filetype.image.as_str()),
                            ("audio", self.conf.filetype.audio.as_str()),
                            ("archive", self.conf.filetype.archive.as_str()),
                            ("document", self.conf.filetype.document.as_str()),
                            ("other", self.conf.filetype.other.as_str()),
                        ]
                        .into_iter()
                        .map(|(category, leaf)| (category.to_string(), leaf.to_string()))
                        .collect(),
                        other: self.conf.filetype.other.clone(),
                    }),
                )
            }
        }
    }

    async fn list(&self) -> Response {
        Response::List {
            downloads: self.current_snapshot().await,
        }
    }

    async fn status(&self, needle: &str) -> Response {
        let controllers = self.state.controllers.lock().await;
        let live = self.state.live_bytes.lock().await;
        match self.state.registry.by_id_or_url(needle) {
            Some(entry) => Response::Status {
                download: Some(to_info(entry, &controllers, live.get(&entry.id).copied())),
            },
            None => Response::Status { download: None },
        }
    }

    async fn control(&self, id: &str, action: impl Fn(&Controller)) -> Response {
        let controllers = self.state.controllers.lock().await;
        match controllers.get(id) {
            Some(controller) => {
                action(controller);
                Response::Ack { id: id.to_string() }
            }
            None => Response::Error {
                code: "not_running".into(),
                message: format!("download {id} is not running in this daemon"),
            },
        }
    }

    async fn current_snapshot(&self) -> Vec<DownloadInfo> {
        let controllers = self.state.controllers.lock().await;
        let live = self.state.live_bytes.lock().await;
        snapshot_info(&self.state.registry.entries, &controllers, &live)
    }
}

#[async_trait::async_trait]
impl Backend for DaemonBackend {
    async fn handle(&self, request: Request) -> Response {
        match request {
            Request::Ping => Response::Pong,
            Request::Start { url, dest, .. } => self.start_download(&url, dest).await,
            Request::List => self.list().await,
            Request::Status { id } => self.status(&id).await,
            Request::Pause { id } => self.control(&id, Controller::pause).await,
            Request::Resume { id } => self.control(&id, Controller::resume).await,
            Request::Cancel { id } => self.control(&id, Controller::cancel).await,
            Request::Watch => Response::Error {
                code: "watch_via_socket".into(),
                message: "the transport streams Watch snapshots itself".into(),
            },
        }
    }

    async fn snapshot(&self) -> Vec<DownloadInfo> {
        self.current_snapshot().await
    }

    async fn watch(&self) -> broadcast::Receiver<Vec<DownloadInfo>> {
        self.state.bus.subscribe()
    }
}

fn to_info(
    entry: &Entry,
    controllers: &HashMap<String, Controller>,
    live: Option<(u64, u64)>,
) -> DownloadInfo {
    let paused = entry.status == EntryStatus::Downloading
        && controllers
            .get(&entry.id)
            .is_some_and(|controller| controller.is_paused());
    let status = if paused {
        DownloadStatus::Paused
    } else {
        match entry.status {
            EntryStatus::Downloading => DownloadStatus::Downloading,
            EntryStatus::Completed => DownloadStatus::Completed,
            EntryStatus::Cancelled => DownloadStatus::Cancelled,
            EntryStatus::Failed => DownloadStatus::Failed,
        }
    };
    let (size, done_bytes) = live.unwrap_or((0, 0));
    DownloadInfo {
        id: entry.id.clone(),
        url: entry.url.clone(),
        dest: entry.dest.to_string_lossy().into_owned(),
        status,
        size,
        done_bytes,
        sha256: String::new(),
        error: None,
    }
}

fn snapshot_info(
    entries: &[Entry],
    controllers: &HashMap<String, Controller>,
    live: &HashMap<String, (u64, u64)>,
) -> Vec<DownloadInfo> {
    entries
        .iter()
        .map(|entry| to_info(entry, controllers, live.get(&entry.id).copied()))
        .collect()
}

async fn emit_snapshot(state: &DaemonState, id: &str, total: u64, done: u64, finished: bool) {
    let controllers = state.controllers.lock().await;
    let mut live = state.live_bytes.lock().await;
    if finished {
        live.remove(id);
    } else {
        live.insert(id.to_string(), (total, done));
    }
    let snapshot = snapshot_info(&state.registry.entries, &controllers, &live);
    let _ = state.bus.send(snapshot);
}

async fn complete_download(
    state: &DaemonState,
    entry: &Entry,
    outcome: Result<shard_core::engine::DownloadOutcome, shard_core::engine::DownloadError>,
) {
    let (status, final_url, size, sha256) = match outcome {
        Ok(outcome) => match outcome.status {
            OutcomeStatus::Completed => (
                EntryStatus::Completed,
                Some(outcome.final_url.clone()),
                Some(outcome.size),
                Some(outcome.sha256.clone()),
            ),
            OutcomeStatus::Cancelled => (
                EntryStatus::Cancelled,
                Some(outcome.final_url),
                Some(0),
                Some(String::new()),
            ),
        },
        Err(_) => (EntryStatus::Failed, None, None, None),
    };
    let _ = state.registry.update(&entry.id, |entry| {
        entry.status = status;
        entry.updated_at = iso_now();
    });
    let _ = state.history.lock().await.record(&HistoryEntry {
        id: entry.id.clone(),
        url: entry.url.clone(),
        final_url,
        dest: entry.dest.clone(),
        status,
        size: size.unwrap_or(0),
        sha256,
        started_at: entry.started_at.clone(),
        updated_at: iso_now(),
    });
    state.controllers.lock().await.remove(&entry.id);
    emit_snapshot(state, &entry.id, 0, 0, true).await;
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        PathBuf::from(home).join(rest)
    } else {
        PathBuf::from(path)
    }
}
