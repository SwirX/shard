use crate::engine::dispatch::ChunkDispatcher;
use crate::engine::error::{DownloadError, DownloadResult};
use crate::engine::manifest::{ChunkEntry, ManifestTemplate};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

pub struct CheckpointGuard {
    inner: Arc<CheckpointCoordinator>,
    quit_tx: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

struct CheckpointCoordinator {
    template: ManifestTemplate,
    dispatcher: Arc<ChunkDispatcher>,
    dest: PathBuf,
    interval: Duration,
    integrity_sha: Mutex<Option<String>>,
    flush_error: Mutex<Option<String>>,
}

impl CheckpointGuard {
    pub fn spawn(
        dest: &Path,
        template: ManifestTemplate,
        dispatcher: Arc<ChunkDispatcher>,
        interval: Duration,
    ) -> Self {
        let (quit_tx, quit_rx) = watch::channel(false);
        let inner = Arc::new(CheckpointCoordinator {
            template,
            dispatcher,
            dest: dest.to_path_buf(),
            interval,
            integrity_sha: Mutex::new(None),
            flush_error: Mutex::new(None),
        });
        let task = {
            let inner = Arc::clone(&inner);
            let mut quit_rx = quit_rx.clone();
            tokio::spawn(async move {
                inner.periodic_flush_loop(&mut quit_rx).await;
            })
        };
        Self { inner, quit_tx, task: Some(task) }
    }

    pub fn set_integrity_sha(&self, sha256: String) {
        *self.inner.integrity_sha.lock().unwrap() = Some(sha256);
    }

    pub async fn finish(mut self) -> DownloadResult<()> {
        let _ = self.quit_tx.send(true);
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|join| DownloadError::Io(std::io::Error::other(join.to_string())))?;
        }
        self.inner.flush()?;
        if let Some(message) = self.inner.take_error() {
            return Err(DownloadError::Io(std::io::Error::other(message)));
        }
        Ok(())
    }
}

impl CheckpointCoordinator {
    async fn periodic_flush_loop(self: &Arc<Self>, quit_rx: &mut watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + Self::settle_before_first_flush(self.interval),
            self.interval,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if *quit_rx.borrow() {
                break;
            }
            tokio::select! {
                _ = ticker.tick() => {
                    if self.flush().is_err() {
                        break;
                    }
                }
                changed = quit_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
    }

    fn settle_before_first_flush(interval: Duration) -> Duration {
        interval.min(Duration::from_secs(1))
    }

    fn flush(&self) -> DownloadResult<()> {
        let sha256 = self.integrity_sha.lock().unwrap().clone();
        let chunks = self
            .dispatcher
            .downloaded_counts()
            .into_iter()
            .enumerate()
            .map(|(index, downloaded)| {
                let chunk = self.dispatcher.chunk(index);
                ChunkEntry {
                    start: chunk.start,
                    end: chunk.end,
                    downloaded,
                }
            })
            .collect::<Vec<_>>();
        let manifest = self.template.clone().into_manifest(chunks, sha256);
        match manifest.save_atomic(&self.dest) {
            Ok(()) => Ok(()),
            Err(err) => {
                *self.flush_error.lock().unwrap() = Some(err.to_string());
                Err(err)
            }
        }
    }

    fn take_error(&self) -> Option<String> {
        self.flush_error.lock().unwrap().take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::manifest::{Manifest, RemoteSnapshot};
    use crate::engine::planner::ChunkPlan;

    #[tokio::test]
    async fn finish_writes_a_final_manifest_for_the_given_dispatcher_state() {
        let dest = std::env::temp_dir().join("shard-checkpoint-finish.bin");
        let plan = Arc::new(ChunkPlan::build(512, 128));
        let dispatcher = Arc::new(ChunkDispatcher::new(plan));
        dispatcher.record_progress(0, 128);
        dispatcher.record_progress(1, 128);
        dispatcher.settle_complete(0);
        dispatcher.settle_complete(1);

        let template = ManifestTemplate {
            url: "http://x".into(),
            final_url: "http://x/file.bin".into(),
            filename: "checkpoint-finish.bin".into(),
            remote: RemoteSnapshot {
                size: 512,
                etag: None,
                last_modified: None,
            },
            chunk_size: 128,
        };
        let guard = CheckpointGuard::spawn(&dest, template, dispatcher.clone(), Duration::from_secs(1));
        guard.finish().await.unwrap();

        let manifest = Manifest::load(&dest).unwrap().unwrap();
        assert_eq!(manifest.chunks.len(), 4);
        let complete = manifest.chunks.iter().filter(|entry| entry.downloaded == 128).count();
        assert_eq!(complete, 2);
        let _ = std::fs::remove_file(crate::engine::manifest::sidecar_path(&dest));
    }
}