use crate::engine::error::{DownloadError, DownloadResult};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Clone, Default)]
pub struct Controller {
    inner: Arc<ControlState>,
}

struct ControlState {
    paused: AtomicBool,
    cancelled: AtomicBool,
    pause_notify: Notify,
    cancel_notify: Notify,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            paused: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            pause_notify: Notify::new(),
            cancel_notify: Notify::new(),
        }
    }
}

impl Controller {
    pub fn pause(&self) {
        self.inner.paused.store(true, Ordering::SeqCst);
        self.inner.pause_notify.notify_waiters();
    }

    pub fn resume(&self) {
        self.inner.paused.store(false, Ordering::SeqCst);
        self.inner.pause_notify.notify_waiters();
    }

    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
        self.inner.pause_notify.notify_waiters();
        self.inner.cancel_notify.notify_waiters();
    }

    pub fn is_paused(&self) -> bool {
        self.inner.paused.load(Ordering::Acquire)
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub async fn wait_while_paused(&self) {
        self.await_until(|paused, cancelled| !paused || cancelled, &self.inner.pause_notify).await;
    }

    pub async fn wait_until_paused(&self) {
        self.await_until(|paused, _| paused, &self.inner.pause_notify).await;
    }

    pub async fn cancelled(&self) {
        self.await_until(|_, cancelled| cancelled, &self.inner.cancel_notify).await;
    }

    pub async fn next_body_chunk(
        &self,
        stream: &mut (impl futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin),
    ) -> DownloadResult<Option<reqwest::Result<bytes::Bytes>>> {
        loop {
            if self.is_paused() {
                self.wait_while_paused().await;
                if self.is_cancelled() {
                    return Err(DownloadError::Canceled);
                }
            }
            tokio::select! {
                item = stream.next() => return Ok(item),
                _ = self.wait_until_paused() => {}
                _ = self.cancelled() => return Err(DownloadError::Canceled),
            }
        }
    }

    async fn await_until(
        &self,
        satisfied: impl Fn(bool, bool) -> bool,
        wake: &Notify,
    ) {
        loop {
            let paused = self.is_paused();
            let cancelled = self.is_cancelled();
            if satisfied(paused, cancelled) {
                return;
            }
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if satisfied(self.is_paused(), self.is_cancelled()) {
                return;
            }
            notified.as_mut().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn pause_blocks_wait_until_resumed() {
        let controller = Controller::default();
        controller.pause();
        let parked = tokio::spawn({
            let controller = controller.clone();
            async move { controller.wait_while_paused().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!parked.is_finished());
        controller.resume();
        tokio::time::timeout(Duration::from_secs(1), parked).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn unpaused_controller_never_blocks() {
        let controller = Controller::default();
        tokio::time::timeout(Duration::from_millis(100), controller.wait_while_paused())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancel_wakes_a_parked_pause_waiter() {
        let controller = Controller::default();
        controller.pause();
        let parked = tokio::spawn({
            let controller = controller.clone();
            async move { controller.wait_while_paused().await }
        });
        controller.cancel();
        tokio::time::timeout(Duration::from_secs(1), parked).await.unwrap().unwrap();
        assert!(controller.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_future_resolves_once_cancelled() {
        let controller = Controller::default();
        let waiter = tokio::spawn({
            let controller = controller.clone();
            async move { controller.cancelled().await }
        });
        controller.cancel();
        tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
    }
}