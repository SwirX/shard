use super::planner::{Chunk, ChunkPlan};
use std::collections::VecDeque;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

pub struct ChunkDispatcher {
    plan: Arc<ChunkPlan>,
    pending: Mutex<VecDeque<usize>>,
    attempts: Vec<AtomicU64>,
    downloaded: Vec<AtomicU64>,
    completed: Vec<AtomicBool>,
    settled: AtomicUsize,
    failed: AtomicUsize,
    notify: Notify,
}

impl ChunkDispatcher {
    pub fn new(plan: Arc<ChunkPlan>) -> Self {
        let total = plan.chunks.len();
        let mut pending = VecDeque::with_capacity(total);
        let mut completed = Vec::with_capacity(total);
        let mut downloaded = Vec::with_capacity(total);
        let mut settled = 0usize;
        for (index, chunk) in plan.chunks.iter().enumerate() {
            let done = chunk.downloaded >= chunk.range_len();
            downloaded.push(AtomicU64::new(chunk.downloaded));
            completed.push(AtomicBool::new(done));
            if done {
                settled += 1;
            } else {
                pending.push_back(index);
            }
        }
        Self {
            plan,
            pending: Mutex::new(pending),
            attempts: (0..total).map(|_| AtomicU64::new(0)).collect(),
            downloaded,
            completed,
            settled: AtomicUsize::new(settled),
            failed: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    pub fn total(&self) -> usize {
        self.plan.chunks.len()
    }

    pub fn chunk(&self, index: usize) -> &Chunk {
        &self.plan.chunks[index]
    }

    pub async fn take(&self) -> Option<usize> {
        loop {
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            {
                let mut guard = self.pending.lock().await;
                if let Some(index) = guard.pop_front() {
                    return Some(index);
                }
                if self.settled.load(Ordering::Acquire) == self.total() {
                    return None;
                }
            }
            notified.as_mut().await;
        }
    }

    pub fn attempt(&self, index: usize) -> u64 {
        self.attempts[index].fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn reset_progress(&self, index: usize) {
        self.downloaded[index].store(0, Ordering::SeqCst);
    }

    pub fn record_progress(&self, index: usize, written: u64) {
        self.downloaded[index].store(written, Ordering::SeqCst);
    }

    pub async fn requeue(&self, index: usize) {
        let mut guard = self.pending.lock().await;
        guard.push_back(index);
        drop(guard);
        self.notify.notify_waiters();
    }

    pub fn settle_complete(&self, index: usize) {
        self.completed[index].store(true, Ordering::SeqCst);
        self.settled.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    pub fn settle_failed(&self, _index: usize) {
        self.failed.fetch_add(1, Ordering::AcqRel);
        self.settled.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    pub fn failed_count(&self) -> usize {
        self.failed.load(Ordering::Acquire)
    }

    pub fn completed_count(&self) -> usize {
        self.completed.iter().filter(|flag| flag.load(Ordering::Acquire)).count()
    }

    pub fn is_fully_settled(&self) -> bool {
        self.settled.load(Ordering::Acquire) == self.total()
    }

    pub fn downloaded_counts(&self) -> Vec<u64> {
        self.downloaded
            .iter()
            .map(|count| count.load(Ordering::Acquire))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_chunk_is_dispensed_exactly_once() {
        let plan = Arc::new(ChunkPlan::build(1024, 256));
        let dispatcher = ChunkDispatcher::new(plan);
        let mut dispensed = Vec::new();
        while let Some(index) = dispatcher.take().await {
            dispensed.push(index);
            dispatcher.settle_complete(index);
        }
        assert_eq!(dispensed.len(), 4);
        assert!(dispatcher.is_fully_settled());
        assert_eq!(dispatcher.completed_count(), 4);
        assert_eq!(dispatcher.failed_count(), 0);
    }

    #[tokio::test]
    async fn requeued_chunk_is_dispensed_again_before_completion() {
        let plan = Arc::new(ChunkPlan::build(512, 128));
        let dispatcher = ChunkDispatcher::new(plan);
        let failed_chunk = dispatcher.take().await.unwrap();
        dispatcher.requeue(failed_chunk).await;
        let mut dispensed_again = false;
        while let Some(index) = dispatcher.take().await {
            if index == failed_chunk {
                dispensed_again = true;
            }
            dispatcher.settle_complete(index);
        }
        assert!(dispensed_again);
        assert!(dispatcher.is_fully_settled());
        assert_eq!(dispatcher.completed_count(), 4);
        assert_eq!(dispatcher.failed_count(), 0);
    }

    #[tokio::test]
    async fn pre_completed_chunks_are_never_dispensed() {
        use crate::engine::planner::Chunk;
        let plan = Arc::new(ChunkPlan::from_chunks(vec![
            Chunk { start: 0, end: 63, downloaded: 64 },
            Chunk { start: 64, end: 127, downloaded: 0 },
        ]));
        let dispatcher = ChunkDispatcher::new(plan);
        assert_eq!(dispatcher.completed_count(), 1);
        let mut dispensed = Vec::new();
        while let Some(index) = dispatcher.take().await {
            dispensed.push(index);
            dispatcher.settle_complete(index);
        }
        assert_eq!(dispensed, vec![1]);
        assert_eq!(dispatcher.completed_count(), 2);
    }
}