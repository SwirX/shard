use shard_core::engine::progress::ProgressEvent;
use std::collections::HashMap;

pub struct ProgressTracker {
    pub total: Option<u64>,
    chunk_size: Option<u64>,
    written: Vec<u64>,
    done: Vec<bool>,
    started: Vec<bool>,
    chunk_worker: HashMap<usize, usize>,
    worker_written: Vec<HashMap<usize, u64>>,
    worker_active_chunk: Vec<Option<usize>>,
}

impl ProgressTracker {
    pub fn new(workers: usize) -> ProgressTracker {
        ProgressTracker {
            total: None,
            chunk_size: None,
            written: Vec::new(),
            done: Vec::new(),
            started: Vec::new(),
            chunk_worker: HashMap::new(),
            worker_written: vec![HashMap::new(); workers],
            worker_active_chunk: vec![None; workers],
        }
    }

    pub fn handle(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::Start { total, chunk_size } => {
                self.total = Some(total);
                self.chunk_size = Some(chunk_size);
            }
            ProgressEvent::ChunkStarted { worker, index } => {
                self.ensure_index(index);
                self.started[index] = true;
                self.chunk_worker.insert(index, worker);
                self.worker_active_chunk[worker] = Some(index);
                self.worker_written[worker].entry(index).or_insert(0);
            }
            ProgressEvent::ChunkAdvanced {
                worker,
                index,
                written,
            } => {
                self.ensure_index(index);
                self.written[index] = self.written[index].max(written);
                self.chunk_worker.insert(index, worker);
                self.worker_written[worker].insert(index, written);
            }
            ProgressEvent::ChunkComplete { worker, index } => {
                self.ensure_index(index);
                self.done[index] = true;
                if self.worker_active_chunk[worker] == Some(index) {
                    self.worker_active_chunk[worker] = None;
                }
            }
            ProgressEvent::Finite { .. } => {}
        }
    }

    pub fn done_bytes(&self) -> u64 {
        self.written.iter().sum()
    }

    pub fn chunk_count(&self) -> usize {
        self.chunk_worker.len().max(self.written.len())
    }

    pub fn planned_chunk_count(&self) -> usize {
        match (self.total, self.chunk_size) {
            (Some(total), Some(size)) if size > 0 => total.div_ceil(size).max(1) as usize,
            _ => 0,
        }
    }

    pub fn chunk_for_offset(&self, offset: u64) -> Option<usize> {
        let size = self.chunk_size?;
        if size == 0 {
            return None;
        }
        let index = (offset / size) as usize;
        Some(index.min(self.planned_chunk_count().saturating_sub(1)))
    }

    pub fn chunk_worker(&self, index: usize) -> Option<usize> {
        self.chunk_worker.get(&index).copied()
    }

    pub fn finished_chunks(&self) -> usize {
        self.done.iter().filter(|&&is_done| is_done).count()
    }

    pub fn active_worker_count(&self) -> usize {
        self.worker_active_chunk
            .iter()
            .filter(|active| active.is_some())
            .count()
    }

    pub fn worker_bytes(&self, worker: usize) -> u64 {
        self.worker_written
            .get(worker)
            .map(|written| written.values().sum())
            .unwrap_or(0)
    }

    pub fn worker_active_chunk(&self, worker: usize) -> Option<usize> {
        self.worker_active_chunk.get(worker).copied().flatten()
    }

    fn ensure_index(&mut self, index: usize) {
        if self.written.len() <= index {
            self.written.resize(index + 1, 0);
            self.done.resize(index + 1, false);
            self.started.resize(index + 1, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(total: u64) -> ProgressEvent {
        ProgressEvent::Start {
            total,
            chunk_size: 100,
        }
    }

    #[test]
    fn written_bytes_track_the_max_across_retries() {
        let mut tracker = ProgressTracker::new(2);
        tracker.handle(start(200));
        tracker.handle(ProgressEvent::ChunkAdvanced {
            worker: 0,
            index: 0,
            written: 60,
        });
        tracker.handle(ProgressEvent::ChunkAdvanced {
            worker: 0,
            index: 0,
            written: 30,
        });
        assert_eq!(tracker.done_bytes(), 60);
    }

    #[test]
    fn chunks_are_attributed_to_the_worker_that_worked_them() {
        let mut tracker = ProgressTracker::new(2);
        tracker.handle(start(400));
        tracker.handle(ProgressEvent::ChunkStarted {
            worker: 1,
            index: 3,
        });
        tracker.handle(ProgressEvent::ChunkAdvanced {
            worker: 1,
            index: 3,
            written: 90,
        });
        assert_eq!(tracker.chunk_worker(3), Some(1));
        assert_eq!(tracker.worker_bytes(1), 90);
        assert_eq!(tracker.worker_active_chunk(1), Some(3));
    }

    #[test]
    fn completion_marks_worker_idle_and_chunk_done() {
        let mut tracker = ProgressTracker::new(1);
        tracker.handle(start(300));
        tracker.handle(ProgressEvent::ChunkStarted {
            worker: 0,
            index: 2,
        });
        tracker.handle(ProgressEvent::ChunkAdvanced {
            worker: 0,
            index: 2,
            written: 100,
        });
        tracker.handle(ProgressEvent::ChunkComplete {
            worker: 0,
            index: 2,
        });
        assert_eq!(tracker.worker_active_chunk(0), None);
        assert_eq!(tracker.finished_chunks(), 1);
    }

    #[test]
    fn retried_chunk_keeps_byte_max_from_both_workers() {
        let mut tracker = ProgressTracker::new(3);
        tracker.handle(start(500));
        tracker.handle(ProgressEvent::ChunkAdvanced {
            worker: 0,
            index: 4,
            written: 40,
        });
        tracker.handle(ProgressEvent::ChunkAdvanced {
            worker: 2,
            index: 4,
            written: 80,
        });
        assert_eq!(tracker.done_bytes(), 80);
        assert_eq!(tracker.chunk_worker(4), Some(2));
    }

    #[test]
    fn planned_chunks_and_offsets_map_back_to_chunks() {
        let mut tracker = ProgressTracker::new(2);
        tracker.handle(ProgressEvent::Start {
            total: 1000,
            chunk_size: 100,
        });
        assert_eq!(tracker.planned_chunk_count(), 10);
        assert_eq!(tracker.chunk_for_offset(0), Some(0));
        assert_eq!(tracker.chunk_for_offset(250), Some(2));
        assert_eq!(tracker.chunk_for_offset(999), Some(9));
    }
}
