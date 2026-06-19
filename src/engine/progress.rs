#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressEvent {
    Start { total: u64 },
    ChunkStarted { index: usize },
    ChunkAdvanced { index: usize, written: u64 },
    ChunkComplete { index: usize },
    Finite { total: u64, done: u64 },
}
