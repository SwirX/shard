#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressEvent {
    ChunkStarted { index: usize },
    ChunkAdvanced { index: usize, written: u64 },
    ChunkComplete { index: usize },
    Finite { total: u64, done: u64 },
}
