#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressEvent {
    Start {
        total: u64,
        chunk_size: u64,
    },
    ChunkStarted {
        worker: usize,
        index: usize,
    },
    ChunkAdvanced {
        worker: usize,
        index: usize,
        written: u64,
    },
    ChunkComplete {
        worker: usize,
        index: usize,
    },
    Finite {
        total: u64,
        done: u64,
    },
}
