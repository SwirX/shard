#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    Pending,
    Partial,
    Complete,
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
}

impl Chunk {
    pub fn range_len(&self) -> u64 {
        self.end - self.start + 1
    }

    pub fn state(&self) -> ChunkState {
        if self.downloaded == 0 {
            ChunkState::Pending
        } else if self.downloaded >= self.range_len() {
            ChunkState::Complete
        } else {
            ChunkState::Partial
        }
    }
}
