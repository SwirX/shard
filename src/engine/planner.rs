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

#[derive(Debug, Clone)]
pub struct ChunkPlan {
    pub chunks: Vec<Chunk>,
}

impl ChunkPlan {
    pub fn build(total_size: u64, chunk_size: u64) -> Self {
        debug_assert!(chunk_size > 0);
        let mut chunks = Vec::new();
        let mut start = 0u64;
        while start < total_size {
            let end = start.saturating_add(chunk_size).saturating_sub(1).min(total_size - 1);
            chunks.push(Chunk {
                start,
                end,
                downloaded: 0,
            });
            start = end + 1;
        }
        Self { chunks }
    }

    pub fn progress(&self) -> u64 {
        self.chunks.iter().map(|chunk| chunk.downloaded).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_covers_every_byte_exactly_once() {
        let total_size = 1_000_000_000u64;
        let chunk_size = 64u64 * 1024 * 1024;
        let plan = ChunkPlan::build(total_size, chunk_size);

        let covered: u64 = plan.chunks.iter().map(|c| c.range_len()).sum();
        assert_eq!(covered, total_size);

        for window in plan.chunks.windows(2) {
            assert_eq!(window[0].end + 1, window[1].start);
        }
        assert_eq!(plan.chunks[0].start, 0);
        assert_eq!(plan.chunks.last().unwrap().end, total_size - 1);
    }

    #[test]
    fn exact_multiple_produces_no_remainder_chunk() {
        let plan = ChunkPlan::build(1024, 256);
        assert_eq!(plan.chunks.len(), 4);
        assert!(plan.chunks.iter().all(|c| c.state() == ChunkState::Pending));
    }
}
