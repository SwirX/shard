use crate::engine::error::{DownloadError, DownloadResult};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub struct PositionalWriter {
    file: File,
}

impl PositionalWriter {
    pub fn open(path: &Path) -> DownloadResult<Self> {
        let file = File::create(path)?;
        Ok(Self { file })
    }

    pub fn open_existing(path: &Path) -> DownloadResult<Self> {
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        Ok(Self { file })
    }

    pub fn preallocate(&mut self, size: u64) -> DownloadResult<()> {
        self.file.set_len(size)?;
        Ok(())
    }

    pub fn write_at(&mut self, buffer: &[u8], offset: u64) -> DownloadResult<()> {
        self.file.write_at(buffer, offset)?;
        Ok(())
    }

    pub fn sync_all(&self) -> DownloadResult<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn current_len(&self) -> DownloadResult<u64> {
        Ok(self.file.metadata()?.len())
    }
}

pub fn file_len(path: &Path) -> DownloadResult<u64> {
    std::fs::metadata(path)
        .map(|meta| meta.len())
        .map_err(DownloadError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    fn scratch_path() -> std::path::PathBuf {
        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("shard-writer-test-{id}"))
    }

    #[test]
    fn positional_writes_land_at_their_offsets() {
        let path = scratch_path();
        let mut writer = PositionalWriter::open(&path).unwrap();
        writer.preallocate(16).unwrap();

        writer.write_at(&[1, 2, 3], 0).unwrap();
        writer.write_at(&[7, 8, 9], 8).unwrap();

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes, vec![1, 2, 3, 0, 0, 0, 0, 0, 7, 8, 9, 0, 0, 0, 0, 0]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn preallocation_sets_expected_length() {
        let path = scratch_path();
        let mut writer = PositionalWriter::open(&path).unwrap();
        writer.preallocate(4 * 1024 * 1024).unwrap();
        assert_eq!(writer.current_len().unwrap(), 4 * 1024 * 1024);
        let _ = std::fs::remove_file(&path);
    }
}
