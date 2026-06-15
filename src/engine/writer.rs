use anyhow::Result;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub struct PositionalWriter {
    file: File,
}

impl PositionalWriter {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self { file })
    }

    pub fn preallocate(&mut self, size: u64) -> Result<()> {
        self.file.set_len(size)?;
        Ok(())
    }

    pub fn write_at(&mut self, buffer: &[u8], offset: u64) -> Result<()> {
        self.file.write_at(buffer, offset)?;
        Ok(())
    }
}
