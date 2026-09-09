use std::io::Write;
use std::path::Path;

/// Atomically write `body` to `path`: write to a temp file next to the target,
/// fsync it, rename over the target, then fsync the parent directory.
pub fn atomic_write(path: &Path, body: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    atomic_replace(&tmp, path, body)
}

/// Atomically replace `target` with `body`, using `tmp` as the scratch file.
/// Callers with custom suffix conventions supply the temp path themselves.
pub fn atomic_replace(tmp: &Path, target: &Path, body: &[u8]) -> std::io::Result<()> {
    {
        let mut file = std::fs::File::create(tmp)?;
        file.write_all(body)?;
        file.sync_all()?;
    }
    std::fs::rename(tmp, target)?;
    if let Some(parent) = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
