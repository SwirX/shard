use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};

use crate::registry::{EntryStatus, data_dir};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS downloads (
    id         TEXT PRIMARY KEY,
    url        TEXT NOT NULL,
    final_url  TEXT,
    dest       TEXT NOT NULL,
    status     TEXT NOT NULL,
    size       INTEGER NOT NULL DEFAULT 0,
    sha256     TEXT,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
";

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub id: String,
    pub url: String,
    pub final_url: Option<String>,
    pub dest: PathBuf,
    pub status: EntryStatus,
    pub size: u64,
    pub sha256: Option<String>,
    pub started_at: String,
    pub updated_at: String,
}

pub struct History {
    conn: Connection,
}

pub fn history_path() -> PathBuf {
    data_dir().join("history.db")
}

impl History {
    pub fn open() -> std::io::Result<History> {
        Self::open_at(&history_path())
    }

    fn open_at(path: &Path) -> std::io::Result<History> {
        std::fs::create_dir_all(path.parent().expect("history db has a parent directory"))?;
        let conn = Connection::open(path).map_err(|err| {
            std::io::Error::other(format!("open history {}: {err}", path.display()))
        })?;
        conn.execute_batch(SCHEMA)
            .map_err(|err| std::io::Error::other(format!("init history schema: {err}")))?;
        Ok(History { conn })
    }

    /// Insert or fully replace the row for this id.
    pub fn record(&self, entry: &HistoryEntry) -> std::io::Result<()> {
        use std::io::Error as IoError;
        let size = i64::try_from(entry.size).map_err(|_| {
            IoError::other(format!("size {} exceeds i64", entry.size))
        })?;
        self.conn
            .execute(
                "INSERT OR REPLACE INTO downloads
                   (id, url, final_url, dest, status, size, sha256, started_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    entry.id,
                    entry.url,
                    entry.final_url,
                    entry.dest.to_string_lossy(),
                    entry.status.to_string(),
                    size,
                    entry.sha256,
                    entry.started_at,
                    entry.updated_at,
                ],
            )
            .map_err(|err| std::io::Error::other(format!("record history: {err}")))?;
        Ok(())
    }

    pub fn list(&self) -> std::io::Result<Vec<HistoryEntry>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, url, final_url, dest, status, size, sha256, started_at, updated_at
                      FROM downloads ORDER BY updated_at DESC",
            )
            .map_err(|err| std::io::Error::other(format!("prepare history list: {err}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok(HistoryEntry {
                    id: row.get(0)?,
                    url: row.get(1)?,
                    final_url: row.get(2)?,
                    dest: PathBuf::from(row.get::<_, String>(3)?),
                    status: parse_status(&row.get::<_, String>(4)?),
                    size: {
                        let raw = row.get::<_, i64>(5)?;
                        u64::try_from(raw)
                            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, raw))?
                    },
                    sha256: row.get(6)?,
                    started_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })
            .map_err(|err| std::io::Error::other(format!("query history list: {err}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| std::io::Error::other(format!("read history list: {err}")))
    }

    pub fn by_id_or_url(&self, needle: &str) -> std::io::Result<Option<HistoryEntry>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, url, final_url, dest, status, size, sha256, started_at, updated_at
                      FROM downloads WHERE id = ?1 OR url = ?1 ORDER BY updated_at DESC LIMIT 1",
            )
            .map_err(|err| std::io::Error::other(format!("prepare history lookup: {err}")))?;
        let mut rows = statement
            .query_map([needle], |row| {
                Ok(HistoryEntry {
                    id: row.get(0)?,
                    url: row.get(1)?,
                    final_url: row.get(2)?,
                    dest: PathBuf::from(row.get::<_, String>(3)?),
                    status: parse_status(&row.get::<_, String>(4)?),
                    size: {
                        let raw = row.get::<_, i64>(5)?;
                        u64::try_from(raw)
                            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, raw))?
                    },
                    sha256: row.get(6)?,
                    started_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })
            .map_err(|err| std::io::Error::other(format!("query history lookup: {err}")))?;
        rows.next()
            .transpose()
            .map_err(|err| std::io::Error::other(format!("read history lookup: {err}")))
    }
}

fn parse_status(value: &str) -> EntryStatus {
    match value {
        "completed" => EntryStatus::Completed,
        "cancelled" => EntryStatus::Cancelled,
        "failed" => EntryStatus::Failed,
        _ => EntryStatus::Downloading,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "shard-history-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("history.db")
    }

    static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn entry(id: &str) -> HistoryEntry {
        HistoryEntry {
            id: id.to_string(),
            url: format!("https://example.com/{id}.bin"),
            final_url: None,
            dest: PathBuf::from(format!("/tmp/shard-{id}.bin")),
            status: EntryStatus::Completed,
            size: 1024,
            sha256: None,
            started_at: "2026-07-04T10:00:00Z".to_string(),
            updated_at: "2026-07-04T10:00:00Z".to_string(),
        }
    }

    #[test]
    fn record_then_list_round_trips() {
        let history = History::open_at(&scratch()).unwrap();
        history.record(&entry("abc")).unwrap();
        history.record(&entry("def")).unwrap();
        let rows = history.list().unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn same_id_replaces_existing_row() {
        let history = History::open_at(&scratch()).unwrap();
        history.record(&entry("abc")).unwrap();
        let mut replaced = entry("abc");
        replaced.size = 2048;
        history.record(&replaced).unwrap();
        let rows = history.list().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].size, 2048);
    }

    #[test]
    fn lookups_match_by_id_and_url() {
        let history = History::open_at(&scratch()).unwrap();
        history.record(&entry("abc")).unwrap();
        assert_eq!(history.by_id_or_url("abc").unwrap().unwrap().id, "abc");
        let by_url = history
            .by_id_or_url("https://example.com/abc.bin")
            .unwrap()
            .unwrap();
        assert_eq!(by_url.id, "abc");
        assert!(history.by_id_or_url("nope").unwrap().is_none());
    }

    #[test]
    fn missing_file_creates_empty_history() {
        let history = History::open_at(&scratch()).unwrap();
        assert!(history.list().unwrap().is_empty());
    }
}
