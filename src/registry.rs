use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryStatus {
    Downloading,
    Completed,
    Cancelled,
    Failed,
}

impl std::fmt::Display for EntryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            EntryStatus::Downloading => "downloading",
            EntryStatus::Completed => "completed",
            EntryStatus::Cancelled => "cancelled",
            EntryStatus::Failed => "failed",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub id: String,
    pub url: String,
    pub dest: PathBuf,
    pub final_dest: Option<PathBuf>,
    pub status: EntryStatus,
    pub pid: u32,
    pub started_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Registry {
    pub entries: Vec<Entry>,
}

impl Registry {
    fn entries_dir() -> PathBuf {
        data_dir().join("entries")
    }

    fn entry_path(id: &str) -> PathBuf {
        Self::entries_dir().join(format!("{id}.json"))
    }

    fn write_atomic(path: &PathBuf, entry: &Entry) -> std::io::Result<()> {
        std::fs::create_dir_all(path.parent().expect("entry has a parent"))?;
        let body = serde_json::to_vec_pretty(entry).expect("entry serializes");
        let tmp = path.with_extension("tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&body)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    pub fn load() -> Registry {
        let mut entries: Vec<Entry> = Vec::new();
        let Ok(dir) = std::fs::read_dir(Self::entries_dir()) else {
            return Registry::default();
        };
        let mut paths: Vec<PathBuf> = dir
            .flatten()
            .filter(|item| item.path().extension().is_some_and(|ext| ext == "json"))
            .map(|item| item.path())
            .collect();
        paths.sort();
        for path in paths {
            let Ok(body) = std::fs::read_to_string(&path) else {
                continue;
            };
            if let Ok(entry) = serde_json::from_str::<Entry>(&body) {
                entries.push(entry);
            }
        }
        Registry { entries }
    }

    pub fn add(&self, entry: &Entry) -> std::io::Result<()> {
        Self::write_atomic(&Self::entry_path(&entry.id), entry)
    }

    pub fn update(&self, id: &str, f: impl FnOnce(&mut Entry)) -> std::io::Result<()> {
        let path = Self::entry_path(id);
        let mut entry = serde_json::from_str::<Entry>(&std::fs::read_to_string(&path)?)
            .map_err(std::io::Error::other)?;
        f(&mut entry);
        entry.updated_at = iso_now();
        Self::write_atomic(&path, &entry)
    }

    pub fn by_id_or_url(&self, needle: &str) -> Option<&Entry> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.id == needle || entry.url == needle)
    }

    pub fn active(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter().filter(|entry| entry.status == EntryStatus::Downloading)
    }
}

pub fn data_dir() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local").join("share"))
        })
        .unwrap_or_default();
    base.join("shard")
}

pub fn socket_path(id: &str) -> PathBuf {
    data_dir().join("run").join(format!("{id}.sock"))
}

pub fn new_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{millis:x}-{}", std::process::id())
}

pub fn iso_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_now_is_rfc3339_utc() {
        let stamp = iso_now();
        assert_eq!(stamp.len(), 20);
        assert!(stamp.ends_with('Z'));
        assert_eq!(&stamp[10..11], "T");
    }

    #[test]
    fn civil_dates_round_trip_known_epochs() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_641), (2026, 7, 7));
    }

    #[test]
    fn lookup_matches_by_id_and_url_and_ignores_stale_entries() {
        let mut registry = Registry::default();
        registry.entries.push(Entry {
            id: "old".into(),
            url: "https://example.com/f.bin".into(),
            dest: PathBuf::from("/tmp/f.bin"),
            final_dest: None,
            status: EntryStatus::Cancelled,
            pid: 1234,
            started_at: iso_now(),
            updated_at: iso_now(),
        });
        registry.entries.push(Entry {
            id: "new".into(),
            url: "https://example.com/g.bin".into(),
            dest: PathBuf::from("/tmp/g.bin"),
            final_dest: None,
            status: EntryStatus::Downloading,
            pid: 1235,
            started_at: iso_now(),
            updated_at: iso_now(),
        });
        assert_eq!(registry.by_id_or_url("new").unwrap().status, EntryStatus::Downloading);
        assert!(registry.by_id_or_url("https://example.com/f.bin").is_some());
        assert_eq!(registry.active().count(), 1);
    }

    #[test]
    fn entry_files_round_trip_through_the_entries_dir() {
        // unsafety: single-threaded test; mutating XDG_DATA_HOME is process-local
        unsafe { std::env::set_var("XDG_DATA_HOME", "/tmp/opencode/shard-registry-test") };
        let _ = std::fs::remove_dir_all(Registry::entries_dir());
        let registry = Registry::default();
        let entry = Entry {
            id: "1a2b3c".into(),
            url: "https://example.com/h.bin".into(),
            dest: PathBuf::from("/tmp/h.bin"),
            final_dest: None,
            status: EntryStatus::Downloading,
            pid: 42,
            started_at: iso_now(),
            updated_at: iso_now(),
        };
        registry.add(&entry).expect("add succeeds");
        registry
            .update("1a2b3c", |e| e.status = EntryStatus::Completed)
            .expect("update succeeds");
        let reloaded = Registry::load();
        let found = reloaded.by_id_or_url(&entry.id).unwrap();
        assert_eq!(found.status, EntryStatus::Completed);
    }
}