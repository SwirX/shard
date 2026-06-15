use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct RemoteMetadata {
    pub final_url: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<SystemTime>,
    pub accepts_ranges: bool,
}
