pub mod dispatch;
pub mod error;
pub mod http;
pub mod manager;
pub mod metadata;
pub mod planner;
pub mod progress;
pub mod retry;
pub mod verify;
pub mod worker;
pub mod writer;

#[cfg(test)]
pub mod test_server;

#[cfg(test)]
mod tests_pool;

pub use error::{DownloadError, DownloadResult};
pub use manager::{DownloadManager, DownloadOptions, DownloadOutcome};
