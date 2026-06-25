use thiserror::Error;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("resume refused: {0}")]
    ResumeRefused(String),
    #[error("remote does not support byte ranges")]
    RangeUnsupported,
    #[error("{0} chunks failed after exhausting retry attempts")]
    ChunksFailed(usize),
    #[error("download cancelled by user")]
    Canceled,
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type DownloadResult<T> = Result<T, DownloadError>;
