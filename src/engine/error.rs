use thiserror::Error;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("remote does not support byte ranges")]
    RangeUnsupported,
    #[error("{0} chunks failed after exhausting retry attempts")]
    ChunksFailed(usize),
}

pub type DownloadResult<T> = Result<T, DownloadError>;
