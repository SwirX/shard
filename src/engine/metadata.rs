use crate::engine::error::{DownloadError, DownloadResult};
use crate::engine::http::EngineHttp;
use http::HeaderValue;
use reqwest::Client as HttpClient;

#[derive(Debug, Clone)]
pub struct RemoteMetadata {
    pub final_url: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub accepts_ranges: bool,
    pub content_disposition: Option<String>,
}

pub struct RemoteResolver {
    client: HttpClient,
}

impl RemoteResolver {
    pub fn new() -> DownloadResult<Self> {
        let client = EngineHttp::client()?;
        Ok(Self { client })
    }

    pub fn with_client(client: HttpClient) -> Self {
        Self { client }
    }

    pub async fn resolve(&self, url: &str) -> DownloadResult<RemoteMetadata> {
        let final_url = self.resolve_redirects(url).await?;
        let size = self.probe_size(&final_url).await?;
        let probe = self.probe_range(&final_url).await?;
        Ok(RemoteMetadata {
            size,
            etag: probe.etag,
            last_modified: probe.last_modified,
            accepts_ranges: probe.accepts_ranges,
            final_url,
            content_disposition: probe.content_disposition,
        })
    }

    async fn resolve_redirects(&self, url: &str) -> DownloadResult<String> {
        let response = self.client.head(url).send().await.map_err(|err| {
            if err.is_redirect() {
                DownloadError::InvalidArgument("too many redirects".into())
            } else {
                DownloadError::Http(err)
            }
        })?;
        let final_url = response.url().clone().to_string();
        Ok(final_url)
    }

    async fn probe_size(&self, url: &str) -> DownloadResult<u64> {
        let response = self.client.head(url).send().await?;
        let length = response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| DownloadError::InvalidArgument("missing content length".into()))?;
        Ok(length)
    }

    async fn probe_range(&self, url: &str) -> DownloadResult<RangeProbe> {
        let response = self
            .client
            .get(url)
            .header(http::header::RANGE, "bytes=0-0")
            .send()
            .await?;
        let status = response.status();
        let etag = header_string(response.headers(), http::header::ETAG);
        let last_modified = header_string(response.headers(), http::header::LAST_MODIFIED);
        let content_disposition =
            header_string(response.headers(), http::header::CONTENT_DISPOSITION);
        let mut accepts_ranges = false;
        if status == http::StatusCode::PARTIAL_CONTENT {
            accepts_ranges = response
                .headers()
                .get(http::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.starts_with("bytes"))
                .unwrap_or(false);
        }
        Ok(RangeProbe {
            accepts_ranges,
            etag,
            last_modified,
            content_disposition,
        })
    }
}

#[derive(Debug)]
struct RangeProbe {
    accepts_ranges: bool,
    etag: Option<String>,
    last_modified: Option<String>,
    content_disposition: Option<String>,
}

fn header_string(
    headers: &http::HeaderMap<HeaderValue>,
    name: http::header::HeaderName,
) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}
