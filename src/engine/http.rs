use crate::engine::error::DownloadResult;
use reqwest::Client as HttpClient;
use reqwest::redirect::Policy as RedirectPolicy;

pub struct EngineHttp;

impl EngineHttp {
    pub fn client() -> DownloadResult<HttpClient> {
        let client = HttpClient::builder()
            .redirect(RedirectPolicy::limited(10))
            .build()?;
        Ok(client)
    }
}
