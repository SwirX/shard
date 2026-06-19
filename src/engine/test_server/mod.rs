pub mod handler;

use handler::{serve, ServerFeatures};
use tokio::net::TcpListener;

pub struct TestServer {
    pub addr: std::net::SocketAddr,
    pub body: Vec<u8>,
    pub etag: String,
}

impl TestServer {
    pub async fn spawn(body: Vec<u8>) -> Self {
        Self::spawn_with(body, ServerFeatures::default()).await
    }

    pub async fn spawn_with(body: Vec<u8>, features: ServerFeatures) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let etag = format!("\"fake-{}-{}\"", body.len(), body.len() + 1);
        let body_clone = body.clone();
        let etag_clone = etag.clone();
        tokio::spawn(serve(listener, body_clone, etag_clone, features));
        Self { addr, body, etag }
    }

    pub fn url(&self) -> String {
        format!("http://{}/file.bin", self.addr)
    }
}

pub fn deterministic_body(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xff) as u8
        })
        .collect()
}

pub fn expected_sha256(body: &[u8]) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(body);
    let output = hasher.finalize();
    output.iter().map(|b| format!("{:02x}", b)).collect()
}