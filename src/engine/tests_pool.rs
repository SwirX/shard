use crate::engine::error::DownloadError;
use crate::engine::manager::{DownloadManager, DownloadOptions};
use crate::engine::progress::ProgressEvent;
use crate::engine::test_server::handler::{DelayProfile, ServerFeatures};
use crate::engine::test_server::{deterministic_body, expected_sha256, TestServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

fn scratch_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("shard-{name}"))
}

async fn collect_progress(body: Vec<u8>, features: ServerFeatures) -> (Vec<usize>, Vec<u8>, u64) {
    let server = TestServer::spawn_with(body.clone(), features).await;
    let dest = scratch_path("scrambled.bin");
    let (tx, mut rx) = mpsc::channel::<ProgressEvent>(128);
    let collector = tokio::spawn(async move {
        let mut completions = Vec::new();
        while let Some(event) = rx.recv().await {
            if let ProgressEvent::ChunkComplete { index } = event {
                completions.push(index);
            }
        }
        completions
    });
    let manager = DownloadManager::new().unwrap();
    let outcome = manager
        .download(
            &DownloadOptions {
                url: server.url(),
                dest_path: dest.clone(),
                chunk_size: 64 * 1024,
                connections: 8,
                max_attempts: 4,
            },
            Some(tx),
        )
        .await
        .unwrap();
    let completions = collector.await.unwrap();
    let on_disk = std::fs::read(&dest).unwrap();
    let _ = std::fs::remove_file(&dest);
    (completions, on_disk, outcome.size)
}

#[tokio::test]
async fn scrambled_completion_stays_byte_perfect() {
    let body = deterministic_body(2 * 1024 * 1024, 424242);
    let features = ServerFeatures {
        delay: DelayProfile::Reverse { max_delay_ms: 300 },
        fragment_bytes: Some(512),
        ..Default::default()
    };

    let (completions, on_disk, size) = collect_progress(body.clone(), features).await;

    assert_eq!(on_disk, body);
    assert_eq!(size, body.len() as u64);
    assert_eq!(completions.len(), 32);
    assert!(
        completions.windows(2).any(|pair| pair[0] > pair[1]),
        "completion order must differ from chunk order"
    );
    let sorted: Vec<usize> = (0..32).collect();
    assert_ne!(completions, sorted);
}

#[tokio::test]
async fn failed_chunk_is_requeued_and_recovers() {
    let body = deterministic_body(256 * 1024, 99);
    let mut drop_once = HashMap::new();
    drop_once.insert((0u64, 65535u64), 1);
    let features = ServerFeatures {
        drop_once: Arc::new(std::sync::Mutex::new(drop_once)),
        ..Default::default()
    };
    let drop_tracker = Arc::clone(&features.drop_once);

    let (completions, on_disk, _) = collect_progress(body.clone(), features).await;

    assert_eq!(on_disk, body);
    assert_eq!(completions.len(), 4);
    let remaining = drop_tracker.lock().unwrap().get(&(0, 65535)).copied();
    assert_eq!(remaining, Some(0), "drop counter must be consumed exactly once");
}

#[tokio::test]
async fn attempt_exhaustion_fails_deterministically() {
    let body = deterministic_body(256 * 1024, 7);
    let drop_forever = Arc::new([(0u64, 65535u64)].into_iter().collect());
    let features = ServerFeatures {
        drop_forever,
        ..Default::default()
    };
    let server = TestServer::spawn_with(body.clone(), features).await;
    let dest = scratch_path("exhaustion.bin");
    let manager = DownloadManager::new().unwrap();
    let result = manager
        .download(
            &DownloadOptions {
                url: server.url(),
                dest_path: dest.clone(),
                chunk_size: 64 * 1024,
                connections: 2,
                max_attempts: 2,
            },
            None,
        )
        .await;

    let _ = std::fs::remove_file(&dest);
    match result {
        Err(DownloadError::ChunksFailed(count)) => assert_eq!(count, 1),
        Err(err) => panic!("expected ChunksFailed, got {err:?}"),
        Ok(_) => panic!("expected download to fail"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run manually: cargo test --release -- --ignored --nocapture throughput_benchmark"]
async fn throughput_benchmark() {
    let body = deterministic_body(64 * 1024 * 1024, 777);
    let server = TestServer::spawn(body).await;
    let expected = expected_sha256(&server.body);
    let manager = DownloadManager::new().unwrap();

    let mut rows = Vec::new();
    for connections in [1usize, 2, 4, 8, 16] {
        let dest = scratch_path(&format!("bench-{connections}.bin"));
        let start = Instant::now();
        let outcome = manager
            .download(
                &DownloadOptions {
                    url: server.url(),
                    dest_path: dest.clone(),
                    chunk_size: 1024 * 1024,
                    connections,
                    max_attempts: 3,
                },
                None,
            )
            .await
            .unwrap();
        let elapsed = start.elapsed().as_secs_f64();
        assert_eq!(outcome.sha256, expected);
        let throughput = server.body.len() as f64 / 1_000_000.0 / elapsed;
        rows.push(format!("{connections:>2} workers  {elapsed:>6.2}s  {throughput:>8.2} MB/s"));
        let _ = std::fs::remove_file(&dest);
    }
    println!("\nshard throughput against local test server (64 MiB):");
    for row in rows {
        println!("{row}");
    }
}