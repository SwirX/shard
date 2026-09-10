use shard_client::Client;
use shard_native_host::{read_frame, write_frame};
use shard_rpc::{Request, ServerEvent};
use std::io;
use std::sync::mpsc;

/// Relay requests between the browser's native-messaging pipe and the daemon.
///
/// Standard input is read on the main thread; replies and watch snapshots are
/// handed to a dedicated stdout thread via a channel so the async runtime
/// never borrows the (non-Send) terminal streams.
fn main() -> io::Result<()> {
    let mut stdin = io::stdin().lock();

    let (frames_tx, frames_rx) = mpsc::channel::<Vec<u8>>();
    let writer = std::thread::spawn(move || -> io::Result<()> {
        let stdout = io::stdout();
        let mut out = stdout.lock();
        while let Ok(frame) = frames_rx.recv() {
            write_frame(&mut out, &frame)?;
        }
        Ok(())
    });

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    while let Some(frame) = read_frame(&mut stdin)? {
        let request: Option<Request> = serde_json::from_slice(&frame).ok();
        let Some(request) = request else {
            continue;
        };
        if request == Request::Watch {
            relay_watch(&runtime, &frames_tx)?;
        } else {
            relay_request(&runtime, &request, &frames_tx)?;
        }
    }
    drop(frames_tx);
    match writer.join() {
        Ok(result) => result?,
        Err(panic) => {
            return Err(io::Error::other(format!(
                "writer thread crashed: {panic:?}"
            )));
        }
    }
    Ok(())
}

/// One request/response hop. An unreachable daemon becomes a structured
/// error so the extension can surface it.
fn relay_request(
    runtime: &tokio::runtime::Runtime,
    request: &Request,
    frames_tx: &mpsc::Sender<Vec<u8>>,
) -> io::Result<()> {
    let response = runtime.block_on(async {
        let mut client = match Client::connect().await {
            Ok(client) => client,
            Err(err) => {
                return shard_rpc::Response::Error {
                    code: "daemon_unreachable".into(),
                    message: err.to_string(),
                };
            }
        };
        match client.request(request).await {
            Ok(response) => response,
            Err(err) => shard_rpc::Response::Error {
                code: "daemon_error".into(),
                message: err.to_string(),
            },
        }
    });
    let frame = serde_json::to_vec(&response).expect("protocol responses serialize");
    frames_tx.send(frame).map_err(io::Error::other)
}

/// Streaming watch: write snapshots until the daemon closes the socket.
/// An empty snapshot tells the extension the daemon is not answering.
fn relay_watch(
    runtime: &tokio::runtime::Runtime,
    frames_tx: &mpsc::Sender<Vec<u8>>,
) -> io::Result<()> {
    runtime.block_on(async {
        let mut client = match Client::connect().await {
            Ok(client) => client,
            Err(_) => {
                let event = ServerEvent::Snapshot {
                    downloads: Vec::new(),
                };
                let frame = serde_json::to_vec(&event).expect("events serialize");
                frames_tx.send(frame).map_err(io::Error::other)?;
                return Ok(());
            }
        };
        if client.watch().await.is_err() {
            return Ok(());
        }
        while let Ok(event) = client.next_event().await {
            let frame = serde_json::to_vec(&event).expect("events serialize");
            frames_tx.send(frame).map_err(io::Error::other)?;
        }
        Ok(())
    })
}
