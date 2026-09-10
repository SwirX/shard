use clap::{Parser, Subcommand};
use shard_client::Client;
use shard_native_host::{Browser, read_frame, write_frame};
use shard_rpc::{Request, ServerEvent};
use std::io;
use std::sync::mpsc;

#[derive(Parser)]
#[command(
    name = "shard-native-host",
    version,
    about = "Script shard's native-messaging bridge or run it"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Write browser native-messaging manifests pointing at this binary.
    Install {
        #[arg(
            long,
            help = "extension id to allow (repeatable; default one placeholder)"
        )]
        extension: Vec<String>,
        #[arg(long, value_enum, help = "only write the manifest for this browser")]
        browser: Option<Browser>,
    },
}

/// Relay requests between the browser's native-messaging pipe and the daemon,
/// or act on the `install` subcommand when given.
fn main() -> io::Result<()> {
    let cli = Cli::parse();
    if let Some(Command::Install { extension, browser }) = cli.command {
        return install(&extension, browser);
    }
    run_bridge()
}

fn install(extensions: &[String], browser: Option<Browser>) -> io::Result<()> {
    let host_path = std::env::current_exe()?;
    let browsers = match browser {
        Some(browser) => vec![browser],
        None => vec![Browser::Firefox, Browser::Chrome, Browser::Chromium],
    };
    for browser in browsers {
        let file = shard_native_host::install(browser, &host_path, extensions)?;
        println!("{} -> {}", browser_short(browser), file.display());
    }
    Ok(())
}

fn browser_short(browser: Browser) -> &'static str {
    match browser {
        Browser::Firefox => "firefox",
        Browser::Chrome => "google-chrome",
        Browser::Chromium => "chromium",
    }
}

/// The streaming bridge itself. Standard input is read on the main thread;
/// replies and watch snapshots go to a dedicated stdout thread via a channel
/// so the async runtime never borrows the (non-Send) terminal streams.
fn run_bridge() -> io::Result<()> {
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
        let mut client = match Client::connect_auto().await {
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
        let mut client = match Client::connect_auto().await {
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
