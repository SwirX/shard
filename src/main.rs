use clap::{Parser, Subcommand};
use shard::engine::{DownloadManager, DownloadOptions};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "shard", version, about = "Native concurrent HTTP range download engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Download {
        url: String,
        #[arg(short, long, default_value_t = 8, help = "number of concurrent workers")]
        connections: usize,
        #[arg(short, long, help = "output path (defaults to current directory)")]
        output: Option<PathBuf>,
        #[arg(long, default_value_t = 8 * 1024 * 1024, help = "chunk size in bytes")]
        chunk_size: u64,
        #[arg(long, default_value_t = 5, help = "max attempts per chunk")]
        max_attempts: u32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Download {
            url,
            connections,
            output,
            chunk_size,
            max_attempts,
        } => {
            let dest_path = output.unwrap_or_else(|| derive_output_path(&url));
            let manager = DownloadManager::new()?;
            let outcome = manager
                .download(
                    &DownloadOptions {
                        url,
                        dest_path: dest_path.clone(),
                        chunk_size,
                        connections,
                        max_attempts,
                    },
                    None,
                )
                .await?;
            println!("saved {} bytes -> {}", outcome.size, dest_path.display());
            println!("sha256 {}", outcome.sha256);
        }
    }
    Ok(())
}

fn derive_output_path(url: &str) -> PathBuf {
    std::env::current_dir()
        .ok()
        .zip(url::Url::parse(url).ok())
        .and_then(|(cwd, parsed)| {
            parsed
                .path_segments()
                .and_then(|mut segments| segments.next_back())
                .filter(|name| !name.is_empty())
                .map(|name| cwd.join(name))
        })
        .unwrap_or_else(|| PathBuf::from("download.bin"))
}