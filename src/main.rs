use crate::cli::style::{ColorChoice, ProgressMode, Style};
use clap::{Parser, Subcommand, ValueEnum};
use shard::engine::{DownloadManager, DownloadOptions};
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

mod cli;
mod config;

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
        #[arg(short, long, help = "number of concurrent workers (default: config or 8)")]
        connections: Option<usize>,
        #[arg(short, long, help = "output path or directory (defaults to current directory)")]
        output: Option<PathBuf>,
        #[arg(long, help = "chunk size in bytes (default: config or 8 MiB)")]
        chunk_size: Option<u64>,
        #[arg(long, help = "max attempts per chunk (default: config or 5)")]
        max_attempts: Option<u32>,
        #[arg(long, value_enum, help = "progress style (overrides shard.conf)")]
        progress: Option<ProgressModeArg>,
        #[arg(long, value_enum, help = "colorize output (overrides shard.conf)")]
        color: Option<ColorArg>,
    },
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    Show,
    Init,
    Set {
        #[arg(value_name = "KEY")]
        key: String,
        #[arg(value_name = "VALUE")]
        value: String,
    },
    Edit,
    Keys,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProgressModeArg {
    Plain,
    Nerd,
}

impl From<ProgressModeArg> for ProgressMode {
    fn from(value: ProgressModeArg) -> Self {
        match value {
            ProgressModeArg::Plain => ProgressMode::Plain,
            ProgressModeArg::Nerd => ProgressMode::Nerd,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ColorArg {
    Auto,
    Always,
    Never,
}

impl From<ColorArg> for ColorChoice {
    fn from(value: ColorArg) -> Self {
        match value {
            ColorArg::Auto => ColorChoice::Auto,
            ColorArg::Always => ColorChoice::Always,
            ColorArg::Never => ColorChoice::Never,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Config { action } => match action {
            ConfigAction::Show => show_config()?,
            ConfigAction::Init => config::Config::init()?,
            ConfigAction::Set { key, value } => config::Config::set(&key, &value)?,
            ConfigAction::Edit => config::Config::edit()?,
            ConfigAction::Keys => {
                for key in config::Config::keys() {
                    println!("{key}");
                }
            }
        },
        Command::Download {
            url,
            connections,
            output,
            chunk_size,
            max_attempts,
            progress,
            color,
        } => {
            let dest_dir = output.unwrap_or_else(|| {
                std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
            });
            let style = resolve_style(progress.map(Into::into), color.map(Into::into))?;
            let conf = config::Config::load()?;
            let opts = DownloadOptions {
                url,
                dest_path: dest_dir,
                chunk_size: chunk_size.unwrap_or(conf.download.chunk_size),
                connections: connections.unwrap_or(conf.download.connections),
                max_attempts: max_attempts.unwrap_or(conf.download.max_attempts),
                retry_base_delay: Duration::from_millis(conf.download.retry_base_ms),
                retry_max_delay: Duration::from_millis(conf.download.retry_max_ms),
                checkpoint_interval: Duration::from_millis(conf.download.checkpoint_ms),
                resume: conf.download.resume,
            };
            let manager = DownloadManager::new()?;
            let (progress_tx, progress_rx) = mpsc::channel(1024);
            let (key_tx, key_rx) = mpsc::channel(64);
            if let Some(keys) = crate::cli::keys::spawn_key_listener(key_tx.clone()) {
                let renderer = tokio::spawn(crate::cli::render::run_progress_renderer(
                    progress_rx,
                    key_rx,
                    style,
                    opts.connections,
                ));
                let result = manager.download(&opts, Some(progress_tx)).await;
                drop(key_tx);
                let _ = renderer.await;
                let _ = keys.await;
                let outcome = result?;
                println!();
                println!(
                    "saved {} bytes -> {}",
                    outcome.size,
                    outcome.dest_path.display()
                );
                println!("sha256 {}", outcome.sha256);
                if interactive() {
                    prompt_enter()?;
                }
            } else {
                let result = manager.download(&opts, Some(progress_tx)).await;
                let outcome = result?;
                println!(
                    "saved {} bytes -> {}",
                    outcome.size,
                    outcome.dest_path.display()
                );
                println!("sha256 {}", outcome.sha256);
            }
        }
    }
    Ok(())
}

fn interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn prompt_enter() -> std::io::Result<()> {
    use std::io::BufRead;
    print!("Press Enter to continue...");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    Ok(())
}

fn resolve_style(
    progress_flag: Option<ProgressMode>,
    color_flag: Option<ColorChoice>,
) -> anyhow::Result<Style> {
    let is_tty = std::io::stdout().is_terminal();
    let conf = config::Config::load()?;
    let mode = progress_flag
        .or(Some(conf.style.progress))
        .unwrap_or(ProgressMode::Plain);
    let color_on = color_flag
        .or(Some(conf.style.color))
        .map(|choice| choice.resolves_to(is_tty))
        .unwrap_or(is_tty);
    Ok(Style::with_color(color_on, mode))
}

fn show_config() -> anyhow::Result<()> {
    let path = config::config_path();
    let exists = path.exists();
    println!("config file: {}", path.display());
    if !exists {
        println!("  (not present - using defaults)");
    }
    let conf = config::Config::load()?;
    let is_tty = std::io::stdout().is_terminal();
    let color_on = conf
        .style
        .color
        .resolves_to(is_tty);
    println!("color: {} ({})", conf.style.color, if color_on { "on" } else { "off" });
    println!("progress style: {}", conf.style.progress);
    let d = &conf.download;
    println!("download:");
    println!("  connections = {}", d.connections);
    println!("  chunk_size = {} ({:.1} MiB)", d.chunk_size, d.chunk_size as f64 / 1_048_576.0);
    println!("  max_attempts = {}", d.max_attempts);
    println!("  retry_base_ms = {}", d.retry_base_ms);
    println!("  retry_max_ms = {}", d.retry_max_ms);
    println!("  checkpoint_ms = {}", d.checkpoint_ms);
    println!("  resume = {}", d.resume);
    println!("use `shard config set <key> <value>` to change one setting");
    Ok(())
}