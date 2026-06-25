use crate::cli::style::{ColorChoice, ProgressMode, Style};
use clap::{Parser, Subcommand, ValueEnum};
use shard::engine::{DownloadManager, DownloadOptions};
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
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
        #[arg(short, long, default_value_t = 8, help = "number of concurrent workers")]
        connections: usize,
        #[arg(short, long, help = "output path or directory (defaults to current directory)")]
        output: Option<PathBuf>,
        #[arg(long, default_value_t = 8 * 1024 * 1024, help = "chunk size in bytes")]
        chunk_size: u64,
        #[arg(long, default_value_t = 5, help = "max attempts per chunk")]
        max_attempts: u32,
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
        Command::Config { action } => {
            let conf = config::Config::load();
            match action {
                ConfigAction::Show => show_config(&conf),
                ConfigAction::Init => config::Config::init()?,
            }
        }
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
            let style = resolve_style(progress.map(Into::into), color.map(Into::into));
            let manager = DownloadManager::new()?;
            let (progress_tx, progress_rx) = mpsc::channel(1024);
            let (key_tx, key_rx) = mpsc::channel(64);
            if let Some(keys) = crate::cli::keys::spawn_key_listener(key_tx.clone()) {
                let renderer = tokio::spawn(crate::cli::render::run_progress_renderer(
                    progress_rx,
                    key_rx,
                    style,
                    connections,
                ));
                let result = manager
                    .download(
                        &DownloadOptions {
                            url,
                            dest_path: dest_dir,
                            chunk_size,
                            connections,
                            max_attempts,
                        },
                        Some(progress_tx),
                    )
                    .await;
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
                let result = manager
                    .download(
                        &DownloadOptions {
                            url,
                            dest_path: dest_dir,
                            chunk_size,
                            connections,
                            max_attempts,
                        },
                        Some(progress_tx),
                    )
                    .await;
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
) -> Style {
    let is_tty = std::io::stdout().is_terminal();
    let conf = config::Config::load();
    let mode = progress_flag.or(conf.progress).unwrap_or(ProgressMode::Plain);
    let color_on = color_flag
        .or(conf.color)
        .map(|choice| choice.resolves_to(is_tty))
        .unwrap_or(is_tty);
    Style::with_color(color_on, mode)
}

fn show_config(conf: &config::Config) {
    let path = conf.path();
    let exists = path.exists();
    println!("config file: {}", path.display());
    if !exists {
        println!("  (not present - using defaults)");
    }
    let conf = config::Config::load();
    let is_tty = std::io::stdout().is_terminal();
    let color_on = conf
        .color
        .map(|choice| choice.resolves_to(is_tty))
        .unwrap_or(is_tty);
    let mode = conf.progress.unwrap_or(ProgressMode::Plain);
    println!("color: {}", if color_on { "on" } else { "off" });
    println!("progress style: {mode}");
}