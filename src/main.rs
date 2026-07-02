use crate::cli::style::{ColorChoice, Style};
use clap::{Parser, Subcommand, ValueEnum};
use shard::engine::{DownloadManager, DownloadOptions};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

mod cli;
mod config;
mod history;
mod registry;

#[derive(Parser)]
#[command(name = "shard", version, about = "Native concurrent HTTP range download engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Download {
        #[arg(help = "URL; when omitted, read from clipboard (wl-paste/xclip/xsel)")]
        url: Option<String>,
        #[arg(short, long, help = "number of concurrent workers (default: config or 8)")]
        connections: Option<usize>,
        #[arg(short, long, help = "output path or directory (defaults to download dir)")]
        output: Option<PathBuf>,
        #[arg(long, help = "chunk size in bytes (default: config or 8 MiB)")]
        chunk_size: Option<u64>,
        #[arg(long, help = "max attempts per chunk (default: config or 5)")]
        max_attempts: Option<u32>,
        #[arg(
            long,
            num_args = 0..=1,
            default_missing_value = "true",
            help = "prefer Nerd Font eyecandy over TTY-safe hash glyphs (overrides shard.conf)"
        )]
        eyecandy: Option<bool>,
        #[arg(long, value_enum, help = "colorize output (overrides shard.conf)")]
        color: Option<ColorArg>,
    },
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    List,
    Status {
        id: Option<String>,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Cancel {
        id: String,
    },
    History,
    Redo {
        #[arg(value_name = "ID|URL")]
        id_or_url: String,
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
            eyecandy,
            color,
        } => {
            let url = match url {
                Some(url) => url,
                None => clipboard_url()?,
            };
            let style = resolve_style(eyecandy, color.map(Into::into))?;
            let conf = config::Config::load()?;
            let (dest_path, routing) = if let Some(output) = output {
                (output, None)
            } else {
                let base = expand_tilde(&conf.download.download_dir);
                (
                    base.clone(),
                    Some(shard::engine::FiletypeRouting {
                        dirs: [
                            ("video", conf.filetype.video.as_str()),
                            ("image", conf.filetype.image.as_str()),
                            ("audio", conf.filetype.audio.as_str()),
                            ("archive", conf.filetype.archive.as_str()),
                            ("document", conf.filetype.document.as_str()),
                            ("other", conf.filetype.other.as_str()),
                        ]
                        .into_iter()
                        .map(|(category, leaf)| (category.to_string(), leaf.to_string()))
                        .collect(),
                        other: conf.filetype.other.clone(),
                    }),
                )
            };
            let opts = DownloadOptions {
                url,
                dest_path,
                chunk_size: chunk_size.unwrap_or(conf.download.chunk_size),
                connections: connections.unwrap_or(conf.download.connections),
                max_attempts: max_attempts.unwrap_or(conf.download.max_attempts),
                retry_base_delay: Duration::from_millis(conf.download.retry_base_ms),
                retry_max_delay: Duration::from_millis(conf.download.retry_max_ms),
                checkpoint_interval: Duration::from_millis(conf.download.checkpoint_ms),
                resume: conf.download.resume,
                routing,
            };
            run_download_cli(opts, style, None).await?;
        }
        Command::List => list_cli()?,
        Command::Status { id } => status_cli(id.as_deref())?,
        Command::Pause { id } => {
            send_control(&id, crate::cli::control::ControlCommand::Pause)?;
            println!("paused {id}");
        }
        Command::Cancel { id } => {
            send_control(&id, crate::cli::control::ControlCommand::Cancel)?;
            println!("cancelled {id}");
        }
        Command::Resume { id } => resume_cli(&id).await?,
        Command::History => history_cli()?,
        Command::Redo { id_or_url } => redo_cli(&id_or_url).await?,
    }
    Ok(())
}

async fn resume_cli(id: &str) -> anyhow::Result<()> {
    let store = registry::Registry::load();
    let entry = store
        .by_id_or_url(id)
        .ok_or_else(|| anyhow::anyhow!("no download matches {id:?}"))?;
    let socket = registry::socket_path(&entry.id);
    if let Some(reply) = crate::cli::control::probe(&socket) {
        if reply == "cancelled" {
            println!("{} is already cancelled; use `shard resume` later to relaunch", entry.id);
            return Ok(());
        }
        crate::cli::control::send(&socket, crate::cli::control::ControlCommand::Resume)?;
        println!("resumed {} (live process says: {reply})", entry.id);
        return Ok(());
    }
    let conf = config::Config::load()?;
    let dest = entry
        .final_dest
        .clone()
        .unwrap_or_else(|| entry.dest.clone());
    let opts = DownloadOptions {
        url: entry.url.clone(),
        dest_path: dest,
        chunk_size: conf.download.chunk_size,
        connections: conf.download.connections,
        max_attempts: conf.download.max_attempts,
        retry_base_delay: Duration::from_millis(conf.download.retry_base_ms),
        retry_max_delay: Duration::from_millis(conf.download.retry_max_ms),
        checkpoint_interval: Duration::from_millis(conf.download.checkpoint_ms),
        resume: true,
        routing: None,
    };
    println!("relaunching {} (partial data resumes via sidecar)", entry.id);
    run_download_cli(opts, Style::new(false, false), Some(entry.id.clone())).await
}

fn send_control(id: &str, command: crate::cli::control::ControlCommand) -> anyhow::Result<()> {
    let store = registry::Registry::load();
    let entry = store
        .by_id_or_url(id)
        .ok_or_else(|| anyhow::anyhow!("no download matches {id:?}"))?;
    let socket = registry::socket_path(&entry.id);
    crate::cli::control::send(&socket, command)?;
    Ok(())
}

fn list_cli() -> anyhow::Result<()> {
    let store = registry::Registry::load();
    if store.entries.is_empty() {
        println!("no downloads registered yet");
        return Ok(());
    }
    println!("{:<22} {:<11} {:<7} DEST", "ID", "STATUS", "LIVE");
    for entry in store.entries.iter().rev() {
        let live = match crate::cli::control::probe(&registry::socket_path(&entry.id)) {
            Some(reply) => match reply.as_str() {
                "cancelled" | "ok" => "off".to_string(),
                other => other.to_string(),
            },
            None => "-".to_string(),
        };
        let dest = entry.final_dest.as_ref().unwrap_or(&entry.dest);
        println!(
            "{:<22} {:<11} {:<7} {}",
            entry.id,
            entry.status,
            live,
            dest.display()
        );
    }
    Ok(())
}

fn status_cli(id: Option<&str>) -> anyhow::Result<()> {
    let store = registry::Registry::load();
    let needle = match id {
        Some(id) => id.to_string(),
        None => {
            let active = store.active().last().map(|entry| entry.id.clone());
            match active {
                Some(id) => id,
                None => {
                    println!("no active downloads; use `shard list` to see history");
                    return Ok(());
                }
            }
        }
    };
    let entry = store
        .by_id_or_url(&needle)
        .ok_or_else(|| anyhow::anyhow!("no download matches {needle:?}"))?;
    let socket = registry::socket_path(&entry.id);
    let live = match crate::cli::control::probe(&socket) {
        Some(reply) => format!("running ({reply})"),
        None => "not running".to_string(),
    };
    println!("id:       {}", entry.id);
    println!("url:      {}", entry.url);
    println!("dest:     {}", entry.final_dest.as_ref().unwrap_or(&entry.dest).display());
    println!("status:   {} ({live})", entry.status);
    println!("pid:      {}", entry.pid);
    println!("started:  {}", entry.started_at);
    println!("updated:  {}", entry.updated_at);
    if entry.status != registry::EntryStatus::Completed {
        println!("hint:     `shard resume {}` to continue where it left off", entry.id);
    }
    Ok(())
}

async fn run_download_cli(
    opts: DownloadOptions,
    style: Style,
    reuse_id: Option<String>,
) -> anyhow::Result<()> {
    let id = match reuse_id {
        Some(id) => id,
        None => registry::new_id(),
    };
    let socket = registry::socket_path(&id);
    let store = registry::Registry::load();
    let entry = registry::Entry {
        id: id.clone(),
        url: opts.url.clone(),
        dest: opts.dest_path.clone(),
        final_dest: None,
        status: registry::EntryStatus::Downloading,
        pid: std::process::id(),
        started_at: registry::iso_now(),
        updated_at: registry::iso_now(),
    };
    store.add(&entry)?;
    let history_store = history::History::open()?;
    history_store.record(&history::HistoryEntry {
        id: id.clone(),
        url: opts.url.clone(),
        final_url: None,
        dest: opts.dest_path.clone(),
        status: registry::EntryStatus::Downloading,
        size: 0,
        sha256: None,
        started_at: registry::iso_now(),
        updated_at: registry::iso_now(),
    })?;
    println!("download {id} -> {}", opts.dest_path.display());
    println!("  control via: shard status|pause|resume|cancel {id}");
    let manager = DownloadManager::new()?;
    let (progress_tx, progress_rx) = mpsc::channel(1024);
    let (key_tx, key_rx) = mpsc::channel(64);
    let mut handle = manager.start(&opts, Some(progress_tx))?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let serve_socket = socket.clone();
    let serve_controller = handle.controller.clone();
    let socket_task = tokio::spawn(async move {
        let _ = shard::engine::sockets::serve(&serve_socket, serve_controller, shutdown_rx).await;
    });
    let renderer = if let Some(keys) = crate::cli::keys::spawn_key_listener(key_tx.clone()) {
        let renderer = tokio::spawn(crate::cli::render::run_progress_renderer(
            progress_rx,
            key_rx,
            style,
            opts.connections,
        ));
        Some((renderer, keys))
    } else {
        None
    };

    let outcome = match handle.wait().await {
        Ok(outcome) => outcome,
        Err(err) => {
            drop(key_tx);
            let _ = store.update(&id, |entry| entry.status = registry::EntryStatus::Failed);
            let _ = history_store.record(&history::HistoryEntry {
                id: id.clone(),
                url: opts.url.clone(),
                final_url: None,
                dest: opts.dest_path.clone(),
                status: registry::EntryStatus::Failed,
                size: 0,
                sha256: None,
                started_at: registry::iso_now(),
                updated_at: registry::iso_now(),
            });
            shutdown_tx.send(true).ok();
            drop(socket_task);
            return Err(anyhow::anyhow!("{err}"));
        }
    };
    shutdown_tx.send(true).ok();
    drop(socket_task);
    let (renderer, keys) = match renderer {
        Some((renderer, keys)) => (Some(renderer), Some(keys)),
        None => (None, None),
    };
    drop(key_tx);
    if let Some(renderer) = renderer {
        let _ = renderer.await;
    }
    if let Some(keys) = keys {
        let _ = keys.await;
    }
    let status = if outcome.status == shard::engine::OutcomeStatus::Cancelled {
        registry::EntryStatus::Cancelled
    } else {
        registry::EntryStatus::Completed
    };
    store.update(&id, |entry| {
        entry.status = status;
        entry.final_dest = Some(outcome.dest_path.clone());
        entry.dest = outcome.dest_path.clone();
    })?;
    let final_url = outcome.final_url.clone();
    let sha256 = outcome.sha256.clone();
    let size = outcome.size;
    let final_dest = outcome.dest_path.clone();
    history_store.record(&history::HistoryEntry {
        id: id.clone(),
        url: opts.url.clone(),
        final_url: Some(final_url),
        dest: final_dest,
        status,
        size,
        sha256: Some(sha256),
        started_at: registry::iso_now(),
        updated_at: registry::iso_now(),
    })?;
    println!();
    println!("saved {} bytes -> {}", outcome.size, outcome.dest_path.display());
    if !outcome.sha256.is_empty() {
        println!("sha256 {}", outcome.sha256);
    }
    Ok(())
}

fn history_cli() -> anyhow::Result<()> {
    let history = history::History::open()?;
    let rows = history.list()?;
    if rows.is_empty() {
        println!("no downloads in history yet");
        return Ok(());
    }
    println!("{:<22} {:<11} {:<10} {:<64} SRC", "ID", "STATUS", "SIZE", "DEST");
    for row in rows {
        let exists = if row.dest.exists() { "on-disk" } else { "gone" };
        let size = if row.size > 0 {
            let (_size, unit) = humansize(row.size);
            format!("{_size} {unit}")
        } else {
            "-".to_string()
        };
        println!(
            "{:<22} {:<11} {:<10} {:<64} {exists}",
            row.id,
            row.status,
            size,
            row.dest.display()
        );
    }
    Ok(())
}

fn humansize(bytes: u64) -> (f64, &'static str) {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    (value, UNITS[unit])
}

async fn redo_cli(id_or_url: &str) -> anyhow::Result<()> {
    let history = history::History::open()?;
    let row = history
        .by_id_or_url(id_or_url)?
        .ok_or_else(|| anyhow::anyhow!("no download in history matches {id_or_url:?}"))?;
    let style = resolve_style(None, None)?;
    let conf = config::Config::load()?;
    let opts = DownloadOptions {
        url: row.url.clone(),
        dest_path: row.dest.clone(),
        connections: conf.download.connections,
        chunk_size: conf.download.chunk_size,
        max_attempts: conf.download.max_attempts,
        retry_base_delay: Duration::from_millis(conf.download.retry_base_ms),
        retry_max_delay: Duration::from_millis(conf.download.retry_max_ms),
        checkpoint_interval: Duration::from_millis(conf.download.checkpoint_ms),
        resume: conf.download.resume,
        routing: None,
    };
    run_download_cli(opts, style, Some(row.id.clone())).await
}

fn clipboard_url() -> anyhow::Result<String> {
    let candidates = [
        ("wl-paste", &["wl-paste", "--no-newline"][..]),
        ("xclip", &["xclip", "-selection", "clipboard", "-o"][..]),
        ("xsel", &["xsel", "--clipboard", "--output"][..]),
    ];
    for (name, args) in candidates {
        let Ok(output) = std::process::Command::new(name).args(args).output() else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if is_http_url(&text) {
            return Ok(text);
        }
        return Err(anyhow::anyhow!(
            "clipboard ({name}) does not contain a URL: {text:?}"
        ));
    }
    Err(anyhow::anyhow!(
        "no clipboard tool found (tried wl-paste, xclip, xsel) and no URL given"
    ))
}

fn is_http_url(text: &str) -> bool {
    text.starts_with("http://") || text.starts_with("https://")
}

fn expand_tilde(path: &str) -> PathBuf {
    let Some(rest) = path.strip_prefix('~') else {
        return PathBuf::from(path);
    };
    let home = std::env::var_os("HOME").unwrap_or_default();
    PathBuf::from(home).join(rest.trim_start_matches(['/', '\\']))
}

fn resolve_style(
    eyecandy_flag: Option<bool>,
    color_flag: Option<ColorChoice>,
) -> anyhow::Result<Style> {
    let is_tty = std::io::stdout().is_terminal();
    let conf = config::Config::load()?;
    let eyecandy = eyecandy_flag
        .or(Some(conf.style.prefer_eyecandy))
        .unwrap_or(false);
    let color_on = color_flag
        .or(Some(conf.style.color))
        .map(|choice| choice.resolves_to(is_tty))
        .unwrap_or(is_tty);
    Ok(Style::new(color_on, eyecandy))
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
    if conf.style.prefer_eyecandy {
        println!("eyecandy: yes (Nerd Font glyphs; needs a Nerd Font terminal)");
    } else {
        println!("eyecandy: no (TTY-safe hash/ascii glyphs)");
    }
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