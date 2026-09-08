use crate::cli::render::KeyCommand;
use crossterm::event::{Event, KeyCode, KeyEventKind};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use std::io::IsTerminal;
use std::time::Duration;
use tokio::sync::mpsc;

const POLL_INTERVAL: Duration = Duration::from_millis(100);

struct RawModeGuard;

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

pub fn spawn_key_listener(tx: mpsc::Sender<KeyCommand>) -> Option<tokio::task::JoinHandle<()>> {
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        return None;
    }
    Some(tokio::task::spawn_blocking(move || {
        let _guard = RawModeGuard;
        if enable_raw_mode().is_err() {
            return;
        }
        loop {
            if tx.is_closed() {
                break;
            }
            match crossterm::event::poll(POLL_INTERVAL) {
                Ok(true) => match crossterm::event::read() {
                    Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                        if key.code == KeyCode::Char('v') {
                            let _ = tx.try_send(KeyCommand::ToggleView);
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break,
                },
                Ok(false) => continue,
                Err(_) => break,
            }
        }
    }))
}
