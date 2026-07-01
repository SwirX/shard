use crate::cli::lines::{summary_line, worker_lines, FrameStats};
use crate::cli::state::ProgressTracker;
use crate::cli::style::Style;
use shard::engine::progress::ProgressEvent;
use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const PAINT_INTERVAL: Duration = Duration::from_millis(80);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    Bar,
    Workers,
}

#[derive(Debug, Clone, Copy)]
pub enum KeyCommand {
    ToggleView,
}

pub struct Renderer {
    style: Style,
    tracker: ProgressTracker,
    view: View,
    workers: usize,
    interactive: bool,
    terminal_width: usize,
    last_paint: Option<Instant>,
    prev_lines: usize,
    started_at: Instant,
    hint_consumed: bool,
}

impl Renderer {
    pub fn new(style: Style, workers: usize) -> Renderer {
        let interactive = std::io::stdout().is_terminal();
        let terminal_width = crossterm::terminal::size()
            .map(|(columns, _)| columns as usize)
            .unwrap_or(80)
            .max(20);
        Renderer {
            style,
            tracker: ProgressTracker::new(workers),
            view: View::Bar,
            workers,
            interactive,
            terminal_width,
            last_paint: None,
            prev_lines: 0,
            started_at: Instant::now(),
            hint_consumed: false,
        }
    }

    pub fn handle_progress(&mut self, event: ProgressEvent) {
        self.tracker.handle(event);
        self.paint_throttled();
    }

    pub fn handle_key(&mut self, command: KeyCommand) {
        match command {
            KeyCommand::ToggleView => {
                self.view = match self.view {
                    View::Bar => View::Workers,
                    View::Workers => View::Bar,
                };
                self.hint_consumed = true;
                self.paint_now();
            }
        }
    }

    pub fn finish(&mut self) {
        self.view = View::Bar;
        self.paint_now();
    }

    fn paint_throttled(&mut self) {
        let now = Instant::now();
        if self.last_paint.is_some_and(|last| now - last < PAINT_INTERVAL) {
            return;
        }
        self.last_paint = Some(now);
        self.paint_now();
    }

    fn paint_now(&mut self) {
        if !self.interactive {
            return;
        }
        let stats = FrameStats {
            started_at: self.started_at,
            last_paint_at: Instant::now(),
        };
        let lines = self.compose_lines(&stats);
        self.write_lines(&lines);
        self.prev_lines = lines.len();
    }

    fn compose_lines(&self, stats: &FrameStats) -> Vec<String> {
        let mut lines = match self.view {
            View::Bar => vec![summary_line(&self.tracker, &self.style, stats, self.terminal_width)],
            View::Workers => worker_lines(&self.tracker, &self.style, self.tracker_workers()),
        };
        if self.view == View::Bar && !self.hint_consumed {
            let hint = self.style.paint("38;5;240", "v toggle view");
            lines.push(hint);
        }
        lines
    }

    fn tracker_workers(&self) -> usize {
        self.workers
    }

    fn write_lines(&self, lines: &[String]) {
        let mut out = String::new();
        if self.prev_lines > 0 {
            out.push_str(&format!("\x1b[{}A", self.prev_lines));
        }
        for line in lines {
            out.push_str("\r\x1b[2K");
            out.push_str(&truncate_line(line, self.terminal_width));
            out.push('\n');
        }
        if lines.len() < self.prev_lines {
            for _ in lines.len()..self.prev_lines {
                out.push_str("\r\x1b[2K\n");
            }
            out.push_str(&format!("\x1b[{}A", self.prev_lines - lines.len()));
        }
        if self.interactive {
            let _ = std::io::stdout().write_all(out.as_bytes());
            let _ = std::io::stdout().flush();
        }
    }
}

pub fn truncate_line(line: &str, width: usize) -> String {
    let mut visible = 0;
    let mut result = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            result.push(ch);
            while let Some(&next) = chars.peek() {
                if next.is_ascii_alphabetic() {
                    result.push(next);
                    chars.next();
                    break;
                }
                result.push(next);
                chars.next();
            }
            continue;
        }
        if visible >= width {
            break;
        }
        if unicode_width(ch) >= 1 {
            visible += 1;
        }
        result.push(ch);
    }
    result
}

fn unicode_width(ch: char) -> usize {
    if ch.is_ascii() {
        1
    } else {
        2
    }
}

pub async fn run_progress_renderer(
    mut progress_rx: mpsc::Receiver<ProgressEvent>,
    mut key_rx: mpsc::Receiver<KeyCommand>,
    style: Style,
    workers: usize,
) {
    let mut renderer = Renderer::new(style, workers);
    loop {
        tokio::select! {
            event = progress_rx.recv() => {
                match event {
                    Some(event) => renderer.handle_progress(event),
                    None => break,
                }
            }
            command = key_rx.recv() => {
                if let Some(command) = command {
                    renderer.handle_key(command);
                }
            }
        }
    }
    renderer.finish();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_respects_terminal_width_and_keeps_ansi() {
        let styled = "\x1b[38;5;39mhello\x1b[0m world, this is long";
        let narrowed = truncate_line(styled, 10);
        assert!(narrowed.contains("\x1b[38;5;39m"));
        assert!(narrowed.contains("hello"));
        assert!(!narrowed.contains("this is long"));
    }

    #[test]
    fn toggle_switches_views() {
        let mut renderer = Renderer::new(Style::new(false, false), 2);
        assert_eq!(renderer.view, View::Bar);
        renderer.handle_key(KeyCommand::ToggleView);
        assert_eq!(renderer.view, View::Workers);
        renderer.handle_key(KeyCommand::ToggleView);
        assert_eq!(renderer.view, View::Bar);
    }

    #[test]
    fn hint_only_in_bar_view_before_first_toggle() {
        let style = Style::new(false, false);
        let stats = FrameStats { started_at: Instant::now(), last_paint_at: Instant::now() };
        let mut renderer = Renderer::new(style, 2);
        let lines = renderer.compose_lines(&stats);
        assert!(lines.iter().any(|line| line.contains("toggle view")));
        renderer.handle_key(KeyCommand::ToggleView);
        let lines = renderer.compose_lines(&stats);
        assert!(lines.iter().all(|line| !line.contains("toggle view")));
    }

    #[test]
    fn finish_switches_back_to_the_bar_view() {
        let mut renderer = Renderer::new(
            Style::new(false, false),
            2,
        );
        renderer.handle_key(KeyCommand::ToggleView);
        assert_eq!(renderer.view, View::Workers);
        renderer.finish();
        assert_eq!(renderer.view, View::Bar);
    }
}