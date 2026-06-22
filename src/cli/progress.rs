use shard::engine::progress::ProgressEvent;
use std::io::{IsTerminal, Write};
use std::time::Instant;

const BAR_WIDTH: usize = 40;
const THROTTLE: std::time::Duration = std::time::Duration::from_millis(80);

pub struct ProgressView {
    started: Vec<bool>,
    done: Vec<bool>,
    written: Vec<u64>,
    total: Option<u64>,
    started_at: Option<Instant>,
    last_render: Option<Instant>,
    prev_line_len: usize,
    rendered_any: bool,
    tty: bool,
}

impl Default for ProgressView {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressView {
    pub fn new() -> Self {
        Self::with_tty(std::io::stdout().is_terminal())
    }

    pub fn with_tty(tty: bool) -> Self {
        Self {
            started: Vec::new(),
            done: Vec::new(),
            written: Vec::new(),
            total: None,
            started_at: None,
            last_render: None,
            prev_line_len: 0,
            rendered_any: false,
            tty,
        }
    }

    pub fn handle(&mut self, event: ProgressEvent) {
        if self.started_at.is_none() {
            self.started_at = Some(Instant::now());
        }
        match event {
            ProgressEvent::Start { total } => self.total = Some(total),
            ProgressEvent::ChunkStarted { index } => {
                self.ensure(index);
                self.started[index] = true;
            }
            ProgressEvent::ChunkAdvanced { index, written } => {
                self.ensure(index);
                self.written[index] = written;
            }
            ProgressEvent::ChunkComplete { index } => {
                self.ensure(index);
                self.done[index] = true;
            }
            ProgressEvent::Finite { .. } => {}
        }
        self.render_throttled();
    }

    pub fn finish(&mut self) {
        if self.rendered_any && self.tty {
            println!();
        }
    }

    fn ensure(&mut self, index: usize) {
        if self.written.len() <= index {
            self.written.resize(index + 1, 0);
            self.started.resize(index + 1, false);
            self.done.resize(index + 1, false);
        }
    }

    fn render_throttled(&mut self) {
        let now = Instant::now();
        if self.last_render.is_some_and(|last| now - last < THROTTLE) {
            return;
        }
        self.last_render = Some(now);
        self.render();
    }

    fn render(&mut self) {
        if !self.tty {
            return;
        }
        self.rendered_any = true;

        let line = self.format_line();
        let padding = self.prev_line_len.saturating_sub(line.len());
        print!("\r{line}{}", " ".repeat(padding));
        let _ = std::io::stdout().flush();
        self.prev_line_len = line.len();
    }

    pub fn format_line(&mut self) -> String {
        let done_bytes = self.written.iter().sum::<u64>();
        let total = self.total.unwrap_or(0);
        let ratio = if total > 0 { done_bytes as f64 / total as f64 } else { 0.0 };
        let filled = (BAR_WIDTH as f64 * ratio).round() as usize;
        let bar: String = (0..BAR_WIDTH)
            .map(|i| if i < filled { '#' } else { '-' })
            .collect();

        let elapsed = self
            .started_at
            .map(|start| start.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        let speed_mbs = if elapsed > 0.0 { done_bytes as f64 / elapsed / 1_000_000.0 } else { 0.0 };
        let eta_secs = if speed_mbs > 0.0 && total > 0 {
            (total.saturating_sub(done_bytes) as f64) / (speed_mbs * 1_000_000.0)
        } else {
            0.0
        };

        let finished = self.done.iter().filter(|&&is_done| is_done).count();
        let active = self
            .started
            .iter()
            .zip(self.done.iter())
            .filter(|(is_started, is_done)| **is_started && !**is_done)
            .count();

        format!(
            "[{bar}] {:>6.1}% {speed_mbs:>7.2} MB/s  eta {eta_secs:>5.0}s  chunks {finished:>3} done {active:>2} active  {elapsed:>5.1}s",
            ratio * 100.0
        )
    }
}

pub async fn run_progress_renderer(mut rx: tokio::sync::mpsc::Receiver<ProgressEvent>) {
    let mut view = ProgressView::new();
    while let Some(event) = rx.recv().await {
        view.handle(event);
    }
    view.finish();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bar_and_percentage_reflect_downloaded_bytes() {
        let mut view = ProgressView::with_tty(false);
        view.handle(ProgressEvent::Start { total: 400 });
        view.handle(ProgressEvent::ChunkAdvanced { index: 0, written: 100 });
        let line = view.format_line();
        assert!(
            line.contains("25.0%"),
            "actual: {line} | written {:?} total {:?}",
            view.written,
            view.total
        );
        let hashes = line.chars().skip(1).take_while(|&c| c == '#').count();
        assert_eq!(hashes, 10, "25 percent of a 40-char bar");
    }

    #[test]
    fn chunk_counts_track_started_and_completed() {
        let mut view = ProgressView::with_tty(false);
        view.handle(ProgressEvent::Start { total: 1000 });
        view.handle(ProgressEvent::ChunkStarted { index: 0 });
        view.handle(ProgressEvent::ChunkStarted { index: 1 });
        assert!(view.format_line().contains("chunks   0 done  2 active"));
        view.handle(ProgressEvent::ChunkComplete { index: 0 });
        assert!(view.format_line().contains("chunks   1 done  1 active"));
        view.handle(ProgressEvent::ChunkComplete { index: 1 });
        assert!(view.format_line().contains("chunks   2 done  0 active"));
    }

    #[test]
    fn finish_is_silent_before_any_render() {
        let mut view = ProgressView::with_tty(true);
        assert!(!view.rendered_any);
        view.finish();
        assert!(!view.rendered_any);
    }
}