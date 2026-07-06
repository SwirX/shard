use crate::cli::state::ProgressTracker;
use crate::cli::style::Style;
use std::time::Instant;

const MAX_BAR_CELLS: usize = 80;
const BAR_TEXT_PADDING: usize = 62;
const DIM_ANSI: &str = "38;5;240";

pub struct FrameStats {
    pub started_at: Instant,
    pub last_paint_at: Instant,
}

pub fn summary_line(
    tracker: &ProgressTracker,
    style: &Style,
    stats: &FrameStats,
    bar_width_cells: usize,
) -> String {
    let done_bytes = tracker.done_bytes();
    let total = tracker.total.unwrap_or(0);
    let ratio = if total > 0 {
        done_bytes as f64 / total as f64
    } else {
        0.0
    };
    let cells = bar_width_cells
        .saturating_sub(BAR_TEXT_PADDING)
        .clamp(14, MAX_BAR_CELLS);
    let filled = (cells as f64 * ratio).round() as usize;

    let mut bar = String::new();
    for cell in 0..cells {
        let glyph = if cell < filled {
            style.bar_filled()
        } else {
            style.bar_empty()
        };
        let chunk = tracker
            .chunk_for_offset(total.saturating_mul(cell as u64) / cells as u64)
            .unwrap_or(cell);
        let ansi = match tracker.chunk_worker(chunk) {
            Some(worker) => style.worker_ansi(worker),
            None => DIM_ANSI.to_string(),
        };
        bar.push_str(&style.paint(&ansi, &glyph.to_string()));
    }

    let elapsed = stats
        .last_paint_at
        .duration_since(stats.started_at)
        .as_secs_f64();
    let speed_mbs = if elapsed > 0.0 {
        done_bytes as f64 / elapsed / 1_000_000.0
    } else {
        0.0
    };
    let speed_text = style.paint("38;5;114", &format!("{speed_mbs:>6.2} MB/s"));
    let percent_text = style.paint("38;5;228", &format!("{:>6.1}%", ratio * 100.0));
    let eta_secs = if speed_mbs > 0.0 && total > 0 {
        (total.saturating_sub(done_bytes) as f64) / (speed_mbs * 1_000_000.0)
    } else {
        0.0
    };
    let eta_text = style.paint("38;5;213", &format!("eta {:>5.0}s", eta_secs));
    let finished = tracker.finished_chunks();
    let planned = tracker.planned_chunk_count();
    let chunk_total = if planned > 0 {
        planned
    } else {
        tracker.chunk_count().max(finished)
    };
    let chunk_text = style.paint("38;5;117", &format!("chunks {finished}/{chunk_total}"));
    let active_text = style.paint(
        "38;5;180",
        &format!("{} active", tracker.active_worker_count()),
    );

    format!(
        "{glyph} {bar}  {percent_text}  {speed_text}  {eta_text}  {chunk_text} {active_text}",
        glyph = style.paint("38;5;39", &style.download_glyph().to_string()),
    )
}

pub fn worker_lines(tracker: &ProgressTracker, style: &Style, workers: usize) -> Vec<String> {
    let total = tracker.total.unwrap_or(0).max(1);
    let share = (total / workers.max(1) as u64).max(1);
    let mut lines = Vec::new();
    for worker in 0..workers {
        let bytes = tracker.worker_bytes(worker);
        let share_ratio = bytes as f64 / share as f64;
        let file_ratio = bytes as f64 / total as f64;
        let ansi = style.worker_ansi(worker);
        let glyph = style.paint(&ansi, &style.worker_glyph().to_string());
        let id = style.paint(&ansi, &format!("W{worker}"));
        let working = match tracker.worker_active_chunk(worker) {
            Some(chunk) => style.paint("38;5;180", &format!("chunk #{chunk}")),
            None => style.paint(DIM_ANSI, "idle"),
        };
        let share_pct = style.paint(&ansi, &format!("{:>6.1}%", share_ratio * 100.0));
        let file_pct = style.paint("38;5;117", &format!("{:>6.1}% file", file_ratio * 100.0));
        let mb = format!("{:>7.2} MB", bytes as f64 / 1_000_000.0);
        let bar = worker_minibar(share_ratio, style);
        lines.push(format!(
            "{glyph} {id} {working} {bar} {share_pct} share  {file_pct} {mb}"
        ));
    }
    lines
}

fn worker_minibar(ratio: f64, style: &Style) -> String {
    const WIDTH: usize = 20;
    let filled = (WIDTH as f64 * ratio).round() as usize;
    (0..WIDTH)
        .map(|cell| {
            let glyph = if cell < filled {
                style.bar_filled()
            } else {
                style.bar_empty()
            };
            style.paint(DIM_ANSI, &glyph.to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shard::engine::progress::ProgressEvent;

    fn tracker(events: Vec<ProgressEvent>) -> ProgressTracker {
        let mut tracker = ProgressTracker::new(3);
        for event in events {
            tracker.handle(event);
        }
        tracker
    }

    fn stats() -> FrameStats {
        let now = Instant::now();
        FrameStats {
            started_at: now,
            last_paint_at: now,
        }
    }

    #[test]
    fn summary_line_shows_percentage_and_chunk_counts() {
        let tracker = tracker(vec![
            ProgressEvent::Start {
                total: 1000,
                chunk_size: 100,
            },
            ProgressEvent::ChunkStarted {
                worker: 0,
                index: 0,
            },
            ProgressEvent::ChunkAdvanced {
                worker: 0,
                index: 0,
                written: 100,
            },
            ProgressEvent::ChunkComplete {
                worker: 0,
                index: 0,
            },
        ]);
        let style = Style::new(false, false);
        let text = summary_line(&tracker, &style, &stats(), 60);
        assert!(
            text.contains("10.0%"),
            "done={} text={text}",
            tracker.done_bytes()
        );
        assert!(text.contains("chunks 1/"), "one chunk marked done");
    }

    #[test]
    fn plain_summary_uses_ascii_glyphs() {
        let tracker = tracker(vec![ProgressEvent::Start {
            total: 100,
            chunk_size: 100,
        }]);
        let style = Style::new(false, false);
        let text = summary_line(&tracker, &style, &stats(), 10);
        assert!(text.starts_with("> "), "plain download glyph is >");
    }

    #[test]
    fn worker_lines_render_one_line_per_worker() {
        let tracker = tracker(vec![
            ProgressEvent::Start {
                total: 1000,
                chunk_size: 100,
            },
            ProgressEvent::ChunkStarted {
                worker: 1,
                index: 2,
            },
            ProgressEvent::ChunkAdvanced {
                worker: 1,
                index: 2,
                written: 500,
            },
        ]);
        let style = Style::new(false, true);
        let lines = worker_lines(&tracker, &style, 3);
        assert_eq!(lines.len(), 3);
        assert!(lines[1].contains("W1"));
        assert!(lines[1].contains("chunk #2"));
        assert!(lines[1].contains("share"));
        assert!(lines[1].contains("% file"));
        assert!(lines[0].contains("idle"));
    }

    #[test]
    fn worker_colors_differ_across_workers() {
        let tracker = tracker(vec![
            ProgressEvent::Start {
                total: 1000,
                chunk_size: 100,
            },
            ProgressEvent::ChunkStarted {
                worker: 0,
                index: 0,
            },
        ]);
        let style = Style::new(true, false);
        let lines = worker_lines(&tracker, &style, 2);
        assert_ne!(lines[0], lines[1]);
        assert!(lines[0].contains("\x1b["));
    }
}
