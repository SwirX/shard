use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressMode {
    Plain,
    Nerd,
}

impl fmt::Display for ProgressMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ProgressMode::Plain => "plain",
            ProgressMode::Nerd => "nerd",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorChoice {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Style {
    pub color_on: bool,
    pub mode: ProgressMode,
}

const WORKER_PALETTE: [u8; 9] = [39, 69, 208, 198, 34, 226, 51, 141, 44];

impl Style {
    pub fn with_color(color_on: bool, mode: ProgressMode) -> Style {
        Style { color_on, mode }
    }

    pub fn paint(&self, ansi_prefix: &str, text: &str) -> String {
        if self.color_on {
            format!("\x1b[{ansi_prefix}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    pub fn worker_ansi(&self, worker: usize) -> String {
        let code = WORKER_PALETTE[worker % WORKER_PALETTE.len()];
        format!("38;5;{code}")
    }

    pub fn bar_filled(&self) -> char {
        match self.mode {
            ProgressMode::Plain => '#',
            ProgressMode::Nerd => '\u{25b0}',
        }
    }

    pub fn bar_empty(&self) -> char {
        match self.mode {
            ProgressMode::Plain => '-',
            ProgressMode::Nerd => '\u{25b1}',
        }
    }

    pub fn download_glyph(&self) -> char {
        match self.mode {
            ProgressMode::Plain => '>',
            ProgressMode::Nerd => '\u{f019}',
        }
    }

    pub fn worker_glyph(&self) -> char {
        match self.mode {
            ProgressMode::Plain => '*',
            ProgressMode::Nerd => '\u{e0b0}',
        }
    }
}

impl ColorChoice {
    pub fn resolves_to(self, is_tty: bool) -> bool {
        match self {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            ColorChoice::Auto => is_tty,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn color_off_returns_plain_text() {
        let style = Style::with_color(false, ProgressMode::Nerd);
        assert_eq!(style.paint("38;5;39", "x"), "x");
    }

    #[test]
    fn color_on_wraps_in_ansi() {
        let style = Style::with_color(true, ProgressMode::Plain);
        assert_eq!(style.paint("33", "x"), "\x1b[33mx\x1b[0m");
    }

    #[test]
    fn worker_palette_cycles() {
        let style = Style::with_color(true, ProgressMode::Plain);
        assert_eq!(style.worker_ansi(0), style.worker_ansi(9));
        assert_ne!(style.worker_ansi(0), style.worker_ansi(1));
    }

    #[test]
    fn nerd_mode_uses_block_glyphs() {
        let style = Style::with_color(false, ProgressMode::Nerd);
        assert_eq!(style.bar_filled(), '\u{25b0}');
        assert_eq!(style.bar_empty(), '\u{25b1}');
        assert_eq!(style.download_glyph(), '\u{f019}');
    }

    #[test]
    fn plain_mode_uses_ascii() {
        let style = Style::with_color(false, ProgressMode::Plain);
        assert_eq!(style.bar_filled(), '#');
        assert_eq!(style.bar_empty(), '-');
        assert_eq!(style.download_glyph(), '>');
    }
}