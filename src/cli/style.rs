use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColorChoice {
    Auto,
    Always,
    Never,
}

impl fmt::Display for ColorChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ColorChoice::Auto => "auto",
            ColorChoice::Always => "always",
            ColorChoice::Never => "never",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Style {
    pub color_on: bool,
    /// Prefer Nerd Font glyphs (eyecandy) over plain TTY-safe ASCII.
    pub eyecandy: bool,
}

const WORKER_PALETTE: [u8; 9] = [39, 69, 208, 198, 34, 226, 51, 141, 44];

impl Style {
    pub fn new(color_on: bool, eyecandy: bool) -> Style {
        Style { color_on, eyecandy }
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
        if self.eyecandy { '\u{25b0}' } else { '#' }
    }

    pub fn bar_empty(&self) -> char {
        if self.eyecandy { '\u{25b1}' } else { '-' }
    }

    pub fn download_glyph(&self) -> char {
        if self.eyecandy { '\u{f019}' } else { '>' }
    }

    pub fn worker_glyph(&self) -> char {
        if self.eyecandy { '\u{e0b0}' } else { '*' }
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
        let style = Style::new(false, true);
        assert_eq!(style.paint("38;5;39", "x"), "x");
    }

    #[test]
    fn color_on_wraps_in_ansi() {
        let style = Style::new(true, false);
        assert_eq!(style.paint("33", "x"), "\x1b[33mx\x1b[0m");
    }

    #[test]
    fn worker_palette_cycles() {
        let style = Style::new(true, false);
        assert_eq!(style.worker_ansi(0), style.worker_ansi(9));
        assert_ne!(style.worker_ansi(0), style.worker_ansi(1));
    }

    #[test]
    fn eyecandy_uses_nerd_font_block_glyphs() {
        let style = Style::new(false, true);
        assert_eq!(style.bar_filled(), '\u{25b0}');
        assert_eq!(style.bar_empty(), '\u{25b1}');
        assert_eq!(style.download_glyph(), '\u{f019}');
    }

    #[test]
    fn plain_prefers_tty_safe_ascii_hashes() {
        let style = Style::new(false, false);
        assert_eq!(style.bar_filled(), '#');
        assert_eq!(style.bar_empty(), '-');
        assert_eq!(style.download_glyph(), '>');
        assert_eq!(style.worker_glyph(), '*');
    }
}