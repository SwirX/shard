use crate::cli::style::{ColorChoice, ProgressMode};
use std::io::Write;
use std::path::PathBuf;

pub struct Config {
    path: PathBuf,
    pub color: Option<ColorChoice>,
    pub progress: Option<ProgressMode>,
}

impl Config {
    pub fn load() -> Config {
        let path = config_path();
        let Ok(body) = std::fs::read_to_string(&path) else {
            return Config { path, color: None, progress: None };
        };
        let (color, progress) = parse_options(&body);
        Config { path, color, progress }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn init() -> std::io::Result<()> {
        let path = config_path();
        if path.exists() {
            return Err(std::io::Error::other(format!(
                "config already exists at {}",
                path.display()
            )));
        }
        std::fs::create_dir_all(path.parent().expect("config dir has a parent"))?;
        let mut file = std::fs::File::create(&path)?;
        file.write_all(SHARD_CONF_SAMPLE.as_bytes())?;
        Ok(())
    }
}

pub fn config_path() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .unwrap_or_default();
    base.join("shard").join("shard.conf")
}

fn parse_options(body: &str) -> (Option<ColorChoice>, Option<ProgressMode>) {
    let mut color = None;
    let mut progress = None;
    for raw_line in body.lines() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('[') {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix('#') {
            let name = rest.trim();
            if matches!(name, "Color" | "color") {
                color = Some(ColorChoice::Never);
            }
            continue;
        }
        if matches!(trimmed, "Color" | "color") {
            color = Some(ColorChoice::Always);
            continue;
        }
        if let Some((key, value)) = trimmed.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            match key {
                "Color" | "color" => match value {
                    "always" => color = Some(ColorChoice::Always),
                    "never" => color = Some(ColorChoice::Never),
                    "auto" => color = Some(ColorChoice::Auto),
                    _ => {}
                },
                "Progress" | "progress" => match value {
                    "plain" => progress = Some(ProgressMode::Plain),
                    "nerd" => progress = Some(ProgressMode::Nerd),
                    _ => {}
                },
                _ => {}
            }
        }
    }
    (color, progress)
}

const SHARD_CONF_SAMPLE: &str = "[options]
# Pacman-style toggles: a bare flag enables it, comment it out (#Color) to disable,
# or use an explicit \"Color = never\" for off.
Color

# Progress eyecandy: plain (ascii, everything) or nerd (block glyphs + icons, Nerd Font terminals).
# Progress = nerd
Progress = plain
";

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &str) -> (Option<ColorChoice>, Option<ProgressMode>) {
        parse_options(body)
    }

    #[test]
    fn bare_color_flag_enables_color() {
        let (color, _) = parse("Color\n");
        assert_eq!(color, Some(ColorChoice::Always));
    }

    #[test]
    fn commented_color_flag_disables_color() {
        let (color, _) = parse("#Color\n");
        assert_eq!(color, Some(ColorChoice::Never));
    }

    #[test]
    fn value_lines_set_progress_and_color() {
        let (color, progress) = parse("Progress = nerd\nColor = auto\n");
        assert_eq!(progress, Some(ProgressMode::Nerd));
        assert_eq!(color, Some(ColorChoice::Auto));
    }

    #[test]
    fn unknown_lines_are_ignored() {
        let (color, progress) = parse("[unrelated]\nFoo = 1\n");
        assert_eq!(color, None);
        assert_eq!(progress, None);
    }

    #[test]
    fn empty_config_defaults_to_nothing() {
        let (color, progress) = parse("");
        assert_eq!(color, None);
        assert_eq!(progress, None);
    }
}