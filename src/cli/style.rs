//! Tty-gated ANSI styling for the human renderer.
//!
//! Hand-rolled escapes rather than a dependency: the palette is four tones
//! wide and is not meant to grow into a theme. Color is decided once, at the
//! top level, and passed down as a value — so every render function stays a
//! pure function of its inputs and the plain path is the one unit tests
//! exercise by construction rather than by mocking a terminal.

use std::io::IsTerminal;

/// The tones the renderer draws from. Restraint is the whole point: a colored
/// token means "look here", so anything merely normal stays unstyled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// A failure that stopped something.
    Red,
    /// A failure that was stepped over, or work waiting on a human.
    Yellow,
    /// A pass.
    Green,
    /// Settled and unremarkable — present for completeness, not attention.
    Dim,
}

impl Tone {
    fn code(self) -> &'static str {
        match self {
            Self::Red => "\u{1b}[31m",
            Self::Yellow => "\u{1b}[33m",
            Self::Green => "\u{1b}[32m",
            Self::Dim => "\u{1b}[2m",
        }
    }
}

const RESET: &str = "\u{1b}[0m";

/// Whether a rendering may emit escape sequences and non-ASCII glyphs. Both
/// ask the same question — is a human watching, or a pipe — so both hang off
/// the one gate rather than growing a second flag that could disagree with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Style {
    color: bool,
}

impl Style {
    /// The two ends of the decision, named for tests. Production reaches a
    /// `Style` only through `detect`, so these exist to let the renderer's
    /// tests assert both the plain and the styled path without owning a
    /// terminal or mutating the environment.
    #[cfg(test)]
    pub const PLAIN: Self = Self { color: false };
    #[cfg(test)]
    pub const COLOR: Self = Self { color: true };

    /// Color only when stdout is a terminal and `NO_COLOR` is unset. The
    /// `NO_COLOR` convention is presence, not value: `NO_COLOR=` disables it
    /// too, so the check is on the variable existing.
    pub fn detect() -> Self {
        Self {
            color: std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
        }
    }

    /// `text` in `tone`, or `text` unchanged when the tone is absent or color
    /// is off.
    pub fn paint(self, tone: Option<Tone>, text: &str) -> String {
        match tone.filter(|_| self.color) {
            Some(tone) => format!("{}{text}{RESET}", tone.code()),
            None => text.to_owned(),
        }
    }

    /// `styled` where the tty gate is open, `plain` otherwise. The caller
    /// supplies both forms so the fallback is chosen next to the glyph it
    /// stands in for, and so a glyph can never reach a pipe that would mangle
    /// it.
    pub fn glyph(self, styled: &'static str, plain: &'static str) -> &'static str {
        if self.color { styled } else { plain }
    }

    /// `text` in `tone`, padded on the right to `width` *visible* columns.
    /// The padding sits outside the escapes, so a colored column still lines
    /// up with the plain ones beside it — `{:width$}` would count the escape
    /// bytes and shift the row.
    pub fn column(self, tone: Option<Tone>, text: &str, width: usize) -> String {
        let mut padded = self.paint(tone, text);
        for _ in text.chars().count()..width {
            padded.push(' ');
        }
        padded
    }
}

/// The tone for a ticket or run state wherever one is printed as its own
/// token. `merged` is dimmed rather than colored: it is the most common state
/// in a healthy repository, and the eye should slide over it.
pub fn state_tone(state: &str) -> Option<Tone> {
    match state {
        "failed" => Some(Tone::Red),
        "needs_review" => Some(Tone::Yellow),
        "merged" => Some(Tone::Dim),
        _ => None,
    }
}

/// The tone for one marker in a stage scan strip. The strip is read at a
/// glance, and color is what makes the one `FAIL` in a row of `ok`s the thing
/// the eye lands on.
pub fn marker_tone(marker: &str) -> Option<Tone> {
    match marker {
        "FAIL" => Some(Tone::Red),
        "warn" => Some(Tone::Yellow),
        "ok" => Some(Tone::Green),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Style, Tone, marker_tone, state_tone};

    #[test]
    fn the_plain_style_never_emits_an_escape() {
        assert_eq!(Style::PLAIN.paint(Some(Tone::Red), "FAIL"), "FAIL");
        assert_eq!(
            Style::PLAIN.column(Some(Tone::Red), "failed", 8),
            "failed  "
        );
        assert!(
            !Style::PLAIN
                .paint(Some(Tone::Dim), "merged")
                .contains('\u{1b}')
        );
    }

    /// The same gate decides escapes and glyphs, so a rendering that reaches a
    /// pipe is ASCII throughout rather than ASCII except for one arrow.
    #[test]
    fn the_plain_style_falls_back_to_the_ascii_form_of_a_glyph() {
        assert_eq!(Style::PLAIN.glyph(" \u{2192} ", " -> "), " -> ");
        assert_eq!(Style::COLOR.glyph(" \u{2192} ", " -> "), " \u{2192} ");
    }

    #[test]
    fn the_color_style_wraps_the_token_and_resets() {
        assert_eq!(
            Style::COLOR.paint(Some(Tone::Red), "FAIL"),
            "\u{1b}[31mFAIL\u{1b}[0m"
        );
        assert_eq!(Style::COLOR.paint(None, "ready"), "ready");
    }

    /// Padding outside the escapes is what keeps a colored column the same
    /// visible width as the plain one above it.
    #[test]
    fn padding_counts_visible_columns_only() {
        let colored = Style::COLOR.column(Some(Tone::Yellow), "needs_review", 14);
        assert_eq!(colored, "\u{1b}[33mneeds_review\u{1b}[0m  ");
        assert_eq!(Style::COLOR.column(None, "ok", 4), "ok  ");
    }

    #[test]
    fn only_the_states_and_markers_worth_flagging_have_a_tone() {
        assert_eq!(state_tone("failed"), Some(Tone::Red));
        assert_eq!(state_tone("needs_review"), Some(Tone::Yellow));
        assert_eq!(state_tone("merged"), Some(Tone::Dim));
        assert_eq!(state_tone("ready"), None);
        assert_eq!(state_tone("claimed"), None);
        assert_eq!(marker_tone("FAIL"), Some(Tone::Red));
        assert_eq!(marker_tone("warn"), Some(Tone::Yellow));
        assert_eq!(marker_tone("ok"), Some(Tone::Green));
        assert_eq!(marker_tone(".."), None);
        assert_eq!(marker_tone("-"), None);
    }
}
