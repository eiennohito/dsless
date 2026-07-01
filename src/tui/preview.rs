use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use crate::preview::{SchemaPath, TruncatedField};

/// State backing the preview system: the last-previewed path (for `V`),
/// an open field-number overlay, and any expanded field content on screen.
pub struct PreviewState {
    pub last_path: Option<SchemaPath>,
    pub overlay: Option<FieldOverlay>,
    pub active_preview: Option<ActivePreview>,
    /// Transient status-bar feedback (e.g. "no truncated fields"), cleared
    /// on the next key press.
    pub message: Option<String>,
}

impl PreviewState {
    pub fn new() -> Self {
        PreviewState {
            last_path: None,
            overlay: None,
            active_preview: None,
            message: None,
        }
    }

    pub fn dismiss(&mut self) {
        self.overlay = None;
        self.active_preview = None;
        self.message = None;
    }
}

impl Default for PreviewState {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether `row` is the one an inline (non-popup) preview should be
/// appended after in the draw pass. Popups render as a separate floating
/// widget instead, so they're excluded here.
pub fn is_current_row_with_inline_preview(preview: &PreviewState, row: usize) -> bool {
    preview
        .active_preview
        .as_ref()
        .is_some_and(|p| !p.is_popup && p.row == row)
}

/// Field numbers shown after bare `v`, keyed to the record they describe.
pub struct FieldOverlay {
    pub row: usize,
    pub fields: Vec<TruncatedField>,
}

/// Expanded content for a single field, either shown inline (short) or as
/// a scrollable popup (long). The path that produced this isn't kept here —
/// `PreviewState::last_path` is the one place that tracks it, for `V`.
pub struct ActivePreview {
    pub row: usize,
    pub name: String,
    pub content: String,
    pub line_count: usize,
    pub scroll_offset: usize,
    pub is_popup: bool,
}

const INLINE_LINE_LIMIT: usize = 3;

impl ActivePreview {
    pub fn new(row: usize, name: String, content: String, line_count: usize) -> Self {
        ActivePreview {
            row,
            name,
            content,
            line_count,
            scroll_offset: 0,
            is_popup: line_count > INLINE_LINE_LIMIT,
        }
    }

    pub fn lines(&self) -> Vec<&str> {
        self.content.lines().collect()
    }

    pub fn scroll(&mut self, delta: isize) {
        if !self.is_popup {
            return;
        }
        let max_offset = self.line_count.saturating_sub(1);
        let new_offset = (self.scroll_offset as isize + delta).clamp(0, max_offset as isize);
        self.scroll_offset = new_offset as usize;
    }
}

/// Render field-number annotations for the current record's truncated fields.
/// Table mode: numbers go on a line inserted above the header (aligned to
/// each truncated column's position). Vertical mode: numbers prefix the
/// matching field lines, matched by field name.
pub fn overlay_header_line(overlay: &FieldOverlay, col_widths: &[usize]) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    for (ci, &cw) in col_widths.iter().enumerate() {
        if ci > 0 {
            spans.push(Span::raw(" │ "));
        }
        let label = overlay
            .fields
            .iter()
            .position(|f| f.path == SchemaPath(vec![ci]))
            .map(|idx| format!("[{}]", idx + 1));
        match label {
            Some(text) => {
                let pad = cw.saturating_sub(text.len());
                spans.push(Span::styled(
                    text,
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ));
                spans.push(Span::raw(" ".repeat(pad)));
            }
            None => spans.push(Span::raw(" ".repeat(cw))),
        }
    }
    Line::from(spans)
}

/// Vertical mode: prefix a field's rendered line with its overlay number,
/// if this line is the one that renders that field. Rendered lines carry
/// no structural back-reference to the schema path, so the match is
/// text-based: a field named `a.b.c` renders as a line starting with
/// (guide chars, then) `"c: "` — the last path segment is unique among
/// its own siblings, which is enough to disambiguate in practice.
pub fn annotate_vertical_line(line: &str, overlay: &FieldOverlay) -> String {
    let trimmed = line.trim_start_matches(['│', ' ']);
    let matched = overlay.fields.iter().position(|f| {
        let leaf = f.name.rsplit('.').next().unwrap_or(&f.name);
        trimmed.starts_with(leaf) && trimmed[leaf.len()..].starts_with(": ")
    });
    match matched {
        Some(idx) => format!("[{}] {}", idx + 1, line),
        None => line.to_string(),
    }
}

/// Render the inline preview lines (<=3 lines) to append after the source line.
pub fn render_inline_lines(preview: &ActivePreview) -> Vec<Line<'static>> {
    preview
        .lines()
        .into_iter()
        .map(|l| {
            Line::from(Span::styled(
                format!("  {}", l),
                Style::default().fg(Color::Green),
            ))
        })
        .collect()
}

/// Render the popup preview (>3 lines) as a floating scrollable box,
/// following the same Clear+Paragraph+Block pattern as the help popup.
pub fn render_preview_popup(frame: &mut ratatui::Frame, area: Rect, preview: &ActivePreview) {
    let width = (area.width * 3 / 4).max(20).min(area.width);
    let height = (area.height * 3 / 4).max(6).min(area.height);
    let x = area.width.saturating_sub(width) / 2;
    let y = area.height.saturating_sub(height) / 2;
    let popup_area = Rect::new(x, y, width, height);

    let inner_height = height.saturating_sub(2) as usize;
    let lines: Vec<Line<'static>> = preview
        .lines()
        .into_iter()
        .skip(preview.scroll_offset)
        .take(inner_height)
        .map(|l| Line::from(l.to_string()))
        .collect();

    frame.render_widget(Clear, popup_area);
    let popup = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" {} ", preview.name))
            .border_style(Style::default().fg(Color::Cyan)),
    );
    frame.render_widget(popup, popup_area);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(path: Vec<usize>, name: &str) -> TruncatedField {
        TruncatedField {
            path: SchemaPath(path),
            name: name.to_string(),
        }
    }

    fn active_preview(content: &str, line_count: usize) -> ActivePreview {
        ActivePreview::new(0, "field".to_string(), content.to_string(), line_count)
    }

    #[test]
    fn schema_path_equality() {
        assert_eq!(SchemaPath(vec![1, 2]), SchemaPath(vec![1, 2]));
        assert_ne!(SchemaPath(vec![1, 2]), SchemaPath(vec![1, 3]));
        assert_ne!(SchemaPath(vec![1]), SchemaPath(vec![1, 2]));
    }

    #[test]
    fn preview_state_dismiss_clears_overlay_and_active() {
        let mut state = PreviewState::new();
        state.overlay = Some(FieldOverlay {
            row: 0,
            fields: vec![field(vec![0], "a")],
        });
        state.active_preview = Some(active_preview("hello", 1));
        state.dismiss();
        assert!(state.overlay.is_none());
        assert!(state.active_preview.is_none());
    }

    #[test]
    fn preview_state_dismiss_keeps_last_path() {
        let mut state = PreviewState::new();
        state.last_path = Some(SchemaPath(vec![3]));
        state.dismiss();
        assert_eq!(state.last_path, Some(SchemaPath(vec![3])));
    }

    #[test]
    fn active_preview_classifies_inline_vs_popup() {
        let short = active_preview("a\nb", 2);
        assert!(!short.is_popup);
        let long = active_preview("a\nb\nc\nd", 4);
        assert!(long.is_popup);
    }

    #[test]
    fn active_preview_scroll_clamps_within_bounds() {
        let mut preview = active_preview("1\n2\n3\n4\n5", 5);
        assert!(preview.is_popup);
        preview.scroll(-5);
        assert_eq!(preview.scroll_offset, 0);
        preview.scroll(100);
        assert_eq!(preview.scroll_offset, 4);
        preview.scroll(-1);
        assert_eq!(preview.scroll_offset, 3);
    }

    #[test]
    fn active_preview_scroll_noop_when_inline() {
        let mut preview = active_preview("a\nb", 2);
        preview.scroll(1);
        assert_eq!(preview.scroll_offset, 0);
    }

    #[test]
    fn annotate_vertical_line_prefixes_numbered_field() {
        let overlay = FieldOverlay {
            row: 0,
            fields: vec![field(vec![0], "a"), field(vec![2, 0], "c.d")],
        };
        assert_eq!(
            annotate_vertical_line("│ a: \"value\"", &overlay),
            "[1] │ a: \"value\""
        );
        assert_eq!(
            annotate_vertical_line("│ │ d: \"value\"", &overlay),
            "[2] │ │ d: \"value\""
        );
        assert_eq!(annotate_vertical_line("│ b: 42", &overlay), "│ b: 42");
    }
}
