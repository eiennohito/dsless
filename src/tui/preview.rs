use ratatui::prelude::*;
use ratatui::widgets::{
    Block, Borders, Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
};
use smallvec::{SmallVec, smallvec};

use crate::preview::{SchemaPath, TruncatedField};
use crate::render::RenderedRow;
use crate::tui::label::field_label;
use crate::unicode::display_width;

const MAX_WRAP_LINES_PER_ENTRY: usize = 20;

/// The `v`/`V`/`<space>` preview flow, modeled as a state machine so each
/// state only carries the data that exists at that point — instead of a
/// grab-bag of `Option`s on `App` whose validity depended on which other
/// `Option`s happened to be set. Transitions:
///
/// ```text
/// Idle ──(v)───────────────────────────────> WaitingForFields
/// WaitingForFields ──(fields arrive, empty)─> Message
/// WaitingForFields ──(fields arrive, some)──> Overlay
/// Message ──(v/esc/nav)─────────────────────> Idle
/// Overlay ──(label resolved)────────────────> WaitingForContent { fallback: Some }
/// Overlay ──(v/esc/nav)──────────────────────> Idle
/// WaitingForContent ──(content arrives)─────> Preview
/// (V or <space>, no overlay involved) ───────> WaitingForContent { fallback: None }
/// Preview ──(v/esc, has fallback)────────────> Overlay
/// Preview ──(v/esc, no fallback)/nav─────────> Idle
/// ```
pub enum PreviewPhase {
    Idle,
    WaitingForFields {
        row: usize,
    },
    Message(String),
    Overlay {
        overlay: FieldOverlay,
        label_buf: SmallVec<[char; 2]>,
    },
    WaitingForContent {
        row: usize,
        path: SchemaPath,
        fallback: Option<FieldOverlay>,
    },
    Preview {
        active: ActivePreview,
        fallback: Option<FieldOverlay>,
    },
}

pub struct PreviewState {
    pub phase: PreviewPhase,
    pub last_path: Option<SchemaPath>,
}

impl PreviewState {
    pub fn new() -> Self {
        PreviewState {
            phase: PreviewPhase::Idle,
            last_path: None,
        }
    }

    /// Full reset — used when navigation moves the cursor away from the
    /// previewed row/field, where none of the in-flight state still applies.
    pub fn dismiss(&mut self) {
        self.phase = PreviewPhase::Idle;
    }

    pub fn overlay(&self) -> Option<&FieldOverlay> {
        match &self.phase {
            PreviewPhase::Overlay { overlay, .. } => Some(overlay),
            PreviewPhase::WaitingForContent { fallback, .. }
            | PreviewPhase::Preview { fallback, .. } => fallback.as_ref(),
            _ => None,
        }
    }

    pub fn message(&self) -> Option<&str> {
        match &self.phase {
            PreviewPhase::Message(m) => Some(m.as_str()),
            _ => None,
        }
    }

    pub fn active_preview(&self) -> Option<&ActivePreview> {
        match &self.phase {
            PreviewPhase::Preview { active, .. } => Some(active),
            _ => None,
        }
    }

    pub fn active_preview_mut(&mut self) -> Option<&mut ActivePreview> {
        match &mut self.phase {
            PreviewPhase::Preview { active, .. } => Some(active),
            _ => None,
        }
    }
}

impl Default for PreviewState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FieldOverlay {
    pub row: usize,
    pub fields: Vec<TruncatedField>,
}

pub struct ActivePreview {
    pub name: String,
    pub lines: Vec<String>,
    pub scroll_offset: usize,
}

impl ActivePreview {
    pub fn new(name: String, content: String, wrap_width: usize) -> Self {
        ActivePreview {
            name,
            lines: wrap_content(&content, wrap_width),
            scroll_offset: 0,
        }
    }

    pub fn scroll(&mut self, delta: isize, visible_height: u16) {
        let inner = visible_height.saturating_sub(2) as usize;
        let max_offset = self.lines.len().saturating_sub(inner);
        let new = (self.scroll_offset as isize + delta).clamp(0, max_offset as isize);
        self.scroll_offset = new as usize;
    }
}

fn wrap_content(content: &str, width: usize) -> Vec<String> {
    let width = width.max(10);
    let mut out = Vec::new();
    for logical in content.lines() {
        let w = display_width(logical);
        if w <= width {
            out.push(logical.to_string());
        } else {
            let physical = wrap_line(logical, width);
            if physical.len() > MAX_WRAP_LINES_PER_ENTRY {
                out.extend(physical.into_iter().take(MAX_WRAP_LINES_PER_ENTRY));
                out.push("  ...".to_string());
            } else {
                out.extend(physical);
            }
        }
    }
    out
}

fn wrap_line(s: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    let mut current_width = 0;

    for c in s.chars() {
        let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if current_width + cw > width && current_width > 0 {
            lines.push(std::mem::take(&mut current));
            current_width = 0;
        }
        current.push(c);
        current_width += cw;
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

/// Fit a line to exactly `target` display cells. Truncates if wider
/// (replacing a straddling fullwidth char with a space), pads with
/// spaces if narrower. Prevents fullwidth chars from corrupting the
/// popup's right border.
fn fit_line(s: &str, target: usize) -> String {
    let content_w = target.saturating_sub(1);
    let mut out = String::with_capacity(target);
    out.push(' ');
    let mut w = 0;
    for c in s.chars() {
        let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if w + cw > content_w {
            break;
        }
        out.push(c);
        w += cw;
    }
    while w < content_w {
        out.push(' ');
        w += 1;
    }
    out
}

// ── Overlay annotations ─────────────────────────────────────

pub const LABEL_STYLE: Style = Style::new().fg(Color::DarkGray);

pub fn overlay_header_line(overlay: &FieldOverlay, col_widths: &[usize]) -> Line<'static> {
    let total = overlay.fields.len();
    let mut spans: Vec<Span<'static>> = Vec::new();
    for (ci, &cw) in col_widths.iter().enumerate() {
        if ci > 0 {
            spans.push(Span::raw(" │ "));
        }
        let label = overlay
            .fields
            .iter()
            .position(|f| f.path == SchemaPath(smallvec![ci]))
            .map(|idx| field_label(idx, total))
            .map(|l| format!("[{}]", l));
        match label {
            Some(text) => {
                let pad = cw.saturating_sub(text.len());
                spans.push(Span::styled(text, LABEL_STYLE));
                spans.push(Span::raw(" ".repeat(pad)));
            }
            None => spans.push(Span::raw(" ".repeat(cw))),
        }
    }
    Line::from(spans)
}

pub fn overlay_label_for_line(line: &str, overlay: &FieldOverlay) -> Option<String> {
    let total = overlay.fields.len();
    let trimmed = line.trim_start_matches(['│', ' ']);
    let idx = overlay.fields.iter().position(|f| {
        let leaf = f.name.rsplit('.').next().unwrap_or(&f.name);
        trimmed.starts_with(leaf) && trimmed[leaf.len()..].starts_with(": ")
    })?;
    Some(format!("[{}] ", field_label(idx, total)))
}

// ── Preview rendering ────────────────────────────────────────

/// Unified preview widget. Anchored near the cursor line, growing toward
/// the screen center. Large content overlays more of the screen.
pub fn render_preview(
    frame: &mut ratatui::Frame,
    area: Rect,
    preview: &ActivePreview,
    cursor_screen_y: u16,
) {
    let max_h = area.height.saturating_sub(1); // leave status bar
    let desired_h = (preview.lines.len() as u16 + 2).min(max_h).max(3);
    let width = (area.width * 3 / 4).max(20).min(area.width);

    let y = if cursor_screen_y < area.height / 2 {
        // cursor in top half → grow down
        let y = cursor_screen_y + 1;
        if y + desired_h > max_h {
            max_h.saturating_sub(desired_h)
        } else {
            y
        }
    } else {
        // cursor in bottom half → grow up
        cursor_screen_y.saturating_sub(desired_h)
    };

    let height = desired_h.min(max_h.saturating_sub(y));
    let x = area.width.saturating_sub(width) / 2;
    let popup_area = Rect::new(x, y, width, height);

    let inner_h = height.saturating_sub(2) as usize;
    let inner_w = width.saturating_sub(2) as usize;
    let visible_lines: Vec<Line<'static>> = preview
        .lines
        .iter()
        .skip(preview.scroll_offset)
        .take(inner_h)
        .map(|l| Line::from(fit_line(l, inner_w)))
        .collect();

    // Clear 1 extra cell on each side so fullwidth chars from the
    // underlying content that straddle the popup edge get wiped fully
    // (otherwise the second half is cleared but the first half remains
    // as a corrupted half-character artifact at the border).
    let clear_x = x.saturating_sub(1);
    let clear_w = (width + 2).min(area.width.saturating_sub(clear_x));
    frame.render_widget(Clear, Rect::new(clear_x, y, clear_w, height));
    let scrollable = preview.lines.len() > inner_h;
    let title = if scrollable {
        let current = preview.scroll_offset + 1;
        let total = preview.lines.len();
        format!(" {} [{}/{}] ", preview.name, current, total)
    } else {
        format!(" {} ", preview.name)
    };
    let popup = Paragraph::new(visible_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    frame.render_widget(popup, popup_area);

    if scrollable {
        let mut state = ScrollbarState::new(preview.lines.len().saturating_sub(inner_h))
            .position(preview.scroll_offset);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None),
            popup_area.inner(ratatui::layout::Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut state,
        );
    }
}

// ── Line → path resolution ──────────────────────────────────

/// Resolve which schema path a rendered line belongs to, from the field
/// tags `LineWriter` recorded during rendering (`RenderedRow::field_path`)
/// rather than re-parsing guide characters and field names back out of
/// the rendered text.
pub fn resolve_line_path(rendered: &RenderedRow, line_idx: usize) -> Option<SchemaPath> {
    if line_idx >= rendered.line_count() {
        return None;
    }
    let path = rendered.field_path(line_idx);
    if path.is_empty() {
        None
    } else {
        Some(SchemaPath(path))
    }
}

/// Find the first rendered line whose field path matches `target`.
pub fn find_line_for_path(rendered: &RenderedRow, target: &SchemaPath) -> Option<usize> {
    (0..rendered.line_count()).find(|&li| rendered.field_path(li)[..] == target.0[..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{Layout, RenderSpec};
    use crate::render::{LineWriter, render_record};
    use crate::source::DataSource;
    use crate::source::test_support::FakeDataSource;

    fn nested_struct_source() -> FakeDataSource {
        use arrow::array::{Array, Int32Array, StringArray, StructArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let inner = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn arrow::array::Array>,
            ),
            (
                Arc::new(Field::new("desc", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["d"])) as Arc<dyn arrow::array::Array>,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", inner.data_type().clone(), false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![7])) as Arc<dyn arrow::array::Array>,
                Arc::new(inner) as Arc<dyn arrow::array::Array>,
            ],
        )
        .unwrap();
        FakeDataSource::from_batch(batch)
    }

    #[test]
    fn resolve_line_path_uses_field_tags_not_text_parsing() {
        let mut source = nested_struct_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        assert!(!spec.is_table(), "id + nested struct is vertical");

        let mut writer = LineWriter::new();
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let rendered = render_record(&spec, batch, local_row, 0, &mut writer);

        // line 0: "── Row 0 ──"; line 1: "id: 7"; line 2: "│ nested: ";
        // line 3: "│ │ x: 1"; line 4: "│ │ desc: \"d\""
        assert_eq!(
            resolve_line_path(&rendered, 0),
            None,
            "row header has no field"
        );
        assert_eq!(
            resolve_line_path(&rendered, 1),
            Some(SchemaPath(smallvec![0]))
        );
        assert_eq!(
            resolve_line_path(&rendered, 2),
            Some(SchemaPath(smallvec![1]))
        );
        assert_eq!(
            resolve_line_path(&rendered, 3),
            Some(SchemaPath(smallvec![1, 0]))
        );
        assert_eq!(
            resolve_line_path(&rendered, 4),
            Some(SchemaPath(smallvec![1, 1]))
        );
    }

    #[test]
    fn resolve_line_path_out_of_range_is_none() {
        let mut source = FakeDataSource::two_columns(&[("alice", 1)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);

        let mut writer = LineWriter::new();
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let rendered = render_record(&spec, batch, local_row, 0, &mut writer);

        assert_eq!(resolve_line_path(&rendered, 99), None);
    }

    fn field(path: Vec<usize>, name: &str) -> TruncatedField {
        TruncatedField {
            path: SchemaPath(path.into()),
            name: name.to_string(),
        }
    }

    #[test]
    fn wrap_short_string_is_single_line() {
        let lines = wrap_content("hello", 40);
        assert_eq!(lines, vec!["hello"]);
    }

    #[test]
    fn wrap_long_string_wraps_at_width() {
        let s = "a".repeat(100);
        let lines = wrap_content(&s, 40);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].len(), 40);
        assert_eq!(lines[1].len(), 40);
        assert_eq!(lines[2].len(), 20);
    }

    #[test]
    fn wrap_truncates_at_max_physical_lines() {
        let s = "x".repeat(1000);
        let lines = wrap_content(&s, 10);
        assert_eq!(lines.len(), MAX_WRAP_LINES_PER_ENTRY + 1);
        assert_eq!(lines.last().unwrap(), "  ...");
    }

    #[test]
    fn wrap_multiline_wraps_each_independently() {
        let s = format!("{}\n{}", "a".repeat(25), "b".repeat(25));
        let lines = wrap_content(&s, 20);
        assert_eq!(lines.len(), 4);
        assert!(lines[0].starts_with('a'));
        assert!(lines[2].starts_with('b'));
    }

    #[test]
    fn scroll_clamps_to_bounds() {
        let mut p = ActivePreview::new("f".into(), "1\n2\n3\n4\n5".into(), 40);
        p.scroll(100, 5); // inner = 3, max_offset = 2
        assert_eq!(p.scroll_offset, 2);
        p.scroll(-100, 5);
        assert_eq!(p.scroll_offset, 0);
    }

    #[test]
    fn preview_state_dismiss_keeps_last_path() {
        let mut state = PreviewState::new();
        state.last_path = Some(SchemaPath(smallvec![3]));
        state.dismiss();
        assert_eq!(state.last_path, Some(SchemaPath(smallvec![3])));
    }

    #[test]
    fn preview_state_dismiss_resets_phase_to_idle() {
        let mut state = PreviewState::new();
        state.phase = PreviewPhase::Overlay {
            overlay: FieldOverlay {
                row: 0,
                fields: vec![field(vec![0], "a")],
            },
            label_buf: SmallVec::new(),
        };
        state.dismiss();
        assert!(state.overlay().is_none());
        assert!(matches!(state.phase, PreviewPhase::Idle));
    }

    #[test]
    fn overlay_accessor_sees_fallback_during_waiting_for_content_and_preview() {
        let mut state = PreviewState::new();
        let overlay = FieldOverlay {
            row: 5,
            fields: vec![field(vec![1], "b")],
        };
        state.phase = PreviewPhase::WaitingForContent {
            row: 5,
            path: SchemaPath(smallvec![1]),
            fallback: Some(overlay),
        };
        assert_eq!(state.overlay().map(|o| o.row), Some(5));

        let overlay = FieldOverlay {
            row: 5,
            fields: vec![field(vec![1], "b")],
        };
        state.phase = PreviewPhase::Preview {
            active: ActivePreview::new("b".into(), "content".into(), 40),
            fallback: Some(overlay),
        };
        assert_eq!(state.overlay().map(|o| o.row), Some(5));
        assert!(state.active_preview().is_some());
    }

    #[test]
    fn message_accessor_only_set_in_message_phase() {
        let mut state = PreviewState::new();
        assert!(state.message().is_none());
        state.phase = PreviewPhase::Message("no truncated fields".to_string());
        assert_eq!(state.message(), Some("no truncated fields"));
        assert!(state.overlay().is_none());
        assert!(state.active_preview().is_none());
    }

    #[test]
    fn overlay_label_for_matching_fields() {
        let overlay = FieldOverlay {
            row: 0,
            fields: vec![field(vec![0], "a"), field(vec![2, 0], "c.d")],
        };
        assert_eq!(
            overlay_label_for_line("│ a: \"value\"", &overlay),
            Some("[1] ".to_string()),
        );
        assert_eq!(
            overlay_label_for_line("│ │ d: \"value\"", &overlay),
            Some("[2] ".to_string()),
        );
        assert_eq!(overlay_label_for_line("│ b: 42", &overlay), None);
    }
}
