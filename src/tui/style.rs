use ratatui::prelude::*;

use crate::render::COLUMN_SEPARATOR;
use crate::search::SearchState;

/// Background used to mark the row the cursor is currently focused on.
const CURRENT_RECORD_BG: Color = Color::DarkGray;

pub fn style_line<'a>(
    line: &str,
    row: usize,
    search: &Option<SearchState>,
    is_current: bool,
    selected_col: Option<usize>,
) -> Line<'a> {
    let is_match_row = search
        .as_ref()
        .is_some_and(|s| s.matched_set.contains(&row));

    if line.starts_with("── Row") {
        let mut style = if is_match_row {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        };
        if is_current {
            style = style.add_modifier(Modifier::REVERSED);
        }
        let text = if is_current {
            format!("> {}", line)
        } else {
            line.to_string()
        };
        return Line::from(Span::styled(text, style));
    }

    if selected_col.is_some() && line.contains(COLUMN_SEPARATOR) {
        return style_table_row(line, is_current, selected_col);
    }

    if is_current && line.contains(COLUMN_SEPARATOR) {
        return style_table_row(line, true, selected_col);
    }

    if is_match_row
        && search
            .as_ref()
            .is_some_and(|s| line.to_lowercase().contains(&s.query_lower))
    {
        return Line::from(Span::styled(
            line.to_string(),
            Style::default().bg(Color::DarkGray).fg(Color::White),
        ));
    }

    if is_current {
        return Line::from(Span::styled(
            line.to_string(),
            Style::default().bg(CURRENT_RECORD_BG),
        ));
    }

    Line::from(line.to_string())
}

/// Split a table-mode row on the column separator so the selected column's
/// cell can be highlighted independently from the rest of the row.
fn style_table_row<'a>(line: &str, is_current: bool, selected_col: Option<usize>) -> Line<'a> {
    let row_bg = if is_current {
        Style::default().bg(CURRENT_RECORD_BG)
    } else {
        Style::default()
    };
    let cell_style = row_bg.add_modifier(Modifier::BOLD | Modifier::REVERSED);

    let columns: Vec<&str> = line.split(COLUMN_SEPARATOR).collect();
    let mut spans = Vec::with_capacity(columns.len() * 2);
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(COLUMN_SEPARATOR.to_string(), row_bg));
        }
        let style = if selected_col == Some(i) {
            cell_style
        } else {
            row_bg
        };
        spans.push(Span::styled(col.to_string(), style));
    }
    Line::from(spans)
}

/// Highlight the selected column's header cell so the cell cursor is visible
/// even when the current record scrolls out of view.
pub fn style_header_line<'a>(line: &str, selected_col: Option<usize>) -> Line<'a> {
    let base = Style::default().fg(Color::Green);
    let Some(selected) = selected_col else {
        return Line::from(Span::styled(line.to_string(), base));
    };
    if !line.contains(COLUMN_SEPARATOR) {
        return Line::from(Span::styled(line.to_string(), base));
    }

    let cell_style = base.add_modifier(Modifier::BOLD | Modifier::REVERSED);
    let columns: Vec<&str> = line.split(COLUMN_SEPARATOR).collect();
    let mut spans = Vec::with_capacity(columns.len() * 2);
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(COLUMN_SEPARATOR.to_string(), base));
        }
        let style = if i == selected { cell_style } else { base };
        spans.push(Span::styled(col.to_string(), style));
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span_texts(line: &Line) -> Vec<String> {
        line.spans.iter().map(|s| s.content.to_string()).collect()
    }

    #[test]
    fn style_line_plain_row_no_cursor() {
        let line = style_line("alice │ 42", 0, &None, false, None);
        assert_eq!(span_texts(&line), vec!["alice │ 42"]);
    }

    #[test]
    fn style_line_cursor_on_table_row_splits_even_without_col_selected() {
        let line = style_line("alice   │ 42", 0, &None, true, None);
        let texts = span_texts(&line);
        assert_eq!(texts, vec!["alice  ", " │ ", "42"]);
        assert_eq!(line.spans[0].style.bg, Some(CURRENT_RECORD_BG));
    }

    #[test]
    fn style_line_cursor_on_plain_line_gets_background() {
        let line = style_line("│ name: alice", 0, &None, true, None);
        assert_eq!(line.spans.len(), 1);
        assert_eq!(line.spans[0].style.bg, Some(CURRENT_RECORD_BG));
    }

    #[test]
    fn style_line_splits_columns_when_col_selected() {
        let line = style_line("alice   │ 42", 0, &None, false, Some(1));
        let texts = span_texts(&line);
        assert_eq!(texts, vec!["alice  ", " │ ", "42"]);
        // Selected column (index 1 => "42") should be reversed/bold.
        assert!(
            line.spans[2]
                .style
                .add_modifier
                .contains(Modifier::REVERSED)
        );
        assert!(!line.spans[0].style.add_modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn style_line_vertical_row_header_marks_current() {
        let line = style_line("── Row 3 ──", 3, &None, true, None);
        let texts = span_texts(&line);
        assert_eq!(texts, vec!["> ── Row 3 ──"]);
    }

    #[test]
    fn style_line_vertical_row_header_not_current_has_no_prefix() {
        let line = style_line("── Row 3 ──", 3, &None, false, None);
        let texts = span_texts(&line);
        assert_eq!(texts, vec!["── Row 3 ──"]);
    }

    #[test]
    fn style_header_line_no_cursor_is_uniform() {
        let line = style_header_line("name  │ age", None);
        assert_eq!(line.spans.len(), 1);
    }

    #[test]
    fn style_header_line_highlights_selected_column() {
        let line = style_header_line("name  │ age", Some(1));
        let texts = span_texts(&line);
        assert_eq!(texts, vec!["name ", " │ ", "age"]);
        assert!(
            line.spans[2]
                .style
                .add_modifier
                .contains(Modifier::REVERSED)
        );
    }
}
