use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState};

use anyhow::Result;

use crate::input::Mode;
use crate::tui::app::App;
use crate::tui::help::render_help_popup;
use crate::tui::preview::{LABEL_STYLE, overlay_header_line, overlay_label_for_line, render_preview};
use crate::tui::style::{style_header_line, style_line};

/// Render the current `App` state into the terminal frame.
///
/// Split out of `app.rs` (which owns state and input handling) purely to
/// keep that file under the project's line-count guidance — this function
/// is pure UI layout/styling and doesn't belong conceptually with the
/// state machine in `handle_action`.
pub(super) fn draw(
    app: &mut App,
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
) -> Result<()> {
    app.draw_had_cache_miss = false;
    app.cursor_line_text.clear();

    let app_ref = &*app;
    let mut cache_miss = false;
    let mut last_row = 0usize;
    let mut clt = String::new();
    let mut cursor_screen_y: u16 = 0;

    terminal.draw(|frame| {
        let area = frame.area();
        let vis_h = area.height.saturating_sub(3) as usize;

        let mut display: Vec<Line> = Vec::with_capacity(vis_h);
        let mut lines_remaining = vis_h;
        let mut screen_line: u16 = 0;

        if app_ref.is_table || app_ref.anchor.is_at_top() {
            for (hi, hline) in app_ref.schema_header.iter().enumerate() {
                if lines_remaining == 0 {
                    break;
                }
                let is_header_row = app_ref.is_table && hi == 0;
                let line = if is_header_row {
                    style_header_line(
                        hline,
                        if app_ref.cursor.visible { app_ref.cursor.selected_col } else { None },
                    )
                } else {
                    Line::from(Span::styled(hline.to_string(), Style::default().fg(Color::Green)))
                };
                display.push(line);
                lines_remaining -= 1;
                screen_line += 1;

                if is_header_row
                    && lines_remaining > 0
                    && let (Some(overlay), Some(col_widths)) =
                        (app_ref.preview.overlay(), app_ref.spec.col_widths())
                {
                    display.push(overlay_header_line(overlay, col_widths));
                    lines_remaining -= 1;
                    screen_line += 1;
                }
            }
        }

        let mut row = app_ref.anchor.row();
        let mut skip = if app_ref.anchor.is_at_top() { 0 } else { app_ref.anchor.line_offset() };

        while lines_remaining > 0 && row < app_ref.total_rows {
            if let Some(rendered) = app_ref.cache.get(row) {
                let overlay_here =
                    app_ref.preview.overlay().filter(|o| o.row == row && !app_ref.is_table);
                for li in skip..rendered.line_count() {
                    if lines_remaining == 0 {
                        break;
                    }
                    let line = rendered.line(li);
                    let is_cursor_line = app_ref.cursor.is_on(row, li);
                    let selected_col = if is_cursor_line { app_ref.cursor.selected_col } else { None };
                    if is_cursor_line {
                        clt.clear();
                        clt.push_str(line);
                        cursor_screen_y = screen_line;
                    }
                    let label_prefix = overlay_here
                        .and_then(|overlay| overlay_label_for_line(line, overlay));
                    let mut styled = style_line(line, row, &app_ref.search, is_cursor_line, selected_col);
                    if let Some(label) = label_prefix {
                        styled.spans.insert(0, Span::styled(label, LABEL_STYLE));
                    }
                    display.push(styled);
                    lines_remaining -= 1;
                    screen_line += 1;
                }
            } else {
                cache_miss = true;
                display.push(Line::from(Span::styled(
                    format!("  Loading row {}...", row),
                    Style::default().fg(Color::DarkGray),
                )));
                lines_remaining -= 1;
                screen_line += 1;
            }

            skip = 0;
            row += 1;
        }
        last_row = row.saturating_sub(1);

        while display.len() < vis_h {
            display.push(Line::from("~"));
        }

        // Status bar
        let status = if app_ref.input.mode() == Mode::Search {
            format!("/{}  ", app_ref.input.search_query())
        } else if let Some(msg) = app_ref.preview.message() {
            msg.to_string()
        } else if app_ref.search.as_ref().is_some_and(|s| s.scanning) {
            let prog = app_ref
                .search
                .as_ref()
                .and_then(|s| s.progress)
                .map_or(String::new(), |r| format!(" (at row {})", r));
            format!("Searching...{}", prog)
        } else {
            let pct = ((app_ref.cursor.record + 1) * 100)
                .checked_div(app_ref.total_rows)
                .unwrap_or(100);
            let count_str = app_ref
                .input
                .pending_count()
                .map_or(String::new(), |n| format!("{}", n));
            let search_info = if let Some(ref s) = app_ref.search {
                format!(
                    " | /{}: {} records, {} in record",
                    s.query,
                    s.match_count_display(),
                    s.record_line_matches.len()
                )
            } else {
                String::new()
            };
            let cursor_info = match app_ref.cursor.selected_col.and_then(|c| app_ref.spec.column_name(c))
            {
                Some(name) => format!(" | [col: {}]", name),
                None => String::new(),
            };
            format!(
                "{}Row {}/{} ({}){}{}",
                count_str,
                app_ref.cursor.record + 1,
                app_ref.total_rows,
                pct,
                cursor_info,
                search_info,
            )
        };

        let block = Block::default()
            .borders(Borders::BOTTOM)
            .title_bottom(Line::from(status).left_aligned());
        frame.render_widget(Paragraph::new(display).block(block), area);

        let mut scrollbar_state =
            ScrollbarState::new(app_ref.total_rows).position(app_ref.anchor.row());
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            area,
            &mut scrollbar_state,
        );

        if app_ref.input.mode() == Mode::Help {
            render_help_popup(frame, area);
        }
        if let Some(active) = app_ref.preview.active_preview() {
            render_preview(frame, area, active, cursor_screen_y);
        }
    })?;

    app.draw_had_cache_miss = cache_miss;
    app.last_visible_row = last_row;
    app.cursor_line_text = clt;
    Ok(())
}
