use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::prelude::*;
use ratatui::widgets::{
    Block, Borders, Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
};

use crate::cache::RowCache;
use crate::layout::{Layout, RenderSpec};
use crate::search::SearchState;
use crate::source::DataSource;
use crate::viewport::{NavContext, NavIntent, ViewportAnchor};
use crate::worker::{WorkerRequest, WorkerResponse, worker_thread};

const SEARCH_BATCH_SIZE: usize = 100;

// ============================================================
// TUI entry
// ============================================================

pub fn run_tui(source: Box<dyn DataSource>) -> Result<()> {
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, source);

    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    result
}

fn build_schema_header(source: &dyn DataSource) -> Vec<String> {
    let mut header = Vec::new();
    header.push(format!(
        "Schema: {} columns",
        source.schema().fields().len()
    ));
    for field in source.schema().fields() {
        header.push(format!("  {} : {}", field.name(), field.data_type()));
    }
    header.push(String::new());
    if source.file_count() > 1 {
        header.push(format!(
            "Files: {} files | {} total rows",
            source.file_count(),
            source.total_rows()
        ));
        header.push(String::new());
    }
    header
}

// ============================================================
// Main event loop
// ============================================================

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    mut source: Box<dyn DataSource>,
) -> Result<()> {
    let total_rows = source.total_rows();
    let initial_size = terminal.size()?;
    let mut terminal_width = initial_size.width as usize;

    // Layout is terminal-independent; RenderSpec is resolved per terminal width
    let layout = Layout::compute(source.as_mut());
    let mut spec = Arc::new(RenderSpec::resolve(&layout, terminal_width));
    let is_table = spec.is_table();

    let vertical_header = if is_table {
        Vec::new()
    } else {
        build_schema_header(source.as_ref())
    };
    let mut schema_header = if is_table {
        spec.render_table_header()
    } else {
        vertical_header.clone()
    };

    let cache = Arc::new(RowCache::new());

    let (worker_tx, worker_rx) = mpsc::channel();
    let (response_tx, response_rx) = mpsc::channel();

    let cache_clone = Arc::clone(&cache);
    let spec_clone = Arc::clone(&spec);
    let worker_handle = thread::spawn(move || {
        worker_thread(source, cache_clone, worker_rx, response_tx, spec_clone);
    });

    // Lookahead margin: extra rows beyond visible to pre-render for smooth scrolling.
    // In table mode (1 line/row), we need more rows; in vertical mode fewer.
    let lookahead = if is_table { 20 } else { 5 };

    /// Build a render range: from `start` row, covering enough rows for the screen + margin.
    /// For table mode each row = 1 line so we need ~visible_height rows.
    /// For vertical mode each row = many lines so fewer rows suffice.
    fn render_range_for(
        start: usize,
        visible_height: usize,
        is_table: bool,
        lookahead: usize,
        total: usize,
    ) -> WorkerRequest {
        let rows_needed = if is_table {
            visible_height + lookahead
        } else {
            // Assume ~10 lines per row in vertical mode; overshoot is fine (cached rows are skipped)
            visible_height / 5 + lookahead
        };
        WorkerRequest::RenderRange {
            start,
            end: (start + rows_needed).min(total),
        }
    }

    let mut visible_height = initial_size.height.saturating_sub(3) as usize;
    worker_tx.send(render_range_for(
        0,
        visible_height,
        is_table,
        lookahead,
        total_rows,
    ))?;

    let mut anchor = ViewportAnchor::top();
    let mut pending_count: Option<usize> = None;
    let mut input_buf = String::new();
    let mut input_mode = false;
    let mut show_help = false;

    let mut search: Option<SearchState> = None;
    let mut searching = false; // worker is currently scanning
    let mut search_progress: Option<usize> = None;

    // Tracks the last visible row from the most recent draw pass
    let mut last_visible_row: usize = 0;
    // Set during draw when a cache miss is detected
    let mut draw_had_cache_miss;

    loop {
        // Drain background responses
        while let Ok(resp) = response_rx.try_recv() {
            match resp {
                WorkerResponse::RowsReady => {}
                WorkerResponse::MatchingRecords {
                    matches,
                    exhausted,
                    scanned_up_to,
                } => {
                    searching = false;
                    search_progress = None;
                    if let Some(ref mut s) = search {
                        let first_batch = s.matched_rows.is_empty();
                        s.extend_matches(matches);
                        s.exhausted = exhausted;
                        s.scan_cursor = scanned_up_to;

                        if first_batch && let Some(&row) = s.matched_rows.first() {
                            s.current_idx = 0;
                            s.update_record_matches(cache.get(row));
                            let match_line = s.record_line_matches.first().copied().unwrap_or(0);
                            let ctx = NavContext {
                                heights: &*cache,
                                total_rows,
                                visible_height,
                            };
                            anchor.apply(NavIntent::JumpToMatch { row, match_line }, &ctx);
                            worker_tx.send(render_range_for(
                                row,
                                visible_height,
                                is_table,
                                lookahead,
                                total_rows,
                            ))?;
                        }
                    }
                }
                WorkerResponse::SearchProgress(row) => {
                    search_progress = Some(row);
                }
            }
        }

        // Draw
        draw_had_cache_miss = false;
        terminal.draw(|frame| {
            let area = frame.area();
            visible_height = area.height.saturating_sub(3) as usize;

            let mut display: Vec<Line> = Vec::with_capacity(visible_height);
            let mut lines_remaining = visible_height;

            if is_table || anchor.is_at_top() {
                for hline in &schema_header {
                    if lines_remaining == 0 {
                        break;
                    }
                    display.push(Line::from(Span::styled(
                        hline.to_string(),
                        Style::default().fg(Color::Green),
                    )));
                    lines_remaining -= 1;
                }
            }

            let mut row = anchor.row();
            let mut skip = if anchor.is_at_top() {
                0
            } else {
                anchor.line_offset()
            };

            while lines_remaining > 0 && row < total_rows {
                if let Some(rendered) = cache.get(row) {
                    for li in skip..rendered.line_count() {
                        if lines_remaining == 0 {
                            break;
                        }
                        let line = rendered.line(li);
                        let styled = style_line(line, row, &search);
                        display.push(styled);
                        lines_remaining -= 1;
                    }
                } else {
                    draw_had_cache_miss = true;
                    display.push(Line::from(Span::styled(
                        format!("  Loading row {}...", row),
                        Style::default().fg(Color::DarkGray),
                    )));
                    lines_remaining -= 1;
                }
                skip = 0;
                row += 1;
            }
            last_visible_row = row.saturating_sub(1);

            while display.len() < visible_height {
                display.push(Line::from("~"));
            }

            // Status bar
            let status = if input_mode {
                format!("/{}  ", input_buf)
            } else if searching {
                let prog = search_progress.map_or(String::new(), |r| format!(" (at row {})", r));
                format!("Searching...{}", prog)
            } else {
                let pct = if total_rows == 0 {
                    100
                } else {
                    (anchor.row() + 1) * 100 / total_rows
                };
                let count_str = pending_count.map_or(String::new(), |n| format!("{}", n));
                let search_info = if let Some(ref s) = search {
                    let record_matches = s.record_line_matches.len();
                    format!(
                        " | /{}: {} records, {} in record",
                        s.query,
                        s.match_count_display(),
                        record_matches
                    )
                } else {
                    String::new()
                };
                format!(
                    "{}Row {}/{} ({}){}",
                    count_str,
                    anchor.row() + 1,
                    total_rows,
                    pct,
                    search_info,
                )
            };

            let block = Block::default()
                .borders(Borders::BOTTOM)
                .title_bottom(Line::from(status).left_aligned());

            let paragraph = Paragraph::new(display).block(block);
            frame.render_widget(paragraph, area);

            let mut scrollbar_state = ScrollbarState::new(total_rows).position(anchor.row());
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
            frame.render_stateful_widget(scrollbar, area, &mut scrollbar_state);

            if show_help {
                render_help_popup(frame, area);
            }
        })?;

        // If the draw had cache misses, request the visible range + lookahead.
        if draw_had_cache_miss {
            // last_visible_row is the furthest row the draw loop tried to show.
            // Request from current_row to beyond last_visible_row so the worker
            // covers all missing rows.
            let end = last_visible_row + 1 + lookahead;
            worker_tx.send(WorkerRequest::RenderRange {
                start: anchor.row(),
                end: end.min(total_rows),
            })?;
        }

        if !event::poll(Duration::from_millis(50))? {
            continue;
        }

        match event::read()? {
            Event::Resize(w, _h) => {
                let new_width = w as usize;
                if new_width != terminal_width {
                    terminal_width = new_width;
                    spec = Arc::new(RenderSpec::resolve(&layout, terminal_width));
                    schema_header = if is_table {
                        spec.render_table_header()
                    } else {
                        vertical_header.clone()
                    };
                    cache.clear();
                    worker_tx.send(WorkerRequest::UpdateSpec(Arc::clone(&spec)))?;
                    worker_tx.send(render_range_for(
                        anchor.row(),
                        visible_height,
                        is_table,
                        lookahead,
                        total_rows,
                    ))?;
                }
            }
            Event::Key(key) => {
                // Help popup intercepts all keys
                if show_help {
                    show_help = false;
                    continue;
                }

                if input_mode {
                    match key.code {
                        KeyCode::Enter => {
                            input_mode = false;
                            if !input_buf.is_empty() {
                                let query = input_buf.clone();
                                let mut s = SearchState::new(query.clone());
                                s.scan_cursor = 0;
                                search = Some(s);
                                searching = true;
                                worker_tx.send(WorkerRequest::FindMatchingRecords {
                                    query,
                                    scan_from: 0,
                                    limit: SEARCH_BATCH_SIZE,
                                })?;
                            }
                        }
                        KeyCode::Esc => {
                            input_mode = false;
                            input_buf.clear();
                        }
                        KeyCode::Backspace => {
                            input_buf.pop();
                        }
                        KeyCode::Char(c) => {
                            input_buf.push(c);
                        }
                        _ => {}
                    }
                    continue;
                }

                match key.code {
                    KeyCode::Char('q') | KeyCode::Char('Q') => {
                        let _ = worker_tx.send(WorkerRequest::Shutdown);
                        let _ = worker_handle.join();
                        break;
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        let _ = worker_tx.send(WorkerRequest::Shutdown);
                        let _ = worker_handle.join();
                        break;
                    }

                    // --- Scroll ---
                    KeyCode::Char('j') | KeyCode::Down => {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        anchor.apply(NavIntent::Scroll(1), &ctx);
                    }
                    KeyCode::Char('k') | KeyCode::Up => {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        anchor.apply(NavIntent::Scroll(-1), &ctx);
                    }
                    KeyCode::Char('J') | KeyCode::PageDown => {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        anchor.apply(NavIntent::Scroll(visible_height as isize), &ctx);
                    }
                    KeyCode::Char('K') | KeyCode::PageUp => {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        anchor.apply(NavIntent::Scroll(-(visible_height as isize)), &ctx);
                    }
                    KeyCode::Char(' ') | KeyCode::Char('d')
                        if key.modifiers.contains(KeyModifiers::CONTROL) =>
                    {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        anchor.apply(NavIntent::Scroll((visible_height / 2) as isize), &ctx);
                    }
                    KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        anchor.apply(NavIntent::Scroll(-((visible_height / 2) as isize)), &ctx);
                    }

                    // --- Record navigation ---
                    KeyCode::Char('g') => {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        match pending_count.take() {
                            Some(n) => {
                                let target = n.saturating_sub(1);
                                anchor.apply(NavIntent::JumpToRecord(target), &ctx);
                                worker_tx.send(render_range_for(
                                    anchor.row(),
                                    visible_height,
                                    is_table,
                                    lookahead,
                                    total_rows,
                                ))?;
                            }
                            None => {
                                anchor.apply(NavIntent::PrevRecordBoundary, &ctx);
                            }
                        }
                    }
                    KeyCode::Char('G') => {
                        let ctx = NavContext {
                            heights: &*cache,
                            total_rows,
                            visible_height,
                        };
                        match pending_count.take() {
                            Some(n) => {
                                let target = n.saturating_sub(1);
                                anchor.apply(NavIntent::JumpToRecord(target), &ctx);
                                worker_tx.send(render_range_for(
                                    anchor.row(),
                                    visible_height,
                                    is_table,
                                    lookahead,
                                    total_rows,
                                ))?;
                            }
                            None => {
                                anchor.apply(NavIntent::NextRecordBoundary, &ctx);
                            }
                        }
                    }
                    KeyCode::Char('%') => {
                        if let Some(n) = pending_count.take() {
                            let pct = n.min(100);
                            let target = if total_rows == 0 {
                                0
                            } else {
                                (total_rows.saturating_sub(1) * pct) / 100
                            };
                            let ctx = NavContext {
                                heights: &*cache,
                                total_rows,
                                visible_height,
                            };
                            anchor.apply(NavIntent::JumpToRecord(target), &ctx);
                            worker_tx.send(render_range_for(
                                anchor.row(),
                                visible_height,
                                is_table,
                                lookahead,
                                total_rows,
                            ))?;
                        }
                    }

                    // --- Numeric prefix ---
                    KeyCode::Char(c @ '1'..='9') => {
                        let digit = c as usize - '0' as usize;
                        pending_count = Some(pending_count.unwrap_or(0) * 10 + digit);
                        continue;
                    }
                    KeyCode::Char('0') if pending_count.is_some() => {
                        pending_count = Some(pending_count.unwrap() * 10);
                        continue;
                    }

                    // --- Search ---
                    KeyCode::Char('/') => {
                        input_mode = true;
                        input_buf.clear();
                    }
                    KeyCode::Char('n') => {
                        if let Some(ref mut s) = search {
                            if let Some(idx) = s.next_after(last_visible_row) {
                                s.current_idx = idx;
                                let row = s.matched_rows[idx];
                                s.update_record_matches(cache.get(row));
                                let match_line =
                                    s.record_line_matches.first().copied().unwrap_or(0);
                                let ctx = NavContext {
                                    heights: &*cache,
                                    total_rows,
                                    visible_height,
                                };
                                anchor.apply(NavIntent::JumpToMatch { row, match_line }, &ctx);
                                worker_tx.send(render_range_for(
                                    row,
                                    visible_height,
                                    is_table,
                                    lookahead,
                                    total_rows,
                                ))?;
                            } else if !s.exhausted {
                                searching = true;
                                worker_tx.send(WorkerRequest::FindMatchingRecords {
                                    query: s.query.clone(),
                                    scan_from: s.scan_cursor,
                                    limit: SEARCH_BATCH_SIZE,
                                })?;
                            }
                        }
                    }
                    KeyCode::Char('N') => {
                        if let Some(ref mut s) = search {
                            if let Some(idx) = s.prev_before(anchor.row()) {
                                s.current_idx = idx;
                                let row = s.matched_rows[idx];
                                s.update_record_matches(cache.get(row));
                                let match_line =
                                    s.record_line_matches.first().copied().unwrap_or(0);
                                let ctx = NavContext {
                                    heights: &*cache,
                                    total_rows,
                                    visible_height,
                                };
                                anchor.apply(NavIntent::JumpToMatch { row, match_line }, &ctx);
                                worker_tx.send(render_range_for(
                                    row,
                                    visible_height,
                                    is_table,
                                    lookahead,
                                    total_rows,
                                ))?;
                            }
                        }
                    }
                    KeyCode::Esc => {
                        // Clear search
                        search = None;
                        searching = false;
                        search_progress = None;
                    }

                    KeyCode::Char('?') => {
                        show_help = true;
                    }

                    _ => {}
                }

                pending_count = None;
                worker_tx.send(render_range_for(
                    anchor.row(),
                    visible_height,
                    is_table,
                    lookahead,
                    total_rows,
                ))?;
            }
            _ => {}
        }
    }

    Ok(())
}

// ============================================================
// Help popup
// ============================================================

fn render_help_popup(frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
    let help_text = vec![
        Line::from(Span::styled(
            " dsless ",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            " Scrolling",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  j / Down      line down"),
        Line::from("  k / Up        line up"),
        Line::from("  J / PageDown  page down"),
        Line::from("  K / PageUp    page up"),
        Line::from("  Space/Ctrl-d  half page down"),
        Line::from("  Ctrl-u        half page up"),
        Line::from(""),
        Line::from(Span::styled(
            " Records",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  g             start of record / prev record"),
        Line::from("  G             next record"),
        Line::from("  <N>g / <N>G   go to record N"),
        Line::from("  <N>%          go to N% of dataset"),
        Line::from(""),
        Line::from(Span::styled(
            " Search",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  /             search"),
        Line::from("  n             next match (off-screen)"),
        Line::from("  N             previous match"),
        Line::from("  Esc           clear search"),
        Line::from(""),
        Line::from(Span::styled(
            " Other",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  q / Ctrl-c    quit"),
        Line::from("  ?             this help"),
        Line::from(""),
        Line::from(Span::styled(
            "       press any key to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let height = help_text.len() as u16 + 2; // +2 for borders
    let width = 42;
    let x = area.width.saturating_sub(width) / 2;
    let y = area.height.saturating_sub(height) / 2;
    let popup_area =
        ratatui::layout::Rect::new(x, y, width.min(area.width), height.min(area.height));

    frame.render_widget(Clear, popup_area);
    let popup = Paragraph::new(help_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    frame.render_widget(popup, popup_area);
}

// ============================================================
// Line styling
// ============================================================

fn style_line<'a>(line: &str, row: usize, search: &Option<SearchState>) -> Line<'a> {
    let is_match_row = search
        .as_ref()
        .is_some_and(|s| s.matched_set.contains(&row));

    if line.starts_with("── Row") {
        let style = if is_match_row {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        };
        Line::from(Span::styled(line.to_string(), style))
    } else if is_match_row
        && search
            .as_ref()
            .is_some_and(|s| line.to_lowercase().contains(&s.query_lower))
    {
        Line::from(Span::styled(
            line.to_string(),
            Style::default().bg(Color::DarkGray).fg(Color::White),
        ))
    } else {
        Line::from(line.to_string())
    }
}
