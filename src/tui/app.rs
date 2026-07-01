use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState};

use crate::cache::RowCache;
use crate::input::{Action, InputHandler, Mode};
use crate::layout::{Layout, RenderSpec};
use crate::search::SearchState;
use crate::source::DataSource;
use crate::tui::cursor::{CursorState, keep_record_visible};
use crate::tui::help::render_help_popup;
use crate::tui::style::{style_header_line, style_line};
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
    let mut input = InputHandler::new();
    let mut cursor = CursorState::new(anchor.row());

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
                            cursor.current_record = row;
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
                for (hi, hline) in schema_header.iter().enumerate() {
                    if lines_remaining == 0 {
                        break;
                    }
                    let is_header_row = is_table && hi == 0;
                    let line = if is_header_row {
                        style_header_line(hline, cursor.selected_col)
                    } else {
                        Line::from(Span::styled(
                            hline.to_string(),
                            Style::default().fg(Color::Green),
                        ))
                    };
                    display.push(line);
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
                        let is_current = row == cursor.current_record;
                        let selected_col = if is_table { cursor.selected_col } else { None };
                        let styled = style_line(line, row, &search, is_current, selected_col);
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
            let status = if input.mode() == Mode::Search {
                format!("/{}  ", input.search_query())
            } else if searching {
                let prog = search_progress.map_or(String::new(), |r| format!(" (at row {})", r));
                format!("Searching...{}", prog)
            } else {
                let pct = if total_rows == 0 {
                    100
                } else {
                    (cursor.current_record + 1) * 100 / total_rows
                };
                let count_str = input.pending_count().map_or(String::new(), |n| format!("{}", n));
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
                let cursor_info = match cursor.selected_col.and_then(|c| spec.column_name(c)) {
                    Some(name) => format!(" | [col: {}]", name),
                    None => String::new(),
                };
                format!(
                    "{}Row {}/{} ({}){}{}",
                    count_str,
                    cursor.current_record + 1,
                    total_rows,
                    pct,
                    cursor_info,
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

            if input.mode() == Mode::Help {
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
                input.set_has_active_search(search.is_some());
                let action = input.handle(key);

                let ctx = NavContext {
                    heights: &*cache,
                    total_rows,
                    visible_height,
                };

                match action {
                    Action::None => continue,

                    Action::Quit => {
                        let _ = worker_tx.send(WorkerRequest::Shutdown);
                        let _ = worker_handle.join();
                        break;
                    }

                    // --- Scroll ---
                    Action::ScrollLines(n) => {
                        anchor.apply(NavIntent::Scroll(n), &ctx);
                    }
                    Action::ScrollPage(n) => {
                        anchor.apply(NavIntent::Scroll(n * visible_height as isize), &ctx);
                    }
                    Action::ScrollHalfPage(n) => {
                        anchor.apply(
                            NavIntent::Scroll(n * (visible_height / 2) as isize),
                            &ctx,
                        );
                    }

                    // --- Record navigation ---
                    Action::PrevRecord => {
                        anchor.apply(NavIntent::PrevRecordBoundary, &ctx);
                        cursor.current_record = anchor.row();
                    }
                    Action::NextRecord => {
                        anchor.apply(NavIntent::NextRecordBoundary, &ctx);
                        cursor.current_record = anchor.row();
                    }
                    Action::JumpToRecord(target) => {
                        anchor.apply(NavIntent::JumpToRecord(target), &ctx);
                        cursor.current_record = anchor.row();
                    }
                    Action::JumpPercent(n) => {
                        let pct = n.min(100);
                        let target = if total_rows == 0 {
                            0
                        } else {
                            (total_rows.saturating_sub(1) * pct) / 100
                        };
                        anchor.apply(NavIntent::JumpToRecord(target), &ctx);
                        cursor.current_record = anchor.row();
                    }

                    // --- Cell cursor ---
                    Action::CursorRecordNext => {
                        if cursor.current_record + 1 < total_rows {
                            cursor.current_record += 1;
                            keep_record_visible(
                                &mut anchor,
                                cursor.current_record,
                                last_visible_row,
                                &ctx,
                            );
                        }
                    }
                    Action::CursorRecordPrev => {
                        if cursor.current_record > 0 {
                            cursor.current_record -= 1;
                            keep_record_visible(
                                &mut anchor,
                                cursor.current_record,
                                last_visible_row,
                                &ctx,
                            );
                        }
                    }
                    Action::CellLeft => cursor.move_left(),
                    Action::CellRight => cursor.move_right(spec.column_count()),

                    // --- Search ---
                    Action::EnterSearch | Action::SearchQueryChanged => {}
                    Action::SubmitSearch(query) => {
                        if !query.is_empty() {
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
                    Action::CancelSearch => {}
                    Action::SearchNext => {
                        if let Some(ref mut s) = search {
                            if let Some(idx) = s.next_after(last_visible_row) {
                                s.current_idx = idx;
                                let row = s.matched_rows[idx];
                                s.update_record_matches(cache.get(row));
                                let match_line =
                                    s.record_line_matches.first().copied().unwrap_or(0);
                                anchor.apply(NavIntent::JumpToMatch { row, match_line }, &ctx);
                                cursor.current_record = row;
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
                    Action::SearchPrev => {
                        if let Some(ref mut s) = search {
                            if let Some(idx) = s.prev_before(anchor.row()) {
                                s.current_idx = idx;
                                let row = s.matched_rows[idx];
                                s.update_record_matches(cache.get(row));
                                let match_line =
                                    s.record_line_matches.first().copied().unwrap_or(0);
                                anchor.apply(NavIntent::JumpToMatch { row, match_line }, &ctx);
                                cursor.current_record = row;
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
                    Action::DismissOverlay => {
                        search = None;
                        searching = false;
                        search_progress = None;
                    }

                    Action::ShowHelp | Action::DismissHelp => {}

                    // --- Preview: Phase 3 will consume these ---
                    Action::ShowFieldNumbers
                    | Action::PreviewField(_)
                    | Action::RepeatPreview
                    | Action::PreviewCursorCell
                    | Action::PreviewScroll(_) => {}
                }

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
