use std::sync::{Arc, mpsc};
use std::thread;

use anyhow::Result;
use crossterm::event::{self, Event};
use ratatui::prelude::*;
use smallvec::SmallVec;

use crate::cache::RowCache;
use crate::input::{Action, InputHandler, Mode};
use crate::layout::{Layout, RenderSpec};
use crate::preview::{self, DataPath};
use crate::search::SearchState;
use crate::source::DataSource;
use crate::tui::cursor::{CursorDir, CursorState, keep_cursor_visible};
use crate::tui::draw::draw;
use crate::tui::label::{LabelMatch, resolve_label};
use crate::tui::preview::{ActivePreview, FieldOverlay, PreviewPhase, PreviewState};
use crate::viewport::{
    NavContext, NavIntent, VERTICAL_MODE_LINES_PER_ROW_ESTIMATE, ViewportAnchor,
};
use crate::worker::{WorkerRequest, WorkerResponse, worker_thread};

const SEARCH_BATCH_SIZE: usize = 100;

enum AppEvent {
    Term(Event),
    Worker(WorkerResponse),
}

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

/// `pub(super)` (struct and fields) so `tui::draw` — a sibling module split
/// out to keep this file under the project's line-count guidance — can
/// read them.
pub(super) struct App {
    pub(super) anchor: ViewportAnchor,
    pub(super) input: InputHandler,
    pub(super) cursor: CursorState,
    pub(super) search: Option<SearchState>,
    pub(super) preview: PreviewState,
    pub(super) last_visible_row: usize,
    pub(super) draw_had_cache_miss: bool,

    pub(super) cache: Arc<RowCache>,
    pub(super) spec: Arc<RenderSpec>,
    pub(super) schema_header: Vec<String>,
    pub(super) is_table: bool,
    pub(super) total_rows: usize,
    pub(super) visible_height: usize,
    lookahead: usize,
    terminal_width: usize,
    worker_tx: mpsc::Sender<WorkerRequest>,
}

impl App {
    fn apply_nav(&mut self, intent: NavIntent) {
        let ctx = NavContext {
            heights: &*self.cache,
            total_rows: self.total_rows,
            visible_height: self.visible_height,
        };
        self.anchor.apply(intent, &ctx);
    }

    fn dismiss_preview(&mut self) {
        self.preview.dismiss();
        if matches!(self.input.mode(), Mode::VOverlay | Mode::Preview) {
            self.input.set_mode(Mode::Normal);
        }
    }

    fn move_cursor_to_node(&mut self, row: usize, node: crate::render::NodeRef) {
        self.cursor.visible = true;
        self.cursor.record = row;
        if let Some(rendered) = self.cache.get(row) {
            self.cursor.line = rendered.line_for_node(node).unwrap_or(0);
            self.cursor.selected_col = None;
        }
        self.sync_cursor_visible(CursorDir::Down);
    }

    fn sync_cursor_visible(&mut self, dir: CursorDir) {
        let ctx = NavContext {
            heights: &*self.cache,
            total_rows: self.total_rows,
            visible_height: self.visible_height,
        };
        keep_cursor_visible(&mut self.anchor, &self.cursor, &ctx, dir);
    }

    /// Number of table columns on the cursor's current line, from the
    /// DataNode tree. Works for both top-level and nested table rows.
    fn cursor_column_count(&self) -> usize {
        self.cache
            .get(self.cursor.record)
            .and_then(|r| r.line_table_info(self.cursor.line))
            .map_or(0, |info| info.columns.len())
    }

    /// Clamp or clear the cursor's selected_col based on the current line's
    /// actual table structure. Called after every cursor move.
    fn clamp_cursor_column(&mut self) {
        let col_count = self.cursor_column_count();
        self.cursor.clamp_selected_col(col_count);
    }

    /// Move the viewport and cursor onto a search match and make sure its
    /// row is rendered. Shared by the first-batch jump in
    /// `handle_worker_response` and by `SearchNext`/`SearchPrev`.
    fn jump_to_search_match(&mut self, row: usize, match_line: usize) -> Result<()> {
        self.apply_nav(NavIntent::JumpToMatch { row, match_line });
        self.cursor.record = row;
        self.cursor.line = match_line;
        self.cursor.selected_col = None;
        self.cursor.visible = true;
        self.send_render_range()
    }

    fn send_render_range(&self) -> Result<()> {
        let rows_needed = if self.is_table {
            self.visible_height + self.lookahead
        } else {
            self.visible_height / VERTICAL_MODE_LINES_PER_ROW_ESTIMATE + self.lookahead
        };
        self.worker_tx.send(WorkerRequest::RenderRange {
            start: self.anchor.row(),
            end: (self.anchor.row() + rows_needed).min(self.total_rows),
        })?;
        Ok(())
    }

    fn handle_worker_response(&mut self, resp: WorkerResponse) -> Result<()> {
        match resp {
            WorkerResponse::RowsReady => {}
            WorkerResponse::MatchingRecords {
                matches,
                exhausted,
                scanned_up_to,
            } => {
                let mut jump = None;
                if let Some(ref mut s) = self.search {
                    s.scanning = false;
                    s.progress = None;
                    let first_batch = s.matched_rows.is_empty();
                    s.extend_matches(matches);
                    s.exhausted = exhausted;
                    s.scan_cursor = scanned_up_to;

                    if first_batch && let Some(&row) = s.matched_rows.first() {
                        s.current_idx = 0;
                        s.update_record_matches(self.cache.get(row));
                        let match_line = s.record_line_matches.first().copied().unwrap_or(0);
                        jump = Some((row, match_line));
                    }
                }
                if let Some((row, match_line)) = jump {
                    self.jump_to_search_match(row, match_line)?;
                }
            }
            WorkerResponse::SearchProgress(row) => {
                if let Some(ref mut s) = self.search {
                    s.progress = Some(row);
                }
            }
            WorkerResponse::FieldRendered {
                row,
                path,
                name,
                content,
                line_count,
            } => {
                let matches_pending = matches!(
                    &self.preview.phase,
                    PreviewPhase::WaitingForContent { row: r, path: p, .. }
                        if *r == row && *p == path
                );
                if matches_pending {
                    let fallback =
                        match std::mem::replace(&mut self.preview.phase, PreviewPhase::Idle) {
                            PreviewPhase::WaitingForContent { fallback, .. } => fallback,
                            _ => unreachable!(),
                        };
                    if line_count == 0 {
                        // Failed/empty render: fall back to the overlay that
                        // requested it (if any) rather than leaving a
                        // dangling Normal-mode-but-overlay-drawn mismatch.
                        self.preview.phase = match fallback {
                            Some(overlay) => {
                                self.input.set_mode(Mode::VOverlay);
                                PreviewPhase::Overlay {
                                    overlay,
                                    label_buf: SmallVec::new(),
                                }
                            }
                            None => {
                                self.input.set_mode(Mode::Normal);
                                PreviewPhase::Idle
                            }
                        };
                    } else {
                        let wrap_width = (self.terminal_width * 2 / 3).max(20);
                        let active = ActivePreview::new(name, content, wrap_width);
                        self.input.set_mode(Mode::Preview);
                        self.preview.last_path = Some(path);
                        self.preview.phase = PreviewPhase::Preview { active, fallback };
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_action(&mut self, action: Action) -> Result<bool> {
        match action {
            Action::None => return Ok(false),

            Action::Quit => return Ok(true),

            Action::ScrollLines(n) => {
                self.dismiss_preview();
                self.apply_nav(NavIntent::Scroll(n));
            }
            Action::ScrollHalfPage(n) => {
                self.dismiss_preview();
                self.apply_nav(NavIntent::Scroll(n * (self.visible_height / 2) as isize));
            }

            Action::PrevRecord => {
                self.dismiss_preview();
                self.apply_nav(NavIntent::PrevRecordBoundary);
                self.cursor.jump_to_record(self.anchor.row());
            }
            Action::NextRecord => {
                self.dismiss_preview();
                self.apply_nav(NavIntent::NextRecordBoundary);
                self.cursor.jump_to_record(self.anchor.row());
            }
            Action::JumpToRecord(target) => {
                self.dismiss_preview();
                self.apply_nav(NavIntent::JumpToRecord(target));
                self.cursor.jump_to_record(self.anchor.row());
            }
            Action::JumpPercent(n) => {
                self.dismiss_preview();
                let pct = n.min(100);
                let target = if self.total_rows == 0 {
                    0
                } else {
                    (self.total_rows.saturating_sub(1) * pct) / 100
                };
                self.apply_nav(NavIntent::JumpToRecord(target));
                self.cursor.jump_to_record(self.anchor.row());
            }

            Action::CursorRecordNext | Action::CursorPageDown => {
                self.dismiss_preview();
                let count = if action == Action::CursorPageDown {
                    self.visible_height
                } else {
                    1
                };
                if !self.cursor.visible {
                    self.cursor.place_at_first_visible(&self.anchor);
                } else if self
                    .cursor
                    .step(CursorDir::Down, count, &*self.cache, self.total_rows)
                {
                    self.sync_cursor_visible(CursorDir::Down);
                }
                self.clamp_cursor_column();
            }
            Action::CursorRecordPrev | Action::CursorPageUp => {
                self.dismiss_preview();
                let count = if action == Action::CursorPageUp {
                    self.visible_height
                } else {
                    1
                };
                if !self.cursor.visible {
                    self.cursor.place_at_last_visible(
                        &self.anchor,
                        &*self.cache,
                        self.visible_height,
                    );
                } else if self
                    .cursor
                    .step(CursorDir::Up, count, &*self.cache, self.total_rows)
                {
                    self.sync_cursor_visible(CursorDir::Up);
                }
                self.clamp_cursor_column();
            }
            Action::CellLeft => {
                self.dismiss_preview();
                let cols = self.cursor_column_count();
                self.cursor.move_left(cols);
            }
            Action::CellRight => {
                self.dismiss_preview();
                let cols = self.cursor_column_count();
                self.cursor.move_right(cols);
            }

            Action::EnterSearch | Action::SearchQueryChanged | Action::CancelSearch => {}
            Action::SubmitSearch(query) => {
                self.dismiss_preview();
                if !query.is_empty() {
                    let mut s = SearchState::new(query.clone());
                    s.scan_cursor = 0;
                    s.scanning = true;
                    self.search = Some(s);
                    self.worker_tx.send(WorkerRequest::FindMatchingRecords {
                        query,
                        scan_from: 0,
                        limit: SEARCH_BATCH_SIZE,
                    })?;
                }
            }
            Action::SearchNext => {
                self.dismiss_preview();
                let mut jump = None;
                if let Some(ref mut s) = self.search {
                    if let Some(idx) = s.next_after(self.last_visible_row) {
                        s.current_idx = idx;
                        let row = s.matched_rows[idx];
                        s.update_record_matches(self.cache.get(row));
                        let match_line = s.record_line_matches.first().copied().unwrap_or(0);
                        jump = Some((row, match_line));
                    } else if !s.exhausted {
                        s.scanning = true;
                        self.worker_tx.send(WorkerRequest::FindMatchingRecords {
                            query: s.query.clone(),
                            scan_from: s.scan_cursor,
                            limit: SEARCH_BATCH_SIZE,
                        })?;
                    }
                }
                if let Some((row, match_line)) = jump {
                    self.jump_to_search_match(row, match_line)?;
                }
            }
            Action::SearchPrev => {
                self.dismiss_preview();
                let mut jump = None;
                if let Some(ref mut s) = self.search
                    && let Some(idx) = s.prev_before(self.anchor.row())
                {
                    s.current_idx = idx;
                    let row = s.matched_rows[idx];
                    s.update_record_matches(self.cache.get(row));
                    let match_line = s.record_line_matches.first().copied().unwrap_or(0);
                    jump = Some((row, match_line));
                }
                if let Some((row, match_line)) = jump {
                    self.jump_to_search_match(row, match_line)?;
                }
            }
            Action::DismissOverlay => {
                self.dismiss_preview();
                self.search = None;
                self.cursor.hide();
            }
            Action::DismissPreview => {
                self.preview.phase =
                    match std::mem::replace(&mut self.preview.phase, PreviewPhase::Idle) {
                        PreviewPhase::Preview {
                            fallback: Some(overlay),
                            ..
                        } => PreviewPhase::Overlay {
                            overlay,
                            label_buf: SmallVec::new(),
                        },
                        _ => {
                            self.input.set_mode(Mode::Normal);
                            PreviewPhase::Idle
                        }
                    };
            }

            Action::ShowHelp | Action::DismissHelp => {}

            Action::ShowFieldNumbers => {
                let target = self.cursor.record;
                if let Some(rendered) = self.cache.get(target) {
                    let fields = preview::expandable_fields(&rendered, &self.spec);
                    if fields.is_empty() {
                        self.preview.phase =
                            PreviewPhase::Message("no expandable fields".to_string());
                    } else {
                        self.preview.phase = PreviewPhase::Overlay {
                            overlay: FieldOverlay::new(target, fields, &rendered),
                            label_buf: SmallVec::new(),
                        };
                    }
                }
            }
            Action::OverlayInput(c) => {
                // If we're in Preview with a fallback overlay, promote it
                // so the label char is processed against the overlay.
                if let PreviewPhase::Preview {
                    fallback: Some(_), ..
                } = &self.preview.phase
                {
                    self.preview.phase =
                        match std::mem::replace(&mut self.preview.phase, PreviewPhase::Idle) {
                            PreviewPhase::Preview {
                                fallback: Some(overlay),
                                ..
                            } => PreviewPhase::Overlay {
                                overlay,
                                label_buf: SmallVec::new(),
                            },
                            _ => unreachable!(),
                        };
                }
                if let PreviewPhase::Overlay { overlay, label_buf } = &mut self.preview.phase {
                    label_buf.push(c);
                    let total = overlay.fields.len();
                    match resolve_label(label_buf, total) {
                        LabelMatch::Resolved(idx) => {
                            if let Some(f) = overlay.fields.get(idx) {
                                let overlay_row = overlay.row;
                                let node_ref = f.node;
                                self.move_cursor_to_node(overlay_row, node_ref);
                                let path = self
                                    .cache
                                    .get(overlay_row)
                                    .map(|r| r.data_path(node_ref))
                                    .unwrap_or_else(|| DataPath {
                                        steps: SmallVec::new(),
                                    });
                                let fallback = match std::mem::replace(
                                    &mut self.preview.phase,
                                    PreviewPhase::Idle,
                                ) {
                                    PreviewPhase::Overlay { overlay, .. } => Some(overlay),
                                    _ => unreachable!(),
                                };
                                self.preview.phase = PreviewPhase::WaitingForContent {
                                    row: overlay_row,
                                    path: path.clone(),
                                    fallback,
                                };
                                self.worker_tx.send(WorkerRequest::RenderFullField {
                                    row: overlay_row,
                                    path,
                                })?;
                            }
                        }
                        LabelMatch::Invalid => label_buf.clear(),
                        LabelMatch::Incomplete => {}
                    }
                }
            }
            Action::RepeatPreview => {
                if let Some(path) = self.preview.last_path.clone() {
                    self.preview.phase = PreviewPhase::WaitingForContent {
                        row: self.cursor.record,
                        path: path.clone(),
                        fallback: None,
                    };
                    self.worker_tx.send(WorkerRequest::RenderFullField {
                        row: self.cursor.record,
                        path,
                    })?;
                }
            }
            Action::PreviewCursorCell => {
                let path = self.cache.get(self.cursor.record).and_then(|rendered| {
                    rendered
                        .node_for_position(self.cursor.line, self.cursor.selected_col)
                        .map(|n| rendered.data_path(n))
                });
                if let Some(path) = path {
                    self.preview.last_path = Some(path.clone());
                    self.preview.phase = PreviewPhase::WaitingForContent {
                        row: self.cursor.record,
                        path: path.clone(),
                        fallback: None,
                    };
                    self.worker_tx.send(WorkerRequest::RenderFullField {
                        row: self.cursor.record,
                        path,
                    })?;
                }
            }
            Action::PreviewScroll(n) => {
                if let Some(active) = self.preview.active_preview_mut() {
                    active.scroll(n, self.visible_height as u16);
                }
            }
        }

        self.send_render_range()?;
        Ok(false)
    }
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    mut source: Box<dyn DataSource>,
) -> Result<()> {
    let total_rows = source.total_rows();
    let initial_size = terminal.size()?;
    let terminal_width = initial_size.width as usize;

    let layout = Layout::compute(source.as_mut());
    let spec = Arc::new(RenderSpec::resolve(&layout, terminal_width));
    let is_table = spec.is_table();

    let vertical_header = if is_table {
        Vec::new()
    } else {
        build_schema_header(source.as_ref())
    };
    let schema_header = if is_table {
        spec.render_table_header()
    } else {
        vertical_header.clone()
    };

    let cache = Arc::new(RowCache::new());

    let (event_tx, event_rx) = mpsc::channel::<AppEvent>();

    // Terminal event reader thread
    let term_tx = event_tx.clone();
    thread::spawn(move || {
        while let Ok(ev) = event::read() {
            if term_tx.send(AppEvent::Term(ev)).is_err() {
                break;
            }
        }
    });

    // Worker thread
    let (worker_tx, worker_rx) = mpsc::channel();
    let (response_tx, response_rx) = mpsc::channel();

    let cache_clone = Arc::clone(&cache);
    let spec_clone = Arc::clone(&spec);
    let worker_handle = thread::spawn(move || {
        worker_thread(source, cache_clone, worker_rx, response_tx, spec_clone);
    });

    // Bridge worker responses into the unified event channel
    let bridge_tx = event_tx;
    thread::spawn(move || {
        while let Ok(resp) = response_rx.recv() {
            if bridge_tx.send(AppEvent::Worker(resp)).is_err() {
                break;
            }
        }
    });

    let lookahead = if is_table { 20 } else { 5 };
    let visible_height = initial_size.height.saturating_sub(3) as usize;

    let mut app = App {
        anchor: ViewportAnchor::top(),
        input: InputHandler::new(),
        cursor: CursorState::new(),
        search: None,
        preview: PreviewState::new(),
        last_visible_row: 0,
        draw_had_cache_miss: false,
        cache,
        spec,
        schema_header,
        is_table,
        total_rows,
        visible_height,
        lookahead,
        terminal_width,
        worker_tx,
    };

    app.send_render_range()?;
    draw(&mut app, terminal)?;
    if app.draw_had_cache_miss {
        app.send_render_range()?;
    }

    while let Ok(ev) = event_rx.recv() {
        match ev {
            AppEvent::Worker(resp) => {
                app.handle_worker_response(resp)?;
            }
            AppEvent::Term(Event::Resize(w, h)) => {
                let new_width = w as usize;
                let new_height = h.saturating_sub(3) as usize;
                app.visible_height = new_height;
                if new_width != app.terminal_width {
                    app.terminal_width = new_width;
                    app.spec = Arc::new(RenderSpec::resolve(&layout, new_width));
                    app.schema_header = if is_table {
                        app.spec.render_table_header()
                    } else {
                        vertical_header.clone()
                    };
                    app.cache.clear();
                    app.worker_tx
                        .send(WorkerRequest::UpdateSpec(Arc::clone(&app.spec)))?;
                    app.send_render_range()?;
                }
            }
            AppEvent::Term(Event::Key(key)) => {
                app.input.set_has_active_search(app.search.is_some());
                let action = app.input.handle(key);
                if app.handle_action(action)? {
                    let _ = app.worker_tx.send(WorkerRequest::Shutdown);
                    let _ = worker_handle.join();
                    break;
                }
            }
            _ => continue,
        }

        draw(&mut app, terminal)?;
        if app.draw_had_cache_miss {
            app.send_render_range()?;
        }
    }

    Ok(())
}
