use std::sync::{Arc, mpsc};

use crate::cache::RowCache;
use crate::layout::RenderSpec;
use crate::preview::{self, DataPath};
use crate::render::{self, LineWriter};
use crate::search;
use crate::source::DataSource;

pub enum WorkerRequest {
    /// Render rows in `start..end`, skipping already-cached rows.
    RenderRange {
        start: usize,
        end: usize,
    },
    /// Find records matching query, scanning from `scan_from`, up to `limit` matches.
    FindMatchingRecords {
        query: String,
        scan_from: usize,
        limit: usize,
    },
    /// Render one field's value with no width constraints, for the preview popup.
    RenderFullField {
        row: usize,
        path: DataPath,
    },
    /// Terminal resized — adopt a new RenderSpec.
    UpdateSpec(Arc<RenderSpec>),
    Shutdown,
}

pub enum WorkerResponse {
    RowsReady,
    /// Result of FindMatchingRecords
    MatchingRecords {
        matches: Vec<usize>,
        exhausted: bool,
        scanned_up_to: usize,
    },
    SearchProgress(usize),
    FieldRendered {
        row: usize,
        path: DataPath,
        name: String,
        content: String,
        line_count: usize,
    },
}

pub fn worker_thread(
    mut source: Box<dyn DataSource>,
    cache: Arc<RowCache>,
    rx: mpsc::Receiver<WorkerRequest>,
    tx: mpsc::Sender<WorkerResponse>,
    mut spec: Arc<RenderSpec>,
) {
    let mut writer = LineWriter::new();

    while let Ok(req) = rx.recv() {
        let req = drain_latest(req, &rx);
        if matches!(req, WorkerRequest::Shutdown) {
            break;
        }

        // RenderRange gets a special continuation: peek for one more request
        // right after finishing, so a RowsReady isn't sent (and redrawn on)
        // when there's already newer work queued.
        if let WorkerRequest::RenderRange { start, end } = req {
            render_range(&mut source, &cache, &mut writer, &spec, start, end);
            match rx.try_recv() {
                Ok(newer) => {
                    let newer = drain_latest(newer, &rx);
                    if matches!(newer, WorkerRequest::Shutdown) {
                        break;
                    }
                    handle_request(newer, &mut source, &cache, &mut writer, &mut spec, &tx);
                }
                Err(_) => {
                    let _ = tx.send(WorkerResponse::RowsReady);
                }
            }
        } else {
            handle_request(req, &mut source, &cache, &mut writer, &mut spec, &tx);
        }
    }
}

fn render_range(
    source: &mut Box<dyn DataSource>,
    cache: &RowCache,
    writer: &mut LineWriter,
    spec: &RenderSpec,
    start: usize,
    end: usize,
) {
    let end = end.min(source.total_rows());
    for row in start..end {
        if cache.contains(row) {
            continue;
        }
        if let Ok(()) = source.ensure_loaded(row) {
            let (batch, local_row) = source.get_row(row);
            let rendered = render::render_record(spec, batch, local_row, row, writer);
            cache.put(row, rendered);
        }
    }
}

fn do_search(
    source: &mut Box<dyn DataSource>,
    spec: &RenderSpec,
    query: &str,
    scan_from: usize,
    limit: usize,
    writer: &mut LineWriter,
    tx: &mpsc::Sender<WorkerResponse>,
) {
    let result = search::find_matching_records(
        source.as_mut(),
        spec,
        query,
        scan_from,
        limit,
        writer,
        &mut |progress| {
            let _ = tx.send(WorkerResponse::SearchProgress(progress));
        },
    );
    let _ = tx.send(WorkerResponse::MatchingRecords {
        matches: result.matches,
        exhausted: result.exhausted,
        scanned_up_to: result.scanned_up_to,
    });
}

fn handle_request(
    req: WorkerRequest,
    source: &mut Box<dyn DataSource>,
    cache: &Arc<RowCache>,
    writer: &mut LineWriter,
    spec: &mut Arc<RenderSpec>,
    tx: &mpsc::Sender<WorkerResponse>,
) {
    match req {
        WorkerRequest::RenderRange { start, end } => {
            render_range(source, cache, writer, spec, start, end);
            let _ = tx.send(WorkerResponse::RowsReady);
        }
        WorkerRequest::FindMatchingRecords {
            query,
            scan_from,
            limit,
        } => {
            do_search(source, spec, &query, scan_from, limit, writer, tx);
        }
        WorkerRequest::RenderFullField { row, path } => {
            render_full_field(source, spec, writer, row, path, tx);
        }
        WorkerRequest::UpdateSpec(new_spec) => {
            *spec = new_spec;
        }
        WorkerRequest::Shutdown => {}
    }
}

fn render_full_field(
    source: &mut Box<dyn DataSource>,
    spec: &RenderSpec,
    writer: &mut LineWriter,
    row: usize,
    path: DataPath,
    tx: &mpsc::Sender<WorkerResponse>,
) {
    let result = source.ensure_loaded(row).ok().and_then(|()| {
        let (batch, local_row) = source.get_row(row);
        preview::render_field_full(spec, batch, local_row, &path, writer)
    });

    match result {
        Some((name, rendered)) => {
            let content = rendered.lines().collect::<Vec<_>>().join("\n");
            let line_count = rendered.line_count();
            let _ = tx.send(WorkerResponse::FieldRendered {
                row,
                path,
                name,
                content,
                line_count,
            });
        }
        None => {
            let _ = tx.send(WorkerResponse::FieldRendered {
                row,
                path,
                name: String::new(),
                content: String::new(),
                line_count: 0,
            });
        }
    }
}

/// Coalesce consecutive `RenderRange` requests into the newest one — the UI
/// sends these on every scroll tick, and only the latest range matters.
/// Every other request type (search, preview, resize, shutdown) represents
/// a discrete user action that must be handled, so draining stops as soon
/// as one is seen instead of silently dropping it.
fn drain_latest(initial: WorkerRequest, rx: &mpsc::Receiver<WorkerRequest>) -> WorkerRequest {
    let mut latest = initial;
    while matches!(latest, WorkerRequest::RenderRange { .. }) {
        match rx.try_recv() {
            Ok(newer) => latest = newer,
            Err(_) => break,
        }
    }
    latest
}
