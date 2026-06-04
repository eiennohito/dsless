use std::sync::{Arc, mpsc};

use crate::cache::RowCache;
use crate::layout::RenderSpec;
use crate::render::{self, LineWriter};
use crate::search;
use crate::source::DataSource;

pub enum WorkerRequest {
    /// Render rows in `start..end`, skipping already-cached rows.
    RenderRange { start: usize, end: usize },
    /// Find records matching query, scanning from `scan_from`, up to `limit` matches.
    FindMatchingRecords {
        query: String,
        scan_from: usize,
        limit: usize,
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

        match req {
            WorkerRequest::UpdateSpec(new_spec) => {
                spec = new_spec;
            }
            WorkerRequest::RenderRange { start, end } => {
                render_range(&mut source, &cache, &mut writer, &spec, start, end);

                if let Ok(newer) = rx.try_recv() {
                    let newer = drain_latest(newer, &rx);
                    handle_request(newer, &mut source, &cache, &mut writer, &mut spec, &tx);
                } else {
                    let _ = tx.send(WorkerResponse::RowsReady);
                }
            }
            WorkerRequest::FindMatchingRecords {
                query,
                scan_from,
                limit,
            } => {
                do_search(&mut source, &spec, &query, scan_from, limit, &mut writer, &tx);
            }
            WorkerRequest::Shutdown => break,
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
        WorkerRequest::UpdateSpec(new_spec) => {
            *spec = new_spec;
        }
        WorkerRequest::Shutdown => {}
    }
}

fn drain_latest(initial: WorkerRequest, rx: &mpsc::Receiver<WorkerRequest>) -> WorkerRequest {
    let mut latest = initial;
    while let Ok(newer) = rx.try_recv() {
        if matches!(newer, WorkerRequest::Shutdown) {
            return newer;
        }
        latest = newer;
    }
    latest
}
