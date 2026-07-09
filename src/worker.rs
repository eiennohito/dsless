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
    /// Render a full field for clipboard copy (same as RenderFullField but
    /// the response goes to CopyReady instead of FieldRendered).
    RenderCellForCopy {
        row: usize,
        path: DataPath,
    },
    /// Re-render a record at the pre-cached copy width for clipboard copy.
    RenderForCopy {
        row: usize,
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
    CopyReady(String),
}

struct WorkerCtx {
    source: Box<dyn DataSource>,
    cache: Arc<RowCache>,
    writer: LineWriter,
    spec: Arc<RenderSpec>,
    copy_spec: RenderSpec,
    tx: mpsc::Sender<WorkerResponse>,
}

pub fn worker_thread(
    source: Box<dyn DataSource>,
    cache: Arc<RowCache>,
    rx: mpsc::Receiver<WorkerRequest>,
    tx: mpsc::Sender<WorkerResponse>,
    spec: Arc<RenderSpec>,
    copy_spec: RenderSpec,
) {
    let mut ctx = WorkerCtx {
        source,
        cache,
        writer: LineWriter::new(),
        spec,
        copy_spec,
        tx,
    };

    while let Ok(req) = rx.recv() {
        let req = drain_latest(req, &rx);
        if matches!(req, WorkerRequest::Shutdown) {
            break;
        }

        // RenderRange gets a special continuation: peek for one more request
        // right after finishing, so a RowsReady isn't sent (and redrawn on)
        // when there's already newer work queued.
        if let WorkerRequest::RenderRange { start, end } = req {
            ctx.render_range(start, end);
            match rx.try_recv() {
                Ok(newer) => {
                    let newer = drain_latest(newer, &rx);
                    if matches!(newer, WorkerRequest::Shutdown) {
                        break;
                    }
                    ctx.handle_request(newer);
                }
                Err(_) => {
                    let _ = ctx.tx.send(WorkerResponse::RowsReady);
                }
            }
        } else {
            ctx.handle_request(req);
        }
    }
}

impl WorkerCtx {
    fn render_range(&mut self, start: usize, end: usize) {
        let end = end.min(self.source.total_rows());
        for row in start..end {
            if self.cache.contains(row) {
                continue;
            }
            if let Ok((batch, local_row)) = self.source.load_row(row) {
                let rendered =
                    render::render_record(&self.spec, batch, local_row, row, &mut self.writer);
                self.cache.put(row, rendered);
            }
        }
    }

    fn handle_request(&mut self, req: WorkerRequest) {
        match req {
            WorkerRequest::RenderRange { start, end } => {
                self.render_range(start, end);
                let _ = self.tx.send(WorkerResponse::RowsReady);
            }
            WorkerRequest::FindMatchingRecords {
                query,
                scan_from,
                limit,
            } => {
                self.do_search(&query, scan_from, limit);
            }
            WorkerRequest::RenderFullField { row, path } => {
                self.render_full_field(row, path);
            }
            WorkerRequest::RenderCellForCopy { row, path } => {
                self.render_cell_for_copy(row, path);
            }
            WorkerRequest::RenderForCopy { row } => {
                self.render_for_copy(row);
            }
            WorkerRequest::UpdateSpec(new_spec) => {
                self.spec = new_spec;
            }
            WorkerRequest::Shutdown => {}
        }
    }

    fn do_search(&mut self, query: &str, scan_from: usize, limit: usize) {
        let tx = &self.tx;
        let result = search::find_matching_records(
            self.source.as_mut(),
            &self.spec,
            query,
            scan_from,
            limit,
            &mut self.writer,
            &mut |progress| {
                let _ = tx.send(WorkerResponse::SearchProgress(progress));
            },
        );
        let _ = self.tx.send(WorkerResponse::MatchingRecords {
            matches: result.matches,
            exhausted: result.exhausted,
            scanned_up_to: result.scanned_up_to,
        });
    }

    fn render_full_field(&mut self, row: usize, path: DataPath) {
        let result = self
            .source
            .load_row(row)
            .ok()
            .and_then(|(batch, local_row)| {
                preview::render_field_full(&self.spec, batch, local_row, &path, &mut self.writer)
            });

        match result {
            Some((name, rendered)) => {
                let content = rendered.to_text();
                let line_count = rendered.line_count();
                let _ = self.tx.send(WorkerResponse::FieldRendered {
                    row,
                    path,
                    name,
                    content,
                    line_count,
                });
            }
            None => {
                let _ = self.tx.send(WorkerResponse::FieldRendered {
                    row,
                    path,
                    name: String::new(),
                    content: String::new(),
                    line_count: 0,
                });
            }
        }
    }

    fn render_cell_for_copy(&mut self, row: usize, path: DataPath) {
        let text = self
            .source
            .load_row(row)
            .ok()
            .and_then(|(batch, local_row)| {
                preview::render_field_full(&self.spec, batch, local_row, &path, &mut self.writer)
            })
            .map(|(_, rendered)| rendered.to_text())
            .unwrap_or_default();
        let _ = self.tx.send(WorkerResponse::CopyReady(text));
    }

    fn render_for_copy(&mut self, row: usize) {
        let text = self
            .source
            .load_row(row)
            .ok()
            .map(|(batch, local_row)| {
                let rendered =
                    render::render_record(&self.copy_spec, batch, local_row, row, &mut self.writer);
                rendered.to_text()
            })
            .unwrap_or_default();
        let _ = self.tx.send(WorkerResponse::CopyReady(text));
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
