use std::fmt::Write;
use std::sync::{Arc, mpsc};

use arrow::array::*;
use arrow::datatypes::DataType;

use crate::cache::RowCache;
use crate::layout::RenderSpec;
use crate::render::{LineWriter, RenderedRow};
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
    spec: Arc<RenderSpec>,
) {
    let mut writer = LineWriter::new();

    while let Ok(req) = rx.recv() {
        let req = drain_latest(req, &rx);

        match req {
            WorkerRequest::RenderRange { start, end } => {
                let end = end.min(source.total_rows());
                for row in start..end {
                    if cache.contains(row) {
                        continue;
                    }
                    if let Ok(()) = source.ensure_loaded(row) {
                        let rendered = render_one_row(&mut source, row, &mut writer, &spec);
                        cache.put(row, rendered);
                    }

                    if let Ok(newer) = rx.try_recv() {
                        let newer = drain_latest(newer, &rx);
                        render_range(
                            &newer, &mut source, &cache, &mut writer, &spec, &tx,
                        );
                        break;
                    }
                }
                let _ = tx.send(WorkerResponse::RowsReady);
            }
            WorkerRequest::FindMatchingRecords {
                query,
                scan_from,
                limit,
            } => {
                find_matching_records(
                    &mut source,
                    &cache,
                    &query,
                    scan_from,
                    limit,
                    &mut writer,
                    &spec,
                    &tx,
                );
            }
            WorkerRequest::Shutdown => break,
        }
    }
}

fn render_one_row(
    source: &mut Box<dyn DataSource>,
    global_row: usize,
    writer: &mut LineWriter,
    spec: &RenderSpec,
) -> RenderedRow {
    writer.clear();
    if !spec.is_table() {
        let _ = write!(writer, "── Row {} ──", global_row);
        writer.newline();
    }
    let (batch, local_row) = source.get_row(global_row);
    spec.render_row(batch, local_row, writer);
    writer.finish()
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

fn render_range(
    req: &WorkerRequest,
    source: &mut Box<dyn DataSource>,
    cache: &Arc<RowCache>,
    writer: &mut LineWriter,
    spec: &RenderSpec,
    tx: &mpsc::Sender<WorkerResponse>,
) {
    match req {
        WorkerRequest::RenderRange { start, end } => {
            let end = (*end).min(source.total_rows());
            for row in *start..end {
                if cache.contains(row) {
                    continue;
                }
                if let Ok(()) = source.ensure_loaded(row) {
                    let rendered = render_one_row(source, row, writer, spec);
                    cache.put(row, rendered);
                }
            }
            let _ = tx.send(WorkerResponse::RowsReady);
        }
        WorkerRequest::FindMatchingRecords {
            query,
            scan_from,
            limit,
        } => {
            find_matching_records(source, cache, query, *scan_from, *limit, writer, spec, tx);
        }
        WorkerRequest::Shutdown => {}
    }
}

// --- Search: find matching records ---

#[allow(clippy::too_many_arguments)]
fn find_matching_records(
    source: &mut Box<dyn DataSource>,
    cache: &Arc<RowCache>,
    query: &str,
    scan_from: usize,
    limit: usize,
    writer: &mut LineWriter,
    spec: &RenderSpec,
    tx: &mpsc::Sender<WorkerResponse>,
) {
    let query_lower = query.to_lowercase();
    let total = source.total_rows();
    let mut matches = Vec::new();
    let mut last_progress = scan_from;

    let mut cursor = scan_from;
    while cursor < total && matches.len() < limit {
        if cursor.abs_diff(last_progress) >= 1000 {
            let _ = tx.send(WorkerResponse::SearchProgress(cursor));
            last_progress = cursor;
        }

        if source.ensure_loaded(cursor).is_err() {
            cursor += 1;
            continue;
        }

        // Quick parquet-level check
        let (batch, local_row) = source.get_row(cursor);
        if !row_might_match(batch, local_row, &query_lower) {
            cursor += 1;
            continue;
        }

        // Render and verify
        let rendered = ensure_rendered(source, cache, cursor, writer, spec);
        let has_match = (0..rendered.line_count())
            .any(|i| rendered.line(i).to_lowercase().contains(&query_lower));

        if has_match {
            matches.push(cursor);
        }
        cursor += 1;
    }

    let exhausted = cursor >= total;
    let _ = tx.send(WorkerResponse::MatchingRecords {
        matches,
        exhausted,
        scanned_up_to: cursor,
    });
}

fn ensure_rendered(
    source: &mut Box<dyn DataSource>,
    cache: &Arc<RowCache>,
    global_row: usize,
    writer: &mut LineWriter,
    spec: &RenderSpec,
) -> Arc<RenderedRow> {
    if let Some(cached) = cache.get(global_row) {
        return cached;
    }
    let _ = source.ensure_loaded(global_row);
    let rendered = render_one_row(source, global_row, writer, spec);
    // We don't cache search-rendered rows to avoid evicting nearby rows.
    // The RenderAround pass will cache them when the user navigates there.
    Arc::new(rendered)
}

// --- Parquet column matching ---

fn row_might_match(batch: &RecordBatch, local_row: usize, query: &str) -> bool {
    for col in batch.columns() {
        if column_value_contains(col.as_ref(), local_row, query) {
            return true;
        }
    }
    false
}

fn column_value_contains(array: &dyn Array, row: usize, query: &str) -> bool {
    if array.is_null(row) {
        return false;
    }
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(row).to_lowercase().contains(query)
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(row).to_lowercase().contains(query)
        }
        DataType::Struct(_) => {
            let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
            sa.columns()
                .iter()
                .any(|col| column_value_contains(col.as_ref(), row, query))
        }
        DataType::List(_) => {
            let la = array.as_any().downcast_ref::<ListArray>().unwrap();
            let offsets = la.offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = la.values();
            (start..end).any(|i| column_value_contains(values.as_ref(), i, query))
        }
        DataType::LargeList(_) => {
            let la = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let offsets = la.offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = la.values();
            (start..end).any(|i| column_value_contains(values.as_ref(), i, query))
        }
        _ => false,
    }
}
