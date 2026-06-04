use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use rustc_hash::FxHashSet;

use crate::layout::RenderSpec;
use crate::render::{self, LineWriter, RenderedRow};
use crate::source::DataSource;

// ============================================================
// SearchState — tracks search progress and matched rows
// ============================================================

pub struct SearchState {
    pub query: String,
    pub query_lower: String,
    pub matched_rows: Vec<usize>,
    pub matched_set: FxHashSet<usize>,
    pub exhausted: bool,
    pub scan_cursor: usize,
    pub current_idx: usize,
    pub record_line_matches: Vec<usize>,
}

impl SearchState {
    pub fn new(query: String) -> Self {
        let query_lower = query.to_lowercase();
        SearchState {
            query,
            query_lower,
            matched_rows: Vec::new(),
            matched_set: FxHashSet::default(),
            exhausted: false,
            scan_cursor: 0,
            current_idx: 0,
            record_line_matches: Vec::new(),
        }
    }

    pub fn extend_matches(&mut self, matches: Vec<usize>) {
        for &row in &matches {
            self.matched_set.insert(row);
        }
        self.matched_rows.extend(matches);
    }

    pub fn match_count_display(&self) -> String {
        if self.exhausted {
            format!("{}", self.matched_rows.len())
        } else {
            format!("{}+", self.matched_rows.len())
        }
    }

    pub fn update_record_matches(&mut self, rendered: Option<Arc<RenderedRow>>) {
        self.record_line_matches.clear();
        if let Some(rendered) = rendered {
            for i in 0..rendered.line_count() {
                if rendered.line(i).to_lowercase().contains(&self.query_lower) {
                    self.record_line_matches.push(i);
                }
            }
        }
    }

    pub fn next_after(&self, last_visible_row: usize) -> Option<usize> {
        let idx = self.matched_rows.partition_point(|&r| r <= last_visible_row);
        if idx < self.matched_rows.len() {
            Some(idx)
        } else {
            None
        }
    }

    pub fn prev_before(&self, first_visible_row: usize) -> Option<usize> {
        let idx = self.matched_rows.partition_point(|&r| r < first_visible_row);
        idx.checked_sub(1)
    }
}

// ============================================================
// Search algorithm — find matching records in a DataSource
// ============================================================

pub struct SearchResult {
    pub matches: Vec<usize>,
    pub exhausted: bool,
    pub scanned_up_to: usize,
}

pub fn find_matching_records(
    source: &mut dyn DataSource,
    spec: &RenderSpec,
    query: &str,
    scan_from: usize,
    limit: usize,
    writer: &mut LineWriter,
    on_progress: &mut dyn FnMut(usize),
) -> SearchResult {
    let query_lower = query.to_lowercase();
    let total = source.total_rows();
    let mut matches = Vec::new();
    let mut last_progress = scan_from;

    let mut cursor = scan_from;
    while cursor < total && matches.len() < limit {
        if cursor.abs_diff(last_progress) >= 1000 {
            on_progress(cursor);
            last_progress = cursor;
        }

        if source.ensure_loaded(cursor).is_err() {
            cursor += 1;
            continue;
        }

        let (batch, local_row) = source.get_row(cursor);
        if !row_might_match(batch, local_row, &query_lower) {
            cursor += 1;
            continue;
        }

        let rendered = render::render_record(spec, batch, local_row, cursor, writer);
        let has_match = (0..rendered.line_count())
            .any(|i| rendered.line(i).to_lowercase().contains(&query_lower));

        if has_match {
            matches.push(cursor);
        }
        cursor += 1;
    }

    SearchResult {
        matches,
        exhausted: cursor >= total,
        scanned_up_to: cursor,
    }
}

// ============================================================
// Column-level matching
// ============================================================

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{Layout, RenderSpec};
    use crate::source::test_support::FakeDataSource;

    #[test]
    fn column_match_utf8() {
        let array = StringArray::from(vec!["hello world", "foo bar"]);
        assert!(column_value_contains(&array, 0, "hello"));
        assert!(!column_value_contains(&array, 0, "xyz"));
        assert!(column_value_contains(&array, 1, "bar"));
    }

    #[test]
    fn column_match_null() {
        let array = StringArray::from(vec![Some("hello"), None]);
        assert!(!column_value_contains(&array, 1, "hello"));
    }

    #[test]
    fn column_match_case_insensitive() {
        let array = StringArray::from(vec!["Hello World"]);
        assert!(column_value_contains(&array, 0, "hello world"));
    }

    #[test]
    fn row_match_across_columns() {
        let mut source = FakeDataSource::two_columns(&[("alice", 42), ("bob", 7)]);
        source.ensure_loaded(0).unwrap();
        let (batch, _) = source.get_row(0);
        assert!(row_might_match(batch, 0, "alice"));
        assert!(!row_might_match(batch, 0, "xyz"));
    }

    #[test]
    fn find_records_basic() {
        let mut source = FakeDataSource::single_string_column("text", &["hello", "world", "hello world"]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        let mut writer = LineWriter::new();

        let result = find_matching_records(
            &mut source, &spec, "hello", 0, 100, &mut writer, &mut |_| {},
        );

        assert_eq!(result.matches, vec![0, 2]);
        assert!(result.exhausted);
    }

    #[test]
    fn find_records_with_limit() {
        let mut source = FakeDataSource::single_string_column("text", &["a", "a", "a", "a", "a"]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        let mut writer = LineWriter::new();

        let result = find_matching_records(
            &mut source, &spec, "a", 0, 2, &mut writer, &mut |_| {},
        );

        assert_eq!(result.matches.len(), 2);
        assert!(!result.exhausted);
        assert_eq!(result.scanned_up_to, 2);
    }

    #[test]
    fn find_records_scan_from_offset() {
        let mut source = FakeDataSource::single_string_column("text", &["a", "b", "a", "b", "a"]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        let mut writer = LineWriter::new();

        let result = find_matching_records(
            &mut source, &spec, "a", 2, 100, &mut writer, &mut |_| {},
        );

        assert_eq!(result.matches, vec![2, 4]);
        assert!(result.exhausted);
    }

    #[test]
    fn search_state_navigation() {
        let mut s = SearchState::new("test".to_string());
        s.extend_matches(vec![5, 10, 20, 30]);

        assert_eq!(s.next_after(0), Some(0));
        assert_eq!(s.next_after(5), Some(1));
        assert_eq!(s.next_after(10), Some(2));
        assert_eq!(s.next_after(30), None);

        assert_eq!(s.prev_before(30), Some(2));
        assert_eq!(s.prev_before(10), Some(0));
        assert_eq!(s.prev_before(5), None);
    }

    #[test]
    fn search_state_match_count_display() {
        let mut s = SearchState::new("q".to_string());
        s.extend_matches(vec![1, 2, 3]);
        assert_eq!(s.match_count_display(), "3+");

        s.exhausted = true;
        assert_eq!(s.match_count_display(), "3");
    }
}
