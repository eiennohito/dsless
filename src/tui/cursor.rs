use crate::render::COLUMN_SEPARATOR;
use crate::viewport::{NavContext, NavIntent, RowHeightProvider, ViewportAnchor};

/// Selects a single screen line (and optionally a cell within it).
/// Hidden by default; appears on first cursor-movement key, hidden on Esc.
///
/// In table mode each record = one line, so (record, line 0) is the only
/// option. In vertical mode a record spans many lines; the cursor moves
/// line-by-line through them. When the cursor line contains table-column
/// separators (top-level table or nested table inside vertical mode),
/// h/l selects individual cells.
pub struct CursorState {
    pub record: usize,
    pub line: usize,
    pub selected_col: Option<usize>,
    pub visible: bool,
}

impl CursorState {
    pub fn new() -> Self {
        CursorState {
            record: 0,
            line: 0,
            selected_col: None,
            visible: false,
        }
    }

    pub fn hide(&mut self) {
        self.visible = false;
        self.selected_col = None;
    }

    /// Move cursor down one line. Crosses record boundaries.
    /// Returns true if the cursor actually moved (for viewport sync).
    pub fn move_down(&mut self, heights: &dyn RowHeightProvider, total_rows: usize) -> bool {
        self.visible = true;
        self.selected_col = None;
        if let Some(height) = heights.line_count(self.record) {
            if self.line + 1 < height {
                self.line += 1;
                return true;
            }
            if self.record + 1 < total_rows {
                self.record += 1;
                self.line = 0;
                return true;
            }
        } else if self.record + 1 < total_rows {
            self.record += 1;
            self.line = 0;
            return true;
        }
        false
    }

    /// Move cursor up one line. Crosses record boundaries.
    pub fn move_up(&mut self, heights: &dyn RowHeightProvider) -> bool {
        self.visible = true;
        self.selected_col = None;
        if self.line > 0 {
            self.line -= 1;
            return true;
        }
        if self.record > 0 {
            self.record -= 1;
            self.line = heights
                .line_count(self.record)
                .unwrap_or(1)
                .saturating_sub(1);
            return true;
        }
        false
    }

    /// Jump to the start of a record (e.g. for n/p, g/G, search jumps).
    pub fn jump_to_record(&mut self, record: usize) {
        self.record = record;
        self.line = 0;
        self.selected_col = None;
    }

    /// Move column cursor left within a table row.
    /// `line_col_count` is the number of columns on the current line
    /// (detected from rendered text, not the schema).
    pub fn move_left(&mut self, line_col_count: usize) {
        if line_col_count < 2 {
            return;
        }
        self.visible = true;
        self.selected_col = Some(self.selected_col.map_or(0, |c| c.saturating_sub(1)));
    }

    /// Move column cursor right within a table row.
    pub fn move_right(&mut self, line_col_count: usize) {
        if line_col_count < 2 {
            return;
        }
        self.visible = true;
        let max_col = line_col_count.saturating_sub(1);
        self.selected_col = Some(self.selected_col.map_or(0, |c| (c + 1).min(max_col)));
    }

    /// Check if the cursor is on this (row, line_index) pair.
    pub fn is_on(&self, row: usize, line_index: usize) -> bool {
        self.visible && self.record == row && self.line == line_index
    }
}

impl Default for CursorState {
    fn default() -> Self {
        Self::new()
    }
}

/// Count columns in a rendered line by counting ` │ ` separators.
/// Works for top-level and nested table rows alike.
pub fn line_column_count(line: &str) -> usize {
    if !line.contains(COLUMN_SEPARATOR) {
        return 0;
    }
    line.matches(COLUMN_SEPARATOR).count() + 1
}

/// Scroll the viewport anchor just enough to keep the cursor's current
/// line visible. Only scrolls by 1 line at a time — feels like the cursor
/// drags the viewport edge, never re-centering.
pub fn keep_cursor_visible(
    anchor: &mut ViewportAnchor,
    cursor: &CursorState,
    last_visible_row: usize,
    ctx: &NavContext,
) {
    if cursor.record < anchor.row()
        || (cursor.record == anchor.row() && cursor.line < anchor.line_offset())
    {
        anchor.apply(NavIntent::Scroll(-1), ctx);
    } else if cursor.record > last_visible_row {
        anchor.apply(NavIntent::Scroll(1), ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct FakeHeights(HashMap<usize, usize>);

    impl RowHeightProvider for FakeHeights {
        fn line_count(&self, row: usize) -> Option<usize> {
            self.0.get(&row).copied()
        }
    }

    #[test]
    fn move_down_within_record() {
        // No height info (cache miss) → treated as a 1-line record, jumps
        // straight to the next record.
        let h = FakeHeights(HashMap::new());
        let mut c = CursorState::new();
        c.visible = true;
        assert!(c.move_down(&h, 5));
        assert_eq!(c.record, 1);
        assert_eq!(c.line, 0);
    }

    #[test]
    fn move_down_within_multiline_record() {
        let h = FakeHeights([(0, 5)].into());
        let mut c = CursorState::new();
        c.visible = true;
        assert!(c.move_down(&h, 5));
        assert_eq!(c.record, 0);
        assert_eq!(c.line, 1);
    }

    #[test]
    fn move_down_crosses_multiline_record_boundary() {
        let h = FakeHeights([(0, 2), (1, 3)].into());
        let mut c = CursorState::new();
        c.visible = true;
        c.line = 1; // last line of a 2-line record 0
        assert!(c.move_down(&h, 5));
        assert_eq!(c.record, 1);
        assert_eq!(c.line, 0);
    }

    #[test]
    fn move_up_at_top_stays() {
        let h = FakeHeights(HashMap::new());
        let mut c = CursorState::new();
        c.visible = true;
        assert!(!c.move_up(&h));
        assert_eq!(c.record, 0);
        assert_eq!(c.line, 0);
    }

    #[test]
    fn move_up_within_multiline_record() {
        let h = FakeHeights([(0, 5)].into());
        let mut c = CursorState::new();
        c.visible = true;
        c.line = 2;
        assert!(c.move_up(&h));
        assert_eq!(c.record, 0);
        assert_eq!(c.line, 1);
    }

    #[test]
    fn move_up_crosses_multiline_record_boundary() {
        let h = FakeHeights([(0, 4)].into());
        let mut c = CursorState::new();
        c.visible = true;
        c.record = 1;
        c.line = 0;
        assert!(c.move_up(&h));
        assert_eq!(c.record, 0);
        assert_eq!(c.line, 3); // lands on the last line of the 4-line record
    }

    #[test]
    fn move_down_at_end_stays() {
        let h = FakeHeights(HashMap::new());
        let mut c = CursorState::new();
        c.visible = true;
        c.record = 4;
        assert!(!c.move_down(&h, 5));
        assert_eq!(c.record, 4);
    }

    #[test]
    fn jump_to_record_resets_line() {
        let mut c = CursorState::new();
        c.line = 5;
        c.selected_col = Some(2);
        c.jump_to_record(3);
        assert_eq!(c.record, 3);
        assert_eq!(c.line, 0);
        assert_eq!(c.selected_col, None);
    }

    #[test]
    fn move_left_right_on_table_line() {
        let mut c = CursorState::new();
        c.move_right(3);
        assert_eq!(c.selected_col, Some(0));
        assert!(c.visible);
        c.move_right(3);
        assert_eq!(c.selected_col, Some(1));
        c.move_right(3);
        assert_eq!(c.selected_col, Some(2));
        c.move_right(3);
        assert_eq!(c.selected_col, Some(2)); // clamped
        c.move_left(3);
        assert_eq!(c.selected_col, Some(1));
    }

    #[test]
    fn move_left_right_noop_on_non_table_line() {
        let mut c = CursorState::new();
        c.move_right(0); // 0 columns → no-op
        assert_eq!(c.selected_col, None);
        c.move_right(1); // 1 column → no-op (need ≥2 for selection)
        assert_eq!(c.selected_col, None);
    }

    #[test]
    fn line_column_count_detects_separators() {
        assert_eq!(line_column_count("alice │ 42"), 2);
        assert_eq!(line_column_count("a │ b │ c"), 3);
        assert_eq!(line_column_count("no columns here"), 0);
        assert_eq!(line_column_count("│ not a separator"), 0); // no spaces around │
    }

    #[test]
    fn hide_clears_selection() {
        let mut c = CursorState::new();
        c.visible = true;
        c.selected_col = Some(2);
        c.hide();
        assert!(!c.visible);
        assert_eq!(c.selected_col, None);
    }

    #[test]
    fn is_on_checks_visibility_and_position() {
        let mut c = CursorState::new();
        c.record = 2;
        c.line = 3;
        assert!(!c.is_on(2, 3)); // not visible
        c.visible = true;
        assert!(c.is_on(2, 3));
        assert!(!c.is_on(2, 0));
        assert!(!c.is_on(1, 3));
    }

    #[test]
    fn keep_cursor_visible_scrolls_down() {
        let h = FakeHeights([(5, 1), (6, 1)].into());
        let ctx = NavContext {
            heights: &h,
            total_rows: 10,
            visible_height: 5,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(5), &ctx);
        let c = CursorState {
            record: 6,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, 5, &ctx);
        assert_eq!(anchor.row(), 6);
    }

    #[test]
    fn keep_cursor_visible_noop_when_already_visible() {
        let h = FakeHeights(HashMap::new());
        let ctx = NavContext {
            heights: &h,
            total_rows: 10,
            visible_height: 5,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(2), &ctx);
        let c = CursorState {
            record: 4,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, 8, &ctx);
        assert_eq!(anchor.row(), 2);
    }
}
