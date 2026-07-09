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
    /// Does NOT clear selected_col — caller must clamp via
    /// `clamp_selected_col` after the move using the new line's table info.
    pub fn move_down(&mut self, heights: &dyn RowHeightProvider, total_rows: usize) -> bool {
        self.visible = true;
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
    /// Does NOT clear selected_col — caller must clamp after the move.
    pub fn move_up(&mut self, heights: &dyn RowHeightProvider) -> bool {
        self.visible = true;
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
    /// At column 0, exits cell selection back to whole-row mode.
    pub fn move_left(&mut self, line_col_count: usize) {
        if line_col_count < 2 {
            return;
        }
        self.visible = true;
        self.selected_col = match self.selected_col {
            Some(0) => None,
            Some(c) => Some(c - 1),
            None => None,
        };
    }

    /// Move column cursor right within a table row.
    /// From whole-row mode (None), enters cell selection at column 0.
    pub fn move_right(&mut self, line_col_count: usize) {
        if line_col_count < 2 {
            return;
        }
        self.visible = true;
        let max_col = line_col_count.saturating_sub(1);
        self.selected_col = Some(self.selected_col.map_or(0, |c| (c + 1).min(max_col)));
    }

    /// Place cursor on the first visible line (viewport top).
    pub fn place_at_first_visible(&mut self, anchor: &ViewportAnchor) {
        self.record = anchor.row();
        self.line = anchor.line_offset();
        self.selected_col = None;
        self.visible = true;
    }

    /// Place cursor on the last visible line (viewport bottom).
    pub fn place_at_last_visible(
        &mut self,
        anchor: &ViewportAnchor,
        heights: &dyn RowHeightProvider,
        visible_height: usize,
    ) {
        let mut row = anchor.row();
        let mut line = anchor.line_offset();
        let mut remaining = visible_height.saturating_sub(1);
        while remaining > 0 {
            let row_height = heights.line_count(row).unwrap_or(1);
            let lines_left_in_row = row_height.saturating_sub(line);
            if remaining < lines_left_in_row {
                line += remaining;
                break;
            }
            remaining -= lines_left_in_row;
            row += 1;
            line = 0;
            if heights.line_count(row).is_none() {
                // Past known data — back up to last known position
                row -= 1;
                line = heights.line_count(row).unwrap_or(1).saturating_sub(1);
                break;
            }
        }
        self.record = row;
        self.line = line;
        self.selected_col = None;
        self.visible = true;
    }

    /// Move cursor N lines in the given direction. Returns true if it moved
    /// at least once.
    pub fn step(
        &mut self,
        dir: CursorDir,
        count: usize,
        heights: &dyn RowHeightProvider,
        total_rows: usize,
    ) -> bool {
        let mut moved = false;
        for _ in 0..count {
            let ok = match dir {
                CursorDir::Down => self.move_down(heights, total_rows),
                CursorDir::Up => self.move_up(heights),
            };
            if !ok {
                break;
            }
            moved = true;
        }
        moved
    }

    /// Clamp selected_col to the actual column count on the current line.
    /// Called after every cursor move to preserve column selection within
    /// tables and clear it on non-table lines.
    pub fn clamp_selected_col(&mut self, column_count: usize) {
        match self.selected_col {
            Some(col) if column_count >= 2 => {
                self.selected_col = Some(col.min(column_count - 1));
            }
            Some(_) => {
                self.selected_col = None;
            }
            None => {}
        }
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

/// Compute the cursor's screen-line distance from the viewport top.
/// Returns None if the cursor is above the viewport.
fn cursor_screen_line(
    anchor: &ViewportAnchor,
    cursor: &CursorState,
    heights: &dyn RowHeightProvider,
) -> Option<usize> {
    if cursor.record < anchor.row() {
        return None;
    }
    if cursor.record == anchor.row() {
        return cursor.line.checked_sub(anchor.line_offset());
    }
    let anchor_row_height = heights.line_count(anchor.row()).unwrap_or(1);
    let mut lines = anchor_row_height.saturating_sub(anchor.line_offset());
    for row in (anchor.row() + 1)..cursor.record {
        lines += heights.line_count(row).unwrap_or(1);
    }
    lines += cursor.line;
    Some(lines)
}

/// Direction the cursor just moved — controls which margin is enforced
/// when the cursor is on-screen. Off-screen cursors always snap.
#[derive(Clone, Copy)]
pub enum CursorDir {
    Down,
    Up,
}

/// Scroll the viewport to keep the cursor visible with a scrolloff margin.
///
/// Margin = min(10, visible_height / 10).
///
/// - **Off-screen** (above or below viewport): always snap the viewport to
///   place the cursor at the appropriate margin edge. No-backjump doesn't
///   apply — the cursor must be visible.
/// - **On-screen**: enforce margin only in the movement direction. Moving
///   down can only scroll down; moving up can only scroll up. This prevents
///   backjumps after operations that deliberately place the cursor at a
///   viewport edge.
pub fn keep_cursor_visible(
    anchor: &mut ViewportAnchor,
    cursor: &CursorState,
    ctx: &NavContext,
    dir: CursorDir,
) {
    let margin = 10usize.min(ctx.visible_height / 10);
    let top_target = margin;
    let bottom_target = ctx.visible_height.saturating_sub(margin + 1);

    match cursor_screen_line(anchor, cursor, ctx.heights) {
        None => {
            snap_cursor_to_line(anchor, cursor, ctx, top_target);
        }
        Some(screen_line) if screen_line >= ctx.visible_height => {
            snap_cursor_to_line(anchor, cursor, ctx, bottom_target);
        }
        Some(screen_line) => match dir {
            CursorDir::Down => {
                if screen_line > bottom_target {
                    let scroll = screen_line - bottom_target;
                    anchor.apply(NavIntent::Scroll(scroll as isize), ctx);
                }
            }
            CursorDir::Up => {
                if screen_line < top_target {
                    let scroll = top_target - screen_line;
                    anchor.apply(NavIntent::Scroll(-(scroll as isize)), ctx);
                }
            }
        },
    }
}

/// Jump anchor to cursor's record, then scroll so cursor.line lands at
/// `target_screen_line`.
fn snap_cursor_to_line(
    anchor: &mut ViewportAnchor,
    cursor: &CursorState,
    ctx: &NavContext,
    target_screen_line: usize,
) {
    anchor.apply(NavIntent::JumpToRecord(cursor.record), ctx);
    // After jump: anchor is at (cursor.record, 0), so cursor sits at
    // screen_line == cursor.line. Adjust to reach target_screen_line.
    if cursor.line < target_screen_line {
        let scroll = target_screen_line - cursor.line;
        anchor.apply(NavIntent::Scroll(-(scroll as isize)), ctx);
    } else if cursor.line > target_screen_line {
        let scroll = cursor.line - target_screen_line;
        anchor.apply(NavIntent::Scroll(scroll as isize), ctx);
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
        // l from None enters cell mode at col 0
        c.move_right(3);
        assert_eq!(c.selected_col, Some(0));
        assert!(c.visible);
        c.move_right(3);
        assert_eq!(c.selected_col, Some(1));
        c.move_right(3);
        assert_eq!(c.selected_col, Some(2));
        c.move_right(3);
        assert_eq!(c.selected_col, Some(2)); // clamped at last col
        c.move_left(3);
        assert_eq!(c.selected_col, Some(1));
        c.move_left(3);
        assert_eq!(c.selected_col, Some(0));
        // h at col 0 exits to row selection
        c.move_left(3);
        assert_eq!(c.selected_col, None);
        // h from None is no-op
        c.move_left(3);
        assert_eq!(c.selected_col, None);
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
    fn move_down_preserves_selected_col() {
        let h = FakeHeights([(0, 5)].into());
        let mut c = CursorState::new();
        c.visible = true;
        c.selected_col = Some(2);
        assert!(c.move_down(&h, 5));
        assert_eq!(c.selected_col, Some(2));
    }

    #[test]
    fn move_up_preserves_selected_col() {
        let h = FakeHeights([(0, 5)].into());
        let mut c = CursorState::new();
        c.visible = true;
        c.line = 3;
        c.selected_col = Some(1);
        assert!(c.move_up(&h));
        assert_eq!(c.selected_col, Some(1));
    }

    #[test]
    fn clamp_selected_col_within_table() {
        let mut c = CursorState::new();
        c.selected_col = Some(5);
        c.clamp_selected_col(3);
        assert_eq!(c.selected_col, Some(2));
    }

    #[test]
    fn clamp_selected_col_clears_on_non_table() {
        let mut c = CursorState::new();
        c.selected_col = Some(2);
        c.clamp_selected_col(0);
        assert_eq!(c.selected_col, None);
    }

    #[test]
    fn clamp_selected_col_clears_on_single_column() {
        let mut c = CursorState::new();
        c.selected_col = Some(0);
        c.clamp_selected_col(1);
        assert_eq!(c.selected_col, None);
    }

    #[test]
    fn clamp_selected_col_noop_when_none() {
        let mut c = CursorState::new();
        c.clamp_selected_col(5);
        assert_eq!(c.selected_col, None);
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
    fn moving_down_enforces_bottom_margin_only() {
        // visible_height=50, margin=5. Cursor at screen line 46 (row 46).
        // Moving down: bottom margin at 50-5-1=44, scroll down by 46-44=2.
        let h: FakeHeights = FakeHeights((0..50).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 50,
            visible_height: 50,
        };
        let mut anchor = ViewportAnchor::top();
        let c = CursorState {
            record: 46,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Down);
        assert_eq!(anchor.row(), 2);
    }

    #[test]
    fn moving_down_does_not_enforce_top_margin() {
        // Cursor at screen line 0 (top of viewport). Moving down should NOT
        // scroll up to create top margin — no backjumps.
        let h: FakeHeights = FakeHeights((0..50).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 50,
            visible_height: 50,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(10), &ctx);
        let c = CursorState {
            record: 10,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Down);
        assert_eq!(anchor.row(), 10); // unchanged — no backjump
    }

    #[test]
    fn moving_up_enforces_top_margin_only() {
        // visible_height=50, margin=5. Anchor at row 10, cursor at row 12.
        // screen_line = 2, which is < margin(5). Moving up: scroll up by 3.
        let h: FakeHeights = FakeHeights((0..50).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 50,
            visible_height: 50,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(10), &ctx);
        let c = CursorState {
            record: 12,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Up);
        assert_eq!(anchor.row(), 7);
    }

    #[test]
    fn moving_up_does_not_enforce_bottom_margin() {
        // Cursor at screen line 49 (last visible). Moving up should NOT
        // scroll down to create bottom margin.
        let h: FakeHeights = FakeHeights((0..50).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 50,
            visible_height: 50,
        };
        let mut anchor = ViewportAnchor::top();
        let c = CursorState {
            record: 49,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Up);
        assert_eq!(anchor.row(), 0); // unchanged — no forward jump
    }

    #[test]
    fn noop_within_margin() {
        // Cursor at screen line 10, margin=5. Both directions: no scroll.
        let h: FakeHeights = FakeHeights((0..50).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 50,
            visible_height: 50,
        };
        let mut anchor = ViewportAnchor::top();
        let c = CursorState {
            record: 10,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Down);
        assert_eq!(anchor.row(), 0);
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Up);
        assert_eq!(anchor.row(), 0);
    }

    #[test]
    fn cursor_above_viewport_moving_up_snaps() {
        // Anchor at row 20, cursor at row 5. Moving up: cursor is above
        // viewport, snap so cursor sits at top margin (screen line 5).
        let h: FakeHeights = FakeHeights((0..50).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 50,
            visible_height: 50,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(20), &ctx);
        let c = CursorState {
            record: 5,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Up);
        assert_eq!(anchor.row(), 0);
    }

    #[test]
    fn cursor_above_viewport_moving_down_also_snaps() {
        // Cursor at row 5, viewport scrolled to row 20 via Ctrl+j.
        // Pressing j (dir=Down) — cursor is off-screen, so snap regardless
        // of direction. No-backjump only applies on-screen.
        let h: FakeHeights = FakeHeights((0..50).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 50,
            visible_height: 50,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(20), &ctx);
        let c = CursorState {
            record: 5,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Down);
        assert_eq!(anchor.row(), 0); // snapped back — cursor must be visible
    }

    #[test]
    fn cursor_below_viewport_snaps() {
        // 100 rows, visible_height=20, margin=2. Anchor at 0, cursor at row 25.
        // screen_line=25 >= visible_height=20, so cursor is below viewport.
        // Should snap to bottom_target = 20 - 2 - 1 = 17.
        let h: FakeHeights = FakeHeights((0..100).map(|i| (i, 1)).collect());
        let ctx = NavContext {
            heights: &h,
            total_rows: 100,
            visible_height: 20,
        };
        let mut anchor = ViewportAnchor::top();
        let c = CursorState {
            record: 25,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Down);
        // cursor at row 25 should be at screen line 17, so anchor at row 8
        assert_eq!(anchor.row(), 8);
    }

    #[test]
    fn zero_margin_for_small_viewport() {
        // visible_height=5, margin=min(10,0)=0. No margin enforcement.
        let h = FakeHeights([(0, 1), (1, 1), (2, 1), (3, 1), (4, 1)].into());
        let ctx = NavContext {
            heights: &h,
            total_rows: 5,
            visible_height: 5,
        };
        let mut anchor = ViewportAnchor::top();
        let c = CursorState {
            record: 4,
            line: 0,
            selected_col: None,
            visible: true,
        };
        keep_cursor_visible(&mut anchor, &c, &ctx, CursorDir::Down);
        assert_eq!(anchor.row(), 0);
    }
}
