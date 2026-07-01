use crate::viewport::{NavContext, NavIntent, ViewportAnchor};

/// Tracks which record is "focused" (for future preview) and, in table mode,
/// which column. Independent of the viewport anchor so the cursor can move
/// within an already-visible screen without triggering a scroll.
pub struct CursorState {
    pub current_record: usize,
    pub selected_col: Option<usize>,
}

impl CursorState {
    pub fn new(start_row: usize) -> Self {
        CursorState {
            current_record: start_row,
            selected_col: None,
        }
    }

    pub fn move_left(&mut self) {
        self.selected_col = Some(self.selected_col.map_or(0, |c| c.saturating_sub(1)));
    }

    pub fn move_right(&mut self, column_count: usize) {
        let max_col = column_count.saturating_sub(1);
        self.selected_col = Some(self.selected_col.map_or(0, |c| (c + 1).min(max_col)));
    }
}

/// Scroll the viewport anchor just enough to bring `record` back into the
/// visible range [anchor.row(), last_visible_row]. Cursor movement should
/// feel like moving within the screen, only dragging the viewport when the
/// cursor would otherwise leave it — never re-centering.
pub fn keep_record_visible(
    anchor: &mut ViewportAnchor,
    record: usize,
    last_visible_row: usize,
    ctx: &NavContext,
) {
    if record < anchor.row() {
        anchor.apply(NavIntent::Scroll(-1), ctx);
    } else if record > last_visible_row {
        anchor.apply(NavIntent::Scroll(1), ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn move_left_starts_at_zero() {
        let mut c = CursorState::new(0);
        c.move_left();
        assert_eq!(c.selected_col, Some(0));
    }

    #[test]
    fn move_left_decrements_and_saturates() {
        let mut c = CursorState::new(0);
        c.selected_col = Some(2);
        c.move_left();
        assert_eq!(c.selected_col, Some(1));
        c.move_left();
        assert_eq!(c.selected_col, Some(0));
        c.move_left();
        assert_eq!(c.selected_col, Some(0));
    }

    #[test]
    fn move_right_starts_at_zero() {
        let mut c = CursorState::new(0);
        c.move_right(5);
        assert_eq!(c.selected_col, Some(0));
    }

    #[test]
    fn move_right_increments_and_clamps_to_max() {
        let mut c = CursorState::new(0);
        c.selected_col = Some(0);
        c.move_right(3);
        assert_eq!(c.selected_col, Some(1));
        c.move_right(3);
        assert_eq!(c.selected_col, Some(2));
        c.move_right(3);
        assert_eq!(c.selected_col, Some(2), "should clamp to last column index");
    }

    #[test]
    fn move_right_with_single_column_clamps_to_zero() {
        let mut c = CursorState::new(0);
        c.move_right(1);
        assert_eq!(c.selected_col, Some(0));
        c.move_right(1);
        assert_eq!(c.selected_col, Some(0));
    }

    struct FakeHeights(HashMap<usize, usize>);

    impl crate::viewport::RowHeightProvider for FakeHeights {
        fn line_count(&self, row: usize) -> Option<usize> {
            self.0.get(&row).copied()
        }
    }

    #[test]
    fn keep_record_visible_scrolls_down_past_bottom() {
        let h = FakeHeights([(5, 1), (6, 1)].into());
        let ctx = NavContext {
            heights: &h,
            total_rows: 10,
            visible_height: 5,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(5), &ctx);
        keep_record_visible(&mut anchor, 6, 5, &ctx);
        assert_eq!(anchor.row(), 6);
    }

    #[test]
    fn keep_record_visible_scrolls_up_past_top() {
        let h = FakeHeights([(3, 1), (4, 1)].into());
        let ctx = NavContext {
            heights: &h,
            total_rows: 10,
            visible_height: 5,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(4), &ctx);
        keep_record_visible(&mut anchor, 3, 8, &ctx);
        assert_eq!(anchor.row(), 3);
    }

    #[test]
    fn keep_record_visible_noop_when_already_visible() {
        let h = FakeHeights(HashMap::new());
        let ctx = NavContext {
            heights: &h,
            total_rows: 10,
            visible_height: 5,
        };
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(2), &ctx);
        keep_record_visible(&mut anchor, 4, 8, &ctx);
        assert_eq!(anchor.row(), 2, "record already within [anchor, last_visible] should not move anchor");
    }
}
