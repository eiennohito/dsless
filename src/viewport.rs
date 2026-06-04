/// Trait for querying rendered row heights. Decouples navigation logic
/// from the concrete cache implementation.
pub trait RowHeightProvider {
    fn line_count(&self, row: usize) -> Option<usize>;
}

#[derive(Clone, Copy, Debug)]
pub struct ViewportAnchor {
    row: usize,
    line_offset: usize,
}

impl ViewportAnchor {
    pub fn top() -> Self {
        ViewportAnchor {
            row: 0,
            line_offset: 0,
        }
    }

    pub fn row(&self) -> usize {
        self.row
    }

    pub fn line_offset(&self) -> usize {
        self.line_offset
    }

    pub fn is_at_top(&self) -> bool {
        self.row == 0 && self.line_offset == 0
    }

    pub fn apply(&mut self, intent: NavIntent, ctx: &NavContext) {
        match intent {
            NavIntent::Scroll(lines) => {
                if lines > 0 {
                    self.scroll_down(lines as usize, ctx.heights, ctx.total_rows);
                } else if lines < 0 {
                    self.scroll_up((-lines) as usize, ctx.heights);
                }
            }
            NavIntent::JumpToRecord(row) => {
                self.row = row.min(ctx.total_rows.saturating_sub(1));
                self.line_offset = 0;
            }
            NavIntent::PrevRecordBoundary => {
                if self.line_offset == 0 && self.row > 0 {
                    self.row -= 1;
                }
                self.line_offset = 0;
            }
            NavIntent::NextRecordBoundary => {
                if self.row + 1 < ctx.total_rows {
                    self.row += 1;
                    self.line_offset = 0;
                }
            }
            NavIntent::JumpToMatch { row, match_line } => {
                self.row = row;
                self.line_offset = match_line.saturating_sub(ctx.visible_height / 5);
            }
        }
    }

    fn scroll_down(&mut self, count: usize, heights: &dyn RowHeightProvider, total_rows: usize) {
        let mut remaining = count;
        while remaining > 0 {
            if let Some(row_lines) = heights.line_count(self.row) {
                let lines_below = row_lines.saturating_sub(self.line_offset);
                if remaining < lines_below {
                    self.line_offset += remaining;
                    return;
                }
                remaining -= lines_below;
                if self.row + 1 < total_rows {
                    self.row += 1;
                    self.line_offset = 0;
                } else {
                    self.line_offset = row_lines.saturating_sub(1);
                    return;
                }
            } else {
                if self.row + 1 < total_rows {
                    self.row += 1;
                    self.line_offset = 0;
                }
                return;
            }
        }
    }

    fn scroll_up(&mut self, count: usize, heights: &dyn RowHeightProvider) {
        let mut remaining = count;
        while remaining > 0 {
            if self.line_offset >= remaining {
                self.line_offset -= remaining;
                return;
            }
            remaining -= self.line_offset;
            self.line_offset = 0;
            if self.row == 0 {
                return;
            }
            remaining -= 1;
            self.row -= 1;
            if let Some(row_lines) = heights.line_count(self.row) {
                self.line_offset = row_lines.saturating_sub(1);
            } else {
                return;
            }
        }
    }
}

pub enum NavIntent {
    Scroll(isize),
    JumpToRecord(usize),
    PrevRecordBoundary,
    NextRecordBoundary,
    JumpToMatch { row: usize, match_line: usize },
}

pub struct NavContext<'a> {
    pub heights: &'a dyn RowHeightProvider,
    pub total_rows: usize,
    pub visible_height: usize,
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

    fn ctx<'a>(heights: &'a dyn RowHeightProvider, total_rows: usize) -> NavContext<'a> {
        NavContext {
            heights,
            total_rows,
            visible_height: 10,
        }
    }

    #[test]
    fn scroll_down_single_line_rows() {
        let h = FakeHeights([(0, 1), (1, 1), (2, 1)].into());
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::Scroll(1), &ctx(&h, 3));
        assert_eq!(anchor.row(), 1);
        assert_eq!(anchor.line_offset(), 0);
    }

    #[test]
    fn scroll_down_multi_line_row() {
        let h = FakeHeights([(0, 5), (1, 3)].into());
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::Scroll(3), &ctx(&h, 2));
        assert_eq!(anchor.row(), 0);
        assert_eq!(anchor.line_offset(), 3);
    }

    #[test]
    fn scroll_down_crosses_row_boundary() {
        let h = FakeHeights([(0, 3), (1, 5)].into());
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::Scroll(4), &ctx(&h, 2));
        assert_eq!(anchor.row(), 1);
        assert_eq!(anchor.line_offset(), 1);
    }

    #[test]
    fn scroll_up_within_row() {
        let h = FakeHeights([(0, 5)].into());
        let mut anchor = ViewportAnchor {
            row: 0,
            line_offset: 3,
        };
        anchor.apply(NavIntent::Scroll(-2), &ctx(&h, 1));
        assert_eq!(anchor.row(), 0);
        assert_eq!(anchor.line_offset(), 1);
    }

    #[test]
    fn scroll_up_crosses_row_boundary() {
        let h = FakeHeights([(0, 3), (1, 5)].into());
        let mut anchor = ViewportAnchor {
            row: 1,
            line_offset: 0,
        };
        anchor.apply(NavIntent::Scroll(-2), &ctx(&h, 2));
        assert_eq!(anchor.row(), 0);
        assert_eq!(anchor.line_offset(), 1);
    }

    #[test]
    fn scroll_up_stops_at_top() {
        let h = FakeHeights([(0, 3)].into());
        let mut anchor = ViewportAnchor {
            row: 0,
            line_offset: 1,
        };
        anchor.apply(NavIntent::Scroll(-100), &ctx(&h, 1));
        assert_eq!(anchor.row(), 0);
        assert_eq!(anchor.line_offset(), 0);
    }

    #[test]
    fn scroll_down_stops_at_end() {
        let h = FakeHeights([(0, 1), (1, 1), (2, 1)].into());
        let mut anchor = ViewportAnchor {
            row: 2,
            line_offset: 0,
        };
        anchor.apply(NavIntent::Scroll(10), &ctx(&h, 3));
        assert_eq!(anchor.row(), 2);
    }

    #[test]
    fn jump_to_record() {
        let h = FakeHeights(HashMap::new());
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(5), &ctx(&h, 10));
        assert_eq!(anchor.row(), 5);
        assert_eq!(anchor.line_offset(), 0);
    }

    #[test]
    fn jump_to_record_clamps_to_last() {
        let h = FakeHeights(HashMap::new());
        let mut anchor = ViewportAnchor::top();
        anchor.apply(NavIntent::JumpToRecord(100), &ctx(&h, 10));
        assert_eq!(anchor.row(), 9);
    }

    #[test]
    fn prev_record_resets_offset() {
        let h = FakeHeights(HashMap::new());
        let mut anchor = ViewportAnchor {
            row: 5,
            line_offset: 3,
        };
        anchor.apply(NavIntent::PrevRecordBoundary, &ctx(&h, 10));
        assert_eq!(anchor.row(), 5);
        assert_eq!(anchor.line_offset(), 0);
    }

    #[test]
    fn prev_record_at_start_goes_back() {
        let h = FakeHeights(HashMap::new());
        let mut anchor = ViewportAnchor {
            row: 5,
            line_offset: 0,
        };
        anchor.apply(NavIntent::PrevRecordBoundary, &ctx(&h, 10));
        assert_eq!(anchor.row(), 4);
        assert_eq!(anchor.line_offset(), 0);
    }

    #[test]
    fn next_record_boundary() {
        let h = FakeHeights(HashMap::new());
        let mut anchor = ViewportAnchor {
            row: 3,
            line_offset: 5,
        };
        anchor.apply(NavIntent::NextRecordBoundary, &ctx(&h, 10));
        assert_eq!(anchor.row(), 4);
        assert_eq!(anchor.line_offset(), 0);
    }

    #[test]
    fn page_scroll_multi_line_rows() {
        let h = FakeHeights([(0, 4), (1, 4), (2, 4), (3, 4)].into());
        let mut anchor = ViewportAnchor::top();
        // Page down (10 lines) across 4-line rows
        anchor.apply(NavIntent::Scroll(10), &ctx(&h, 4));
        assert_eq!(anchor.row(), 2);
        assert_eq!(anchor.line_offset(), 2);
    }
}
