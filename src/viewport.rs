use crate::cache::RowCache;

/// Where the viewport is anchored in the dataset.
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
                    self.scroll_down(lines as usize, ctx.cache, ctx.total_rows);
                } else if lines < 0 {
                    self.scroll_up((-lines) as usize, ctx.cache);
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

    fn row_line_count(cache: &RowCache, row: usize) -> Option<usize> {
        cache.get(row).map(|r| r.line_count())
    }

    fn scroll_down(&mut self, count: usize, cache: &RowCache, total_rows: usize) {
        let mut remaining = count;
        while remaining > 0 {
            if let Some(row_lines) = Self::row_line_count(cache, self.row) {
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

    fn scroll_up(&mut self, count: usize, cache: &RowCache) {
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
            if let Some(row_lines) = Self::row_line_count(cache, self.row) {
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
    pub cache: &'a RowCache,
    pub total_rows: usize,
    pub visible_height: usize,
}
