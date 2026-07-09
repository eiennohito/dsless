use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

pub const LABEL_CHARS: &[u8] = b"1234567890wertyuio";

fn is_label_char(c: char) -> bool {
    c.is_ascii() && LABEL_CHARS.contains(&(c as u8))
}

// ============================================================
// Mode — what the handler is currently interpreting keys as
// ============================================================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Normal,
    Search,
    Help,
    VOverlay, // field numbers shown after `v` — digits + v complete the selection
    Preview,  // field preview popup open — j/k scroll internally, Esc dismisses
}

// ============================================================
// Action — what the app should do, decoupled from which key caused it
// ============================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    None,
    Quit,

    // Viewport scrolling
    ScrollLines(isize),
    ScrollHalfPage(isize),

    // Record navigation
    PrevRecord,
    NextRecord,
    JumpToRecord(usize),
    JumpPercent(usize),

    // Search
    EnterSearch,
    SubmitSearch(String),
    CancelSearch,
    SearchQueryChanged,
    SearchNext,
    SearchPrev,
    DismissOverlay,

    // Row/line cursor
    CursorRecordNext,
    CursorRecordPrev,
    CursorPageDown,
    CursorPageUp,

    // Cell (column) cursor
    CellLeft,
    CellRight,

    // Preview
    ShowFieldNumbers,
    OverlayInput(char),
    DismissPreview,
    RepeatPreview,
    PreviewCursorCell,
    PreviewScroll(isize),

    // Meta
    ShowHelp,
    DismissHelp,
}

// ============================================================
// InputHandler — maps raw key events to Actions, owns modal state
// ============================================================

pub struct InputHandler {
    mode: Mode,
    pending_count: Option<usize>,
    search_buf: String,
    has_active_search: bool,
}

impl InputHandler {
    pub fn new() -> Self {
        InputHandler {
            mode: Mode::Normal,
            pending_count: None,
            search_buf: String::new(),
            has_active_search: false,
        }
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn search_query(&self) -> &str {
        &self.search_buf
    }

    /// Exposed for status-bar rendering only; tui.rs never mutates this.
    pub fn pending_count(&self) -> Option<usize> {
        self.pending_count
    }

    pub fn set_has_active_search(&mut self, v: bool) {
        self.has_active_search = v;
    }

    /// Lets the app force a mode transition when an async event (not a key
    /// press) changes what input should mean next — e.g. entering Preview
    /// mode once `V`/Space's worker response arrives with content to show.
    pub fn set_mode(&mut self, mode: Mode) {
        self.mode = mode;
    }

    pub fn handle(&mut self, key: KeyEvent) -> Action {
        match self.mode {
            Mode::Help => {
                self.mode = Mode::Normal;
                Action::DismissHelp
            }
            Mode::Search => self.handle_search(key),
            Mode::VOverlay => self.handle_voverlay(key),
            Mode::Preview => self.handle_preview(key),
            Mode::Normal => self.handle_normal(key),
        }
    }

    fn handle_search(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Enter => {
                self.mode = Mode::Normal;
                let query = std::mem::take(&mut self.search_buf);
                Action::SubmitSearch(query)
            }
            KeyCode::Esc => {
                self.mode = Mode::Normal;
                self.search_buf.clear();
                Action::CancelSearch
            }
            KeyCode::Backspace => {
                self.search_buf.pop();
                Action::SearchQueryChanged
            }
            KeyCode::Char(c) => {
                self.search_buf.push(c);
                Action::SearchQueryChanged
            }
            _ => Action::None,
        }
    }

    fn handle_voverlay(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Char('v') | KeyCode::Esc => {
                self.mode = Mode::Normal;
                Action::DismissOverlay
            }
            KeyCode::Char(c) if is_label_char(c) => Action::OverlayInput(c),
            KeyCode::Char('q') => {
                self.mode = Mode::Normal;
                Action::Quit
            }
            _ => {
                self.mode = Mode::Normal;
                let followup = self.handle_normal(key);
                if followup == Action::None {
                    Action::DismissOverlay
                } else {
                    followup
                }
            }
        }
    }

    fn handle_preview(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => Action::PreviewScroll(1),
            KeyCode::Char('k') | KeyCode::Up => Action::PreviewScroll(-1),
            KeyCode::Char('v') | KeyCode::Char(' ') | KeyCode::Esc => {
                self.mode = Mode::VOverlay;
                Action::DismissPreview
            }
            KeyCode::Char('q') => {
                self.mode = Mode::Normal;
                Action::Quit
            }
            KeyCode::Char('p') => {
                self.mode = Mode::Normal;
                Action::PrevRecord
            }
            KeyCode::Char(c) if is_label_char(c) => {
                self.mode = Mode::VOverlay;
                Action::OverlayInput(c)
            }
            _ => Action::None,
        }
    }

    fn handle_normal(&mut self, key: KeyEvent) -> Action {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        // Numeric prefix accumulation: digits accumulate and return early,
        // leaving pending_count intact for the next key.
        if let KeyCode::Char(c @ '1'..='9') = key.code {
            let digit = c as usize - '0' as usize;
            self.pending_count = Some(self.pending_count.unwrap_or(0) * 10 + digit);
            return Action::None;
        }
        if key.code == KeyCode::Char('0') && self.pending_count.is_some() {
            self.pending_count = Some(self.pending_count.unwrap() * 10);
            return Action::None;
        }

        let count = self.pending_count.take();

        let action = match key.code {
            KeyCode::Char('q') | KeyCode::Char('Q') => Action::Quit,
            KeyCode::Char('c') if ctrl => Action::Quit,

            // --- Scroll / cursor (ctrl variants must be checked before the
            // plain j/k/h/l/space arms below, since match arms are ordered) ---
            KeyCode::Char(' ') if ctrl => Action::ScrollHalfPage(1),
            KeyCode::Char('d') if ctrl => Action::ScrollHalfPage(1),
            KeyCode::Char('u') if ctrl => Action::ScrollHalfPage(-1),
            KeyCode::Char('h') if ctrl => Action::CellLeft,
            KeyCode::Char('l') if ctrl => Action::CellRight,
            KeyCode::Char('j') if ctrl => Action::ScrollLines(1),
            KeyCode::Char('k') if ctrl => Action::ScrollLines(-1),

            KeyCode::Char('j') | KeyCode::Down => Action::CursorRecordNext,
            KeyCode::Char('k') | KeyCode::Up => Action::CursorRecordPrev,
            KeyCode::Char('J') | KeyCode::PageDown => Action::CursorPageDown,
            KeyCode::Char('K') | KeyCode::PageUp => Action::CursorPageUp,

            // --- Cell cursor ---
            KeyCode::Char(' ') => Action::PreviewCursorCell,
            KeyCode::Char('h') => Action::CellLeft,
            KeyCode::Char('l') => Action::CellRight,

            // --- Record navigation ---
            KeyCode::Char('g') => match count {
                Some(n) => Action::JumpToRecord(n.saturating_sub(1)),
                None => Action::PrevRecord,
            },
            KeyCode::Char('G') => match count {
                Some(n) => Action::JumpToRecord(n.saturating_sub(1)),
                None => Action::NextRecord,
            },
            KeyCode::Char('%') => match count {
                Some(n) => Action::JumpPercent(n),
                None => Action::None,
            },

            // --- Preview (Phase 2/3) ---
            KeyCode::Char('v') => {
                self.mode = Mode::VOverlay;
                Action::ShowFieldNumbers
            }
            KeyCode::Char('V') => Action::RepeatPreview,

            // --- Search ---
            KeyCode::Char('/') => {
                self.mode = Mode::Search;
                self.search_buf.clear();
                Action::EnterSearch
            }
            KeyCode::Char('n') => {
                if self.has_active_search {
                    Action::SearchNext
                } else {
                    Action::NextRecord
                }
            }
            KeyCode::Char('N') => {
                if self.has_active_search {
                    Action::SearchPrev
                } else {
                    Action::PrevRecord
                }
            }
            KeyCode::Char('p') => Action::PrevRecord,
            KeyCode::Esc => Action::DismissOverlay,

            KeyCode::Char('?') => {
                self.mode = Mode::Help;
                Action::ShowHelp
            }

            _ => Action::None,
        };

        self.pending_count = None;
        action
    }
}

impl Default for InputHandler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_ctrl(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::CONTROL)
    }

    fn ch(c: char) -> KeyEvent {
        key(KeyCode::Char(c))
    }

    #[test]
    fn numeric_prefix_accumulates() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('1')), Action::None);
        assert_eq!(h.handle(ch('2')), Action::None);
        assert_eq!(h.handle(ch('g')), Action::JumpToRecord(11));
    }

    #[test]
    fn numeric_prefix_with_zero() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('1')), Action::None);
        assert_eq!(h.handle(ch('0')), Action::None);
        assert_eq!(h.handle(ch('g')), Action::JumpToRecord(9));
    }

    #[test]
    fn leading_zero_is_not_a_prefix() {
        // '0' with no pending count is not consumed as a digit (vim: 0 = start of line,
        // here unmapped) — falls through to default and clears any (nonexistent) prefix.
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('0')), Action::None);
        assert_eq!(h.handle(ch('g')), Action::PrevRecord);
    }

    #[test]
    fn pending_count_clears_after_non_digit_key() {
        let mut h = InputHandler::new();
        h.handle(ch('5'));
        h.handle(ch('j')); // non-digit, consumes+clears prefix
        // prefix should be gone now
        assert_eq!(h.handle(ch('g')), Action::PrevRecord);
    }

    #[test]
    fn g_without_prefix_is_prev_record() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('g')), Action::PrevRecord);
    }

    #[test]
    fn shift_g_without_prefix_is_next_record() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('G')), Action::NextRecord);
    }

    #[test]
    fn prefixed_shift_g_jumps() {
        let mut h = InputHandler::new();
        h.handle(ch('4'));
        h.handle(ch('2'));
        assert_eq!(h.handle(ch('G')), Action::JumpToRecord(41));
    }

    #[test]
    fn percent_jump_needs_prefix() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('%')), Action::None);
        h.handle(ch('5'));
        h.handle(ch('0'));
        assert_eq!(h.handle(ch('%')), Action::JumpPercent(50));
    }

    #[test]
    fn slash_enters_search_mode() {
        let mut h = InputHandler::new();
        assert_eq!(h.mode(), Mode::Normal);
        assert_eq!(h.handle(ch('/')), Action::EnterSearch);
        assert_eq!(h.mode(), Mode::Search);
    }

    #[test]
    fn esc_in_search_cancels_to_normal() {
        let mut h = InputHandler::new();
        h.handle(ch('/'));
        assert_eq!(h.handle(key(KeyCode::Esc)), Action::CancelSearch);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn esc_in_normal_dismisses_overlay() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(key(KeyCode::Esc)), Action::DismissOverlay);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn search_buffer_builds_and_submits() {
        let mut h = InputHandler::new();
        h.handle(ch('/'));
        for c in "hello".chars() {
            assert_eq!(h.handle(ch(c)), Action::SearchQueryChanged);
        }
        assert_eq!(h.search_query(), "hello");
        assert_eq!(
            h.handle(key(KeyCode::Enter)),
            Action::SubmitSearch("hello".to_string())
        );
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn search_buffer_backspace() {
        let mut h = InputHandler::new();
        h.handle(ch('/'));
        h.handle(ch('a'));
        h.handle(ch('b'));
        h.handle(key(KeyCode::Backspace));
        assert_eq!(h.search_query(), "a");
    }

    #[test]
    fn n_with_active_search_is_search_next() {
        let mut h = InputHandler::new();
        h.set_has_active_search(true);
        assert_eq!(h.handle(ch('n')), Action::SearchNext);
    }

    #[test]
    fn n_without_active_search_is_next_record() {
        let mut h = InputHandler::new();
        h.set_has_active_search(false);
        assert_eq!(h.handle(ch('n')), Action::NextRecord);
    }

    #[test]
    fn shift_n_with_active_search_is_search_prev() {
        let mut h = InputHandler::new();
        h.set_has_active_search(true);
        assert_eq!(h.handle(ch('N')), Action::SearchPrev);
    }

    #[test]
    fn shift_n_without_active_search_is_prev_record() {
        let mut h = InputHandler::new();
        h.set_has_active_search(false);
        assert_eq!(h.handle(ch('N')), Action::PrevRecord);
    }

    #[test]
    fn p_is_prev_record() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('p')), Action::PrevRecord);
    }

    #[test]
    fn help_mode_dismisses_on_any_key() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('?')), Action::ShowHelp);
        assert_eq!(h.mode(), Mode::Help);
        assert_eq!(h.handle(ch('x')), Action::DismissHelp);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn quit_keys() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('q')), Action::Quit);
        assert_eq!(h.handle(ch('Q')), Action::Quit);
        assert_eq!(h.handle(key_ctrl(KeyCode::Char('c'))), Action::Quit);
    }

    #[test]
    fn cursor_movement_keys() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('j')), Action::CursorRecordNext);
        assert_eq!(h.handle(key(KeyCode::Down)), Action::CursorRecordNext);
        assert_eq!(h.handle(ch('k')), Action::CursorRecordPrev);
        assert_eq!(h.handle(key(KeyCode::Up)), Action::CursorRecordPrev);
        assert_eq!(h.handle(ch('J')), Action::CursorPageDown);
        assert_eq!(h.handle(key(KeyCode::PageDown)), Action::CursorPageDown);
        assert_eq!(h.handle(ch('K')), Action::CursorPageUp);
        assert_eq!(h.handle(key(KeyCode::PageUp)), Action::CursorPageUp);
    }

    #[test]
    fn scroll_keys() {
        let mut h = InputHandler::new();
        assert_eq!(
            h.handle(key_ctrl(KeyCode::Char('j'))),
            Action::ScrollLines(1)
        );
        assert_eq!(
            h.handle(key_ctrl(KeyCode::Char('k'))),
            Action::ScrollLines(-1)
        );
        assert_eq!(
            h.handle(key_ctrl(KeyCode::Char('d'))),
            Action::ScrollHalfPage(1)
        );
        assert_eq!(
            h.handle(key_ctrl(KeyCode::Char('u'))),
            Action::ScrollHalfPage(-1)
        );
    }

    #[test]
    fn cell_cursor_keys() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('h')), Action::CellLeft);
        assert_eq!(h.handle(key_ctrl(KeyCode::Char('h'))), Action::CellLeft);
        assert_eq!(h.handle(ch('l')), Action::CellRight);
        assert_eq!(h.handle(key_ctrl(KeyCode::Char('l'))), Action::CellRight);
    }

    #[test]
    fn space_previews_cursor_cell() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch(' ')), Action::PreviewCursorCell);
    }

    #[test]
    fn v_without_prefix_opens_voverlay() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('v')), Action::ShowFieldNumbers);
        assert_eq!(h.mode(), Mode::VOverlay);
    }

    #[test]
    fn shift_v_repeats_preview_without_overlay() {
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('V')), Action::RepeatPreview);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn voverlay_label_char_emits_overlay_input() {
        let mut h = InputHandler::new();
        h.handle(ch('v'));
        assert_eq!(h.mode(), Mode::VOverlay);
        assert_eq!(h.handle(ch('3')), Action::OverlayInput('3'));
        assert_eq!(h.mode(), Mode::VOverlay);
    }

    #[test]
    fn voverlay_v_dismisses_to_normal() {
        let mut h = InputHandler::new();
        h.handle(ch('v'));
        assert_eq!(h.handle(ch('v')), Action::DismissOverlay);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn voverlay_esc_dismisses_to_normal() {
        let mut h = InputHandler::new();
        h.handle(ch('v'));
        assert_eq!(h.handle(key(KeyCode::Esc)), Action::DismissOverlay);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn voverlay_q_quits() {
        let mut h = InputHandler::new();
        h.handle(ch('v'));
        assert_eq!(h.handle(ch('q')), Action::Quit);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn voverlay_p_falls_through_to_prev_record() {
        let mut h = InputHandler::new();
        h.handle(ch('v'));
        assert_eq!(h.handle(ch('p')), Action::PrevRecord);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn voverlay_non_label_key_dismisses_and_falls_through() {
        let mut h = InputHandler::new();
        h.handle(ch('v'));
        assert_eq!(h.handle(ch('?')), Action::ShowHelp);
        assert_eq!(h.mode(), Mode::Help);
    }

    #[test]
    fn preview_mode_jk_scrolls() {
        let mut h = InputHandler::new();
        h.set_mode(Mode::Preview);
        assert_eq!(h.handle(ch('j')), Action::PreviewScroll(1));
        assert_eq!(h.handle(key(KeyCode::Down)), Action::PreviewScroll(1));
        assert_eq!(h.handle(ch('k')), Action::PreviewScroll(-1));
        assert_eq!(h.handle(key(KeyCode::Up)), Action::PreviewScroll(-1));
    }

    #[test]
    fn preview_mode_v_dismisses_to_voverlay() {
        let mut h = InputHandler::new();
        h.set_mode(Mode::Preview);
        assert_eq!(h.handle(ch('v')), Action::DismissPreview);
        assert_eq!(h.mode(), Mode::VOverlay);
    }

    #[test]
    fn preview_mode_esc_dismisses_to_voverlay() {
        let mut h = InputHandler::new();
        h.set_mode(Mode::Preview);
        assert_eq!(h.handle(key(KeyCode::Esc)), Action::DismissPreview);
        assert_eq!(h.mode(), Mode::VOverlay);
    }

    #[test]
    fn preview_mode_label_char_emits_overlay_input() {
        let mut h = InputHandler::new();
        h.set_mode(Mode::Preview);
        assert_eq!(h.handle(ch('3')), Action::OverlayInput('3'));
        assert_eq!(h.mode(), Mode::VOverlay);
    }

    #[test]
    fn preview_mode_q_quits() {
        let mut h = InputHandler::new();
        h.set_mode(Mode::Preview);
        assert_eq!(h.handle(ch('q')), Action::Quit);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn preview_mode_p_dismisses_to_prev_record() {
        let mut h = InputHandler::new();
        h.set_mode(Mode::Preview);
        assert_eq!(h.handle(ch('p')), Action::PrevRecord);
        assert_eq!(h.mode(), Mode::Normal);
    }

    #[test]
    fn preview_mode_ignores_non_label_keys() {
        let mut h = InputHandler::new();
        h.set_mode(Mode::Preview);
        assert_eq!(h.handle(ch('x')), Action::None);
        assert_eq!(h.mode(), Mode::Preview);
    }

    #[test]
    fn set_mode_forces_transition_for_async_preview() {
        // V/Space stay in Normal mode on keypress; the app enters Preview
        // once the worker's async response actually has popup content.
        let mut h = InputHandler::new();
        assert_eq!(h.handle(ch('V')), Action::RepeatPreview);
        assert_eq!(h.mode(), Mode::Normal);
        h.set_mode(Mode::Preview);
        assert_eq!(h.mode(), Mode::Preview);
        assert_eq!(h.handle(ch('j')), Action::PreviewScroll(1));
    }
}
