use unicode_width::UnicodeWidthStr;

pub fn display_width(s: &str) -> usize {
    UnicodeWidthStr::width(s)
}

pub fn truncate_to_width(s: &str, max_width: usize) -> String {
    let w = display_width(s);
    if w <= max_width {
        return s.to_string();
    }
    if max_width == 0 {
        return String::new();
    }
    let mut current_width = 0;
    let mut end_byte = 0;
    for (i, c) in s.char_indices() {
        let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if current_width + cw > max_width.saturating_sub(1) {
            break;
        }
        current_width += cw;
        end_byte = i + c.len_utf8();
    }
    format!("{}…", &s[..end_byte])
}
