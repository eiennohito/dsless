use crate::input::LABEL_CHARS;

const LABEL_BASE: usize = 18;

// ── Label encoding (base-20, `1` is zero/pad) ───────────────

pub fn label_width(total: usize) -> usize {
    let mut w = 1;
    let mut capacity = LABEL_BASE;
    while capacity < total {
        w += 1;
        capacity *= LABEL_BASE;
    }
    w
}

pub fn field_label(index: usize, total: usize) -> String {
    let w = label_width(total);
    let mut digits = Vec::with_capacity(w);
    let mut n = index;
    for _ in 0..w {
        digits.push(LABEL_CHARS[n % LABEL_BASE] as char);
        n /= LABEL_BASE;
    }
    digits.reverse();
    digits.into_iter().collect()
}

fn char_to_digit(c: char) -> Option<usize> {
    if c.is_ascii() {
        LABEL_CHARS.iter().position(|&b| b == c as u8)
    } else {
        None
    }
}

/// After each keystroke, check if the accumulated label chars resolve.
pub fn resolve_label(chars: &[char], total: usize) -> LabelMatch {
    let w = label_width(total);
    if chars.len() > w {
        return LabelMatch::Invalid;
    }
    for &c in chars {
        if char_to_digit(c).is_none() {
            return LabelMatch::Invalid;
        }
    }
    if chars.len() < w {
        return LabelMatch::Incomplete;
    }
    let mut value = 0;
    for &c in chars {
        value = value * LABEL_BASE + char_to_digit(c).unwrap();
    }
    if value < total {
        LabelMatch::Resolved(value)
    } else {
        LabelMatch::Invalid
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LabelMatch {
    Resolved(usize),
    Incomplete,
    Invalid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_single_char_for_small_count() {
        assert_eq!(label_width(5), 1);
        assert_eq!(field_label(0, 5), "1");
        assert_eq!(field_label(4, 5), "5");
        assert_eq!(field_label(9, 18), "0");
        assert_eq!(field_label(10, 18), "w");
        assert_eq!(field_label(17, 18), "o");
    }

    #[test]
    fn label_two_chars_for_larger_count() {
        assert_eq!(label_width(19), 2);
        assert_eq!(field_label(0, 25), "11");
        assert_eq!(field_label(1, 25), "12");
        assert_eq!(field_label(18, 25), "21");
        assert_eq!(field_label(24, 25), "27");
    }

    #[test]
    fn resolve_single_char() {
        assert_eq!(resolve_label(&['3'], 10), LabelMatch::Resolved(2));
        assert_eq!(resolve_label(&['w'], 15), LabelMatch::Resolved(10));
        assert_eq!(resolve_label(&['o'], 18), LabelMatch::Resolved(17));
        assert_eq!(resolve_label(&['o'], 10), LabelMatch::Invalid);
    }

    #[test]
    fn resolve_two_chars() {
        assert_eq!(resolve_label(&['2'], 25), LabelMatch::Incomplete);
        assert_eq!(resolve_label(&['2', '1'], 25), LabelMatch::Resolved(18));
        assert_eq!(resolve_label(&['1', '1'], 25), LabelMatch::Resolved(0));
        assert_eq!(resolve_label(&['2', '7'], 25), LabelMatch::Resolved(24));
    }
}
