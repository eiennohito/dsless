use std::io::Write;

pub fn osc52_copy(text: &str) {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(text);
    let mut stdout = std::io::stdout().lock();
    let _ = write!(stdout, "\x1b]52;c;{}\x07", encoded);
    let _ = stdout.flush();
}
