use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

pub fn render_help_popup(frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
    let help_text = vec![
        Line::from(Span::styled(
            " dsless ",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            " Scrolling",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  j / Down      line down"),
        Line::from("  k / Up        line up"),
        Line::from("  J / PageDown  page down"),
        Line::from("  K / PageUp    page up"),
        Line::from("  Space/Ctrl-d  half page down"),
        Line::from("  Ctrl-u        half page up"),
        Line::from(""),
        Line::from(Span::styled(
            " Records",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  g             start of record / prev record"),
        Line::from("  G             next record"),
        Line::from("  <N>g / <N>G   go to record N"),
        Line::from("  <N>%          go to N% of dataset"),
        Line::from(""),
        Line::from(Span::styled(
            " Search",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  /             search"),
        Line::from("  n             next match (off-screen)"),
        Line::from("  N             previous match"),
        Line::from("  Esc           clear search"),
        Line::from(""),
        Line::from(Span::styled(
            " Other",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  q / Ctrl-c    quit"),
        Line::from("  ?             this help"),
        Line::from(""),
        Line::from(Span::styled(
            "       press any key to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let height = help_text.len() as u16 + 2; // +2 for borders
    let width = 42;
    let x = area.width.saturating_sub(width) / 2;
    let y = area.height.saturating_sub(height) / 2;
    let popup_area =
        ratatui::layout::Rect::new(x, y, width.min(area.width), height.min(area.height));

    frame.render_widget(Clear, popup_area);
    let popup = Paragraph::new(help_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    frame.render_widget(popup, popup_area);
}
