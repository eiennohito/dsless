use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState};
use unicode_width::UnicodeWidthStr;

// --- Display-width-aware string helpers ---

/// Display width of a string in terminal columns (CJK = 2, ASCII = 1).
fn display_width(s: &str) -> usize {
    UnicodeWidthStr::width(s)
}

/// Pad a string with trailing spaces to reach `target_width` display columns.
fn pad_to_width(s: &str, target_width: usize) -> String {
    let w = display_width(s);
    if w >= target_width {
        s.to_string()
    } else {
        format!("{}{}", s, " ".repeat(target_width - w))
    }
}

/// Truncate a string to fit within `max_width` display columns, appending "…" if truncated.
fn truncate_to_width(s: &str, max_width: usize) -> String {
    let w = display_width(s);
    if w <= max_width {
        return s.to_string();
    }
    if max_width == 0 {
        return String::new();
    }
    // Walk chars, accumulating display width
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

/// Skip `cols` display columns from the start of a string, returning the remainder.
fn skip_display_cols(s: &str, cols: usize) -> &str {
    if cols == 0 {
        return s;
    }
    let mut skipped = 0;
    for (i, c) in s.char_indices() {
        if skipped >= cols {
            return &s[i..];
        }
        skipped += unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
    }
    ""
}

#[derive(Parser)]
#[command(name = "dsless", about = "A pager for data-science formats")]
struct Cli {
    /// Path to a parquet file or directory of parquet files
    path: PathBuf,

    /// Maximum number of rows to load (default: 1000)
    #[arg(short = 'n', long, default_value = "1000")]
    max_rows: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let lines = load_and_render(&cli.path, cli.max_rows)?;
    if std::io::IsTerminal::is_terminal(&std::io::stdout()) {
        run_tui(lines)
    } else {
        for line in &lines {
            println!("{}", line);
        }
        Ok(())
    }
}

// --- Parquet loading ---

fn load_and_render(path: &PathBuf, max_rows: usize) -> Result<Vec<String>> {
    let files = collect_parquet_files(path)?;
    if files.is_empty() {
        anyhow::bail!("No parquet files found at {:?}", path);
    }

    let mut all_lines = Vec::new();
    let mut total_rows = 0usize;

    for file_path in &files {
        if total_rows >= max_rows {
            break;
        }
        let file = std::fs::File::open(file_path)
            .with_context(|| format!("Failed to open {:?}", file_path))?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("Failed to read parquet metadata from {:?}", file_path))?
            .with_batch_size(1024)
            .build()
            .with_context(|| format!("Failed to build reader for {:?}", file_path))?;

        let schema = reader.schema();

        if all_lines.is_empty() {
            // Print schema header once
            all_lines.push(format!("Schema: {} columns", schema.fields().len()));
            for field in schema.fields() {
                all_lines.push(format!("  {} : {}", field.name(), field.data_type()));
            }
            all_lines.push(String::new());
            if files.len() > 1 {
                all_lines.push(format!("Files: {} parquet files", files.len()));
                all_lines.push(String::new());
            }
        }

        for batch_result in reader {
            if total_rows >= max_rows {
                break;
            }
            let batch = batch_result.context("Failed to read record batch")?;
            let rows_in_batch = batch.num_rows().min(max_rows - total_rows);

            for row_idx in 0..rows_in_batch {
                all_lines.push(format!("── Row {} ──", total_rows));
                render_row(&batch, row_idx, &schema, &mut all_lines, 1);
                all_lines.push(String::new());
                total_rows += 1;
            }
        }
    }

    if total_rows >= max_rows {
        all_lines.push(format!("... (stopped at {} rows, use -n to load more)", max_rows));
    } else {
        all_lines.push(format!("Total: {} rows", total_rows));
    }

    Ok(all_lines)
}

fn collect_parquet_files(path: &PathBuf) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.clone()]);
    }
    if path.is_dir() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(path)
            .with_context(|| format!("Failed to read directory {:?}", path))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .is_some_and(|ext| ext == "parquet")
            })
            .collect();
        files.sort();
        return Ok(files);
    }
    anyhow::bail!("{:?} is not a file or directory", path);
}

// --- Tree rendering ---

fn guide(depth: usize) -> String {
    "│ ".repeat(depth)
}

fn is_scalar_type(dt: &DataType) -> bool {
    !matches!(dt, DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _))
}

fn is_scalar_list(dt: &DataType) -> bool {
    match dt {
        DataType::List(field) | DataType::LargeList(field) => is_scalar_type(field.data_type()),
        _ => false,
    }
}

fn render_scalar_list_inline(values: &dyn Array, start: usize, end: usize) -> String {
    if start == end {
        return "[]".to_string();
    }
    let items: Vec<String> = (start..end)
        .map(|i| scalar_to_string(values, i))
        .collect();
    format!("[{}]", items.join(", "))
}

/// Extract (start, end, values) from a list array at a given row.
fn list_offsets(array: &dyn Array, row: usize) -> (usize, usize, Arc<dyn Array>) {
    match array.data_type() {
        DataType::List(_) => {
            let la = array.as_any().downcast_ref::<ListArray>().unwrap();
            let o = la.offsets();
            (o[row] as usize, o[row + 1] as usize, la.values().clone())
        }
        DataType::LargeList(_) => {
            let la = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let o = la.offsets();
            (o[row] as usize, o[row + 1] as usize, la.values().clone())
        }
        _ => unreachable!(),
    }
}

// --- Table layout for list-of-struct ---

/// Compact inline summary of any array value for use in a table cell.
/// Shows a preview of content with "+N" suffix when truncated.
fn table_cell_value(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Struct(_) => {
            let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
            let fields = sa.fields();
            let total = fields.len();
            // Show first 3 fields, abbreviate rest
            let preview_count = total.min(3);
            let parts: Vec<String> = (0..preview_count)
                .map(|i| format!("{}: {}", fields[i].name(), table_cell_value(sa.column(i).as_ref(), row)))
                .collect();
            if total > preview_count {
                format!("{{{}, +{}}}", parts.join(", "), total - preview_count)
            } else {
                format!("{{{}}}", parts.join(", "))
            }
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let (s, e, values) = list_offsets(array, row);
            let len = e - s;
            if len == 0 {
                return "[]".to_string();
            }
            // For scalar lists, show first few values inline
            if is_scalar_list(array.data_type()) {
                let preview_count = len.min(3);
                let items: Vec<String> = (s..s + preview_count)
                    .map(|i| scalar_to_string(values.as_ref(), i))
                    .collect();
                if len > preview_count {
                    format!("[{}, +{}]", items.join(", "), len - preview_count)
                } else {
                    format!("[{}]", items.join(", "))
                }
            } else {
                // List of complex types — show first item preview
                let first = table_cell_value(values.as_ref(), s);
                if len == 1 {
                    format!("[{}]", first)
                } else {
                    format!("[{}, +{}]", first, len - 1)
                }
            }
        }
        DataType::Map(_, _) => {
            let ma = array.as_any().downcast_ref::<MapArray>().unwrap();
            let o = ma.offsets();
            let start = o[row] as usize;
            let end = o[row + 1] as usize;
            let len = end - start;
            if len == 0 {
                "{}".to_string()
            } else {
                let keys = ma.keys();
                let preview_count = len.min(3);
                let key_previews: Vec<String> = (start..start + preview_count)
                    .map(|i| scalar_to_string(keys.as_ref(), i))
                    .collect();
                if len > preview_count {
                    format!("{{{}: …, +{}}}", key_previews.join(", "), len - preview_count)
                } else {
                    format!("{{{}: …}}", key_previews.join(", "))
                }
            }
        }
        _ => scalar_to_string(array, row),
    }
}

/// Render a list-of-struct as a table. All fields become columns.
fn render_struct_list_as_table(
    struct_array: &StructArray,
    start: usize,
    end: usize,
    prefix: &str,
    out: &mut Vec<String>,
    depth: usize,
) {
    let count = end - start;
    if count == 0 {
        out.push(format!("{}[]", prefix));
        return;
    }

    let num_fields = struct_array.num_columns();
    let g = guide(depth + 1);

    // Collect column names and all cell values
    let col_names: Vec<&str> = struct_array.fields().iter()
        .map(|f| f.name().as_str())
        .collect();
    let cell_values: Vec<Vec<String>> = (0..num_fields)
        .map(|ci| {
            let col = struct_array.column(ci);
            (start..end).map(|row| table_cell_value(col.as_ref(), row)).collect()
        })
        .collect();

    // Compute column widths using display width (CJK-aware), capped
    const MAX_COL_WIDTH: usize = 60;
    let col_widths: Vec<usize> = (0..num_fields)
        .map(|c| {
            let header_w = display_width(col_names[c]);
            let max_cell = cell_values[c].iter()
                .map(|v| display_width(v))
                .max()
                .unwrap_or(0);
            header_w.max(max_cell).min(MAX_COL_WIDTH)
        })
        .collect();

    // Header line
    out.push(format!("{}({} items)", prefix, count));
    let header: String = col_widths.iter().enumerate()
        .map(|(c, &w)| pad_to_width(col_names[c], w))
        .collect::<Vec<_>>()
        .join(" │ ");
    out.push(format!("{}  {}", g, header));

    // Separator
    let sep: String = col_widths.iter()
        .map(|&w| "─".repeat(w))
        .collect::<Vec<_>>()
        .join("─┼─");
    out.push(format!("{}  {}", g, sep));

    // Data rows
    for row_idx in 0..count {
        let row_str: String = col_widths.iter().enumerate()
            .map(|(c, &w)| {
                let val = &cell_values[c][row_idx];
                let vw = display_width(val);
                if vw > w {
                    truncate_to_width(val, w)
                } else {
                    pad_to_width(val, w)
                }
            })
            .collect::<Vec<_>>()
            .join(" │ ");
        out.push(format!("{}  {}", g, row_str));
    }
}

fn render_struct_as_tree(struct_array: &StructArray, row: usize, prefix: &str, out: &mut Vec<String>, depth: usize) {
    out.push(prefix.trim_end().to_string());
    for (i, field) in struct_array.fields().iter().enumerate() {
        let child = struct_array.column(i);
        let child_prefix = format!("{}{}: ", guide(depth + 1), field.name());
        render_value(child.as_ref(), row, &child_prefix, out, depth + 1);
    }
}

// --- Core rendering ---

fn render_row(batch: &RecordBatch, row: usize, schema: &Arc<Schema>, out: &mut Vec<String>, depth: usize) {
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(col_idx);
        let prefix = format!("{}{}: ", guide(depth), field.name());
        render_value(col, row, &prefix, out, depth);
    }
}

fn render_value(array: &dyn Array, row: usize, prefix: &str, out: &mut Vec<String>, depth: usize) {
    if array.is_null(row) {
        out.push(format!("{}null", prefix));
        return;
    }

    match array.data_type() {
        DataType::Struct(_) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            out.push(prefix.trim_end().to_string());
            for (i, field) in struct_array.fields().iter().enumerate() {
                let child = struct_array.column(i);
                let child_prefix = format!("{}{}: ", guide(depth + 1), field.name());
                render_value(child.as_ref(), row, &child_prefix, out, depth + 1);
            }
        }
        DataType::List(field) | DataType::LargeList(field)
            if matches!(field.data_type(), DataType::Struct(_)) =>
        {
            let (start, end, values) = list_offsets(array, row);
            let struct_array = values.as_any().downcast_ref::<StructArray>().unwrap();
            render_struct_list_as_table(struct_array, start, end, prefix, out, depth);
        }
        dt if is_scalar_list(dt) => {
            let (start, end, values) = list_offsets(array, row);
            if start == end {
                out.push(format!("{}[]", prefix));
            } else {
                let inline = render_scalar_list_inline(values.as_ref(), start, end);
                out.push(format!("{}{}", prefix, inline));
            }
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let (start, end, values) = list_offsets(array, row);
            if start == end {
                out.push(format!("{}[]", prefix));
            } else {
                out.push(format!("{}({} items)", prefix, end - start));
                for i in start..end {
                    let item_prefix = format!("{}[{}]: ", guide(depth + 1), i - start);
                    render_value(values.as_ref(), i, &item_prefix, out, depth + 1);
                }
            }
        }
        DataType::Map(_, _) => {
            let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
            let offsets = map_array.offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let keys = map_array.keys();
            let values = map_array.values();

            if start == end {
                out.push(format!("{}{{}}", prefix));
            } else {
                out.push(prefix.trim_end().to_string());
                for i in start..end {
                    let key_str = scalar_to_string(keys.as_ref(), i);
                    let val_prefix = format!("{}{}: ", guide(depth + 1), key_str);
                    render_value(values.as_ref(), i, &val_prefix, out, depth + 1);
                }
            }
        }
        _ => {
            let val = scalar_to_string(array, row);
            out.push(format!("{}{}", prefix, val));
        }
    }
}

fn format_string_value(s: &str) -> String {
    const MAX_DISPLAY_WIDTH: usize = 200;
    let w = display_width(s);
    if w > MAX_DISPLAY_WIDTH {
        // Truncate by display width
        let mut current_width = 0;
        let mut end_byte = 0;
        for (i, c) in s.char_indices() {
            let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
            if current_width + cw > MAX_DISPLAY_WIDTH {
                break;
            }
            current_width += cw;
            end_byte = i + c.len_utf8();
        }
        format!("\"{}...\" ({} chars)", &s[..end_byte], s.chars().count())
    } else {
        format!("\"{}\"", s)
    }
}

fn scalar_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }

    macro_rules! downcast_primitive {
        ($array:expr, $row:expr, $($ArrowType:ty => $ArrayType:ty),+ $(,)?) => {
            match $array.data_type() {
                $(
                    dt if dt == &<$ArrowType as arrow::datatypes::ArrowPrimitiveType>::DATA_TYPE => {
                        let arr = $array.as_any().downcast_ref::<$ArrayType>().unwrap();
                        return arr.value($row).to_string();
                    }
                )+
                _ => {}
            }
        };
    }

    downcast_primitive!(array, row,
        arrow::datatypes::Int8Type => Int8Array,
        arrow::datatypes::Int16Type => Int16Array,
        arrow::datatypes::Int32Type => Int32Array,
        arrow::datatypes::Int64Type => Int64Array,
        arrow::datatypes::UInt8Type => UInt8Array,
        arrow::datatypes::UInt16Type => UInt16Array,
        arrow::datatypes::UInt32Type => UInt32Array,
        arrow::datatypes::UInt64Type => UInt64Array,
        arrow::datatypes::Float32Type => Float32Array,
        arrow::datatypes::Float64Type => Float64Array,
    );

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            format_string_value(arr.value(row))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            format_string_value(arr.value(row))
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            format!("<{} bytes>", arr.value(row).len())
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            format!("<{} bytes>", arr.value(row).len())
        }
        DataType::Timestamp(unit, tz) => {
            let unit_str = match unit {
                arrow::datatypes::TimeUnit::Second => "s",
                arrow::datatypes::TimeUnit::Millisecond => "ms",
                arrow::datatypes::TimeUnit::Microsecond => "us",
                arrow::datatypes::TimeUnit::Nanosecond => "ns",
            };
            // Use arrow's display formatting via array_value_to_string
            arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| {
                    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>();
                    match arr {
                        Some(a) => format!("{} ({}{})", a.value(row), unit_str,
                            tz.as_ref().map_or(String::new(), |t| format!(", {}", t))),
                        None => "<timestamp>".to_string(),
                    }
                })
        }
        _ => {
            // Fallback: use arrow's display
            arrow::util::display::array_value_to_string(array, row)
                .unwrap_or_else(|_| format!("<{}>", array.data_type()))
        }
    }
}

// --- TUI ---

fn run_tui(lines: Vec<String>) -> Result<()> {
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(
        stdout,
        crossterm::terminal::EnterAlternateScreen
    )?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, lines);

    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    result
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>, lines: Vec<String>) -> Result<()> {
    let mut scroll_y: usize = 0;
    let mut scroll_x: usize = 0;
    let total_lines = lines.len();
    let mut search_query = String::new();
    let mut search_mode = false;
    let mut search_matches: Vec<usize> = Vec::new();
    let mut current_match: usize = 0;

    loop {
        terminal.draw(|frame| {
            let area = frame.area();

            let visible_height = area.height.saturating_sub(3) as usize; // borders + status

            // Build visible text
            let visible_lines: Vec<Line> = lines
                .iter()
                .skip(scroll_y)
                .take(visible_height)
                .enumerate()
                .map(|(i, line)| {
                    let line_num = scroll_y + i;
                    let display = if scroll_x == 0 {
                        line.as_str()
                    } else {
                        skip_display_cols(line, scroll_x)
                    };

                    // Highlight search matches
                    if !search_matches.is_empty() && search_matches.contains(&line_num) {
                        let is_current = search_matches.get(current_match) == Some(&line_num);
                        let style = if is_current {
                            Style::default().bg(Color::Yellow).fg(Color::Black)
                        } else {
                            Style::default().bg(Color::DarkGray).fg(Color::White)
                        };
                        Line::from(Span::styled(display.to_string(), style))
                    } else if display.starts_with("── Row") {
                        Line::from(Span::styled(display.to_string(), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)))
                    } else if display.starts_with("Schema:") || display.starts_with("Files:") || display.starts_with("Total:") {
                        Line::from(Span::styled(display.to_string(), Style::default().fg(Color::Green)))
                    } else {
                        Line::from(display.to_string())
                    }
                })
                .collect();

            let status = if search_mode {
                format!("/{}  ", search_query)
            } else {
                let pct = if total_lines == 0 {
                    100
                } else {
                    ((scroll_y + visible_height).min(total_lines)) * 100 / total_lines
                };
                let match_info = if !search_matches.is_empty() {
                    format!("  [{}/{}]", current_match + 1, search_matches.len())
                } else {
                    String::new()
                };
                format!("Line {}/{} ({}%){} | q:quit /:search j/k:scroll g/G:top/bottom",
                    scroll_y + 1, total_lines, pct, match_info)
            };

            let block = Block::default()
                .borders(Borders::BOTTOM)
                .title_bottom(Line::from(status).left_aligned());

            let paragraph = Paragraph::new(visible_lines).block(block);
            frame.render_widget(paragraph, area);

            // Scrollbar
            let mut scrollbar_state = ScrollbarState::new(total_lines.saturating_sub(visible_height))
                .position(scroll_y);
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight);
            frame.render_stateful_widget(scrollbar, area, &mut scrollbar_state);
        })?;

        let visible_height = terminal.size()?.height.saturating_sub(3) as usize;

        if let Event::Key(key) = event::read()? {
            if search_mode {
                match key.code {
                    KeyCode::Enter => {
                        search_mode = false;
                        // Find matches
                        search_matches.clear();
                        if !search_query.is_empty() {
                            let query_lower = search_query.to_lowercase();
                            for (i, line) in lines.iter().enumerate() {
                                if line.to_lowercase().contains(&query_lower) {
                                    search_matches.push(i);
                                }
                            }
                            if !search_matches.is_empty() {
                                // Jump to first match at or after current position
                                current_match = search_matches
                                    .iter()
                                    .position(|&m| m >= scroll_y)
                                    .unwrap_or(0);
                                scroll_y = search_matches[current_match];
                            }
                        }
                    }
                    KeyCode::Esc => {
                        search_mode = false;
                        search_query.clear();
                        search_matches.clear();
                    }
                    KeyCode::Backspace => {
                        search_query.pop();
                    }
                    KeyCode::Char(c) => {
                        search_query.push(c);
                    }
                    _ => {}
                }
                continue;
            }

            match key.code {
                KeyCode::Char('q') | KeyCode::Char('Q') => break,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,

                // Scroll
                KeyCode::Char('j') | KeyCode::Down => {
                    scroll_y = (scroll_y + 1).min(total_lines.saturating_sub(visible_height));
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    scroll_y = scroll_y.saturating_sub(1);
                }
                KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    scroll_y = (scroll_y + visible_height / 2).min(total_lines.saturating_sub(visible_height));
                }
                KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    scroll_y = scroll_y.saturating_sub(visible_height / 2);
                }
                KeyCode::Char('f') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    scroll_y = (scroll_y + visible_height).min(total_lines.saturating_sub(visible_height));
                }
                KeyCode::Char('b') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    scroll_y = scroll_y.saturating_sub(visible_height);
                }
                KeyCode::Char(' ') | KeyCode::PageDown => {
                    scroll_y = (scroll_y + visible_height).min(total_lines.saturating_sub(visible_height));
                }
                KeyCode::PageUp => {
                    scroll_y = scroll_y.saturating_sub(visible_height);
                }
                KeyCode::Char('g') => {
                    scroll_y = 0;
                }
                KeyCode::Char('G') => {
                    scroll_y = total_lines.saturating_sub(visible_height);
                }

                // Horizontal scroll
                KeyCode::Char('h') | KeyCode::Left => {
                    scroll_x = scroll_x.saturating_sub(4);
                }
                KeyCode::Char('l') | KeyCode::Right => {
                    scroll_x += 4;
                }
                KeyCode::Home | KeyCode::Char('0') => {
                    scroll_x = 0;
                }

                // Search
                KeyCode::Char('/') => {
                    search_mode = true;
                    search_query.clear();
                    search_matches.clear();
                }
                KeyCode::Char('n') => {
                    if !search_matches.is_empty() {
                        current_match = (current_match + 1) % search_matches.len();
                        scroll_y = search_matches[current_match];
                    }
                }
                KeyCode::Char('N') => {
                    if !search_matches.is_empty() {
                        current_match = if current_match == 0 {
                            search_matches.len() - 1
                        } else {
                            current_match - 1
                        };
                        scroll_y = search_matches[current_match];
                    }
                }

                _ => {}
            }
        }
    }

    Ok(())
}
