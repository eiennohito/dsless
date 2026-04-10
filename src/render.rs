use std::fmt::{self, Write};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Schema};

use crate::unicode::{display_width, truncate_to_width};

// ============================================================
// RenderedRow — immutable result stored in cache
// ============================================================

pub struct RenderedRow {
    buf: String,
    line_starts: Vec<usize>,
}

impl RenderedRow {
    pub fn line_count(&self) -> usize {
        self.line_starts.len()
    }

    pub fn line(&self, idx: usize) -> &str {
        let start = self.line_starts[idx];
        let end = if idx + 1 < self.line_starts.len() {
            // line_starts[i+1] points past the '\n' of line i
            self.line_starts[idx + 1] - 1
        } else {
            self.buf.len()
        };
        &self.buf[start..end]
    }

    pub fn lines(&self) -> LineIter<'_> {
        LineIter { row: self, idx: 0 }
    }

    pub fn byte_size(&self) -> usize {
        self.buf.len()
            + self.line_starts.len() * std::mem::size_of::<usize>()
            + std::mem::size_of::<Self>()
    }
}

pub struct LineIter<'a> {
    row: &'a RenderedRow,
    idx: usize,
}

impl<'a> Iterator for LineIter<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.row.line_count() {
            let line = self.row.line(self.idx);
            self.idx += 1;
            Some(line)
        } else {
            None
        }
    }
}

// ============================================================
// LineWriter — reusable buffer for rendering
// ============================================================

/// Pre-computed guide string. Max depth 32 should be plenty.
const MAX_GUIDE_DEPTH: usize = 32;

fn guide_str() -> &'static str {
    // "│ " repeated MAX_GUIDE_DEPTH times
    // Each "│ " is 4 bytes (│ = 3 bytes UTF-8 + 1 space)
    static GUIDE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    GUIDE.get_or_init(|| "│ ".repeat(MAX_GUIDE_DEPTH))
}

pub struct LineWriter {
    buf: String,
    line_starts: Vec<usize>,
    scratch: String,
}

impl LineWriter {
    pub fn new() -> Self {
        LineWriter {
            buf: String::new(),
            line_starts: vec![0],
            scratch: String::new(),
        }
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.line_starts.clear();
        self.line_starts.push(0);
    }

    /// End the current line.
    pub fn newline(&mut self) {
        self.buf.push('\n');
        self.line_starts.push(self.buf.len());
    }

    /// Write guide characters for the given depth. No allocation.
    pub fn guide(&mut self, depth: usize) {
        let g = guide_str();
        let byte_len = depth.min(MAX_GUIDE_DEPTH) * "│ ".len();
        self.buf.push_str(&g[..byte_len]);
    }

    /// Clone out the result. 2 allocations (buf + line_starts).
    pub fn finish(&self) -> RenderedRow {
        // Remove trailing empty line if present
        let mut line_starts = self.line_starts.clone();
        if line_starts.len() > 1 && *line_starts.last().unwrap() == self.buf.len() {
            line_starts.pop();
        }
        RenderedRow {
            buf: self.buf.clone(),
            line_starts,
        }
    }

    /// Write a table cell value into scratch, return its display width.
    fn measure_cell(&mut self, array: &dyn Array, row: usize) -> usize {
        self.scratch.clear();
        write_table_cell_value(&mut self.scratch, array, row);
        display_width(&self.scratch)
    }

    /// Write the scratch content (last measured cell) padded/truncated to `width`.
    fn write_cell_padded(&mut self, width: usize) {
        let vw = display_width(&self.scratch);
        if vw > width {
            let truncated = truncate_to_width(&self.scratch, width);
            self.buf.push_str(&truncated);
        } else {
            self.buf.push_str(&self.scratch);
            for _ in 0..(width - vw) {
                self.buf.push(' ');
            }
        }
    }

    /// Write a string padded to `width` display columns. No allocation.
    fn write_padded(&mut self, s: &str, width: usize) {
        let w = display_width(s);
        self.buf.push_str(s);
        for _ in 0..(width.saturating_sub(w)) {
            self.buf.push(' ');
        }
    }
}

impl fmt::Write for LineWriter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.buf.push_str(s);
        Ok(())
    }
}

// ============================================================
// Rendering API
// ============================================================

pub fn render_row(
    batch: &RecordBatch,
    row: usize,
    schema: &Arc<Schema>,
    w: &mut LineWriter,
    depth: usize,
) {
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(col_idx);
        w.guide(depth);
        let _ = write!(w, "{}: ", field.name());
        render_value(col, row, w, depth);
    }
}

fn render_value(array: &dyn Array, row: usize, w: &mut LineWriter, depth: usize) {
    if array.is_null(row) {
        let _ = write!(w, "null");
        w.newline();
        return;
    }

    match array.data_type() {
        DataType::Struct(_) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            w.newline();
            for (i, field) in struct_array.fields().iter().enumerate() {
                let child = struct_array.column(i);
                w.guide(depth + 1);
                let _ = write!(w, "{}: ", field.name());
                render_value(child.as_ref(), row, w, depth + 1);
            }
        }
        DataType::List(field) | DataType::LargeList(field)
            if matches!(field.data_type(), DataType::Struct(_)) =>
        {
            let (start, end, values) = list_offsets(array, row);
            let struct_array = values.as_any().downcast_ref::<StructArray>().unwrap();
            render_struct_list_as_table(struct_array, start, end, w, depth);
        }
        dt if is_scalar_list(dt) => {
            let (start, end, values) = list_offsets(array, row);
            if start == end {
                let _ = write!(w, "[]");
            } else {
                w.buf.push('[');
                for i in start..end {
                    if i > start {
                        w.buf.push_str(", ");
                    }
                    write_scalar_to(w, values.as_ref(), i);
                }
                w.buf.push(']');
            }
            w.newline();
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let (start, end, values) = list_offsets(array, row);
            if start == end {
                let _ = write!(w, "[]");
                w.newline();
            } else {
                let _ = write!(w, "({} items)", end - start);
                w.newline();
                for i in start..end {
                    w.guide(depth + 1);
                    let _ = write!(w, "[{}]: ", i - start);
                    render_value(values.as_ref(), i, w, depth + 1);
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
                let _ = write!(w, "{{}}");
                w.newline();
            } else {
                w.newline();
                for i in start..end {
                    w.guide(depth + 1);
                    write_scalar_to(w, keys.as_ref(), i);
                    w.buf.push_str(": ");
                    render_value(values.as_ref(), i, w, depth + 1);
                }
            }
        }
        _ => {
            write_scalar_to(w, array, row);
            w.newline();
        }
    }
}

// ============================================================
// Table layout for list-of-struct
// ============================================================

const MAX_COL_WIDTH: usize = 60;

fn render_struct_list_as_table(
    struct_array: &StructArray,
    start: usize,
    end: usize,
    w: &mut LineWriter,
    depth: usize,
) {
    let count = end - start;
    if count == 0 {
        let _ = write!(w, "[]");
        w.newline();
        return;
    }

    let num_fields = struct_array.num_columns();

    // Pass 1: measure column widths using scratch buffer
    let mut col_widths: Vec<usize> = Vec::with_capacity(num_fields);
    for ci in 0..num_fields {
        let header_w = display_width(struct_array.fields()[ci].name());
        let col = struct_array.column(ci);
        let mut max_w = header_w;
        for row in start..end {
            let cell_w = w.measure_cell(col.as_ref(), row);
            max_w = max_w.max(cell_w);
        }
        col_widths.push(max_w.min(MAX_COL_WIDTH));
    }

    // Header: "(N items)"
    let _ = write!(w, "({} items)", count);
    w.newline();

    // Column headers
    w.guide(depth + 1);
    w.buf.push_str("  ");
    for (c, &cw) in col_widths.iter().enumerate() {
        if c > 0 {
            w.buf.push_str(" │ ");
        }
        w.write_padded(struct_array.fields()[c].name(), cw);
    }
    w.newline();

    // Separator
    w.guide(depth + 1);
    w.buf.push_str("  ");
    for (c, &cw) in col_widths.iter().enumerate() {
        if c > 0 {
            w.buf.push_str("─┼─");
        }
        for _ in 0..cw {
            w.buf.push('─');
        }
    }
    w.newline();

    // Data rows — pass 2: format each cell into scratch, write padded to buf
    for row in start..end {
        w.guide(depth + 1);
        w.buf.push_str("  ");
        for (ci, &cw) in col_widths.iter().enumerate() {
            if ci > 0 {
                w.buf.push_str(" │ ");
            }
            let col = struct_array.column(ci);
            w.measure_cell(col.as_ref(), row); // formats into scratch
            w.write_cell_padded(cw); // writes scratch content padded to buf
        }
        w.newline();
    }
}

// ============================================================
// Table cell preview (writes to any fmt::Write target)
// ============================================================

fn write_table_cell_value(out: &mut String, array: &dyn Array, row: usize) {
    if array.is_null(row) {
        out.push_str("null");
        return;
    }
    match array.data_type() {
        DataType::Struct(_) => {
            let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
            let fields = sa.fields();
            let total = fields.len();
            let preview_count = total.min(3);
            out.push('{');
            for i in 0..preview_count {
                if i > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "{}: ", fields[i].name());
                write_table_cell_value(out, sa.column(i).as_ref(), row);
            }
            if total > preview_count {
                let _ = write!(out, ", +{}", total - preview_count);
            }
            out.push('}');
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let (s, e, values) = list_offsets(array, row);
            let len = e - s;
            if len == 0 {
                out.push_str("[]");
                return;
            }
            if is_scalar_list(array.data_type()) {
                let preview_count = len.min(3);
                out.push('[');
                for i in 0..preview_count {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    write_scalar_to_string(out, values.as_ref(), s + i);
                }
                if len > preview_count {
                    let _ = write!(out, ", +{}", len - preview_count);
                }
                out.push(']');
            } else {
                out.push('[');
                write_table_cell_value(out, values.as_ref(), s);
                if len > 1 {
                    let _ = write!(out, ", +{}", len - 1);
                }
                out.push(']');
            }
        }
        DataType::Map(_, _) => {
            let ma = array.as_any().downcast_ref::<MapArray>().unwrap();
            let o = ma.offsets();
            let start = o[row] as usize;
            let end = o[row + 1] as usize;
            let len = end - start;
            if len == 0 {
                out.push_str("{}");
            } else {
                let keys = ma.keys();
                let preview_count = len.min(3);
                out.push('{');
                for i in 0..preview_count {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    write_scalar_to_string(out, keys.as_ref(), start + i);
                }
                out.push_str(": …");
                if len > preview_count {
                    let _ = write!(out, ", +{}", len - preview_count);
                }
                out.push('}');
            }
        }
        _ => write_scalar_to_string(out, array, row),
    }
}

// ============================================================
// Helpers
// ============================================================

fn is_scalar_type(dt: &DataType) -> bool {
    !matches!(
        dt,
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    )
}

fn is_scalar_list(dt: &DataType) -> bool {
    match dt {
        DataType::List(field) | DataType::LargeList(field) => is_scalar_type(field.data_type()),
        _ => false,
    }
}

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

// ============================================================
// Scalar formatting — write directly to a target, no alloc
// ============================================================

/// Write a scalar value directly into LineWriter's buf.
fn write_scalar_to(w: &mut LineWriter, array: &dyn Array, row: usize) {
    write_scalar_to_string(&mut w.buf, array, row);
}

/// Write a scalar value into any String buffer.
fn write_scalar_to_string(out: &mut String, array: &dyn Array, row: usize) {
    if array.is_null(row) {
        out.push_str("null");
        return;
    }

    macro_rules! try_primitive {
        ($($ArrowType:ty => $ArrayType:ty),+ $(,)?) => {
            match array.data_type() {
                $(
                    dt if dt == &<$ArrowType as arrow::datatypes::ArrowPrimitiveType>::DATA_TYPE => {
                        let arr = array.as_any().downcast_ref::<$ArrayType>().unwrap();
                        let _ = write!(out, "{}", arr.value(row));
                        return;
                    }
                )+
                _ => {}
            }
        };
    }

    try_primitive!(
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
            write_string_value(out, arr.value(row));
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            write_string_value(out, arr.value(row));
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let _ = write!(out, "{}", arr.value(row));
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let _ = write!(out, "<{} bytes>", arr.value(row).len());
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let _ = write!(out, "<{} bytes>", arr.value(row).len());
        }
        DataType::Timestamp(_, _) => {
            match arrow::util::display::array_value_to_string(array, row) {
                Ok(s) => out.push_str(&s),
                Err(_) => out.push_str("<timestamp>"),
            }
        }
        _ => {
            match arrow::util::display::array_value_to_string(array, row) {
                Ok(s) => out.push_str(&s),
                Err(_) => {
                    let _ = write!(out, "<{}>", array.data_type());
                }
            }
        }
    }
}

fn write_string_value(out: &mut String, s: &str) {
    const MAX_DISPLAY_WIDTH: usize = 200;
    let w = display_width(s);
    if w > MAX_DISPLAY_WIDTH {
        out.push('"');
        let mut current_width = 0;
        for c in s.chars() {
            let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
            if current_width + cw > MAX_DISPLAY_WIDTH {
                break;
            }
            out.push(c);
            current_width += cw;
        }
        let _ = write!(out, "...\" ({} chars)", s.chars().count());
    } else {
        out.push('"');
        out.push_str(s);
        out.push('"');
    }
}
