use std::fmt::{self, Write};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use smallvec::SmallVec;

use crate::layout::{RenderSpec, RenderSpecKind, RenderSpecNode, StructChild};
use crate::unicode::{display_width, truncate_to_width};

/// Controls the two spots where full-field preview rendering (`preview.rs`)
/// diverges from normal row rendering: scalar lists always go one-per-line
/// (so a previewed list reads like a list, not a truncated inline blob),
/// and a struct doesn't emit its leading blank line at depth 0 (there's no
/// preceding "field: " line to separate from when the struct itself is the
/// preview target).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RenderMode {
    Normal,
    Preview,
}

pub fn render_record(
    spec: &RenderSpec,
    batch: &RecordBatch,
    local_row: usize,
    global_row: usize,
    writer: &mut LineWriter,
) -> RenderedRow {
    writer.clear();
    if !spec.is_table() {
        let _ = write!(writer, "── Row {} ──", global_row);
        writer.newline();
    }
    spec.render_row(batch, local_row, writer);
    writer.finish()
}

impl RenderSpec {
    /// Render one data row. Dispatches to table or vertical based on the spec.
    pub fn render_row(&self, batch: &RecordBatch, row: usize, w: &mut LineWriter) {
        self.root.render_row(batch, row, w, 1);
    }

    pub fn render_table_header(&self) -> Vec<String> {
        let RenderSpecKind::Struct {
            children,
            col_widths,
            table_mode: true,
            ..
        } = &self.root.kind
        else {
            return Vec::new();
        };

        let mut header = String::new();
        let mut separator = String::new();
        for (ci, &cw) in col_widths.iter().enumerate() {
            if ci > 0 {
                header.push_str(COLUMN_SEPARATOR);
                separator.push_str("─┼─");
            }
            let name = &children[ci].name;
            let w = display_width(name);
            if w > cw {
                let truncated = truncate_to_width(name, cw);
                let tw = display_width(&truncated);
                header.push_str(&truncated);
                for _ in 0..cw.saturating_sub(tw) {
                    header.push(' ');
                }
            } else {
                header.push_str(name);
                for _ in 0..cw.saturating_sub(w) {
                    header.push(' ');
                }
            }
            for _ in 0..cw {
                separator.push('─');
            }
        }
        vec![header, separator]
    }
}

impl RenderSpecNode {
    /// Render a top-level row. Dispatches to table or vertical based on spec.
    fn render_row(&self, batch: &RecordBatch, row: usize, w: &mut LineWriter, depth: usize) {
        match &self.kind {
            RenderSpecKind::Struct {
                table_mode: true,
                col_widths,
                children,
                ..
            } => {
                for (di, &cw) in col_widths.iter().enumerate() {
                    if di > 0 {
                        w.buf.push_str(COLUMN_SEPARATOR);
                    }
                    let child = &children[di];
                    let col = batch.column(child.schema_idx);
                    child.spec.measure_cell(col.as_ref(), row, &mut w.scratch);
                    w.write_cell_padded(cw);
                }
                w.newline();
            }
            RenderSpecKind::Struct {
                table_mode: false,
                children,
                ..
            } => {
                for child in children {
                    let col = batch.column(child.schema_idx);
                    w.enter_field(depth as u8, child.schema_idx as u16);
                    w.guide(depth);
                    let _ = write!(w, "{}: ", child.name);
                    child.spec.render_value(col.as_ref(), row, w, depth, RenderMode::Normal);
                }
            }
            _ => unreachable!("root spec must be Struct"),
        }
    }

    /// Expand a value across multiple lines with tree guides.
    /// Each spec kind knows its own format: floats use precision,
    /// strings use max_display, structs recurse into children.
    ///
    /// `mode` controls the two spots where a standalone field preview
    /// (`preview::render_field_full`) needs different output than a normal
    /// row: see `RenderMode`.
    pub(crate) fn render_value(
        &self,
        array: &dyn Array,
        row: usize,
        w: &mut LineWriter,
        depth: usize,
        mode: RenderMode,
    ) {
        if array.is_null(row) {
            let _ = write!(w, "null");
            w.newline();
            return;
        }

        match &self.kind {
            RenderSpecKind::Scalar => {
                write_scalar_to(&mut w.buf, array, row);
                w.newline();
            }
            RenderSpecKind::Float {
                precision,
                exponential,
            } => {
                write_float_to(&mut w.buf, array, row, *precision, *exponential);
                w.newline();
            }
            RenderSpecKind::Str { max_display } => {
                write_string_verbose(&mut w.buf, array, row, *max_display);
                w.newline();
            }
            RenderSpecKind::Struct { children, .. } => {
                let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
                // The blank line separates a struct's children from the
                // "field: " text preceding it on the same line. A preview
                // opens directly on the struct with no such prefix, so it
                // skips straight to the children.
                if mode == RenderMode::Normal {
                    w.newline();
                }
                for child in children {
                    let col = sa.column(child.schema_idx);
                    w.enter_field((depth + 1) as u8, child.schema_idx as u16);
                    w.guide(depth + 1);
                    let _ = write!(w, "{}: ", child.name);
                    child.spec.render_value(col.as_ref(), row, w, depth + 1, mode);
                }
            }
            RenderSpecKind::List { element } => {
                let (start, end, values) = list_offsets(array, row);
                if start == end {
                    let _ = write!(w, "[]");
                    w.newline();
                    return;
                }
                // List<Struct> with table_mode: render as nested table
                if let RenderSpecKind::Struct {
                    table_mode: true,
                    children: child_specs,
                    col_widths,
                    row_prefix,
                    ..
                } = &element.kind
                {
                    let sa = values.as_any().downcast_ref::<StructArray>().unwrap();
                    render_nested_table(sa, start, end, child_specs, col_widths, row_prefix, w);
                    return;
                }
                // Scalar list: inline in normal mode (fits in the surrounding
                // row); one-per-line in preview mode (there's a whole popup
                // to spread out in, and it reads better for long lists).
                if mode == RenderMode::Normal && is_scalar_spec(&element.kind) {
                    w.buf.push('[');
                    for i in start..end {
                        if i > start {
                            w.buf.push_str(", ");
                        }
                        element.write_scalar_inline(&mut w.buf, values.as_ref(), i);
                    }
                    w.buf.push(']');
                    w.newline();
                } else {
                    let _ = write!(w, "({} items)", end - start);
                    w.newline();
                    for i in start..end {
                        w.guide(depth + 1);
                        let _ = write!(w, "[{}]: ", i - start);
                        element.render_value(values.as_ref(), i, w, depth + 1, mode);
                    }
                }
            }
            RenderSpecKind::Map { key, value } => {
                let ma = array.as_any().downcast_ref::<MapArray>().unwrap();
                let offsets = ma.offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let keys = ma.keys();
                let vals = ma.values();

                if start == end {
                    let _ = write!(w, "{{}}");
                    w.newline();
                } else {
                    w.newline();
                    for i in start..end {
                        w.guide(depth + 1);
                        key.write_scalar_inline(&mut w.buf, keys.as_ref(), i);
                        w.buf.push_str(": ");
                        value.render_value(vals.as_ref(), i, w, depth + 1, mode);
                    }
                }
            }
        }
    }

    fn measure_cell(&self, array: &dyn Array, row: usize, scratch: &mut String) {
        scratch.clear();
        self.write_cell_preview(scratch, array, row);
    }

    /// Generate a compact inline representation for use inside table cells.
    /// Produces enough content to fill the column; write_cell_padded truncates
    /// to the actual column width. Strings are raw (no "...(N chars)" metadata).
    pub(crate) fn write_cell_preview(&self, out: &mut String, array: &dyn Array, row: usize) {
        if array.is_null(row) {
            out.push_str("null");
            return;
        }
        if out.len() > CELL_PREVIEW_BUDGET {
            out.push('…');
            return;
        }

        match &self.kind {
            RenderSpecKind::Scalar => {
                write_scalar_to(out, array, row);
            }
            RenderSpecKind::Float {
                precision,
                exponential,
            } => {
                write_float_to(out, array, row, *precision, *exponential);
            }
            RenderSpecKind::Str { .. } => {
                write_string_raw(out, array, row);
            }
            RenderSpecKind::Struct { children, .. } => {
                let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
                let total = children.len();
                let preview_count = total.min(3);
                out.push('{');
                for (i, child) in children.iter().enumerate().take(preview_count) {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = write!(out, "{}: ", child.name);
                    child
                        .spec
                        .write_cell_preview(out, sa.column(child.schema_idx).as_ref(), row);
                    if out.len() > CELL_PREVIEW_BUDGET {
                        break;
                    }
                }
                if total > preview_count {
                    let _ = write!(out, ", +{}", total - preview_count);
                }
                out.push('}');
            }
            RenderSpecKind::List { element } => {
                let (s, e, values) = list_offsets(array, row);
                let len = e - s;
                if len == 0 {
                    out.push_str("[]");
                    return;
                }
                out.push('[');
                let mut shown = 0;
                for i in 0..len {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    if out.len() > CELL_PREVIEW_BUDGET {
                        break;
                    }
                    element.write_cell_preview(out, values.as_ref(), s + i);
                    shown += 1;
                }
                if shown < len {
                    let _ = write!(out, ", +{}", len - shown);
                }
                out.push(']');
            }
            RenderSpecKind::Map { key, value } => {
                let ma = array.as_any().downcast_ref::<MapArray>().unwrap();
                let o = ma.offsets();
                let start = o[row] as usize;
                let end = o[row + 1] as usize;
                let len = end - start;
                if len == 0 {
                    out.push_str("{}");
                    return;
                }
                let keys = ma.keys();
                let vals = ma.values();
                out.push('{');
                let mut shown = 0;
                for i in 0..len {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    if out.len() > CELL_PREVIEW_BUDGET {
                        let remaining = len - i;
                        let _ = write!(out, "+{}", remaining);
                        out.push('}');
                        break;
                    }
                    key.write_cell_preview(out, keys.as_ref(), start + i);
                    out.push_str(": ");
                    value.write_cell_preview(out, vals.as_ref(), start + i);
                    shown += 1;
                }
                if shown == len {
                    out.push('}');
                }
            }
        }
    }

    /// Write a scalar value inline (no newline). Used for scalar lists and map keys.
    pub(crate) fn write_scalar_inline(&self, out: &mut String, array: &dyn Array, row: usize) {
        match &self.kind {
            RenderSpecKind::Float {
                precision,
                exponential,
            } => {
                write_float_to(out, array, row, *precision, *exponential);
            }
            RenderSpecKind::Str { .. } => {
                write_string_raw(out, array, row);
            }
            _ => write_scalar_to(out, array, row),
        }
    }
}

fn render_nested_table(
    sa: &StructArray,
    start: usize,
    end: usize,
    children: &[StructChild],
    col_widths: &[usize],
    row_prefix: &str,
    w: &mut LineWriter,
) {
    let count = end - start;
    if count == 0 {
        let _ = write!(w, "[]");
        w.newline();
        return;
    }

    let _ = write!(w, "({} items)", count);
    w.newline();

    // Column headers
    w.buf.push_str(row_prefix);
    for (di, &cw) in col_widths.iter().enumerate() {
        if di > 0 {
            w.buf.push_str(COLUMN_SEPARATOR);
        }
        w.write_padded(&children[di].name, cw);
    }
    w.newline();

    // Separator
    w.buf.push_str(row_prefix);
    for (di, &cw) in col_widths.iter().enumerate() {
        if di > 0 {
            w.buf.push_str("─┼─");
        }
        for _ in 0..cw {
            w.buf.push('─');
        }
    }
    w.newline();

    // Data rows
    for row in start..end {
        w.buf.push_str(row_prefix);
        for (di, &cw) in col_widths.iter().enumerate() {
            if di > 0 {
                w.buf.push_str(COLUMN_SEPARATOR);
            }
            let child = &children[di];
            let col = sa.column(child.schema_idx);
            child.spec.measure_cell(col.as_ref(), row, &mut w.scratch);
            w.write_cell_padded(cw);
        }
        w.newline();
    }
}

fn is_scalar_spec(kind: &RenderSpecKind) -> bool {
    matches!(
        kind,
        RenderSpecKind::Scalar | RenderSpecKind::Float { .. } | RenderSpecKind::Str { .. }
    )
}

/// Which field a run of rendered lines (vertical mode) belongs to. Lines
/// from `start_line` up to the next span's `start_line` (or end of row)
/// are that field's own line plus any of its own un-tagged content (e.g.
/// a struct's blank separator line before its children get their own spans).
#[derive(Clone, Copy)]
struct FieldSpan {
    start_line: u16,
    depth: u8,
    schema_idx: u16,
}

/// Immutable rendered output for one data row. Stored in cache.
pub struct RenderedRow {
    buf: String,
    line_starts: Vec<usize>,
    field_spans: Vec<FieldSpan>,
}

impl RenderedRow {
    pub fn line_count(&self) -> usize {
        self.line_starts.len()
    }

    pub fn line(&self, idx: usize) -> &str {
        let start = self.line_starts[idx];
        let end = if idx + 1 < self.line_starts.len() {
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
            + self.field_spans.len() * std::mem::size_of::<FieldSpan>()
            + std::mem::size_of::<Self>()
    }

    /// Schema path (root to leaf) of the field that rendered `line_idx`,
    /// replacing the old approach of parsing guide characters and field
    /// names back out of the rendered text. Empty outside vertical mode,
    /// or for lines (row header, struct's blank separator) that precede
    /// any field's own content at that depth.
    pub fn field_path(&self, line_idx: usize) -> SmallVec<[usize; 4]> {
        let line_idx = line_idx as u16;
        let mut path: SmallVec<[usize; 4]> = SmallVec::new();
        for span in &self.field_spans {
            if span.start_line > line_idx {
                break;
            }
            path.truncate(span.depth as usize - 1);
            path.push(span.schema_idx as usize);
        }
        path
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

/// One level of tree-guide indentation, prepended to vertical-mode lines.
const GUIDE_UNIT: &str = "│ ";

/// Separates table-mode columns in a rendered line. Shared with `tui/`
/// code that locates column boundaries in already-rendered text
/// (cursor highlighting, column counting).
pub const COLUMN_SEPARATOR: &str = " │ ";

const MAX_GUIDE_DEPTH: usize = 32;

fn guide_str() -> &'static str {
    static GUIDE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    GUIDE.get_or_init(|| GUIDE_UNIT.repeat(MAX_GUIDE_DEPTH))
}

/// Reusable buffer that accumulates rendered output with zero per-line allocation.
pub struct LineWriter {
    pub(crate) buf: String,
    line_starts: Vec<usize>,
    pub(crate) scratch: String,
    field_spans: Vec<FieldSpan>,
}

impl LineWriter {
    pub fn new() -> Self {
        LineWriter {
            buf: String::new(),
            line_starts: vec![0],
            scratch: String::new(),
            field_spans: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.line_starts.clear();
        self.line_starts.push(0);
        self.field_spans.clear();
    }

    pub fn newline(&mut self) {
        self.buf.push('\n');
        self.line_starts.push(self.buf.len());
    }

    pub fn guide(&mut self, depth: usize) {
        let g = guide_str();
        let byte_len = depth.min(MAX_GUIDE_DEPTH) * GUIDE_UNIT.len();
        self.buf.push_str(&g[..byte_len]);
    }

    /// Record that the line about to be written (vertical mode only) is
    /// `schema_idx`'s field at tree depth `depth`, so `RenderedRow::field_path`
    /// can answer "which field is this line?" without re-parsing guide
    /// characters and field names out of the rendered text.
    pub fn enter_field(&mut self, depth: u8, schema_idx: u16) {
        let current_line = (self.line_starts.len() - 1) as u16;
        self.field_spans.push(FieldSpan {
            start_line: current_line,
            depth,
            schema_idx,
        });
    }

    pub fn finish(&self) -> RenderedRow {
        let mut line_starts = self.line_starts.clone();
        if line_starts.len() > 1 && *line_starts.last().unwrap() == self.buf.len() {
            line_starts.pop();
        }
        RenderedRow {
            buf: self.buf.clone(),
            line_starts,
            field_spans: self.field_spans.clone(),
        }
    }

    fn write_cell_padded(&mut self, width: usize) {
        let vw = display_width(&self.scratch);
        if vw > width {
            let truncated = truncate_to_width(&self.scratch, width);
            let tw = display_width(&truncated);
            self.buf.push_str(&truncated);
            for _ in 0..width.saturating_sub(tw) {
                self.buf.push(' ');
            }
        } else {
            self.buf.push_str(&self.scratch);
            for _ in 0..(width - vw) {
                self.buf.push(' ');
            }
        }
    }

    fn write_padded(&mut self, s: &str, width: usize) {
        let w = display_width(s);
        if w > width {
            let truncated = truncate_to_width(s, width);
            let tw = display_width(&truncated);
            self.buf.push_str(&truncated);
            for _ in 0..width.saturating_sub(tw) {
                self.buf.push(' ');
            }
        } else {
            self.buf.push_str(s);
            for _ in 0..(width - w) {
                self.buf.push(' ');
            }
        }
    }
}

impl fmt::Write for LineWriter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.buf.push_str(s);
        Ok(())
    }
}

/// Max bytes to generate for a cell preview before bailing out.
/// Prevents runaway string generation for huge maps/lists.
/// Column truncation (write_cell_padded) handles the visual cut.
const CELL_PREVIEW_BUDGET: usize = 512;

fn write_float_to(
    out: &mut String,
    array: &dyn Array,
    row: usize,
    precision: u8,
    exponential: bool,
) {
    if array.is_null(row) {
        out.push_str("null");
        return;
    }
    let v = crate::layout::extract_float(array, row);
    if !v.is_finite() {
        write_scalar_to(out, array, row);
        return;
    }
    if exponential {
        let _ = write!(out, "{:.prec$e}", v, prec = precision as usize);
    } else {
        let _ = write!(out, "{:.prec$}", v, prec = precision as usize);
    }
}

/// Truncate long strings with a "(N chars)" hint. For vertical mode where
/// the user is looking at one value and wants to know how much was cut.
fn write_string_verbose(out: &mut String, array: &dyn Array, row: usize, max_display: usize) {
    if array.is_null(row) {
        out.push_str("null");
        return;
    }
    let s = extract_str(array, row);
    let w = display_width(s);
    if w > max_display {
        out.push('"');
        let mut current_width = 0;
        for c in s.chars() {
            let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
            if current_width + cw > max_display {
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

/// Quoted string with no truncation. For cell previews where the column
/// width handles the visual cut — we don't want "(N chars)" noise in a
/// map key or list element.
fn write_string_raw(out: &mut String, array: &dyn Array, row: usize) {
    if array.is_null(row) {
        out.push_str("null");
        return;
    }
    let s = extract_str(array, row);
    out.push('"');
    out.push_str(s);
    out.push('"');
}

pub(crate) fn extract_str(array: &dyn Array, row: usize) -> &str {
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        a.value(row)
    } else if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        a.value(row)
    } else {
        ""
    }
}

/// Write a scalar value into a buffer. Used by spec methods and layout sampling.
pub(crate) fn write_scalar_to(out: &mut String, array: &dyn Array, row: usize) {
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
            out.push('"');
            out.push_str(arr.value(row));
            out.push('"');
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            out.push('"');
            out.push_str(arr.value(row));
            out.push('"');
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
        _ => match arrow::util::display::array_value_to_string(array, row) {
            Ok(s) => out.push_str(&s),
            Err(_) => {
                let _ = write!(out, "<{}>", array.data_type());
            }
        },
    }
}

pub(crate) fn list_offsets(array: &dyn Array, row: usize) -> (usize, usize, Arc<dyn Array>) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{Layout, RenderSpec};
    use crate::source::DataSource;
    use crate::source::test_support::FakeDataSource;

    #[test]
    fn render_table_row_contains_values() {
        let mut source = FakeDataSource::two_columns(&[("alice", 42), ("bob", 7)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        assert!(spec.is_table());

        let mut writer = LineWriter::new();
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let rendered = render_record(&spec, batch, local_row, 0, &mut writer);

        assert_eq!(rendered.line_count(), 1);
        let line = rendered.line(0);
        assert!(line.contains("alice"), "expected 'alice' in: {}", line);
        assert!(line.contains("42"), "expected '42' in: {}", line);
    }

    #[test]
    fn render_multiple_rows() {
        let mut source = FakeDataSource::two_columns(&[("alice", 1), ("bob", 2), ("charlie", 3)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);

        let mut writer = LineWriter::new();
        for row in 0..3 {
            source.ensure_loaded(row).unwrap();
            let (batch, local_row) = source.get_row(row);
            let rendered = render_record(&spec, batch, local_row, row, &mut writer);
            assert_eq!(rendered.line_count(), 1);
        }
    }

    #[test]
    fn lines_iterator_matches_indexed_access() {
        let mut source = FakeDataSource::two_columns(&[("x", 1)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);

        let mut writer = LineWriter::new();
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let rendered = render_record(&spec, batch, local_row, 0, &mut writer);

        let lines_vec: Vec<&str> = rendered.lines().collect();
        for (i, line) in lines_vec.iter().enumerate() {
            assert_eq!(*line, rendered.line(i));
        }
    }

    #[test]
    fn table_header_has_separator() {
        let mut source = FakeDataSource::two_columns(&[("alice", 1)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);

        let header = spec.render_table_header();
        assert_eq!(header.len(), 2);
        assert!(
            header[0].contains("│"),
            "header should have column separator"
        );
        assert!(
            header[1].contains("─┼─"),
            "separator row should have crossing"
        );
    }

    #[test]
    fn field_path_empty_in_table_mode() {
        let mut source = FakeDataSource::two_columns(&[("alice", 42)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        assert!(spec.is_table());

        let mut writer = LineWriter::new();
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let rendered = render_record(&spec, batch, local_row, 0, &mut writer);

        assert!(rendered.field_path(0).is_empty());
    }

    fn nested_vertical_source() -> FakeDataSource {
        use arrow::datatypes::{Field, Schema};

        let inner = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("desc", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["d"])) as Arc<dyn Array>,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", inner.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![7])) as Arc<dyn Array>,
                Arc::new(inner) as Arc<dyn Array>,
            ],
        )
        .unwrap();
        FakeDataSource::from_batch(batch)
    }

    #[test]
    fn field_path_tags_top_level_and_nested_struct_fields() {
        let mut source = nested_vertical_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        assert!(!spec.is_table());

        let mut writer = LineWriter::new();
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let rendered = render_record(&spec, batch, local_row, 0, &mut writer);

        let lines: Vec<&str> = rendered.lines().collect();
        // line 0: "── Row 0 ──"; line 1: "id: 7"; line 2: "│ nested: ";
        // line 3: "│ │ x: 1"; line 4: "│ │ desc: \"d\""
        assert!(lines[1].contains("id: 7"), "got {:?}", lines);
        assert!(lines[3].contains("x: 1"), "got {:?}", lines);
        assert!(lines[4].contains("desc"), "got {:?}", lines);

        assert!(rendered.field_path(0).is_empty(), "row header has no field");
        assert_eq!(&rendered.field_path(1)[..], &[0], "id is field 0");
        assert_eq!(&rendered.field_path(2)[..], &[1], "nested's own line is field 1");
        assert_eq!(
            &rendered.field_path(3)[..],
            &[1, 0],
            "nested.x is [1, 0]"
        );
        assert_eq!(
            &rendered.field_path(4)[..],
            &[1, 1],
            "nested.desc is [1, 1]"
        );
    }
}
