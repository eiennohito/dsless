use arrow::array::*;
use arrow::datatypes::{DataType, Schema};

use crate::render;
use crate::source::DataSource;
use crate::unicode::display_width;

/// Capture what the data looks like, independent of how it will be displayed.
/// Computed once from schema + sampled data. Does not know about terminal width.
/// This is the type that will be persisted for per-schema display preferences (#2).
pub struct Layout {
    pub root: LayoutNode,
}

/// Per-field statistics derived from data sampling.
/// Drives width allocation and format decisions during RenderSpec resolution.
pub struct LayoutNode {
    pub natural_width: usize,
    pub max_width: usize,
    pub header_width: usize,
    pub kind: LayoutKind,
}

pub enum LayoutKind {
    Scalar,
    Float {
        precision: u8,
        exponential: bool,
    },
    Str {
        max_display: usize,
    },
    Struct {
        children: Vec<(String, LayoutNode)>,
        prefer_table: bool,
    },
    List {
        element: Box<LayoutNode>,
    },
    Map {
        key: Box<LayoutNode>,
        value: Box<LayoutNode>,
    },
}

/// Concrete rendering decisions for the current terminal width.
/// The single source of truth for all rendering — rendering methods
/// dispatch on RenderSpecKind, never on Arrow DataType.
pub struct RenderSpec {
    pub root: RenderSpecNode,
}

impl RenderSpec {
    /// Whether the root layout is table mode (all-scalar flat schema).
    pub fn is_table(&self) -> bool {
        matches!(
            &self.root.kind,
            RenderSpecKind::Struct {
                table_mode: true,
                ..
            }
        )
    }
}

/// A node in the RenderSpec tree. Each node knows how to render its
/// corresponding Arrow value — precision for floats, truncation for strings,
/// column widths for table structs, preview budgets for collections.
pub struct RenderSpecNode {
    pub kind: RenderSpecKind,
}

pub enum RenderSpecKind {
    Scalar,
    Float {
        precision: u8,
        exponential: bool,
    },
    Str {
        max_display: usize,
    },
    Struct {
        children: Vec<(String, RenderSpecNode)>,
        table_mode: bool,
        col_widths: Vec<usize>,
        /// Mapping from display position to schema column index.
        /// `col_order[display_idx] = schema_idx`. Identity if no reorder.
        col_order: Vec<usize>,
        /// Fixed prefix for each data row (guides + left padding).
        /// Empty for top-level structs.
        row_prefix: String,
    },
    List {
        element: Box<RenderSpecNode>,
    },
    Map {
        key: Box<RenderSpecNode>,
        value: Box<RenderSpecNode>,
    },
}

const SAMPLE_SIZE: usize = 200;
const DEFAULT_STR_MAX_DISPLAY: usize = 200;
/// Max list/map elements to feed per cell during sampling.
const MAX_ELEMENTS_PER_CELL: usize = 5;

impl Layout {
    /// Derive display characteristics from the actual data.
    /// Samples rows and walks the full schema tree to capture
    /// width distributions, float precision needs, and table-mode eligibility.
    pub fn compute(source: &mut dyn DataSource) -> Layout {
        let schema = source.schema().clone();
        let sample_size = source.total_rows().min(SAMPLE_SIZE);

        let mut root = LayoutBuilder::from_schema_struct(&schema);

        let mut scratch = String::new();
        for row in 0..sample_size {
            if source.ensure_loaded(row).is_err() {
                continue;
            }
            let (batch, local_row) = source.get_row(row);
            root.feed_struct(batch, local_row, &mut scratch);
        }

        let root = root.resolve_struct();
        Layout { root }
    }
}

/// Track how wide values are across sampled rows.
struct WidthAccum {
    widths: Vec<usize>,
    max: usize,
}

impl WidthAccum {
    fn new() -> Self {
        Self {
            widths: Vec::new(),
            max: 0,
        }
    }

    fn record(&mut self, width: usize) {
        self.widths.push(width);
        if width > self.max {
            self.max = width;
        }
    }

    /// p80 + 10%, or 1 if empty. Sorts in place — call only when done accumulating.
    fn p80_plus10(&mut self) -> usize {
        if self.widths.is_empty() {
            return 1;
        }
        self.widths.sort_unstable();
        let p80_idx = (self.widths.len() * 4 / 5).min(self.widths.len().saturating_sub(1));
        let p80 = self.widths[p80_idx];
        (p80 + p80 / 10).max(1)
    }
}

/// Schema-shaped accumulator tree. Built empty from the schema, then fed
/// actual values row by row. Each variant accumulates the stats needed to
/// produce its corresponding LayoutKind (widths for all, precision for floats, etc).
enum LayoutBuilder {
    Scalar {
        widths: WidthAccum,
    },
    Float {
        widths: WidthAccum,
        values: Vec<f64>,
    },
    Str {
        widths: WidthAccum,
        lengths: Vec<usize>,
    },
    Struct {
        children: Vec<(String, LayoutBuilder)>,
        widths: WidthAccum,
    },
    List {
        element: Box<LayoutBuilder>,
        widths: WidthAccum,
    },
    Map {
        key: Box<LayoutBuilder>,
        value: Box<LayoutBuilder>,
        widths: WidthAccum,
    },
}

impl LayoutBuilder {
    /// Build a hypo tree for the top-level schema (Struct of fields).
    fn from_schema_struct(schema: &Schema) -> Self {
        let children: Vec<(String, LayoutBuilder)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), Self::from_data_type(f.data_type())))
            .collect();
        LayoutBuilder::Struct {
            children,
            widths: WidthAccum::new(),
        }
    }

    /// Build a hypo node from an Arrow DataType.
    fn from_data_type(dt: &DataType) -> Self {
        match dt {
            DataType::Float32 | DataType::Float64 => LayoutBuilder::Float {
                widths: WidthAccum::new(),
                values: Vec::new(),
            },
            DataType::Utf8 | DataType::LargeUtf8 => LayoutBuilder::Str {
                widths: WidthAccum::new(),
                lengths: Vec::new(),
            },
            DataType::Struct(fields) => {
                let children = fields
                    .iter()
                    .map(|f| (f.name().clone(), Self::from_data_type(f.data_type())))
                    .collect();
                LayoutBuilder::Struct {
                    children,
                    widths: WidthAccum::new(),
                }
            }
            DataType::List(field) | DataType::LargeList(field) => LayoutBuilder::List {
                element: Box::new(Self::from_data_type(field.data_type())),
                widths: WidthAccum::new(),
            },
            DataType::Map(field, _) => {
                let inner_fields = match field.data_type() {
                    DataType::Struct(f) => f,
                    _ => unreachable!("Map inner type must be Struct"),
                };
                LayoutBuilder::Map {
                    key: Box::new(Self::from_data_type(inner_fields[0].data_type())),
                    value: Box::new(Self::from_data_type(inner_fields[1].data_type())),
                    widths: WidthAccum::new(),
                }
            }
            _ => LayoutBuilder::Scalar {
                widths: WidthAccum::new(),
            },
        }
    }

    /// Walk actual data and accumulate stats at each node.
    fn feed_struct(&mut self, batch: &RecordBatch, row: usize, scratch: &mut String) {
        let LayoutBuilder::Struct { children, .. } = self else {
            unreachable!("root must be Struct");
        };
        for (ci, (_, child)) in children.iter_mut().enumerate() {
            let col = batch.column(ci);
            child.feed(col.as_ref(), row, scratch);
        }
    }

    fn feed(&mut self, array: &dyn Array, row: usize, scratch: &mut String) {
        if array.is_null(row) {
            // Record "null" width (4 chars) but don't accumulate type-specific stats
            self.record_width(4);
            return;
        }

        match self {
            LayoutBuilder::Scalar { widths } => {
                measure_cell_width(scratch, array, row);
                widths.record(display_width(scratch));
            }
            LayoutBuilder::Float { widths, values } => {
                measure_cell_width(scratch, array, row);
                widths.record(display_width(scratch));
                let v = extract_float(array, row);
                if v.is_finite() {
                    values.push(v);
                }
            }
            LayoutBuilder::Str { widths, lengths } => {
                measure_cell_width(scratch, array, row);
                widths.record(display_width(scratch));
                let len = extract_string_width(array, row);
                lengths.push(len);
            }
            LayoutBuilder::Struct {
                children,
                widths,
            } => {
                measure_cell_width(scratch, array, row);
                widths.record(display_width(scratch));
                let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
                for (ci, (_, child)) in children.iter_mut().enumerate() {
                    child.feed(sa.column(ci).as_ref(), row, scratch);
                }
            }
            LayoutBuilder::List { element, widths } => {
                measure_cell_width(scratch, array, row);
                widths.record(display_width(scratch));
                let (start, end, values) = list_offsets(array, row);
                let sample_end = end.min(start + MAX_ELEMENTS_PER_CELL);
                for i in start..sample_end {
                    element.feed(values.as_ref(), i, scratch);
                }
            }
            LayoutBuilder::Map { key, value, widths } => {
                measure_cell_width(scratch, array, row);
                widths.record(display_width(scratch));
                let ma = array.as_any().downcast_ref::<MapArray>().unwrap();
                let offsets = ma.offsets();
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let sample_end = end.min(start + MAX_ELEMENTS_PER_CELL);
                let keys = ma.keys();
                let values = ma.values();
                for i in start..sample_end {
                    key.feed(keys.as_ref(), i, scratch);
                    value.feed(values.as_ref(), i, scratch);
                }
            }
        }
    }

    fn record_width(&mut self, w: usize) {
        match self {
            LayoutBuilder::Scalar { widths }
            | LayoutBuilder::Float { widths, .. }
            | LayoutBuilder::Str { widths, .. }
            | LayoutBuilder::Struct { widths, .. }
            | LayoutBuilder::List { widths, .. }
            | LayoutBuilder::Map { widths, .. } => widths.record(w),
        }
    }

    // --------------------------------------------------------
    // Resolution: accumulated stats -> LayoutNode
    // --------------------------------------------------------

    /// Convert accumulated stats into final layout decisions.
    fn resolve_struct(self) -> LayoutNode {
        let LayoutBuilder::Struct { children, .. } = self else {
            unreachable!("root must be Struct");
        };
        // Top-level: table only if all fields are scalar
        let all_scalar = children
            .iter()
            .all(|(_, child)| matches!(child, LayoutBuilder::Scalar { .. } | LayoutBuilder::Float { .. } | LayoutBuilder::Str { .. }));

        let resolved: Vec<(String, LayoutNode)> = children
            .into_iter()
            .map(|(name, child)| {
                let header_width = display_width(&name);
                let node = child.resolve(header_width);
                (name, node)
            })
            .collect();

        let natural_width: usize = resolved.iter().map(|(_, n)| n.natural_width).sum();
        let max_width: usize = resolved.iter().map(|(_, n)| n.max_width).sum();
        LayoutNode {
            natural_width,
            max_width,
            header_width: 0,
            kind: LayoutKind::Struct {
                children: resolved,
                prefer_table: all_scalar,
            },
        }
    }

    fn resolve(self, header_width: usize) -> LayoutNode {
        match self {
            LayoutBuilder::Scalar { mut widths } => LayoutNode {
                natural_width: widths.p80_plus10(),
                max_width: widths.max,
                header_width,
                kind: LayoutKind::Scalar,
            },
            LayoutBuilder::Float { mut widths, mut values } => {
                let (precision, exponential) = compute_float_precision(&mut values);
                LayoutNode {
                    natural_width: widths.p80_plus10(),
                    max_width: widths.max,
                    header_width,
                    kind: LayoutKind::Float {
                        precision,
                        exponential,
                    },
                }
            }
            LayoutBuilder::Str { mut widths, mut lengths } => {
                let max_display = resolve_str_max_display(&mut lengths);
                LayoutNode {
                    natural_width: widths.p80_plus10(),
                    max_width: widths.max,
                    header_width,
                    kind: LayoutKind::Str { max_display },
                }
            }
            LayoutBuilder::Struct { children, mut widths } => {
                let table_ok = children.iter().all(|(_, child)| !has_nested_struct(child));
                let resolved: Vec<(String, LayoutNode)> = children
                    .into_iter()
                    .map(|(name, child)| {
                        let hw = display_width(&name);
                        let node = child.resolve(hw);
                        (name, node)
                    })
                    .collect();
                LayoutNode {
                    natural_width: widths.p80_plus10(),
                    max_width: widths.max,
                    header_width,
                    kind: LayoutKind::Struct {
                        children: resolved,
                        prefer_table: table_ok,
                    },
                }
            }
            LayoutBuilder::List { element, mut widths } => {
                let element_node = element.resolve(0);
                LayoutNode {
                    natural_width: widths.p80_plus10(),
                    max_width: widths.max,
                    header_width,
                    kind: LayoutKind::List {
                        element: Box::new(element_node),
                    },
                }
            }
            LayoutBuilder::Map { key, value, mut widths } => {
                let key_node = key.resolve(0);
                let value_node = value.resolve(0);
                LayoutNode {
                    natural_width: widths.p80_plus10(),
                    max_width: widths.max,
                    header_width,
                    kind: LayoutKind::Map {
                        key: Box::new(key_node),
                        value: Box::new(value_node),
                    },
                }
            }
        }
    }
}

/// Resolve string max_display from accumulated lengths.
fn resolve_str_max_display(lengths: &mut [usize]) -> usize {
    if lengths.is_empty() {
        return DEFAULT_STR_MAX_DISPLAY;
    }
    lengths.sort_unstable();
    let p80_idx = (lengths.len() * 4 / 5).min(lengths.len().saturating_sub(1));
    let p80 = lengths[p80_idx];
    (p80 + p80 / 10).clamp(1, DEFAULT_STR_MAX_DISPLAY)
}

/// True if this child is a List/Map containing a Struct (i.e. would render as a nested table).
fn has_nested_struct(node: &LayoutBuilder) -> bool {
    match node {
        LayoutBuilder::List { element, .. } => matches!(**element, LayoutBuilder::Struct { .. }),
        LayoutBuilder::Map { value, .. } => matches!(**value, LayoutBuilder::Struct { .. }),
        _ => false,
    }
}


pub(crate) fn extract_float(array: &dyn Array, row: usize) -> f64 {
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        a.value(row)
    } else if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        a.value(row) as f64
    } else {
        f64::NAN
    }
}

fn extract_string_width(array: &dyn Array, row: usize) -> usize {
    display_width(render::extract_str(array, row))
}

use render::list_offsets;

fn measure_cell_width(scratch: &mut String, array: &dyn Array, row: usize) {
    scratch.clear();
    render::write_scalar_to(scratch, array, row);
}


/// Count decimal digits in the shortest roundtrip representation of a float.
fn decimal_digit_count(v: f64) -> u8 {
    let mut buf = ryu::Buffer::new();
    let s = buf.format(v);
    match s.find('.') {
        Some(dot) => {
            let frac = &s[dot + 1..];
            let trimmed = frac.trim_end_matches('0');
            if trimmed.is_empty() { 0 } else { trimmed.len() as u8 }
        }
        None => 0,
    }
}

/// Compute float precision and whether to use exponential format.
///
/// Two signals:
/// - Per-value: how many decimal digits each value has in its shortest representation.
/// - Bucket-diff: how many digits needed to distinguish adjacent percentile buckets.
///
/// Per-value consensus is trusted only for low precision (≤ 3) — prices, percentages.
/// Above that, bucket-diff decides: 0.916667 and 0.083333 have 6 digits each but only
/// need 2 to distinguish.
fn compute_float_precision(values: &mut [f64]) -> (u8, bool) {
    if values.is_empty() {
        return (2, false);
    }

    // Signal 1: per-value roundtrip precision
    let mut precisions: Vec<u8> = values.iter().map(|&v| decimal_digit_count(v)).collect();
    precisions.sort_unstable();

    let p10_idx = (precisions.len() / 10).min(precisions.len().saturating_sub(1));
    let p90_idx = (precisions.len() * 9 / 10).min(precisions.len().saturating_sub(1));
    let p10 = precisions[p10_idx];
    let p90 = precisions[p90_idx];

    // Fast path: low uniform precision — trust it (prices, percentages, counts)
    if p90 <= 3 && p10 == p90 {
        return (p90, false);
    }
    if p90 <= 3 {
        let count_at_p90 = precisions.iter().filter(|&&p| p <= p90).count();
        if count_at_p90 >= precisions.len() * 9 / 10 {
            return (p90, false);
        }
    }

    // Signal 2: bucket-difference resolution
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let sorted = values;

    let n = sorted.len();
    let p5_idx = (n * 5 / 100).min(n.saturating_sub(1));
    let p95_idx = (n * 95 / 100).min(n.saturating_sub(1));
    let p5 = sorted[p5_idx];
    let p95 = sorted[p95_idx];
    let span = p95 - p5;

    if span == 0.0 {
        return (p90.min(6), false);
    }

    // Walk 1% buckets and find minimum nonzero diff
    let mut min_diff = f64::MAX;
    let bucket_count = 90;
    for b in 0..bucket_count {
        let lo_idx = ((n as f64) * (5.0 + b as f64) / 100.0) as usize;
        let hi_idx = ((n as f64) * (6.0 + b as f64) / 100.0) as usize;
        let lo_idx = lo_idx.min(n.saturating_sub(1));
        let hi_idx = hi_idx.min(n.saturating_sub(1));
        let diff = sorted[hi_idx] - sorted[lo_idx];
        if diff > 0.0 && diff < min_diff {
            min_diff = diff;
        }
    }

    if min_diff == f64::MAX {
        return (p90.min(6), false);
    }

    // Check if exponential format is needed
    let log_span = span.log10();
    let log_diff = min_diff.log10();
    if log_span - log_diff > 6.0 {
        return (2, true);
    }

    let bucket_precision = (-min_diff.log10()).ceil().max(0.0) as u8;
    let precision = bucket_precision.min(p90).max(1);
    (precision.min(6), false)
}


/// Minimum useful column width.
const MIN_COL: usize = 8;

struct ResolveCtx {
    terminal_width: usize,
    depth: usize,
}

impl ResolveCtx {
    /// Width available for content at the current depth.
    fn content_width(&self) -> usize {
        let guide_chars = self.depth * 2; // "│ " per level
        self.terminal_width.saturating_sub(guide_chars)
    }

    fn deeper(&self) -> ResolveCtx {
        ResolveCtx {
            terminal_width: self.terminal_width,
            depth: self.depth + 1,
        }
    }
}

impl RenderSpec {
    /// Turn data-derived layout into concrete rendering decisions for this terminal width.
    /// Distributes column widths, classifies bounded/unbounded columns,
    /// moves the least-bounded column rightmost, and precomputes row prefixes.
    pub fn resolve(layout: &Layout, terminal_width: usize) -> RenderSpec {
        let ctx = ResolveCtx {
            terminal_width,
            depth: 0,
        };
        let mut root = resolve_node(&layout.root, &ctx);
        align_adjacent_tables(&mut root);
        RenderSpec { root }
    }
}

fn resolve_node(node: &LayoutNode, ctx: &ResolveCtx) -> RenderSpecNode {
    match &node.kind {
        LayoutKind::Scalar => RenderSpecNode {
            kind: RenderSpecKind::Scalar,
        },
        LayoutKind::Float {
            precision,
            exponential,
        } => RenderSpecNode {
            kind: RenderSpecKind::Float {
                precision: *precision,
                exponential: *exponential,
            },
        },
        LayoutKind::Str { max_display } => RenderSpecNode {
            kind: RenderSpecKind::Str {
                max_display: (*max_display).min(ctx.content_width()),
            },
        },
        LayoutKind::Struct {
            children,
            prefer_table,
        } => resolve_struct(children, *prefer_table, ctx),
        LayoutKind::List { element } => {
            let child = resolve_node(element, ctx);
            RenderSpecNode {
                kind: RenderSpecKind::List {
                    element: Box::new(child),
                },
            }
        }
        LayoutKind::Map { key, value } => {
            let key_spec = resolve_node(key, ctx);
            let value_spec = resolve_node(value, ctx);
            RenderSpecNode {
                kind: RenderSpecKind::Map {
                    key: Box::new(key_spec),
                    value: Box::new(value_spec),
                },
            }
        }
    }
}

/// Allocate terminal width across struct fields.
/// Bounded columns get tight widths. The least-bounded column moves
/// rightmost and receives all remaining space.
fn resolve_struct(
    children: &[(String, LayoutNode)],
    prefer_table: bool,
    ctx: &ResolveCtx,
) -> RenderSpecNode {
    if !prefer_table || children.is_empty() {
        let child_ctx = ctx.deeper();
        let resolved_children: Vec<(String, RenderSpecNode)> = children
            .iter()
            .map(|(name, node)| {
                let child = resolve_node(node, &child_ctx);
                (name.clone(), child)
            })
            .collect();
        let col_order: Vec<usize> = (0..children.len()).collect();
        return RenderSpecNode {
            kind: RenderSpecKind::Struct {
                children: resolved_children,
                table_mode: false,
                col_widths: vec![],
                col_order,
                row_prefix: build_row_prefix(ctx.depth + 1, false),
            },
        };
    }

    // Table mode: build the row prefix
    // Top-level (depth 0): no prefix — render_row writes columns directly
    // Nested: guides at depth+1 plus left padding
    let is_nested = ctx.depth > 0;
    let prefix = if is_nested {
        build_row_prefix(ctx.depth + 1, true)
    } else {
        String::new()
    };
    let prefix_width = crate::unicode::display_width(&prefix);

    let num_fields = children.len();
    let separators = if num_fields > 1 {
        (num_fields - 1) * 3
    } else {
        0
    };
    let distributable = ctx.terminal_width.saturating_sub(prefix_width + separators);

    // Classify columns: bounded (max ≈ p80) vs unbounded (max >> p80)
    const BOUNDED_THRESHOLD: f64 = 1.5;

    let boundedness: Vec<f64> = children
        .iter()
        .map(|(_, node)| {
            if node.natural_width == 0 {
                1.0
            } else {
                node.max_width as f64 / node.natural_width as f64
            }
        })
        .collect();

    let natural_widths: Vec<usize> = children
        .iter()
        .map(|(_, node)| {
            // Cap at p80 + 10% so outlier-wide values don't waste space
            let data_w = node.natural_width;
            // Use header width if wider, but don't let it dominate
            data_w.max(node.header_width.min(data_w + data_w / 10)).max(1)
        })
        .collect();

    // Find the least-bounded column (highest ratio)
    let least_bounded_idx = boundedness
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
        .unwrap_or(0);

    let has_unbounded = boundedness[least_bounded_idx] >= BOUNDED_THRESHOLD;

    // Build display order: schema order, but move least-bounded to rightmost
    let mut display_order: Vec<usize> = (0..num_fields).collect();
    if has_unbounded && least_bounded_idx != num_fields - 1 {
        display_order.retain(|&i| i != least_bounded_idx);
        display_order.push(least_bounded_idx);
    }

    // Allocate widths: bounded columns get natural_width, least-bounded gets remainder
    let col_widths: Vec<usize> = if has_unbounded {
        let bounded_total: usize = display_order
            .iter()
            .filter(|&&i| i != least_bounded_idx)
            .map(|&i| natural_widths[i])
            .sum();
        let remainder = distributable.saturating_sub(bounded_total);

        display_order
            .iter()
            .map(|&i| {
                if i == least_bounded_idx {
                    remainder.max(MIN_COL)
                } else {
                    natural_widths[i]
                }
            })
            .collect()
    } else {
        // All bounded: distribute normally in display order
        let ordered_naturals: Vec<usize> = display_order.iter().map(|&i| natural_widths[i]).collect();
        distribute_column_widths(&ordered_naturals, distributable)
    };

    // Build children in display order
    let child_ctx = ctx.deeper();
    let resolved_children: Vec<(String, RenderSpecNode)> = display_order
        .iter()
        .map(|&i| {
            let (name, node) = &children[i];
            let child = resolve_node(node, &child_ctx);
            (name.clone(), child)
        })
        .collect();

    RenderSpecNode {
        kind: RenderSpecKind::Struct {
            children: resolved_children,
            table_mode: true,
            col_widths,
            col_order: display_order,
            row_prefix: prefix,
        },
    }
}

/// Build the fixed prefix string for a row at the given depth.
/// For nested tables, includes left padding ("  ").
fn build_row_prefix(depth: usize, nested_table: bool) -> String {
    let mut prefix = String::new();
    for _ in 0..depth {
        prefix.push_str("│ ");
    }
    if nested_table {
        prefix.push_str("  ");
    }
    prefix
}

// ============================================================
// Cross-table separator alignment
//
// Nested tables at different depths have independently computed column
// widths, so their "│" separators often land 1-2 positions apart.
// Like rivers in typesetting, this is a minor visual defect that becomes
// impossible to unsee once noticed.
//
// Fix: after all tables are resolved, nudge column widths so separators
// line up across visually adjacent tables. Each column can grow by at
// most MAX_ALIGN_ADJUST positions. Optimal alignment across multiple
// tables is found via Viterbi DP — cleaner than special-casing.
// ============================================================

/// Max positions a column can grow to achieve separator alignment.
const MAX_ALIGN_ADJUST: usize = 3;

type NodePath = Vec<usize>;

struct TableEntry {
    path: NodePath,
    info: TableAlignInfo,
}

/// Post-processing pass on the resolved spec tree.
/// Collects all table specs in visual (DFS) order, finds where their
/// separators almost line up, and nudges column widths to close the gap.
fn align_adjacent_tables(root: &mut RenderSpecNode) {
    let mut tables = Vec::new();
    collect_tables(root, &mut vec![], &mut tables);

    if tables.len() < 2 {
        return;
    }

    let group = &tables;
    let mut all_adjustments: Vec<(usize, usize, usize)> = Vec::new();

    // Align each separator index independently across all tables that have it
    let max_seps = group.iter().map(|t| t.info.sep_positions.len()).max().unwrap_or(0);
    for sep_idx in 0..max_seps {
        let participants: Vec<(usize, usize)> = group
            .iter()
            .enumerate()
            .filter_map(|(ti, entry)| {
                entry.info.sep_positions.get(sep_idx).map(|&pos| (ti, pos))
            })
            .collect();

        if participants.len() < 2 {
            continue;
        }

        for (ti, delta) in viterbi_align(&participants) {
            all_adjustments.push((ti, sep_idx, delta));
        }
    }

    for &(ti, sep_idx, delta) in &all_adjustments {
        let path = &group[ti].path;
        let node = navigate_mut(root, path);
        if let RenderSpecKind::Struct { col_widths, .. } = &mut node.kind
            && sep_idx < col_widths.len()
        {
            col_widths[sep_idx] += delta;
        }
    }
}

/// Walk the spec tree in render order, collecting every table-mode struct.
/// Path records struct child indices only; List/Map are transparent.
fn collect_tables(node: &RenderSpecNode, path: &mut Vec<usize>, out: &mut Vec<TableEntry>) {
    match &node.kind {
        RenderSpecKind::Struct { table_mode: true, .. } => {
            out.push(TableEntry {
                path: path.clone(),
                info: TableAlignInfo::from_spec(node),
            });
            // Don't recurse into table children — nested tables inside
            // a table cell are a different visual context
        }
        RenderSpecKind::Struct { children, table_mode: false, .. } => {
            for (i, (_, child)) in children.iter().enumerate() {
                path.push(i);
                collect_tables(child, path, out);
                path.pop();
            }
        }
        RenderSpecKind::List { element } => {
            // List elements render their table spec for nested tables
            collect_tables(element, path, out);
        }
        RenderSpecKind::Map { key, value } => {
            collect_tables(key, path, out);
            collect_tables(value, path, out);
        }
        _ => {}
    }
}

/// Follow a path to a table-mode struct, descending through List/Map transparently.
fn navigate_mut<'a>(root: &'a mut RenderSpecNode, path: &[usize]) -> &'a mut RenderSpecNode {
    let mut node = root;
    let mut i = 0;
    while i < path.len() || matches!(&node.kind, RenderSpecKind::List { .. } | RenderSpecKind::Map { .. }) {
        enum Step { Child(usize), Transparent }
        let step = match &node.kind {
            RenderSpecKind::Struct { .. } if i < path.len() => Step::Child(path[i]),
            RenderSpecKind::List { .. } | RenderSpecKind::Map { .. } => Step::Transparent,
            _ => break,
        };
        match step {
            Step::Child(idx) => {
                let RenderSpecKind::Struct { children, .. } = &mut node.kind else { unreachable!() };
                node = &mut children[idx].1;
                i += 1;
            }
            Step::Transparent => {
                node = match &mut node.kind {
                    RenderSpecKind::List { element } => element.as_mut(),
                    RenderSpecKind::Map { value, .. } => value.as_mut(),
                    _ => unreachable!(),
                };
            }
        }
    }
    node
}

struct TableAlignInfo {
    /// Absolute terminal column of each "│" separator (prefix + cumulative widths).
    /// Two tables align at separator k when sep_positions[k] matches.
    sep_positions: Vec<usize>,
}

impl TableAlignInfo {
    fn from_spec(node: &RenderSpecNode) -> Self {
        let RenderSpecKind::Struct {
            col_widths,
            row_prefix,
            ..
        } = &node.kind
        else {
            return TableAlignInfo {
                sep_positions: vec![],
            };
        };

        let prefix_w = crate::unicode::display_width(row_prefix);
        let mut seps = Vec::with_capacity(col_widths.len().saturating_sub(1));
        let mut cumulative = prefix_w;
        for (i, &cw) in col_widths.iter().enumerate() {
            cumulative += cw;
            if i + 1 < col_widths.len() {
                seps.push(cumulative);
                cumulative += 3; // " │ "
            }
        }

        TableAlignInfo { sep_positions: seps }
    }
}

/// Find the column-width adjustments that maximize separator alignment.
///
/// Each table can widen its column by 0..=MAX_ALIGN_ADJUST positions.
/// Viterbi DP over the sequence of tables: state = chosen position,
/// transition cost = 0 if two adjacent tables agree, 1 if they don't.
/// Naturally handles multiple clusters (e.g. tables at 10,12,20,21
/// → two aligned pairs) without special-casing.
fn viterbi_align(participants: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let states: Vec<Vec<usize>> = participants
        .iter()
        .map(|&(_, pos)| (0..=MAX_ALIGN_ADJUST).map(|d| pos + d).collect())
        .collect();

    let n = participants.len();
    if n == 0 {
        return vec![];
    }

    // cost[i][si] = minimum total misalignment cost to reach state si at table i
    // parent[i][si] = which state at table i-1 led to cost[i][si]
    let mut cost: Vec<Vec<usize>> = Vec::with_capacity(n);
    let mut parent: Vec<Vec<usize>> = Vec::with_capacity(n);

    // Initialize first table
    cost.push(vec![0; states[0].len()]);
    parent.push(vec![0; states[0].len()]);

    // Forward pass
    for i in 1..n {
        let mut row_cost = vec![usize::MAX; states[i].len()];
        let mut row_parent = vec![0usize; states[i].len()];

        for (si, &pos_i) in states[i].iter().enumerate() {
            for (sj, &pos_j) in states[i - 1].iter().enumerate() {
                let transition = if pos_i == pos_j { 0 } else { 1 };
                let total = cost[i - 1][sj] + transition;
                if total < row_cost[si] {
                    row_cost[si] = total;
                    row_parent[si] = sj;
                }
            }
        }

        cost.push(row_cost);
        parent.push(row_parent);
    }

    // Backtrack: find best final state
    let last = n - 1;
    let mut best_state = 0;
    for si in 1..states[last].len() {
        if cost[last][si] < cost[last][best_state] {
            best_state = si;
        }
    }

    let mut chosen = vec![0usize; n];
    chosen[last] = best_state;
    for i in (1..n).rev() {
        chosen[i - 1] = parent[i][chosen[i]];
    }

    // Convert to deltas
    let mut result = Vec::new();
    for (i, &(_, orig_pos)) in participants.iter().enumerate() {
        let target_pos = states[i][chosen[i]];
        let delta = target_pos - orig_pos;
        if delta > 0 {
            result.push((i, delta));
        }
    }
    result
}

/// Distribute `available` width across columns.
/// Columns that fit naturally get their natural width.
/// Remaining space is split evenly among columns that need more.
pub fn distribute_column_widths(natural: &[usize], available: usize) -> Vec<usize> {
    let num = natural.len();
    if num == 0 {
        return vec![];
    }

    let total_natural: usize = natural.iter().sum();
    if total_natural <= available {
        return natural.to_vec();
    }

    let mut allocated = vec![0usize; num];
    let mut settled = vec![false; num];
    let mut remaining = available;

    loop {
        let unsettled: usize = settled.iter().filter(|&&s| !s).count();
        if unsettled == 0 {
            break;
        }
        let fair_share = remaining / unsettled;

        let mut changed = false;
        for i in 0..num {
            if settled[i] {
                continue;
            }
            if natural[i] <= fair_share {
                allocated[i] = natural[i];
                settled[i] = true;
                remaining -= natural[i];
                changed = true;
            }
        }

        if !changed {
            let unsettled_indices: Vec<usize> = (0..num).filter(|&i| !settled[i]).collect();
            let share = remaining / unsettled_indices.len().max(1);
            let mut leftover = remaining % unsettled_indices.len().max(1);
            for &i in &unsettled_indices {
                let w = share + if leftover > 0 { leftover -= 1; 1 } else { 0 };
                allocated[i] = w.max(MIN_COL);
                settled[i] = true;
            }
            break;
        }
    }

    allocated
}


#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // -- Helpers for building test data --

    fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    // A mock DataSource for testing
    struct MockSource {
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    }

    impl MockSource {
        fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
            Self { schema, batches }
        }
    }

    impl DataSource for MockSource {
        fn schema(&self) -> &Arc<Schema> {
            &self.schema
        }
        fn total_rows(&self) -> usize {
            self.batches.iter().map(|b| b.num_rows()).sum()
        }
        fn file_count(&self) -> usize {
            1
        }
        fn ensure_loaded(&mut self, _global_row: usize) -> anyhow::Result<()> {
            Ok(())
        }
        fn get_row(&mut self, global_row: usize) -> (&RecordBatch, usize) {
            let mut offset = 0;
            for batch in &self.batches {
                if global_row < offset + batch.num_rows() {
                    return (batch, global_row - offset);
                }
                offset += batch.num_rows();
            }
            panic!("row {} out of bounds", global_row);
        }
    }

    // ============================================================
    // Type definition tests
    // ============================================================

    #[test]
    fn test_layout_all_scalar_prefer_table() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])) as Arc<dyn Array>,
                Arc::new(BooleanArray::from(vec![true, false, true])) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        match &layout.root.kind {
            LayoutKind::Struct {
                prefer_table,
                children,
            } => {
                assert!(*prefer_table, "all-scalar schema should prefer table");
                assert_eq!(children.len(), 3);
                assert_eq!(children[0].0, "id");
                assert_eq!(children[1].0, "name");
                assert_eq!(children[2].0, "active");
            }
            _ => panic!("root should be Struct"),
        }
    }

    #[test]
    fn test_layout_nested_struct_no_table() {
        let inner = DataType::Struct(
            vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Int32, false),
            ]
            .into(),
        );
        let schema = make_schema(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("point", inner.clone(), false),
        ]);

        let point_array = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![10, 20])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("y", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![30, 40])) as Arc<dyn Array>,
            ),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>,
                Arc::new(point_array) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        match &layout.root.kind {
            LayoutKind::Struct {
                prefer_table,
                children,
            } => {
                assert!(
                    !*prefer_table,
                    "schema with nested struct should not prefer table"
                );
                // The nested struct itself should prefer table (all scalar children)
                match &children[1].1.kind {
                    LayoutKind::Struct {
                        prefer_table,
                        children: inner_children,
                    } => {
                        assert!(
                            *prefer_table,
                            "inner struct with scalar children should prefer table"
                        );
                        assert_eq!(inner_children.len(), 2);
                    }
                    _ => panic!("point field should be Struct"),
                }
            }
            _ => panic!("root should be Struct"),
        }
    }

    #[test]
    fn test_layout_float_detection() {
        let schema = make_schema(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("count", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.50, 2.75, 3.00, 4.25, 5.50])) as Arc<dyn Array>,
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        match &layout.root.kind {
            LayoutKind::Struct { children, .. } => {
                match &children[0].1.kind {
                    LayoutKind::Float {
                        precision,
                        exponential,
                    } => {
                        assert!(!exponential);
                        assert!(
                            *precision <= 2,
                            "prices like 1.50, 2.75 should have precision <= 2, got {}",
                            precision
                        );
                    }
                    _ => panic!("price field should be Float"),
                }
                match &children[1].1.kind {
                    LayoutKind::Scalar => {}
                    _ => panic!("count field should be Scalar"),
                }
            }
            _ => panic!("root should be Struct"),
        }
    }

    #[test]
    fn test_layout_string_detection() {
        let schema = make_schema(vec![Field::new("msg", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["hello", "world", "test"])) as Arc<dyn Array>],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        match &layout.root.kind {
            LayoutKind::Struct { children, .. } => match &children[0].1.kind {
                LayoutKind::Str { max_display } => {
                    assert!(*max_display > 0);
                    assert!(*max_display <= DEFAULT_STR_MAX_DISPLAY);
                }
                _ => panic!("msg field should be Str"),
            },
            _ => panic!("root should be Struct"),
        }
    }

    #[test]
    fn test_layout_list_of_structs() {
        let inner_struct = DataType::Struct(
            vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
            ]
            .into(),
        );
        let list_type = DataType::List(Arc::new(Field::new("item", inner_struct, true)));
        let schema = make_schema(vec![Field::new("people", list_type, true)]);

        // Build list of structs
        let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);
        let age_array = Int32Array::from(vec![30, 25, 35]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(name_array) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                Arc::new(age_array) as Arc<dyn Array>,
            ),
        ]);

        let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", struct_array.data_type().clone(), true)),
            offsets,
            Arc::new(struct_array),
            None,
        );

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(list_array) as Arc<dyn Array>])
                .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        match &layout.root.kind {
            LayoutKind::Struct { children, .. } => match &children[0].1.kind {
                LayoutKind::List { element } => match &element.kind {
                    LayoutKind::Struct {
                        prefer_table,
                        children: inner_children,
                    } => {
                        assert!(
                            *prefer_table,
                            "inner struct in list should prefer table (all scalar)"
                        );
                        assert_eq!(inner_children.len(), 2);
                        assert_eq!(inner_children[0].0, "name");
                        assert_eq!(inner_children[1].0, "age");
                    }
                    _ => panic!("list element should be Struct"),
                },
                _ => panic!("people field should be List"),
            },
            _ => panic!("root should be Struct"),
        }
    }

    // ============================================================
    // Float precision tests
    // ============================================================

    #[test]
    fn test_float_precision_uniform_integers() {
        // [1.0, 2.0, 3.0] → all have 0 decimal digits → precision 0
        let mut values: Vec<f64> = (1..=20).map(|i| i as f64).collect();
        let (precision, exponential) = compute_float_precision(&mut values);
        assert_eq!(precision, 0);
        assert!(!exponential);
    }

    #[test]
    fn test_float_precision_uniform_currency() {
        // All values have exactly 2 decimal places
        let mut values = vec![1.50, 2.75, 3.00, 4.25, 5.99, 10.50, 20.00, 15.75, 8.25, 99.99];
        let (precision, exponential) = compute_float_precision(&mut values);
        assert!(precision <= 2, "currency should have precision <= 2, got {}", precision);
        assert!(!exponential);
    }

    #[test]
    fn test_float_precision_coordinates() {
        // GPS coordinates: 4-5 decimal places
        let mut values = vec![
            35.6762, 139.6503, 35.6812, 139.7671, 35.7100, 139.8107, 35.6585, 139.7454,
            35.6896, 139.6917, 35.7023, 139.7745,
        ];
        let (precision, exponential) = compute_float_precision(&mut values);
        assert!(
            precision >= 3 && precision <= 5,
            "coordinates should have precision 3-5, got {}",
            precision
        );
        assert!(!exponential);
    }

    #[test]
    fn test_float_precision_empty() {
        let (precision, exponential) = compute_float_precision(&mut []);
        assert_eq!(precision, 2);
        assert!(!exponential);
    }

    #[test]
    fn test_float_precision_all_identical() {
        let mut values = vec![1.5; 20];
        let (precision, exponential) = compute_float_precision(&mut values);
        assert_eq!(precision, 1);
        assert!(!exponential);
    }

    #[test]
    fn test_float_precision_scientific_dense() {
        // Dense values requiring high precision
        let mut values: Vec<f64> = (0..100).map(|i| 1.0 + i as f64 * 0.001).collect();
        let (precision, _exponential) = compute_float_precision(&mut values);
        assert!(
            precision >= 3,
            "dense values with 0.001 step should need precision >= 3, got {}",
            precision
        );
    }

    #[test]
    fn test_float_precision_proportions() {
        // Proportions stored with 6 digits but only needing 2 to distinguish
        let mut values = vec![
            0.916667, 0.083333, 0.750000, 0.250000, 0.666667, 0.333333,
            0.833333, 0.166667, 0.583333, 0.416667,
        ];
        let (precision, exponential) = compute_float_precision(&mut values);
        assert!(
            precision <= 3,
            "pre-rounded proportions should need precision <= 3, got {}",
            precision
        );
        assert!(!exponential);
    }

    #[test]
    fn test_float_precision_real_f64_proportions() {
        // Real f64 division results — not pre-rounded
        let mut values: Vec<f64> = (1..=12).map(|i| i as f64 / 12.0).collect();
        let (precision, exponential) = compute_float_precision(&mut values);
        assert!(
            precision <= 3,
            "f64 proportions (N/12) should need precision <= 3, got {}",
            precision
        );
        assert!(!exponential);
    }

    // ============================================================
    // decimal_digit_count tests
    // ============================================================

    #[test]
    fn test_decimal_digit_count() {
        assert_eq!(decimal_digit_count(1.0), 0);
        assert_eq!(decimal_digit_count(1.5), 1);
        assert_eq!(decimal_digit_count(1.25), 2);
        assert_eq!(decimal_digit_count(1.001), 3);
        assert_eq!(decimal_digit_count(0.0), 0);
    }

    // ============================================================
    // distribute_column_widths tests
    // ============================================================

    #[test]
    fn test_distribute_fits() {
        let natural = vec![10, 20, 15];
        let result = distribute_column_widths(&natural, 100);
        assert_eq!(result, vec![10, 20, 15]);
    }

    #[test]
    fn test_distribute_squeeze() {
        let natural = vec![50, 50, 50];
        let result = distribute_column_widths(&natural, 90);
        let total: usize = result.iter().sum();
        assert_eq!(total, 90);
        // Each gets 30
        assert_eq!(result, vec![30, 30, 30]);
    }

    #[test]
    fn test_distribute_mixed() {
        // One small column, two big columns
        let natural = vec![5, 50, 50];
        let result = distribute_column_widths(&natural, 50);
        // Small column gets its natural 5, remaining 45 split between two big ones
        assert_eq!(result[0], 5);
        assert_eq!(result[1] + result[2], 45);
    }

    #[test]
    fn test_distribute_empty() {
        let result = distribute_column_widths(&[], 100);
        assert!(result.is_empty());
    }

    // ============================================================
    // RenderSpec resolution tests
    // ============================================================

    #[test]
    fn test_resolve_simple_table() {
        let schema = make_schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 22, 333])) as Arc<dyn Array>,
                Arc::new(Int32Array::from(vec![4444, 55, 6])) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);

        match &spec.root.kind {
            RenderSpecKind::Struct {
                table_mode,
                col_widths,
                children,
                ..
            } => {
                assert!(*table_mode);
                assert_eq!(col_widths.len(), 2);
                assert_eq!(children.len(), 2);
                // Widths should sum to available (80 - 3 separator = 77) or less
                let total: usize = col_widths.iter().sum();
                assert!(total <= 77, "total {} should be <= 77", total);
            }
            _ => panic!("root should be table Struct"),
        }
    }

    #[test]
    fn test_resolve_vertical_mode() {
        let inner = DataType::Struct(
            vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Int32, false),
            ]
            .into(),
        );
        let schema = make_schema(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", inner, false),
        ]);

        let nested_array = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("y", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![2])) as Arc<dyn Array>,
            ),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>,
                Arc::new(nested_array) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);

        match &spec.root.kind {
            RenderSpecKind::Struct {
                table_mode,
                col_widths,
                ..
            } => {
                assert!(!*table_mode, "mixed schema should be vertical");
                assert!(col_widths.is_empty(), "vertical mode has no col_widths");
            }
            _ => panic!("root should be Struct"),
        }
    }

    #[test]
    fn test_resolve_preview_budget() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let schema = make_schema(vec![Field::new("nums", list_type, true)]);

        let list_array = {
            let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
            let offsets = OffsetBuffer::new(vec![0i32, 3, 5].into());
            ListArray::new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                offsets,
                Arc::new(values),
                None,
            )
        };

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(list_array) as Arc<dyn Array>],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);

        match &spec.root.kind {
            RenderSpecKind::Struct { children, .. } => match &children[0].1.kind {
                RenderSpecKind::List { element } => {
                    match &element.kind {
                        RenderSpecKind::Scalar => {}
                        _ => panic!("list element should be Scalar"),
                    }
                }
                _ => panic!("nums should be List"),
            },
            _ => panic!("root should be Struct"),
        }
    }

    #[test]
    fn test_header_width_rule() {
        // Column with a long header name but short data values
        let schema = make_schema(vec![
            Field::new("very_long_column_name_here", DataType::Int32, false),
            Field::new("x", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>,
                Arc::new(Int32Array::from(vec![100, 200, 300, 400, 500])) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        match &layout.root.kind {
            LayoutKind::Struct { children, .. } => {
                let long_col = &children[0].1;
                let _short_col = &children[1].1;
                assert_eq!(long_col.header_width, 26);
                // Data width (small ints) is much less than header
                // The header width rule should prevent the header from
                // dominating: max bump is data + 10%
                assert!(
                    long_col.natural_width < long_col.header_width / 2,
                    "natural_width {} should be much less than header_width {} for narrow data",
                    long_col.natural_width,
                    long_col.header_width,
                );
            }
            _ => panic!("root should be Struct"),
        }

        // Resolve and check that the long header column doesn't get 25 chars
        let spec = RenderSpec::resolve(&layout, 60);
        match &spec.root.kind {
            RenderSpecKind::Struct { col_widths, .. } => {
                // The long-header column should NOT get 26 chars just because of header
                // (data is ~1-2 chars wide, so natural is small, header rule caps at data*1.1)
                assert!(
                    col_widths[0] < 26,
                    "long header col got {} chars, should be less than 26",
                    col_widths[0]
                );
            }
            _ => panic!("root should be Struct"),
        }
    }

    // ============================================================
    // Viterbi alignment tests
    // ============================================================

    #[test]
    fn test_viterbi_all_close() {
        // Three tables at 20, 22, 21 — all within +3 of max
        let participants = vec![(0, 20), (1, 22), (2, 21)];
        let adj = viterbi_align(&participants);
        let target = 22;
        for &(ti, delta) in &adj {
            assert!(delta <= MAX_ALIGN_ADJUST);
            assert_eq!(participants[ti].1 + delta, target);
        }
    }

    #[test]
    fn test_viterbi_snap_up() {
        // Two tables: 17 and 18 — should align to 18
        let participants = vec![(0, 18), (1, 17)];
        let adj = viterbi_align(&participants);
        let mut positions: Vec<usize> = participants.iter().map(|&(_, p)| p).collect();
        for &(ti, delta) in &adj {
            positions[ti] += delta;
        }
        assert_eq!(positions[0], positions[1], "should align");
        assert_eq!(positions[0], 18);
    }

    #[test]
    fn test_viterbi_too_far_apart() {
        // Two tables: 10 and 20 — can't align (delta > 3)
        let participants = vec![(0, 10), (1, 20)];
        let adj = viterbi_align(&participants);
        for &(_, delta) in &adj {
            assert!(delta <= MAX_ALIGN_ADJUST);
        }
    }

    #[test]
    fn test_viterbi_two_clusters() {
        // A=10, B=12, C=20, D=21 — two natural clusters
        let participants = vec![(0, 10), (1, 12), (2, 20), (3, 21)];
        let adj = viterbi_align(&participants);
        let mut positions: Vec<usize> = participants.iter().map(|&(_, p)| p).collect();
        for &(ti, delta) in &adj {
            positions[ti] += delta;
        }
        assert_eq!(positions[0], positions[1], "A and B should align");
        assert_eq!(positions[2], positions[3], "C and D should align");
    }

    #[test]
    fn test_natural_width_sampling() {
        // Verify that natural_width uses p80 + 10%
        let schema = make_schema(vec![Field::new("val", DataType::Int32, false)]);
        // Create values with varying widths: mostly 1-2 digits, a few 5-digit
        let values: Vec<i32> = (0..100)
            .map(|i| if i < 80 { i % 10 } else { 99999 })
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(values)) as Arc<dyn Array>],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        match &layout.root.kind {
            LayoutKind::Struct { children, .. } => {
                let w = children[0].1.natural_width;
                // p80 of mostly single-digit numbers should be small.
                // The 20% outliers (99999 = 5 chars) shouldn't make it huge.
                assert!(
                    w <= 5,
                    "natural_width {} should be <= 5 (p80 of mostly 1-digit numbers)",
                    w
                );
            }
            _ => panic!("root should be Struct"),
        }
    }
}
