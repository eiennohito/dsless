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
            LayoutBuilder::Struct { children, widths } => {
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
        let all_scalar = children.iter().all(|(_, child)| {
            matches!(
                child,
                LayoutBuilder::Scalar { .. }
                    | LayoutBuilder::Float { .. }
                    | LayoutBuilder::Str { .. }
            )
        });

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
            LayoutBuilder::Float {
                mut widths,
                mut values,
            } => {
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
            LayoutBuilder::Str {
                mut widths,
                mut lengths,
            } => {
                let max_display = resolve_str_max_display(&mut lengths);
                LayoutNode {
                    natural_width: widths.p80_plus10(),
                    max_width: widths.max,
                    header_width,
                    kind: LayoutKind::Str { max_display },
                }
            }
            LayoutBuilder::Struct {
                children,
                mut widths,
            } => {
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
            LayoutBuilder::List {
                element,
                mut widths,
            } => {
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
            LayoutBuilder::Map {
                key,
                value,
                mut widths,
            } => {
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
            if trimmed.is_empty() {
                0
            } else {
                trimmed.len() as u8
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::test_fixtures::{MockSource, make_schema};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

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
        let mut values = vec![
            1.50, 2.75, 3.00, 4.25, 5.99, 10.50, 20.00, 15.75, 8.25, 99.99,
        ];
        let (precision, exponential) = compute_float_precision(&mut values);
        assert!(
            precision <= 2,
            "currency should have precision <= 2, got {}",
            precision
        );
        assert!(!exponential);
    }

    #[test]
    fn test_float_precision_coordinates() {
        // GPS coordinates: 4-5 decimal places
        let mut values = vec![
            35.6762, 139.6503, 35.6812, 139.7671, 35.7100, 139.8107, 35.6585, 139.7454, 35.6896,
            139.6917, 35.7023, 139.7745,
        ];
        let (precision, exponential) = compute_float_precision(&mut values);
        assert!(
            (3..=5).contains(&precision),
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
            0.916667, 0.083333, 0.750000, 0.250000, 0.666667, 0.333333, 0.833333, 0.166667,
            0.583333, 0.416667,
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
