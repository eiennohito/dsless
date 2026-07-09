use super::align::align_adjacent_tables;
use super::compute::{Layout, LayoutKind, LayoutNode};
use crate::unicode::display_width;

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

    /// Column widths if the root is a table-mode struct, else None.
    pub fn col_widths(&self) -> Option<&[usize]> {
        match &self.root.kind {
            RenderSpecKind::Struct {
                table_mode: true,
                col_widths,
                ..
            } => Some(col_widths),
            _ => None,
        }
    }
}

/// A node in the RenderSpec tree. Each node knows how to render its
/// corresponding Arrow value — precision for floats, truncation for strings,
/// column widths for table structs, preview budgets for collections.
///
/// `Clone` exists so preview's full-field render can clone a subtree and
/// mutate only its `Str` leaves (see `preview::unlimit`), rather than
/// hand-rebuilding every variant.
#[derive(Clone)]
pub struct RenderSpecNode {
    pub kind: RenderSpecKind,
}

#[derive(Clone)]
pub struct StructChild {
    pub name: String,
    pub schema_idx: usize,
    pub spec: RenderSpecNode,
}

#[derive(Clone)]
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
        children: Vec<StructChild>,
        table_mode: bool,
        col_widths: Vec<usize>,
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
        let resolved_children: Vec<StructChild> = children
            .iter()
            .enumerate()
            .map(|(i, (name, node))| StructChild {
                name: name.clone(),
                schema_idx: i,
                spec: resolve_node(node, &child_ctx),
            })
            .collect();
        return RenderSpecNode {
            kind: RenderSpecKind::Struct {
                children: resolved_children,
                table_mode: false,
                col_widths: vec![],
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
    let prefix_width = display_width(&prefix);

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
            data_w
                .max(node.header_width.min(data_w + data_w / 10))
                .max(1)
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
        let ordered_naturals: Vec<usize> =
            display_order.iter().map(|&i| natural_widths[i]).collect();
        distribute_column_widths(&ordered_naturals, distributable)
    };

    // Build children in display order
    let child_ctx = ctx.deeper();
    let resolved_children: Vec<StructChild> = display_order
        .iter()
        .map(|&i| {
            let (name, node) = &children[i];
            StructChild {
                name: name.clone(),
                schema_idx: i,
                spec: resolve_node(node, &child_ctx),
            }
        })
        .collect();

    RenderSpecNode {
        kind: RenderSpecKind::Struct {
            children: resolved_children,
            table_mode: true,
            col_widths,
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
                let w = share
                    + if leftover > 0 {
                        leftover -= 1;
                        1
                    } else {
                        0
                    };
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
    use crate::layout::test_fixtures::{MockSource, make_schema};
    use arrow::array::*;
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

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
    fn test_table_column_helpers() {
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

        assert_eq!(spec.col_widths().map(<[usize]>::len), Some(2));
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

        assert_eq!(spec.col_widths(), None);
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

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(list_array) as Arc<dyn Array>])
                .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);

        match &spec.root.kind {
            RenderSpecKind::Struct { children, .. } => match &children[0].spec.kind {
                RenderSpecKind::List { element } => match &element.kind {
                    RenderSpecKind::Scalar => {}
                    _ => panic!("list element should be Scalar"),
                },
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
}
