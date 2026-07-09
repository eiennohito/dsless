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

/// Allocate terminal width across struct fields using width histograms.
/// Fixed-width columns (p80 == max) get exactly their width — no more, no less.
/// Remaining space is split among variable columns proportional to sqrt(p80).
fn resolve_struct(
    children: &[(String, LayoutNode)],
    prefer_table: bool,
    ctx: &ResolveCtx,
) -> RenderSpecNode {
    let identity_order: Vec<usize> = (0..children.len()).collect();

    if !prefer_table || children.is_empty() {
        return RenderSpecNode {
            kind: RenderSpecKind::Struct {
                children: build_struct_children(children, &identity_order, ctx),
                table_mode: false,
                col_widths: vec![],
                row_prefix: build_row_prefix(ctx.depth + 1, false),
            },
        };
    }

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

    let nodes: Vec<&LayoutNode> = children.iter().map(|(_, node)| node).collect();

    let spread_rightmost = find_spread_rightmost(&nodes);
    let mut display_order = identity_order;
    if let Some(ri) = spread_rightmost {
        if ri != num_fields - 1 {
            display_order.retain(|&i| i != ri);
            display_order.push(ri);
        }
    }

    let ordered_nodes: Vec<&LayoutNode> = display_order.iter().map(|&i| nodes[i]).collect();
    let col_widths = allocate_from_histograms(&ordered_nodes, distributable);

    RenderSpecNode {
        kind: RenderSpecKind::Struct {
            children: build_struct_children(children, &display_order, ctx),
            table_mode: true,
            col_widths,
            row_prefix: prefix,
        },
    }
}

fn build_struct_children(
    children: &[(String, LayoutNode)],
    order: &[usize],
    ctx: &ResolveCtx,
) -> Vec<StructChild> {
    let child_ctx = ctx.deeper();
    order
        .iter()
        .map(|&i| {
            let (name, node) = &children[i];
            StructChild {
                name: name.clone(),
                schema_idx: i,
                spec: resolve_node(node, &child_ctx),
            }
        })
        .collect()
}

/// Find the column with the highest spread (p95/p50 ratio).
/// Returns None if no column has meaningful spread.
fn find_spread_rightmost(nodes: &[&LayoutNode]) -> Option<usize> {
    const SPREAD_THRESHOLD: f64 = 1.5;
    let mut best_idx = None;
    let mut best_ratio = SPREAD_THRESHOLD;
    for (i, node) in nodes.iter().enumerate() {
        let p50 = node.widths.percentile(50) as f64;
        if p50 < 1.0 {
            continue;
        }
        let ratio = node.widths.percentile(95) as f64 / p50;
        if ratio > best_ratio {
            best_ratio = ratio;
            best_idx = Some(i);
        }
    }
    best_idx
}

/// Allocate column widths from width histograms.
///
/// Fixed-width columns (p80 ≈ max) are allocated first at their exact width.
/// Remaining space is distributed among variable columns proportional to
/// sqrt(p80) — sqrt dampens wide columns so they don't starve narrow ones.
/// Finally, columns are capped at max with surplus redistributed.
fn allocate_from_histograms(nodes: &[&LayoutNode], available: usize) -> Vec<usize> {
    let n = nodes.len();
    if n == 0 {
        return vec![];
    }

    // p80 per column, bumped by header if header is only slightly wider
    let p80s: Vec<usize> = nodes
        .iter()
        .map(|node| {
            let data_w = node.widths.percentile(80);
            data_w
                .max(node.header_width.min(data_w + data_w / 10))
                .max(1)
        })
        .collect();

    let maxes: Vec<usize> = nodes.iter().map(|node| node.widths.max()).collect();

    // Fixed-width columns: p80 >= max means every cell is the same width
    let is_fixed: Vec<bool> = p80s.iter().zip(&maxes).map(|(&p, &m)| p >= m).collect();

    let total_p80: usize = p80s.iter().sum();
    if total_p80 <= available {
        return distribute_column_widths(&p80s, available);
    }

    // Try allocating fixed columns at exact width, variable columns with remainder
    let fixed_total: usize = p80s
        .iter()
        .zip(&is_fixed)
        .filter(|&(_, f)| *f)
        .map(|(&p, _)| p)
        .sum();
    let has_variable = is_fixed.iter().any(|f| !f);

    if has_variable && fixed_total < available {
        let variable_budget = available - fixed_total;
        let variable_p80s: Vec<usize> = p80s
            .iter()
            .zip(&is_fixed)
            .filter(|&(_, f)| !*f)
            .map(|(&p, _)| p)
            .collect();
        let variable_maxes: Vec<usize> = maxes
            .iter()
            .zip(&is_fixed)
            .filter(|&(_, f)| !*f)
            .map(|(&m, _)| m)
            .collect();
        let variable_allocated =
            allocate_proportional(&variable_p80s, &variable_maxes, variable_budget);

        let mut result = Vec::with_capacity(n);
        let mut vi = 0;
        for i in 0..n {
            if is_fixed[i] {
                result.push(p80s[i]);
            } else {
                result.push(variable_allocated[vi]);
                vi += 1;
            }
        }
        return result;
    }

    // All fixed or fixed columns alone exceed budget — squeeze everything
    allocate_proportional(&p80s, &maxes, available)
}

/// Proportional allocation with sqrt dampening, MIN_COL floor, and cap-at-max.
fn allocate_proportional(p80s: &[usize], maxes: &[usize], available: usize) -> Vec<usize> {
    let n = p80s.len();
    let requests: Vec<f64> = p80s.iter().map(|&w| (w as f64).sqrt().max(1.0)).collect();
    let total_request: f64 = requests.iter().sum();

    let mut allocated: Vec<usize> = requests
        .iter()
        .map(|&r| ((available as f64 * r / total_request) as usize).max(MIN_COL))
        .collect();

    snap_to_total(&mut allocated, available);

    // Cap at max and redistribute surplus until stable
    loop {
        let mut surplus = 0usize;
        let mut uncapped_request = 0.0f64;
        for i in 0..n {
            if allocated[i] > maxes[i] {
                surplus += allocated[i] - maxes[i];
                allocated[i] = maxes[i];
            } else {
                uncapped_request += requests[i];
            }
        }
        if surplus == 0 || uncapped_request == 0.0 {
            break;
        }
        let mut distributed = 0usize;
        for i in 0..n {
            if allocated[i] < maxes[i] {
                let bonus = (surplus as f64 * requests[i] / uncapped_request) as usize;
                let capped = (allocated[i] + bonus).min(maxes[i]);
                distributed += capped - allocated[i];
                allocated[i] = capped;
            }
        }
        if distributed == 0 {
            break;
        }
    }

    allocated
}

/// Adjust allocated widths to sum to exactly `target` by shrinking the largest
/// or growing the smallest.
fn snap_to_total(allocated: &mut [usize], target: usize) {
    let mut total: usize = allocated.iter().sum();
    while total > target {
        if let Some(i) = allocated
            .iter()
            .enumerate()
            .filter(|&(_, w)| *w > MIN_COL)
            .max_by_key(|&(_, w)| *w)
            .map(|(i, _)| i)
        {
            allocated[i] -= 1;
            total -= 1;
        } else {
            break;
        }
    }
    while total < target {
        if let Some(i) = allocated
            .iter()
            .enumerate()
            .min_by_key(|&(_, w)| *w)
            .map(|(i, _)| i)
        {
            allocated[i] += 1;
            total += 1;
        } else {
            break;
        }
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
    use crate::layout::compute::WidthProfile;
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
                let p80 = long_col.widths.percentile(80);
                assert!(
                    p80 < long_col.header_width / 2,
                    "p80 {} should be much less than header_width {} for narrow data",
                    p80,
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

    #[test]
    fn test_wide_columns_squeezed_to_fit() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("wide_a", DataType::Utf8, false),
            Field::new("wide_b", DataType::Utf8, false),
            Field::new("desc", DataType::Utf8, false),
        ]);

        let long_a: Vec<&str> =
            (0..20).map(|_| "aaaa_bbbb_cccc_dddd_eeee_ffff_gggg_hhhh").collect();
        let long_b: Vec<&str> =
            (0..20).map(|_| "1111_2222_3333_4444_5555_6666_7777_8888").collect();
        let very_long: Vec<String> =
            (0..20).map(|i| format!("description_{}_", i).repeat(20)).collect();
        let very_long_refs: Vec<&str> = very_long.iter().map(|s| s.as_str()).collect();
        let ids: Vec<&str> = (0..20).map(|_| "short").collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(long_a)) as Arc<dyn Array>,
                Arc::new(StringArray::from(long_b)) as Arc<dyn Array>,
                Arc::new(StringArray::from(very_long_refs)) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);

        match &spec.root.kind {
            RenderSpecKind::Struct {
                table_mode: true,
                col_widths,
                ..
            } => {
                let separators = (col_widths.len() - 1) * 3;
                let total: usize = col_widths.iter().sum::<usize>() + separators;
                assert!(
                    total <= 80,
                    "table width {} exceeds terminal width 80; col_widths={:?}",
                    total, col_widths,
                );
                for &w in col_widths {
                    assert!(w >= 1, "column width must be positive, got {}", w);
                }
            }
            _ => panic!("all-string schema should be table mode"),
        }
    }

    // ============================================================
    // allocate_from_histograms tests
    // ============================================================

    fn node(sorted: &[usize]) -> LayoutNode {
        LayoutNode {
            widths: WidthProfile::from_sorted(sorted.to_vec()),
            header_width: 0,
            kind: LayoutKind::Scalar,
        }
    }

    fn node_h(sorted: &[usize], header_width: usize) -> LayoutNode {
        LayoutNode {
            widths: WidthProfile::from_sorted(sorted.to_vec()),
            header_width,
            kind: LayoutKind::Scalar,
        }
    }

    #[test]
    fn test_alloc_all_fit() {
        let nodes = [node_h(&[5, 5, 5, 5, 5], 2), node_h(&[10, 10, 10, 10, 10], 3)];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        let result = allocate_from_histograms(&refs, 40);
        assert_eq!(result, vec![5, 10]);
    }

    #[test]
    fn test_alloc_squeeze_proportional() {
        let nodes = [node(&[10; 100]), node(&[400; 100])];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        let result = allocate_from_histograms(&refs, 30);
        let total: usize = result.iter().sum();
        assert_eq!(total, 30);
        assert!(
            result[1] > result[0],
            "larger column should get more space: {:?}",
            result
        );
    }

    #[test]
    fn test_alloc_sqrt_dampening() {
        let nodes = [node(&[10; 100]), node(&[400; 100])];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        let result = allocate_from_histograms(&refs, 60);
        let ratio = result[1] as f64 / result[0] as f64;
        assert!(
            ratio < 10.0,
            "sqrt dampening should keep ratio well below 40: got {:.1} ({:?})",
            ratio, result,
        );
        assert!(
            ratio > 2.0,
            "larger column should still get meaningfully more: got {:.1} ({:?})",
            ratio, result,
        );
    }

    #[test]
    fn test_alloc_min_col_floor() {
        let mut small_data: Vec<usize> = vec![3; 90];
        small_data.extend(vec![12; 10]);
        small_data.sort();
        let nodes = [node(&small_data), node(&[500; 100])];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        let result = allocate_from_histograms(&refs, 40);
        assert!(
            result[0] >= MIN_COL,
            "small column (max={}) should get at least MIN_COL={}, got {}",
            nodes[0].widths.max(), MIN_COL, result[0],
        );
    }

    #[test]
    fn test_alloc_cap_at_max() {
        let nodes = [node(&[3, 4, 5]), node(&[50, 60, 70, 80, 90, 100])];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        let result = allocate_from_histograms(&refs, 60);
        assert!(
            result[0] <= 5,
            "narrow column should be capped at max=5, got {}",
            result[0],
        );
        assert!(
            result[1] >= 55,
            "wide column should get surplus: got {}",
            result[1],
        );
    }

    #[test]
    fn test_alloc_total_respects_available() {
        let nodes = [node(&[13; 50]), node(&[47; 50]), node(&[89; 50]), node(&[201; 50])];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        let n = refs.len();
        for available in [50, 77, 100, 200] {
            let result = allocate_from_histograms(&refs, available);
            let total: usize = result.iter().sum();
            let min_possible = n * MIN_COL;
            let limit = available.max(min_possible);
            assert!(
                total <= limit,
                "total {} exceeds limit {} (available={}, min_possible={}) for {:?}",
                total, limit, available, min_possible, result,
            );
        }
    }

    #[test]
    fn test_alloc_fixed_width_columns_exact() {
        // Fixed-width columns (p80 == max) get exactly their width
        let fixed = node(&[10; 100]); // p80=10, max=10 → fixed
        let variable = node(&[50, 60, 70, 80, 90, 100, 200, 300]);
        let refs: Vec<&LayoutNode> = vec![&fixed, &variable];
        let result = allocate_from_histograms(&refs, 60);
        assert_eq!(result[0], 10, "fixed column should get exactly 10");
        assert_eq!(result[1], 50, "variable column gets the remainder");
    }

    // ============================================================
    // find_spread_rightmost tests
    // ============================================================

    #[test]
    fn test_spread_uniform_no_move() {
        let nodes = [node(&[10; 100]), node(&[20; 100]), node(&[15; 100])];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        assert_eq!(find_spread_rightmost(&refs), None);
    }

    #[test]
    fn test_spread_picks_most_variable() {
        let mut variable_data: Vec<usize> = vec![10; 80];
        variable_data.extend(vec![100; 20]);
        variable_data.sort();
        let nodes = [node(&[10; 100]), node(&variable_data), node(&[20; 100])];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        assert_eq!(find_spread_rightmost(&refs), Some(1));
    }

    #[test]
    fn test_spread_highest_ratio_wins() {
        let mut moderate_data: Vec<usize> = vec![10; 80];
        moderate_data.extend(vec![20; 20]);
        moderate_data.sort();

        let mut extreme_data: Vec<usize> = vec![10; 80];
        extreme_data.extend(vec![100; 20]);
        extreme_data.sort();

        let nodes = [node(&moderate_data), node(&extreme_data)];
        let refs: Vec<&LayoutNode> = nodes.iter().collect();
        assert_eq!(find_spread_rightmost(&refs), Some(1));
    }

    // ============================================================
    // End-to-end: nested table with complex columns fits terminal
    // ============================================================

    #[test]
    fn test_nested_table_with_maps_fits_terminal() {
        // Schema mimicking: List<Struct<id: str, data: Map<str,f64>, desc: str>>
        // The map and long-string columns should not overflow.
        let map_inner = DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]
            .into(),
        );
        let map_type = DataType::Map(Arc::new(Field::new("entries", map_inner, false)), false);
        let inner_struct = DataType::Struct(
            vec![
                Field::new("item_id", DataType::Utf8, false),
                Field::new("scores", map_type, false),
                Field::new("description", DataType::Utf8, false),
            ]
            .into(),
        );
        let list_type = DataType::List(Arc::new(Field::new("item", inner_struct.clone(), true)));
        let schema = make_schema(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("items", list_type, false),
        ]);

        // Build data: name column + list-of-struct with map and long string
        let names = StringArray::from(vec!["alice", "bob", "charlie"]);

        // Inner struct arrays for 6 elements (2 per row)
        let item_ids = StringArray::from(vec!["a1", "a2", "b1", "b2", "c1", "c2"]);
        let descriptions = StringArray::from(vec![
            "A short description",
            "A much longer description that goes on and on and on with many words",
            "Medium length desc here",
            "Another very long description field containing lots of text for testing",
            "Brief",
            "Yet another long description to ensure we have variance in the data",
        ]);

        // Map array: 2 entries per map element
        let map_keys = StringArray::from(vec![
            "math", "science", "math", "science", "math", "science", "math", "science", "math",
            "science", "math", "science",
        ]);
        let map_values = Float64Array::from(vec![
            0.95, 0.87, 0.72, 0.91, 0.88, 0.76, 0.65, 0.93, 0.91, 0.82, 0.78, 0.89,
        ]);
        let map_entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(map_keys) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("value", DataType::Float64, false)),
                Arc::new(map_values) as Arc<dyn Array>,
            ),
        ]);
        let map_offsets = OffsetBuffer::new(vec![0i32, 2, 4, 6, 8, 10, 12].into());
        let map_field = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        );
        let map_array = MapArray::new(Arc::new(map_field), map_offsets, map_entries, None, false);

        let inner_struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("item_id", DataType::Utf8, false)),
                Arc::new(item_ids) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("scores", map_array.data_type().clone(), false)),
                Arc::new(map_array) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("description", DataType::Utf8, false)),
                Arc::new(descriptions) as Arc<dyn Array>,
            ),
        ]);

        let list_offsets = OffsetBuffer::new(vec![0i32, 2, 4, 6].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", inner_struct.clone(), true)),
            list_offsets,
            Arc::new(inner_struct_array),
            None,
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(names) as Arc<dyn Array>,
                Arc::new(list_array) as Arc<dyn Array>,
            ],
        )
        .unwrap();

        let mut source = MockSource::new(schema, vec![batch]);
        let layout = Layout::compute(&mut source);

        for term_width in [60, 80, 120, 200] {
            let spec = RenderSpec::resolve(&layout, term_width);
            // The root is vertical (has a list column), find the nested table spec
            assert_nested_tables_fit(&spec.root, term_width, 0);
        }
    }

    fn assert_nested_tables_fit(node: &RenderSpecNode, term_width: usize, depth: usize) {
        match &node.kind {
            RenderSpecKind::Struct {
                table_mode: true,
                col_widths,
                children,
                row_prefix,
                ..
            } => {
                let prefix_w = crate::unicode::display_width(row_prefix);
                let seps = if col_widths.len() > 1 {
                    (col_widths.len() - 1) * 3
                } else {
                    0
                };
                let total = col_widths.iter().sum::<usize>() + seps + prefix_w;
                assert!(
                    total <= term_width,
                    "nested table at depth {} overflows: {} > {} (col_widths={:?})",
                    depth, total, term_width, col_widths,
                );
                for child in children {
                    assert_nested_tables_fit(&child.spec, term_width, depth + 1);
                }
            }
            RenderSpecKind::Struct {
                children,
                table_mode: false,
                ..
            } => {
                for child in children {
                    assert_nested_tables_fit(&child.spec, term_width, depth + 1);
                }
            }
            RenderSpecKind::List { element } => {
                assert_nested_tables_fit(element, term_width, depth);
            }
            RenderSpecKind::Map { key, value } => {
                assert_nested_tables_fit(key, term_width, depth);
                assert_nested_tables_fit(value, term_width, depth);
            }
            _ => {}
        }
    }
}
