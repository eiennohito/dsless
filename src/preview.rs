use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StructArray};
use crate::layout::{RenderSpec, RenderSpecKind, RenderSpecNode};
pub use crate::render::{DataPath, NodeRef, PathStep};
use crate::render::{
    DataNodeKind, Fidelity, LineWriter, RenderMode, RenderedRow, list_offsets,
};

pub struct ExpandableField {
    pub node: NodeRef,
    #[cfg_attr(not(test), allow(dead_code))]
    pub name: String,
}

/// Query the rendered node tree for fields whose fidelity is less than Full.
/// Struct fields are skipped (their children are listed individually);
/// Lists, Maps, and leaf scalars/strings are listed when constrained or summarized.
pub fn expandable_fields(rendered: &RenderedRow, spec: &RenderSpec) -> Vec<ExpandableField> {
    let mut out = Vec::new();
    let RenderSpecKind::Struct {
        children: _,
        table_mode,
        ..
    } = &spec.root.kind
    else {
        return out;
    };

    for (i, node) in rendered.nodes.iter().enumerate() {
        if !matches!(node.kind, DataNodeKind::Field { .. }) {
            continue;
        }
        if node.fidelity == Fidelity::Full {
            continue;
        }
        // In table mode, only list depth-1 fields (top-level columns)
        if *table_mode && node.depth != 1 {
            continue;
        }
        // In vertical mode, skip structs expanded into children
        if !*table_mode {
            let next_is_direct_field_child = rendered.nodes.get(i + 1).is_some_and(|next| {
                next.depth == node.depth + 1 && matches!(next.kind, DataNodeKind::Field { .. })
            });
            if next_is_direct_field_child {
                continue;
            }
        }
        let path = rendered.data_path(NodeRef(i as u16));
        let name = data_path_name(&spec.root, &path);
        out.push(ExpandableField {
            node: NodeRef(i as u16),
            name,
        });
    }

    out
}

/// Walk a DataPath against a spec tree to build a display name.
pub fn data_path_name(root: &RenderSpecNode, path: &DataPath) -> String {
    let mut name = String::new();
    let mut current = root;
    for step in &path.steps {
        match step {
            PathStep::Field(schema_idx) => {
                if let Some((child_name, child_spec)) = resolve_field(current, *schema_idx) {
                    if !name.is_empty() {
                        name.push('.');
                    }
                    name.push_str(child_name);
                    current = child_spec;
                } else {
                    break;
                }
            }
            PathStep::Index(idx) => {
                use std::fmt::Write;
                let _ = write!(name, "[{}]", idx);
                match &current.kind {
                    RenderSpecKind::List { element } => current = element,
                    RenderSpecKind::Map { value, .. } => current = value,
                    _ => break,
                }
            }
        }
    }
    name
}

fn resolve_field(node: &RenderSpecNode, schema_idx: u16) -> Option<(&str, &RenderSpecNode)> {
    let RenderSpecKind::Struct { children, .. } = &node.kind else {
        return None;
    };
    children
        .iter()
        .find(|c| c.schema_idx == schema_idx as usize)
        .map(|c| (c.name.as_str(), &c.spec))
}

/// Render a single field's value for the preview popup/dropdown.
pub fn render_field_full(
    spec: &RenderSpec,
    batch: &RecordBatch,
    local_row: usize,
    path: &DataPath,
    writer: &mut LineWriter,
) -> Option<(String, RenderedRow)> {
    let result = navigate_data_path(spec, batch, local_row, path)?;
    let unlimited = unlimit(&result.node);

    writer.clear();
    if result.array.is_null(result.row) {
        let _ = std::fmt::Write::write_str(writer, "null");
        writer.newline();
    } else {
        unlimited.render_value(result.array.as_ref(), result.row, writer, 0, RenderMode::Preview);
    }
    let rendered = writer.finish();
    Some((result.name, rendered))
}

struct NavigateResult {
    node: RenderSpecNode,
    array: Arc<dyn Array>,
    row: usize,
    name: String,
}

fn navigate_data_path(
    spec: &RenderSpec,
    batch: &RecordBatch,
    local_row: usize,
    path: &DataPath,
) -> Option<NavigateResult> {
    let mut steps = path.steps.iter();
    let PathStep::Field(first_idx) = steps.next()? else {
        return None;
    };
    let (top_name, top_spec) = resolve_field(&spec.root, *first_idx)?;
    let RenderSpecKind::Struct { children, .. } = &spec.root.kind else {
        return None;
    };
    let top = children
        .iter()
        .find(|c| c.schema_idx == *first_idx as usize)?;
    let mut node = top_spec;
    let mut array: Arc<dyn Array> = batch.column(top.schema_idx).clone();
    let mut row = local_row;
    let mut name = top_name.to_string();

    for step in steps {
        if array.is_null(row) {
            return Some(NavigateResult {
                node: node.clone(),
                array,
                row,
                name,
            });
        }
        match step {
            PathStep::Field(schema_idx) => {
                let sa = array.as_any().downcast_ref::<StructArray>()?;
                let (child_name, child_spec) = resolve_field(node, *schema_idx)?;
                let child = children_of(node)?
                    .iter()
                    .find(|c| c.schema_idx == *schema_idx as usize)?;
                let col = sa.column(child.schema_idx).clone();
                name = format!("{}.{}", name, child_name);
                node = child_spec;
                array = col;
            }
            PathStep::Index(element_idx) => {
                use std::fmt::Write;
                let target_idx = *element_idx as usize;
                match &node.kind {
                    RenderSpecKind::List { element } => {
                        let (start, end, values) = list_offsets(array.as_ref(), row);
                        let target = start + target_idx;
                        if target >= end {
                            return None;
                        }
                        let _ = write!(name, "[{}]", element_idx);
                        row = target;
                        node = element;
                        array = values;
                    }
                    RenderSpecKind::Map { value, .. } => {
                        let ma = array
                            .as_any()
                            .downcast_ref::<arrow::array::MapArray>()?;
                        let offsets = ma.offsets();
                        let start = offsets[row] as usize;
                        let end = offsets[row + 1] as usize;
                        let target = start + target_idx;
                        if target >= end {
                            return None;
                        }
                        let _ = write!(name, "[{}]", element_idx);
                        row = target;
                        node = value;
                        array = ma.values().clone();
                    }
                    _ => return None,
                }
            }
        }
    }

    Some(NavigateResult {
        node: node.clone(),
        array,
        row,
        name,
    })
}

fn children_of(node: &RenderSpecNode) -> Option<&[crate::layout::StructChild]> {
    match &node.kind {
        RenderSpecKind::Struct { children, .. } => Some(children),
        _ => None,
    }
}

/// Clone a spec subtree and set every string's `max_display` to unlimited.
/// Table layout (`col_widths`, `row_prefix`) is left untouched — a
/// full-field preview of a nested table still wants its columns aligned,
/// just not its individual string cells cut short.
fn unlimit(node: &RenderSpecNode) -> RenderSpecNode {
    let mut node = node.clone();
    unlimit_in_place(&mut node);
    node
}

fn unlimit_in_place(node: &mut RenderSpecNode) {
    match &mut node.kind {
        RenderSpecKind::Scalar | RenderSpecKind::Float { .. } => {}
        RenderSpecKind::Str { max_display } => *max_display = usize::MAX,
        RenderSpecKind::Struct {
            children,
            table_mode,
            ..
        } => {
            *table_mode = false;
            children
                .iter_mut()
                .for_each(|c| unlimit_in_place(&mut c.spec));
        }
        RenderSpecKind::List { element } => unlimit_in_place(element),
        RenderSpecKind::Map { key, value } => {
            unlimit_in_place(key);
            unlimit_in_place(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use crate::layout::Layout;
    use crate::source::DataSource;
    use crate::source::test_support::FakeDataSource;

    fn field_path(indices: &[u16]) -> DataPath {
        DataPath {
            steps: indices.iter().map(|&i| PathStep::Field(i)).collect(),
        }
    }

    #[test]
    fn render_field_full_returns_untruncated_string() {
        let long = "y".repeat(300);
        let mut source = FakeDataSource::two_columns(&[(long.as_str(), 1), ("short", 2)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[0]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "name");
        let content: String = rendered.lines().collect::<Vec<_>>().join("\n");
        assert!(content.contains(&long), "full string should be untruncated");
        assert!(!content.contains("chars)"), "no truncation hint expected");
    }

    #[test]
    fn navigate_out_of_range_path_returns_none() {
        let mut source = FakeDataSource::two_columns(&[("a", 1)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 80);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let result = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[99]),
            &mut writer,
        );
        assert!(result.is_none());
    }

    fn nested_struct_source() -> FakeDataSource {
        use arrow::array::{Int32Array, StringArray, StructArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let long_desc = "z".repeat(300);
        let inner = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("desc", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec![long_desc.as_str()])) as Arc<dyn Array>,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "nested",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int32, false),
                        Field::new("desc", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ),
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
    fn vertical_mode_expandable_fields_finds_nested_string() {
        let mut source = nested_struct_source();
        let (spec, rendered) = render_and_query(&mut source, 40);
        assert!(!spec.is_table());

        let fields = expandable_fields(&rendered, &spec);
        assert!(
            fields.iter().any(|f| f.name == "nested.desc"),
            "expected nested.desc to be expandable, got {:?}",
            fields.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn render_field_full_on_whole_struct_field_has_no_leading_blank_line() {
        let mut source = nested_struct_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[1]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "nested");
        assert!(
            !rendered.line(0).trim().is_empty(),
            "first line of a struct-field preview should not be blank, got lines: {:?}",
            rendered.lines().collect::<Vec<_>>()
        );
        let content: String = rendered.lines().collect::<Vec<_>>().join("\n");
        assert!(
            content.contains(&"z".repeat(300)),
            "nested string should render in full, not truncated to the field's normal max_display"
        );
        assert!(!content.contains("chars)"));
    }

    #[test]
    fn render_field_full_navigates_nested_path() {
        let mut source = nested_struct_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[1, 1]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "nested.desc");
        let content: String = rendered.lines().collect::<Vec<_>>().join("\n");
        assert!(content.contains(&"z".repeat(300)));
        assert!(!content.contains("chars)"));
    }

    fn list_of_ints_source() -> FakeDataSource {
        use arrow::array::{Int32Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let values = Int32Array::from(vec![10, 20, 30, 40]);
        let offsets = OffsetBuffer::new(vec![0i32, 4].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "nums",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(list_array) as Arc<dyn Array>]).unwrap();
        FakeDataSource::from_batch(batch)
    }

    #[test]
    fn render_field_full_list_renders_one_item_per_line() {
        let mut source = list_of_ints_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[0]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "nums");

        let lines: Vec<&str> = rendered.lines().collect();
        // "(4 items)" header + one line per element, never inlined as [10, 20, 30, 40].
        assert!(lines[0].contains("4 items"), "got: {:?}", lines);
        assert_eq!(
            lines.len(),
            5,
            "expected header + 4 item lines, got {:?}",
            lines
        );
        assert!(lines[1].contains("[0]: 10"));
        assert!(lines[4].contains("[3]: 40"));
        assert!(
            !lines.iter().any(|l| l.contains('[') && l.contains(',')),
            "list should not be rendered inline in preview mode, got {:?}",
            lines
        );
    }

    fn map_source() -> FakeDataSource {
        use arrow::array::{Int32Array, MapArray};
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let values = Int32Array::from(vec![1, 2]);
        let map_array =
            MapArray::new_from_strings(vec!["a", "b"].into_iter(), &values, &[0, 2]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "counts",
            map_array.data_type().clone(),
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(map_array) as Arc<dyn Array>]).unwrap();
        FakeDataSource::from_batch(batch)
    }

    #[test]
    fn render_field_full_map_renders_one_entry_per_line() {
        let mut source = map_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[0]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "counts");

        // Map (unlike Struct) always opens with a blank line, even in preview
        // mode — the finding's "skip leading blank line" fix targets Struct
        // fields specifically, since only Struct has a leading "field: " on
        // the same line that the blank line separates from.
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(
            lines.len(),
            3,
            "expected blank line + one line per map entry, got {:?}",
            lines
        );
        assert!(lines[0].trim().is_empty());
        assert!(lines[1].contains("\"a\": 1"), "got: {:?}", lines);
        assert!(lines[2].contains("\"b\": 2"), "got: {:?}", lines);
    }

    #[test]
    fn render_field_full_nested_struct_in_struct_has_no_blank_line_at_any_depth() {
        use arrow::array::{Int32Array, StringArray, StructArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let leaf = StructArray::from(vec![(
            Arc::new(Field::new("v", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![9])) as Arc<dyn Array>,
        )]);
        let outer = StructArray::from(vec![
            (
                Arc::new(Field::new("tag", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["t"])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("leaf", leaf.data_type().clone(), false)),
                Arc::new(leaf) as Arc<dyn Array>,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            outer.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(outer) as Arc<dyn Array>]).unwrap();
        let mut source = FakeDataSource::from_batch(batch);

        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[0]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "outer");

        let lines: Vec<&str> = rendered.lines().collect();
        assert!(
            lines.iter().all(|l| !l.trim().is_empty()),
            "no blank lines expected anywhere in a struct preview, got {:?}",
            lines
        );
    }

    // ── Deeply nested schema tests ──────────────────────────────

    /// Schema mirroring real-world deeply nested Parquet:
    ///   id: Utf8
    ///   items: List<Struct{label: Utf8, description: Utf8(long), scores: Map<Utf8, Float64>}>
    ///   meta: Struct{tag: Utf8, details: List<Struct{key: Utf8, value: Utf8(long)}>}
    fn deeply_nested_source() -> FakeDataSource {
        use arrow::array::{Float64Array, ListArray, MapArray, StringArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let long_desc = "d".repeat(300);
        let long_val = "v".repeat(200);

        // items: List<Struct{label, description, scores}>
        let item_label = StringArray::from(vec!["alpha", "beta"]);
        let item_desc = StringArray::from(vec![long_desc.as_str(), long_desc.as_str()]);
        let score_vals = Float64Array::from(vec![0.5, 0.3, 0.8, 0.1]);
        let scores_map = MapArray::new_from_strings(
            vec!["k1", "k2", "k3", "k4"].into_iter(),
            &score_vals,
            &[0, 2, 4],
        )
        .unwrap();
        let item_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("label", DataType::Utf8, false)),
                Arc::new(item_label) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("description", DataType::Utf8, false)),
                Arc::new(item_desc) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("scores", scores_map.data_type().clone(), false)),
                Arc::new(scores_map) as Arc<dyn Array>,
            ),
        ]);
        let items_list = ListArray::new(
            Arc::new(Field::new("element", item_struct.data_type().clone(), true)),
            OffsetBuffer::new(vec![0i32, 2].into()),
            Arc::new(item_struct),
            None,
        );

        // meta: Struct{tag, details: List<Struct{key, value}>}
        let detail_keys = StringArray::from(vec!["x", "y"]);
        let detail_vals = StringArray::from(vec![long_val.as_str(), long_val.as_str()]);
        let detail_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(detail_keys) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("value", DataType::Utf8, false)),
                Arc::new(detail_vals) as Arc<dyn Array>,
            ),
        ]);
        let details_list = ListArray::new(
            Arc::new(Field::new("element", detail_struct.data_type().clone(), true)),
            OffsetBuffer::new(vec![0i32, 2].into()),
            Arc::new(detail_struct),
            None,
        );
        let meta_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("tag", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["t"])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("details", details_list.data_type().clone(), false)),
                Arc::new(details_list) as Arc<dyn Array>,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("items", items_list.data_type().clone(), false),
            Field::new("meta", meta_struct.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["row0"])) as Arc<dyn Array>,
                Arc::new(items_list) as Arc<dyn Array>,
                Arc::new(meta_struct) as Arc<dyn Array>,
            ],
        )
        .unwrap();
        FakeDataSource::from_batch(batch)
    }

    fn render_and_query(source: &mut FakeDataSource, width: usize) -> (RenderSpec, RenderedRow) {
        use crate::render;
        let layout = Layout::compute(source);
        let spec = RenderSpec::resolve(&layout, width);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let rendered = render::render_record(&spec, batch, local_row, 0, &mut writer);
        (spec, rendered)
    }

    #[test]
    fn expandable_fields_detects_nested_table_truncation() {
        let mut source = deeply_nested_source();
        let (spec, rendered) = render_and_query(&mut source, 60);
        assert!(!spec.is_table());

        let fields = expandable_fields(&rendered, &spec);
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert!(
            names.contains(&"items"),
            "items (nested table with truncated columns) should be expandable, got {:?}",
            names
        );
        assert!(
            names.contains(&"meta.details"),
            "meta.details (nested table with truncated value column) should be expandable, got {:?}",
            names
        );
    }

    #[test]
    fn expandable_fields_skips_struct_lists_children_individually() {
        let mut source = deeply_nested_source();
        let (spec, rendered) = render_and_query(&mut source, 60);

        let fields = expandable_fields(&rendered, &spec);
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        assert!(
            !names.contains(&"meta"),
            "meta (struct expanded vertically) should NOT appear directly, got {:?}",
            names
        );
    }

    #[test]
    fn expandable_fields_table_mode_detects_wide_column() {
        let long = "x".repeat(200);
        let mut source = FakeDataSource::two_columns(&[(long.as_str(), 1), ("short", 2)]);
        let (spec, rendered) = render_and_query(&mut source, 40);
        assert!(spec.is_table());

        let fields = expandable_fields(&rendered, &spec);
        assert!(
            fields.iter().any(|f| f.name == "name"),
            "expected 'name' column to be expandable, got {:?}",
            fields.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn expandable_fields_table_mode_empty_when_values_fit() {
        let mut source = FakeDataSource::two_columns(&[("ab", 1), ("cd", 2)]);
        let (spec, rendered) = render_and_query(&mut source, 200);
        assert!(spec.is_table());

        let fields = expandable_fields(&rendered, &spec);
        assert!(fields.is_empty(), "got: {:?}", fields.iter().map(|f| &f.name).collect::<Vec<_>>());
    }

    #[test]
    fn expandable_fields_vertical_string_truncation() {
        let mut source = nested_struct_source();
        let (spec, rendered) = render_and_query(&mut source, 40);
        assert!(!spec.is_table());

        let fields = expandable_fields(&rendered, &spec);
        assert!(
            fields.iter().any(|f| f.name == "nested.desc"),
            "nested.desc should be expandable (300 chars in 40-width terminal), got {:?}",
            fields.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn render_field_full_on_nested_table_renders_vertically() {
        let mut source = deeply_nested_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 60);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);

        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[1]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "items");

        let content: String = rendered.lines().collect::<Vec<_>>().join("\n");
        assert!(
            content.contains(&"d".repeat(300)),
            "description should render in full (unlimit removes max_display)"
        );
        assert!(
            !content.contains("─┼─"),
            "nested table should render vertically in preview (unlimit sets table_mode=false), got:\n{}",
            content
        );
        assert!(
            content.contains("label: "),
            "fields should render as 'name: value' in vertical mode"
        );
    }

    #[test]
    fn render_field_full_on_deeply_nested_list_struct() {
        let mut source = deeply_nested_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 60);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);

        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(
            &spec,
            batch,
            local_row,
            &field_path(&[2, 1]),
            &mut writer,
        )
        .unwrap();
        assert_eq!(name, "meta.details");

        let content: String = rendered.lines().collect::<Vec<_>>().join("\n");
        assert!(
            content.contains(&"v".repeat(200)),
            "deeply nested string should render fully"
        );
        assert!(
            !content.contains("─┼─"),
            "should render vertically, not as table"
        );
    }

    #[test]
    fn preview_single_list_element_via_index_path() {
        let mut source = deeply_nested_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 60);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);

        // Path: items[0] — Field(1) for items, then Index(0) for element 0
        let path = DataPath {
            steps: smallvec![PathStep::Field(1), PathStep::Index(0)],
        };
        let mut writer = LineWriter::new();
        let (name, rendered) = render_field_full(&spec, batch, local_row, &path, &mut writer)
            .unwrap();
        assert_eq!(name, "items[0]");

        let content: String = rendered.lines().collect::<Vec<_>>().join("\n");
        assert!(
            content.contains("label: "),
            "element preview should show struct fields vertically"
        );
        assert!(
            content.contains(&"d".repeat(300)),
            "description should be fully expanded"
        );
        assert!(
            content.contains("scores: "),
            "scores field should be present"
        );
    }

    #[test]
    fn node_for_position_no_col_resolves_to_instance() {
        use crate::render;
        let mut source = deeply_nested_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 60);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let rendered = render::render_record(&spec, batch, local_row, 0, &mut writer);

        let table_data_line = (0..rendered.line_count())
            .find(|&li| rendered.line(li).contains("\"alpha\""))
            .expect("should find nested table row with 'alpha'");

        // No column selected → Instance (whole row)
        let node_ref = rendered
            .node_for_position(table_data_line, None)
            .expect("should resolve");
        let path = rendered.data_path(node_ref);
        assert!(
            path.steps.iter().any(|s| matches!(s, PathStep::Index(_))),
            "no-col path should include Index (whole row), got {:?}",
            path.steps
        );
        // Last step should be Index (the Instance), not Field
        assert!(
            matches!(path.steps.last(), Some(PathStep::Index(_))),
            "last step should be Index for whole-row, got {:?}",
            path.steps
        );
    }

    #[test]
    fn node_for_position_with_col_resolves_to_cell() {
        use crate::render;
        let mut source = deeply_nested_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 60);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let rendered = render::render_record(&spec, batch, local_row, 0, &mut writer);

        let table_data_line = (0..rendered.line_count())
            .find(|&li| rendered.line(li).contains("\"alpha\""))
            .expect("should find nested table row with 'alpha'");

        // Column 1 selected → specific Field within the Instance
        let node_ref = rendered
            .node_for_position(table_data_line, Some(1))
            .expect("should resolve");
        let path = rendered.data_path(node_ref);
        // Last step should be Field (the cell), not Index
        assert!(
            matches!(path.steps.last(), Some(PathStep::Field(_))),
            "last step should be Field for cell selection, got {:?}",
            path.steps
        );
        // Path should still include the Index step (ancestor)
        assert!(
            path.steps.iter().any(|s| matches!(s, PathStep::Index(_))),
            "cell path should still include Index ancestor, got {:?}",
            path.steps
        );
    }

    #[test]
    fn node_for_position_cell_preview_shows_single_field() {
        let mut source = deeply_nested_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 60);
        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);

        let mut writer = LineWriter::new();
        let rendered = crate::render::render_record(&spec, batch, local_row, 0, &mut writer);

        let table_data_line = (0..rendered.line_count())
            .find(|&li| rendered.line(li).contains("\"alpha\""))
            .expect("should find nested table row");

        // Select column 1 (description — the long truncated field)
        let node_ref = rendered
            .node_for_position(table_data_line, Some(1))
            .expect("should resolve");
        let path = rendered.data_path(node_ref);

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let mut writer = LineWriter::new();
        let (name, rendered) =
            render_field_full(&spec, batch, local_row, &path, &mut writer).unwrap();

        assert!(
            name.contains("description"),
            "preview name should be the cell's field, got: {}",
            name
        );
        let content: String = rendered.lines().collect::<Vec<_>>().join("\n");
        assert!(
            content.contains(&"d".repeat(300)),
            "cell preview should show full description"
        );
        assert!(
            !content.contains("label"),
            "cell preview should NOT show sibling fields like label"
        );
    }
}
