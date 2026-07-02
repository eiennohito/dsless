use arrow::array::{Array, RecordBatch, StructArray};
use smallvec::{SmallVec, smallvec};

use crate::layout::{RenderSpec, RenderSpecKind, RenderSpecNode};
use crate::render::{LineWriter, RenderMode, RenderedRow, extract_str};
use crate::unicode::display_width;

/// A path into the schema tree. Table mode uses a single-element path
/// (the column index). Vertical mode can nest through struct children,
/// e.g. `[2, 0]` for field 2's first child. Lists/Maps are not addressable
/// by path — previewing a list/map field previews the whole field.
/// Inline capacity 4 covers observed nesting depths with no heap allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaPath(pub SmallVec<[usize; 4]>);

/// One field the `v` overlay can jump to, numbered 1..N for the user.
pub struct TruncatedField {
    pub path: SchemaPath,
    pub name: String,
}

impl RenderSpec {
    /// List every field of the row that is currently truncated in the
    /// rendered display, so `v` can offer them as numbered preview targets.
    ///
    /// Table mode: a column is truncated when its full preview is wider
    /// than the column width it was given.
    /// Vertical mode: a string field is truncated when its value exceeds
    /// `max_display`; struct fields are walked recursively (building
    /// nested paths) since they render in full rather than as a preview.
    pub fn find_truncated_fields(
        &self,
        batch: &RecordBatch,
        local_row: usize,
    ) -> Vec<TruncatedField> {
        let mut out = Vec::new();
        let RenderSpecKind::Struct {
            children,
            table_mode,
            col_widths,
            ..
        } = &self.root.kind
        else {
            unreachable!("root spec must be Struct");
        };

        if *table_mode {
            let mut scratch = String::new();
            for (ci, child) in children.iter().enumerate() {
                let col = batch.column(child.schema_idx);
                scratch.clear();
                child
                    .spec
                    .write_cell_preview(&mut scratch, col.as_ref(), local_row);
                if display_width(&scratch) > col_widths[ci] {
                    out.push(TruncatedField {
                        path: SchemaPath(smallvec![ci]),
                        name: child.name.clone(),
                    });
                }
            }
        } else {
            for (fi, child) in children.iter().enumerate() {
                let col = batch.column(child.schema_idx);
                find_truncated_in_node(
                    &child.spec,
                    col.as_ref(),
                    local_row,
                    &child.name,
                    SchemaPath(smallvec![fi]),
                    &mut out,
                );
            }
        }

        out
    }
}

/// Struct fields are walked individually (each gets its own path/name so
/// the `v` overlay can offer them separately); List/Map fields are folded
/// to a single yes/no via `is_value_truncated` since previewing them always
/// expands the whole field rather than one element.
fn find_truncated_in_node(
    node: &RenderSpecNode,
    array: &dyn Array,
    row: usize,
    name: &str,
    path: SchemaPath,
    out: &mut Vec<TruncatedField>,
) {
    if array.is_null(row) {
        return;
    }
    match &node.kind {
        RenderSpecKind::Struct { children, .. } => {
            let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
            for child in children {
                let col = sa.column(child.schema_idx);
                let mut child_path = path.clone();
                child_path.0.push(child.schema_idx);
                let full_name = format!("{}.{}", name, child.name);
                find_truncated_in_node(&child.spec, col.as_ref(), row, &full_name, child_path, out);
            }
        }
        _ => {
            if is_value_truncated(node, array, row) {
                out.push(TruncatedField {
                    path,
                    name: name.to_string(),
                });
            }
        }
    }
}

/// Leaf-truncation predicate for vertical mode. Lists and maps render
/// fully expanded (one element per line) or inline with untruncated
/// scalars, so they are never truncated in the default display.
fn is_value_truncated(node: &RenderSpecNode, array: &dyn Array, row: usize) -> bool {
    if array.is_null(row) {
        return false;
    }
    match &node.kind {
        RenderSpecKind::Str { max_display } => {
            display_width(extract_str(array, row)) > *max_display
        }
        RenderSpecKind::Struct { children, .. } => {
            let sa = array.as_any().downcast_ref::<StructArray>().unwrap();
            children
                .iter()
                .any(|c| is_value_truncated(&c.spec, sa.column(c.schema_idx).as_ref(), row))
        }
        _ => false,
    }
}

/// Navigate `path` from the root struct spec + batch down to the target
/// field's spec and Arrow array, following struct children only — Lists
/// and Maps are addressed as a whole (previewing them expands the entire
/// field, not one element).
fn navigate<'a>(
    spec: &'a RenderSpec,
    batch: &'a RecordBatch,
    local_row: usize,
    path: &SchemaPath,
) -> Option<(&'a RenderSpecNode, &'a dyn Array, String)> {
    let RenderSpecKind::Struct { children, .. } = &spec.root.kind else {
        unreachable!("root spec must be Struct");
    };

    let (&first, rest) = path.0.split_first()?;

    // Table mode addresses children by their position in `children`
    // (display order); vertical mode addresses by `schema_idx` since
    // nested-struct paths are built from schema_idx values.
    let is_table = matches!(
        &spec.root.kind,
        RenderSpecKind::Struct {
            table_mode: true,
            ..
        }
    );
    let top = if is_table {
        children.get(first)?
    } else {
        children.iter().find(|c| c.schema_idx == first)?
    };

    let mut node = &top.spec;
    let mut array: &dyn Array = batch.column(top.schema_idx).as_ref();
    let mut name = top.name.clone();

    for &step in rest {
        if array.is_null(local_row) {
            return Some((node, array, name));
        }
        let RenderSpecKind::Struct { children, .. } = &node.kind else {
            return None;
        };
        let sa = array.as_any().downcast_ref::<StructArray>()?;
        let next = children.iter().find(|c| c.schema_idx == step)?;
        node = &next.spec;
        array = sa.column(next.schema_idx).as_ref();
        name = format!("{}.{}", name, next.name);
    }

    Some((node, array, name))
}

/// Render a single field's value for the preview popup/dropdown.
/// Lists and maps render one entry per line (not inline).
/// All string width limits are removed via `unlimit`.
pub fn render_field_full(
    spec: &RenderSpec,
    batch: &RecordBatch,
    local_row: usize,
    path: &SchemaPath,
    writer: &mut LineWriter,
) -> Option<(String, RenderedRow)> {
    let (node, array, name) = navigate(spec, batch, local_row, path)?;
    let unlimited = unlimit(node);

    writer.clear();
    if array.is_null(local_row) {
        let _ = std::fmt::Write::write_str(writer, "null");
        writer.newline();
    } else {
        unlimited.render_value(array, local_row, writer, 0, RenderMode::Preview);
    }
    let rendered = writer.finish();
    Some((name, rendered))
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
        RenderSpecKind::Struct { children, .. } => {
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
    use crate::layout::Layout;
    use crate::source::DataSource;
    use crate::source::test_support::FakeDataSource;

    #[test]
    fn table_mode_flags_wide_column_as_truncated() {
        let long = "x".repeat(200);
        let mut source = FakeDataSource::two_columns(&[(long.as_str(), 1), ("short", 2)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);
        assert!(spec.is_table());

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let truncated = spec.find_truncated_fields(batch, local_row);
        assert!(
            truncated.iter().any(|f| f.path == SchemaPath(smallvec![0])),
            "expected column 0 to be flagged truncated, got {:?}",
            truncated.iter().map(|f| &f.path).collect::<Vec<_>>()
        );
    }

    #[test]
    fn table_mode_no_truncation_when_values_fit() {
        let mut source = FakeDataSource::two_columns(&[("ab", 1), ("cd", 2)]);
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 200);

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let truncated = spec.find_truncated_fields(batch, local_row);
        assert!(truncated.is_empty());
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
            &SchemaPath(smallvec![0]),
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
            &SchemaPath(smallvec![99]),
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
    fn vertical_mode_recurses_into_struct_children() {
        let mut source = nested_struct_source();
        let layout = Layout::compute(&mut source);
        let spec = RenderSpec::resolve(&layout, 40);
        assert!(!spec.is_table());

        source.ensure_loaded(0).unwrap();
        let (batch, local_row) = source.get_row(0);
        let truncated = spec.find_truncated_fields(batch, local_row);
        assert!(
            truncated
                .iter()
                .any(|f| f.path == SchemaPath(smallvec![1, 1])),
            "expected nested.desc at path [1, 1] to be flagged, got {:?}",
            truncated.iter().map(|f| &f.path).collect::<Vec<_>>()
        );
        assert!(truncated.iter().any(|f| f.name == "nested.desc"));
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
            &SchemaPath(smallvec![1]),
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
            &SchemaPath(smallvec![1, 1]),
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
            &SchemaPath(smallvec![0]),
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
            &SchemaPath(smallvec![0]),
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
            &SchemaPath(smallvec![0]),
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
}
