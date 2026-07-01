# Architecture

## Module structure

```
src/
  main.rs              CLI parsing, pipe mode output
  layout/              Two-layer display model: Layout + RenderSpec
  render.rs            Rendering methods on RenderSpec types
  preview.rs           SchemaPath, truncation detection, full-field render
  source/              DataSource trait + format implementations
  cache.rs             SizedLruCache, RowCache
  worker.rs            Background thread: rendering + search
  input.rs             Modal key handling: Mode, Action, InputHandler
  tui/                 Terminal UI: event loop, cursor, line styling
  viewport.rs          ViewportAnchor, NavIntent: scroll/jump math
  unicode.rs           Display-width helpers (CJK-aware)
```

## Data flow

```
                    ┌─────────────┐
                    │  UI Thread  │
                    │   (tui/)    │
                    └──────┬──────┘
                           │ WorkerRequest / WorkerResponse
                    ┌──────┴──────┐
                    │   Worker    │
                    │ (worker.rs) │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
        ┌─────┴─────┐ ┌───┴───┐ ┌──────┴──────┐
        │DataSource  │ │Render │ │  RowCache   │
        │(source/)   │ │Spec   │ │  (cache.rs) │
        └────────────┘ └───────┘ └─────────────┘
```

- **UI thread** handles input and drawing. Never touches files or does heavy computation.
- **Worker thread** owns the `DataSource` and `LineWriter`. Receives requests to render rows or search. Posts results back via channel.
- **RowCache** is the shared state between threads: `Arc<RowCache>` with internal `RwLock`.

## Two-layer display model

All display decisions flow through two types in the `layout/` module:

### Layout (durable, schema-derived)

A recursive tree mirroring the Arrow schema. Computed once from schema + sampled data. Does not know about terminal width. Captures what the data looks like: how wide are values, what precision do floats need, which structs render as tables.

Later (#2), this is the type that gets persisted to config for per-schema display preferences.

### RenderSpec (ephemeral, terminal-resolved)

Layout + terminal width → concrete rendering decisions. Recomputed on resize. This is what rendering consumes. Contains:
- Column widths distributed across available space
- Column display order (bounded columns left, unbounded rightmost)
- Precomputed row prefixes (guide chars + padding)
- Float precision and string truncation limits at every schema level

The RenderSpec is the single source of truth for rendering. Rendering methods live on `RenderSpecNode` — they dispatch on the spec kind, not on Arrow DataType.

### Layout computation

Building a Layout from data uses a schema-shaped accumulator tree (`LayoutBuilder`):

1. **Build** the tree from the schema — one accumulator node per schema field, no data yet.
2. **Feed** sampled rows through the tree — each node accumulates width statistics, float values, string lengths.
3. **Resolve** the accumulated stats into a `LayoutNode` tree — p80 widths, float precision, table-mode decisions.

This handles arbitrary nesting depth (Struct→Map→Struct→List→...) because `feed()` recurses naturally through the data.

### Column width strategy

For table-mode structs, columns are classified as bounded or unbounded based on `max_sampled_width / p80_width`. Bounded columns (ratio < 1.5) get their `max(max_width, p80).min(p80 * 1.1)` — tight fit with minimal waste. The least-bounded column moves to the rightmost position and receives all remaining terminal width. This avoids even splits where both a narrow ID column and a wide map column get 50% each.

## Lazy loading

`ParquetSource::open()` reads only file metadata (parquet footers). Row groups are loaded on demand via `ensure_loaded()` and cached in a 3-slot LRU. Each row group is decompressed into an Arrow `RecordBatch`.

Adding a new format means implementing the `DataSource` trait (5 methods: `schema`, `total_rows`, `file_count`, `ensure_loaded`, `get_row`). All formats convert to Arrow `RecordBatch` — the rendering layer is format-agnostic.

## Rendering

Rendering methods are implemented on `RenderSpec` and `RenderSpecNode`:

- `spec.render_row(batch, row, writer)` — entry point for a single row
- `node.render_value(array, row, writer, depth)` — recursive vertical-mode rendering
- `node.write_cell_preview(out, array, row)` — compact inline preview for table cells
- `spec.render_table_header()` — column headers from spec names and widths

`LineWriter` is a reusable buffer that accumulates rendered output:

- Single `String` buffer for all line content (no per-line allocation)
- `Vec<usize>` tracks line boundaries (byte offsets into the buffer)
- `scratch: String` for temporary formatting (table cell width measurement)
- Pre-computed guide string sliced by depth (no allocation per `guide()` call)

`finish()` clones the buffer into a `RenderedRow` (2 allocations). After warmup, rendering a row costs ~3 heap allocations regardless of complexity.

### Display modes

- **Table mode** (top-level, all-scalar schemas): spreadsheet-style, one row per line
- **Vertical mode** (top-level, mixed schemas): one field per line with tree guides
- **Nested tables** (List\<Struct\> inside vertical mode): auto-table with precomputed column widths from the prototype layout
- **Cell previews**: compact inline representations for complex values in table cells, with a byte budget to prevent runaway generation

## Cache

`SizedLruCache<K, V>` evicts by total byte budget (not item count). Built on two unbounded `LruCache` instances (entries + sizes) with manual eviction.

`RowCache` wraps this behind `RwLock` for thread safety. Budget: 2MB. Stores `Arc<RenderedRow>` keyed by global row index.

## Search

Two-level, async:

1. **Record scan** (worker thread): iterates rows, checks parquet string columns for substring match (`row_might_match`), then renders candidate rows and verifies against rendered text. Collects up to 100 matching row indices per batch.
2. **Within-record** (UI thread): when navigating to a matched record, scans the cached `RenderedRow` lines for the query string. Highlights matching lines.

`n` skips all matches visible on the current screen and jumps to the next off-screen match. If more matches are needed and the scan isn't exhausted, requests another batch from the worker.

## Cursor vs. viewport

`ViewportAnchor` (in `viewport.rs`) tracks what's scrolled into view; `CursorState` (in `tui/cursor.rs`) tracks what's focused — the record a preview action acts on, and in table mode, which column. They're deliberately separate types:

- Plain scrolling (`j`/`k`/`J`/`K`/half-page) moves only the anchor. The cursor does not follow — this matches `less`/`vim` where scrolling and the cursor are independent.
- Cursor movement (`Ctrl+j`/`Ctrl+k` for records, `h`/`l` for columns) moves only the cursor, dragging the anchor by the minimum amount needed to keep the cursor visible (`keep_record_visible`) — never re-centering the screen.
- Actions that jump the anchor to a specific record (`g`/`G` with a count, `%`, search jumps) sync the cursor to match, since after a jump "where the viewport is" and "what's focused" should agree.

This split exists because preview always previews `cursor.current_record`/`cursor.selected_col`, regardless of where the viewport happens to be scrolled.

Navigation positions the first matching line at 20% from the top of the viewport.

## Preview

The UI thread never touches Arrow data, so truncation detection and full-value rendering both happen worker-side and cross the channel as request/response pairs (`ListTruncatedFields`/`TruncatedFields`, `RenderFullField`/`FieldRendered` in `worker.rs`).

`SchemaPath` (`preview.rs`) addresses a field in the schema tree: a single index in table mode (the column), or a chain of `schema_idx` values through nested structs in vertical mode. Lists and maps are not addressable by path — previewing one expands the whole field rather than a specific element, since a path describes schema shape, not a position within row data.

`RenderSpec::find_truncated_fields` walks the row using the already-rendered widths (`write_cell_preview` vs `col_widths` in table mode; `max_display` vs actual string width in vertical mode, recursing into nested structs) to list which fields are currently cut off — these become the numbered targets for the `v` overlay.

`render_field_full` renders one field with no width limits. Because a preview must be untruncated at every depth (not just the target field), it first rebuilds the field's spec subtree with every string's `max_display` set to unlimited (`unlimit`), then renders through the normal recursive `render_value`. A struct-typed field is rendered by writing its children directly rather than going through the nested-struct branch of `render_value`, which opens with a blank line intended for when a `"field: "` prefix already precedes it — there is no such prefix at the top of a standalone preview.

Display size decides inline vs. popup: `<=3` lines renders inline (appended after the row, styled distinctly); more than that opens a scrollable popup and force-transitions `InputHandler` into `Mode::Preview` via `set_mode`, since `V`/Space fire from `Mode::Normal` and the popup's j/k scrolling only exists in Preview mode.

Every worker response carries the `row`/`path` it answers, so the UI can drop stale responses if the user has since dismissed the preview or moved to a different record before the worker replies.
