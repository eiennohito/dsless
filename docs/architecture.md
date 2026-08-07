# Architecture

## Module structure

```
src/
  main.rs              CLI parsing, pipe mode output
  layout/              Two-layer display model: Layout + RenderSpec
  render.rs            Rendering methods on RenderSpec types
  preview.rs           DataPath re-export, expandable-field query, full-field render
  source/              DataSource trait + format implementations
  cache.rs             SizedLruCache, RowCache
  search.rs            SearchState, record-scan search algorithm
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
- Column display order (highest-spread column moved rightmost)
- Precomputed row prefixes (guide chars + padding)
- Float precision and string truncation limits at every schema level

The RenderSpec is the single source of truth for rendering. Rendering methods live on `RenderSpecNode` — they dispatch on the spec kind, not on Arrow DataType.

### Layout computation

Building a Layout from data uses a schema-shaped accumulator tree (`LayoutBuilder`):

1. **Build** the tree from the schema — one accumulator node per schema field, no data yet.
2. **Feed** sampled rows through the tree — each node accumulates width statistics, float values, string lengths.
3. **Resolve** the accumulated stats into a `LayoutNode` tree — full width histograms (`WidthProfile`, queried at arbitrary percentiles later), float precision, table-mode decisions.

This handles arbitrary nesting depth (Struct→Map→Struct→List→...) because `feed()` recurses naturally through the data.

### Column width strategy

Each `LayoutNode` stores a `WidthProfile` — the full sorted sample of cell widths from sampling. Column allocation queries this histogram at arbitrary percentiles rather than relying on precomputed summaries.

**Fixed vs variable**: columns where p80 ≥ max are fixed-width (every cell is the same width). These get exactly their width first. The remaining budget goes to variable columns.

**Variable allocation**: proportional to `sqrt(p80)`. Sqrt dampens huge columns so a 400-char description doesn't starve a 15-char ID column. Columns are floored at `MIN_COL` (8) and capped at their sampled max, with surplus redistributed.

**Rightmost placement**: the column with the highest `p95/p50` spread ratio (>1.5) is moved to the rightmost display position. Stable columns on the left are easier to scan.

## Lazy loading

`ParquetSource::open()` reads only file metadata (parquet footers). Row groups are loaded on demand via `ensure_loaded()` and cached in a 3-slot LRU. Each row group is decompressed into an Arrow `RecordBatch`.

Adding a new format means implementing the `DataSource` trait (5 methods: `schema`, `total_rows`, `file_count`, `ensure_loaded`, `get_row`). All formats convert to Arrow `RecordBatch` — the rendering layer is format-agnostic.

## Rendering

Rendering methods are implemented on `RenderSpec` and `RenderSpecNode`:

- `spec.render_row(batch, row, writer)` — entry point for a single row
- `node.render_value(array, row, writer, depth, mode)` — recursive vertical-mode rendering, shared by normal rows and by `preview::render_field_full` via `RenderMode` (see [Preview](#preview))
- `node.write_cell_preview(out, array, row)` — compact inline preview for table cells
- `spec.render_table_header()` — column headers from spec names and widths

`render.rs` exports `COLUMN_SEPARATOR`, used by rendering to separate table columns in text output. Cell styling (`style_table_row`) and cursor column counting (`App::cursor_column_count`) both derive column boundaries from `RenderedRow::line_table_info` — byte ranges read from the DataNode tree — uniformly for top-level and nested tables. `COLUMN_SEPARATOR` is only needed for building/splitting the static header line (`render_table_header` / `style_header_line`), not for per-row cell detection.

`LineWriter` is a reusable buffer that accumulates rendered output:

- Single `String` buffer for all line content (no per-line allocation)
- `Vec<usize>` tracks line boundaries (byte offsets into the buffer)
- `scratch: String` for temporary formatting (table cell width measurement)
- Pre-computed guide string sliced by depth (no allocation per `guide()` call)
- `nodes: Vec<DataNode>` — a node tree emitted during rendering (see below)

`finish()` clones the buffer (and the node tree) into a `RenderedRow`. After warmup, rendering a row costs a small constant number of heap allocations regardless of complexity.

### DataNode tree

Rendering emits `DataNode`s in DFS order via `LineWriter`'s `open_node`/`close_node` bracket API. Each node carries a byte range (start/end offsets into the rendered buffer), a depth, a kind (`RowHeader`, `Field { schema_idx }`, or `Instance { index }`), and a `Fidelity` (`Full`, `Constrained`, or `Summarized`). Fidelity is set by `mark_constrained` (value truncated to fit column width) and `mark_summarized` (structural elements like list items omitted entirely); `close_node` propagates child fidelity upward so a parent is at least as degraded as its worst child.

Both table mode and vertical mode emit nodes — table-mode rows get one `Instance` per row with `Field` children per column, vertical mode gets `Field` nodes at each struct level with `Instance` nodes inside lists. This uniformity means the same query API works for both modes.

`RenderedRow::line_table_info(line_idx)` queries the node tree for table column info on a given line: it returns `LineTableInfo` with a `SmallVec<[ColumnSlice; 8]>`, each carrying byte-range offsets (relative to line start) and a `NodeRef`. Returns `None` for non-table lines. Works for both top-level table rows (multiple `Field` nodes at depth 1 on one line) and nested table rows (`Field` children of an `Instance` node). `node_for_position(line_idx, selected_col)` delegates to `line_table_info` when a column is selected, falling back to the deepest node at the line start otherwise. Both share `deepest_node_at` and `field_columns` helpers. `data_path(node)` lazily reconstructs the full `DataPath` by walking ancestors backward from the target node — no path is stored per-node, keeping the per-node cost to 12 bytes.

### Display modes

- **Table mode** (top-level, all-scalar schemas): spreadsheet-style, one row per line
- **Vertical mode** (top-level, mixed schemas): one field per line with tree guides
- **Nested tables** (List\<Struct\> inside vertical mode): auto-table with precomputed column widths from the prototype layout
- **Cell previews**: compact inline representations for complex values in table cells, with a byte budget to prevent runaway generation

## Cache

`SizedLruCache<K, V>` evicts by total byte budget (not item count). Built on two unbounded `LruCache` instances (entries + sizes) with manual eviction.

`RowCache` wraps this behind `RwLock` for thread safety. Budget: 4MB. Stores `Arc<RenderedRow>` keyed by global row index.

## Search

Two-level, async:

1. **Record scan** (worker thread): iterates rows, checks parquet string columns for substring match (`row_might_match`), then renders candidate rows and verifies against rendered text. Collects up to 100 matching row indices per batch.
2. **Within-record** (UI thread): when navigating to a matched record, scans the cached `RenderedRow` lines for the query string. Highlights matching lines.

`n` skips all matches visible on the current screen and jumps to the next off-screen match. If more matches are needed and the scan isn't exhausted, requests another batch from the worker.

## Copy

`Y` copies to the system clipboard via OSC 52 (`tui/copy.rs`). Three sources, dispatched contextually:

1. **Preview open** → copies `ActivePreview.content` (the original untruncated field value, not the word-wrapped display lines). Immediate, no worker round-trip.
2. **Cell selected** → `WorkerRequest::RenderCellForCopy` renders the full field value via `render_field_full`, responds with `CopyReady(String)`.
3. **Whole record** → `WorkerRequest::RenderForCopy` re-renders the record at a fixed 100-column width using a pre-cached `copy_spec` (resolved once at startup, not per-request). Responds with `CopyReady(String)`.

The worker thread owns a `WorkerCtx` struct bundling source, cache, writer, spec, copy_spec, and the response channel. The copy spec is a `RenderSpec` resolved from the `Layout` at `COPY_WIDTH` — separate from the terminal-width spec so copy output is consistent regardless of the user's terminal size.

## Event loop

`tui::app::run_app` drives everything through one `AppEvent` channel with two producers:

- A dedicated thread blocks on `crossterm::event::read()` and forwards terminal events (`AppEvent::Term`).
- A bridge thread forwards `WorkerResponse`s from the worker's own channel (`AppEvent::Worker`).

The main thread only ever blocks on `event_rx.recv()`, so terminal input and worker results interleave without polling. Each iteration calls `handle_action`/`handle_worker_response` (state mutation, in `app.rs`) then `draw` (pure rendering, in `tui/draw.rs` — split out from `app.rs` once the file grew past the project's line-count guidance). Any action or resize that leaves the row cache with gaps sets `draw_had_cache_miss`, which triggers an immediate re-request to the worker so missing rows get filled on the next frame instead of waiting for the next keystroke.

## Cursor vs. viewport

`ViewportAnchor` (in `viewport.rs`) tracks what's scrolled into view; `CursorState` (in `tui/cursor.rs`) tracks what's focused — the record (`cursor.record`) a preview action acts on, and the column (`cursor.selected_col`) on whichever line the cursor sits on, as long as that line has table-style column separators (top-level table rows, or a nested table rendered inside vertical mode — column selection isn't limited to top-level table mode). They're deliberately separate types:

- Plain scrolling (`Ctrl+j`/`Ctrl+k` for lines, `Ctrl+d`/`Ctrl+u` for half-page) moves only the anchor. The cursor does not follow.
- Cursor movement (`j`/`k` for lines, `J`/`K` for pages, `h`/`l` for columns) moves only the cursor. A directional scrolloff margin (`min(10, visible_height / 10)` lines) keeps the cursor clear of the viewport edge: when the cursor is on-screen, the margin is enforced only in the movement direction (pressing `j` can scroll the viewport down but never up), preventing backjumps after operations that place the cursor at a viewport edge. When the cursor is off-screen (e.g. after the viewport was scrolled past it with `Ctrl+j`), the viewport always snaps to bring the cursor back into view regardless of direction. `h` at column 0 exits cell selection back to whole-row mode; `l` from row mode enters cell selection at column 0. Column selection persists across navigation instead of resetting on every move: in table mode it persists unconditionally, since every row shares the same schema; in tree mode it persists by column index within the same record, and across records when the landing line has the same structural path as the previous selection (matched by schema path, ignoring array indices) — otherwise it clears.
- Actions that jump the anchor to a specific record (`g`/`G` with a count, `%`, search jumps) sync the cursor to match, since after a jump "where the viewport is" and "what's focused" should agree.

This split exists because preview always previews `cursor.record`/`cursor.selected_col`, regardless of where the viewport happens to be scrolled.

Navigation positions the first matching line at 20% from the top of the viewport (approximated in vertical mode via `VERTICAL_MODE_LINES_PER_ROW_ESTIMATE`, since real per-record line counts aren't known until rendered).

## Preview

The UI thread never touches Arrow data, so full-value rendering happens worker-side and crosses the channel as a request/response pair (`RenderFullField`/`FieldRendered` in `worker.rs`).

Addressing uses `DataPath` / `PathStep` (`render.rs`, re-exported via `preview.rs`): `PathStep::Field(schema_idx)` for struct children, `PathStep::Index(element_idx)` for list elements and map entries. This makes lists and maps addressable — a path describes a position within row data, not just schema shape.

`expandable_fields` (`preview.rs`) queries the rendered `RenderedRow`'s node tree for fields whose `Fidelity` is `Constrained` or `Summarized`. This is synchronous — it reads nodes already present in the cached `RenderedRow`, with no worker round-trip. Pressing `v` lists these as targets labeled with a base-20 alphanumeric code (`tui/label.rs`; digits then `qwertyuiop`, `1` as the zero/pad digit) rather than plain decimal numbers — this keeps the label width down to one character for up to 20 truncated fields, two for up to 400, and so on, and typing the label's characters (`Mode::VOverlay`) resolves incrementally so an invalid or complete prefix is detected after each keystroke (`resolve_label`).

`render_field_full` renders one field with no width limits, reusing the same recursive `RenderSpecNode::render_value` that renders normal rows, parameterized by `RenderMode` (`render.rs`). `RenderMode::Preview` changes two things relative to a normal row: scalar lists render one item per line instead of inline (there's a whole popup to spread out in), and a struct field's own children aren't preceded by the usual blank separator line (that blank line exists to separate a struct's children from the `"field: "` text on the line above it — a standalone preview has no such prefix at any depth, since the previewed field itself has no parent context in the popup). Because a preview must be untruncated at every depth, not just the target field, it first rebuilds the field's spec subtree with every string's `max_display` set to unlimited (`unlimit`) before rendering.

The preview is a single cursor-anchored popup (`tui::preview::render_preview`) — it grows toward the screen center from whichever side of the cursor has more room, sized to the content up to the available height. There is no separate inline-vs-popup distinction; content that fits without scrolling just renders as a small popup. `InputHandler` force-transitions into `Mode::Preview` via `set_mode` only when the content doesn't fit (`ActivePreview::scrollable`), since `V`/Space fire from `Mode::Normal` and the popup's j/k scrolling only exists in `Preview` mode.

Every worker response carries the `row`/`path` it answers, so the UI can drop stale responses if the user has since dismissed the preview or moved to a different record before the worker replies.

`App`'s preview-related state is a single `PreviewPhase` enum (`tui/preview.rs`) rather than several independently-nullable fields: `Idle`, `WaitingForFields`, `Message` (the "no truncated fields" case), `Overlay` (the numbered dropdown, holding its own `label_buf`), `WaitingForContent`, and `Preview` (an open popup). `WaitingForContent`/`Preview` carry an optional `fallback: FieldOverlay` — set when the request came from resolving a label in `Overlay`, `None` when it came from `V`/`<space>` acting directly on `cursor.record`/`last_path` without an overlay involved. Dismissing a popup (`v`/Esc) returns to `Overlay` if a fallback exists, else `Idle`, matching `InputHandler`'s `Preview → VOverlay` mode transition on the same key. Because the old scheme tracked "which async response is still pending" and "what's currently shown" as separate `Option`s that could disagree, an empty/failed `RenderFullField` response previously left the mode reset to `Normal` while a stale overlay was still drawn; the enum makes that combination unrepresentable — `WaitingForContent`'s `fallback` is exactly what an empty response needs to fall back to, and it either becomes the next `Overlay` (with `Mode::VOverlay` set to match) or `Idle` (with `Mode::Normal`), never both.
