# Architecture

## Module structure

```
src/
  main.rs              CLI parsing, pipe mode output
  layout.rs            Layout computation + RenderSpec resolution
  render.rs            Rendering methods on RenderSpec types
  source/
    mod.rs             DataSource trait + format dispatcher
    parquet.rs         ParquetSource: lazy row-group loading
  cache.rs             SizedLruCache, RowCache
  worker.rs            Background thread: rendering + search
  tui.rs               Terminal UI: draw loop, input, scroll
  unicode.rs           Display-width helpers (CJK-aware)
```

## Data flow

```
                    ┌─────────────┐
                    │  UI Thread  │
                    │  (tui.rs)   │
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

All display decisions flow through two types in `layout.rs`:

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

Navigation positions the first matching line at 20% from the top of the viewport.
