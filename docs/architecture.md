# Architecture

## Module structure

```
src/
  main.rs              CLI parsing, pipe mode output
  source/
    mod.rs             DataSource trait + format dispatcher
    parquet.rs         ParquetSource: lazy row-group loading
  render.rs            Tree/table rendering, LineWriter buffer
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
        │(source/)   │ │       │ │  (cache.rs) │
        └────────────┘ └───────┘ └─────────────┘
```

- **UI thread** handles input and drawing. Never touches files or does heavy computation.
- **Worker thread** owns the `DataSource` and `LineWriter`. Receives requests to render rows or search. Posts results back via channel.
- **RowCache** is the shared state between threads: `Arc<RowCache>` with internal `RwLock`.

## Lazy loading

`ParquetSource::open()` reads only file metadata (parquet footers). Row groups are loaded on demand via `ensure_loaded()` and cached in a 3-slot LRU. Each row group is decompressed into an Arrow `RecordBatch`.

Adding a new format means implementing the `DataSource` trait (5 methods: `schema`, `total_rows`, `file_count`, `ensure_loaded`, `get_row`). All formats convert to Arrow `RecordBatch` — the rendering layer is format-agnostic.

## Rendering

`LineWriter` is a reusable buffer that accumulates rendered output:

- Single `String` buffer for all line content (no per-line allocation)
- `Vec<usize>` tracks line boundaries (byte offsets into the buffer)
- `scratch: String` for temporary formatting (table cell width measurement)
- Pre-computed guide string sliced by depth (no allocation per `guide()` call)
- Implements `fmt::Write` so `write!(writer, ...)` works directly

`finish()` clones the buffer into a `RenderedRow` (2 allocations). After warmup, the `LineWriter`'s internal buffers have enough capacity and rendering a row costs ~3 heap allocations regardless of complexity.

### Tree layout

- **Scalars**: `│ │ field_name: value`
- **Structs**: vertical guides (`│ `) replace braces, children indented
- **Scalar lists**: inlined as `["a", "b", "c"]`
- **List-of-struct**: auto-table with `│`-separated columns, CJK-aware alignment
- **Complex values in tables**: compact preview with `+N` overflow (`[{axis: "x", ...}, +5]`)

## Cache

`SizedLruCache<K, V>` evicts by total byte budget (not item count). Built on two unbounded `LruCache` instances (entries + sizes) with manual eviction.

`RowCache` wraps this behind `RwLock` for thread safety. Budget: 2MB. Stores `Arc<RenderedRow>` keyed by global row index.

## Search

Two-level, async:

1. **Record scan** (worker thread): iterates rows, checks parquet string columns for substring match (`row_might_match`), then renders candidate rows and verifies against rendered text. Collects up to 100 matching row indices per batch.
2. **Within-record** (UI thread): when navigating to a matched record, scans the cached `RenderedRow` lines for the query string. Highlights matching lines.

`n` skips all matches visible on the current screen and jumps to the next off-screen match. If more matches are needed and the scan isn't exhausted, requests another batch from the worker.

Navigation positions the first matching line at 20% from the top of the viewport.
