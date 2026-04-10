# dsless

A terminal pager for data-science file formats. Think `less`, but for parquet files with deeply nested schemas.

## TL;DR

```bash
cargo install --path .
dsless data.parquet          # TUI mode
dsless parquet-dir/           # reads all .parquet files in directory
dsless data.parquet | head    # pipe mode, plain text output
```

Renders nested structs, arrays-of-structs-as-tables, and deeply nested lists in a readable tree layout with CJK-aware column alignment.

## AI-Assisted coding disclaimer

This project is written in fully AI-assisted manner. Most of the code is generated. If you do not like AI slop, you know what it is.

## Why

Existing tools (`parquet-tools`, `duckdb`, pandas) choke on complex nested schemas — arrays of structs with nested lists render as unreadable JSON blobs or get truncated. dsless renders them as indented trees with vertical guides and auto-tables:

```
── Row 0 ──
│ id: "abc-123"
│ source:
│ │ title: ["My Page Title"]
│ │ keywords: []
│ │ rank: 4
│ axes: (3 items)
│ │   axis       │ keywords
│ │   ───────────┼─────────────────
│ │   "price"    │ ["cheap", "sale"]
│ │   "brand"    │ ["acme"]
│ │   "category" │ ["tools", "diy"]
```

## Installation

Requires Rust 1.85+.

```bash
# From source
git clone <repo>
cd dsless
cargo install --path .

# Or just build
cargo build --release
# Binary at target/release/dsless
```

## Usage

```bash
dsless <path>              # file or directory of parquet files
dsless -n 50 <path>        # limit to 50 rows (pipe mode default: 1000)
dsless <path> | less        # pipe mode: plain text, no TUI
```

## Keybindings

### Scrolling

| Key | Action |
|---|---|
| `j` / `↓` | Scroll 1 line down |
| `k` / `↑` | Scroll 1 line up |
| `K` / PageDown | Scroll 1 page down |
| `J` / PageUp | Scroll 1 page up |
| Space / Ctrl-d | Half page down |
| Ctrl-u | Half page up |

### Record navigation

| Key | Action |
|---|---|
| `g` | Go to start of current record; if already there, previous record |
| `G` | Go to next record |
| `<N>g` | Go to record N |
| `<N>G` | Go to record N |
| `<N>%` | Go to record at N% of dataset |

### Search

| Key | Action |
|---|---|
| `/` | Enter search query |
| `n` | Next match (skips all on-screen matches) |
| `N` | Previous match |
| Esc | Clear search |

Search is two-level: first finds matching records (scanning parquet columns directly), then highlights matching lines within the current record.

Status bar shows: `/{query}: {N} records, {M} in record`

## Supported formats

- **Parquet** (`.parquet`) — including zstd/snappy/gzip compression, partitioned directories

Planned: JSONL, ORC, CSV.
