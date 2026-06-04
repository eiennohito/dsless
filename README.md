# dsless

A terminal pager for data-science file formats. Think `less`, but for parquet files with deeply nested schemas.

## TL;DR

```bash
# Install binary
curl -fsSL https://raw.githubusercontent.com/eiennohito/dsless/main/install.sh | sh

dsless data.parquet          # TUI mode
dsless parquet-dir/           # reads all .parquet files in directory
dsless data.jsonl             # JSONL/NDJSON files
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

### Binary (recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/eiennohito/dsless/main/install.sh | sh
```

Installs to `~/.local/bin` by default. Override with `DSLESS_INSTALL_DIR`:

```bash
DSLESS_INSTALL_DIR=/usr/local/bin curl -fsSL https://raw.githubusercontent.com/eiennohito/dsless/main/install.sh | sh
```

Supports macOS (universal), Linux x86_64, and Linux aarch64.

### From source

Requires Rust 1.85+.

```bash
git clone https://github.com/eiennohito/dsless.git
cd dsless
cargo install --path .
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
- **JSONL/NDJSON** (`.jsonl`, `.ndjson`) — newline-delimited JSON

Planned: ORC, CSV.
