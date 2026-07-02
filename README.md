# dsless

A terminal pager for parquet and JSONL. Think `less`, but it understands nested schemas.

```bash
dsless data.parquet
```

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

Nested structs render as indented trees. Arrays of structs become tables. Long strings and deep nesting stay readable.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/eiennohito/dsless/main/install.sh | sh
```

macOS (universal), Linux x86_64, Linux aarch64. Installs to `~/.local/bin`.

Or from source (Rust 1.85+): `cargo install --path .`

## Usage

```bash
dsless file.parquet            # TUI mode
dsless parquet-dir/            # reads all files in directory
dsless file.jsonl              # JSONL/NDJSON
dsless file.parquet | head     # pipe mode — plain text, no TUI
dsless -n 50 file.parquet      # limit rows (pipe default: 1000)
```

Format is detected from file content, not extension.

## Keys

Vim-style. `j`/`k` scroll, `J`/`K` page, `g`/`G` jump between records, `q` quits.

**Cursor**: `Ctrl-j`/`Ctrl-k` move between records, `h`/`l` select columns.

**Search**: `/` to search, `n`/`N` for next/prev match. Searches across parquet columns directly, then highlights matching lines.

**Preview**: `v` shows an overlay of truncated fields — type the label to expand. `V` repeats last preview on a new record. `Space` previews the field under the cursor. `j`/`k` scroll inside a preview, `v`/`Space`/`Esc` dismiss.

`?` for the full keymap inside the TUI.

## AI-assisted

Most code is AI-generated. If that bothers you, now you know.
