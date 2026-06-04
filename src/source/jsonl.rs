use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::json::ReaderBuilder;
use lru::LruCache;

use super::DataSource;

const CHUNK_SIZE: usize = 8192;
const CHUNK_CACHE_SIZE: usize = 3;
const SCHEMA_SAMPLE_ROWS: usize = 200;

struct ChunkMeta {
    byte_offset: u64,
    num_rows: usize,
    global_offset: usize,
}

struct FileEntry {
    path: PathBuf,
    chunks: Vec<ChunkMeta>,
}

pub struct JsonlSource {
    files: Vec<FileEntry>,
    total_rows: usize,
    schema: Arc<Schema>,
    chunk_cache: LruCache<(usize, usize), RecordBatch>,
}

impl JsonlSource {
    pub fn open(path: &Path) -> Result<Self> {
        let file_paths = collect_jsonl_files(path)?;
        if file_paths.is_empty() {
            anyhow::bail!("No JSONL files found at {:?}", path);
        }

        let mut files = Vec::new();
        let mut total_rows = 0usize;
        let mut schema: Option<Arc<Schema>> = None;

        for file_path in &file_paths {
            let file = std::fs::File::open(file_path)
                .with_context(|| format!("Failed to open {:?}", file_path))?;
            let mut reader = BufReader::new(file);

            let mut schema_buf = Vec::new();
            let mut schema_lines_collected = 0;

            let mut chunks = Vec::new();
            let mut chunk_start_byte = 0u64;
            let mut byte_pos = 0u64;
            let mut lines_in_chunk = 0usize;
            let mut line = String::new();

            loop {
                line.clear();
                let bytes_read = reader
                    .read_line(&mut line)
                    .with_context(|| format!("Failed to read {:?}", file_path))?;
                if bytes_read == 0 {
                    break;
                }

                byte_pos += bytes_read as u64;

                if line.trim().is_empty() {
                    continue;
                }

                if schema.is_none() && schema_lines_collected < SCHEMA_SAMPLE_ROWS {
                    schema_buf.extend_from_slice(line.as_bytes());
                    if !line.ends_with('\n') {
                        schema_buf.push(b'\n');
                    }
                    schema_lines_collected += 1;
                }

                lines_in_chunk += 1;

                if lines_in_chunk == CHUNK_SIZE {
                    chunks.push(ChunkMeta {
                        byte_offset: chunk_start_byte,
                        num_rows: lines_in_chunk,
                        global_offset: total_rows,
                    });
                    total_rows += lines_in_chunk;
                    chunk_start_byte = byte_pos;
                    lines_in_chunk = 0;
                }
            }

            if lines_in_chunk > 0 {
                chunks.push(ChunkMeta {
                    byte_offset: chunk_start_byte,
                    num_rows: lines_in_chunk,
                    global_offset: total_rows,
                });
                total_rows += lines_in_chunk;
            }

            if schema.is_none() && !schema_buf.is_empty() {
                let cursor = std::io::Cursor::new(&schema_buf);
                let (inferred, _) =
                    arrow::json::reader::infer_json_schema(cursor, Some(SCHEMA_SAMPLE_ROWS))
                        .with_context(|| format!("Failed to infer schema from {:?}", file_path))?;
                schema = Some(Arc::new(inferred));
            }

            files.push(FileEntry {
                path: file_path.clone(),
                chunks,
            });
        }

        let schema = schema.ok_or_else(|| anyhow::anyhow!("No data found in JSONL files"))?;

        Ok(JsonlSource {
            files,
            total_rows,
            schema,
            chunk_cache: LruCache::new(NonZeroUsize::new(CHUNK_CACHE_SIZE).unwrap()),
        })
    }

    fn locate_row(&self, global_row: usize) -> (usize, usize, usize) {
        for (fi, file) in self.files.iter().enumerate() {
            for (ci, chunk) in file.chunks.iter().enumerate() {
                if global_row >= chunk.global_offset
                    && global_row < chunk.global_offset + chunk.num_rows
                {
                    return (fi, ci, global_row - chunk.global_offset);
                }
            }
        }
        panic!(
            "Row {} out of range (total: {})",
            global_row, self.total_rows
        );
    }
}

impl DataSource for JsonlSource {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn total_rows(&self) -> usize {
        self.total_rows
    }

    fn file_count(&self) -> usize {
        self.files.len()
    }

    fn ensure_loaded(&mut self, global_row: usize) -> Result<()> {
        let (file_idx, chunk_idx, _) = self.locate_row(global_row);
        let key = (file_idx, chunk_idx);

        if self.chunk_cache.contains(&key) {
            return Ok(());
        }

        let entry = &self.files[file_idx];
        let chunk = &entry.chunks[chunk_idx];

        let mut file = std::fs::File::open(&entry.path)
            .with_context(|| format!("Failed to open {:?}", entry.path))?;
        file.seek(SeekFrom::Start(chunk.byte_offset))?;
        let reader = BufReader::new(file);

        let mut json_reader = ReaderBuilder::new(self.schema.clone())
            .with_batch_size(chunk.num_rows)
            .build(reader)?;

        let batch = json_reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("Empty chunk at offset {}", chunk.byte_offset))??;

        self.chunk_cache.put(key, batch);
        Ok(())
    }

    fn get_row(&mut self, global_row: usize) -> (&RecordBatch, usize) {
        let (file_idx, chunk_idx, local_row) = self.locate_row(global_row);
        let key = (file_idx, chunk_idx);
        (self.chunk_cache.get(&key).unwrap(), local_row)
    }
}

fn collect_jsonl_files(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if path.is_dir() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(path)
            .with_context(|| format!("Failed to read directory {:?}", path))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .is_some_and(|ext| ext == "jsonl" || ext == "ndjson")
            })
            .collect();
        files.sort();
        return Ok(files);
    }
    anyhow::bail!("{:?} is not a file or directory", path);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::DataType;
    use std::io::Write;

    fn write_jsonl(dir: &Path, name: &str, lines: &[&str]) -> PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
        path
    }

    #[test]
    fn open_single_file_schema_and_row_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(
            dir.path(),
            "data.jsonl",
            &[
                r#"{"name":"alice","age":30}"#,
                r#"{"name":"bob","age":25}"#,
                r#"{"name":"charlie","age":35}"#,
            ],
        );

        let source = JsonlSource::open(&path).unwrap();
        assert_eq!(source.total_rows(), 3);
        assert_eq!(source.file_count(), 1);

        let schema = source.schema();
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("age").is_ok());
    }

    #[test]
    fn reads_correct_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(
            dir.path(),
            "data.jsonl",
            &[r#"{"x":"hello","y":42}"#, r#"{"x":"world","y":99}"#],
        );

        let mut source = JsonlSource::open(&path).unwrap();
        source.ensure_loaded(0).unwrap();
        let (batch, local) = source.get_row(0);
        let x_col = batch.column_by_name("x").unwrap();
        let val = x_col.as_string::<i32>().value(local);
        assert_eq!(val, "hello");

        source.ensure_loaded(1).unwrap();
        let (batch, local) = source.get_row(1);
        let y_col = batch.column_by_name("y").unwrap();
        let val = y_col
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(local);
        assert_eq!(val, 99);
    }

    #[test]
    fn open_directory_combines_files() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(dir.path(), "a.jsonl", &[r#"{"v":1}"#, r#"{"v":2}"#]);
        write_jsonl(dir.path(), "b.jsonl", &[r#"{"v":3}"#]);

        let source = JsonlSource::open(dir.path()).unwrap();
        assert_eq!(source.total_rows(), 3);
        assert_eq!(source.file_count(), 2);
    }

    #[test]
    fn directory_reads_across_file_boundary() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(dir.path(), "a.jsonl", &[r#"{"v":10}"#]);
        write_jsonl(dir.path(), "b.jsonl", &[r#"{"v":20}"#]);

        let mut source = JsonlSource::open(dir.path()).unwrap();

        source.ensure_loaded(0).unwrap();
        let (batch, local) = source.get_row(0);
        let val = batch
            .column_by_name("v")
            .unwrap()
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(local);
        assert_eq!(val, 10);

        source.ensure_loaded(1).unwrap();
        let (batch, local) = source.get_row(1);
        let val = batch
            .column_by_name("v")
            .unwrap()
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(local);
        assert_eq!(val, 20);
    }

    #[test]
    fn skips_blank_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(
            dir.path(),
            "data.jsonl",
            &[r#"{"a":1}"#, "", "   ", r#"{"a":2}"#],
        );

        let source = JsonlSource::open(&path).unwrap();
        assert_eq!(source.total_rows(), 2);
    }

    #[test]
    fn nested_json_infers_struct_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(
            dir.path(),
            "data.jsonl",
            &[
                r#"{"id":1,"info":{"name":"alice","tags":["a","b"]}}"#,
                r#"{"id":2,"info":{"name":"bob","tags":["c"]}}"#,
            ],
        );

        let source = JsonlSource::open(&path).unwrap();
        let info_field = source.schema().field_with_name("info").unwrap();
        assert!(matches!(info_field.data_type(), DataType::Struct(_)));
    }

    #[test]
    fn ndjson_extension_works() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), "data.ndjson", &[r#"{"k":"v"}"#]);

        let source = JsonlSource::open(&path).unwrap();
        assert_eq!(source.total_rows(), 1);
    }

    #[test]
    fn empty_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(dir.path(), "empty.jsonl", &[]);

        let result = JsonlSource::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn empty_directory_errors() {
        let dir = tempfile::tempdir().unwrap();
        let result = JsonlSource::open(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn nullable_fields_handled() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(
            dir.path(),
            "data.jsonl",
            &[r#"{"a":1,"b":"yes"}"#, r#"{"a":2}"#],
        );

        let mut source = JsonlSource::open(&path).unwrap();
        assert_eq!(source.total_rows(), 2);

        source.ensure_loaded(1).unwrap();
        let (batch, local) = source.get_row(1);
        let b_col = batch.column_by_name("b").unwrap();
        assert!(b_col.is_null(local));
    }
}
