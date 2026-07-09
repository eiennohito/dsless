pub mod jsonl;
pub mod parquet;

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;

/// Common interface for tabular data sources.
/// All formats convert to Arrow RecordBatch for rendering.
pub trait DataSource: Send {
    /// Arrow schema of the data.
    fn schema(&self) -> &Arc<Schema>;

    /// Total number of rows across all files/chunks.
    fn total_rows(&self) -> usize;

    /// Number of underlying files (for display purposes).
    fn file_count(&self) -> usize;

    /// Ensure the chunk containing `global_row` is loaded into memory.
    fn ensure_loaded(&mut self, global_row: usize) -> Result<()>;

    /// Get the batch and local row index for a global row.
    /// Must call `ensure_loaded` first.
    fn get_row(&mut self, global_row: usize) -> (&RecordBatch, usize);

    fn load_row(&mut self, global_row: usize) -> Result<(&RecordBatch, usize)> {
        self.ensure_loaded(global_row)?;
        Ok(self.get_row(global_row))
    }
}

enum Format {
    Parquet,
    Jsonl,
}

// Disambiguates format for routing, not validation. 8 bytes suffices:
// Parquet files start with 4-byte magic "PAR1"; JSONL starts with '{' or '['.
fn sniff_format(path: &Path) -> Result<Option<Format>> {
    let mut file = std::fs::File::open(path)?;
    let mut buf = [0u8; 8];
    let n = file.read(&mut buf)?;
    if n >= 4 && buf[..4] == *b"PAR1" {
        return Ok(Some(Format::Parquet));
    }
    let start = if n >= 3 && buf[..3] == [0xEF, 0xBB, 0xBF] {
        3
    } else {
        0
    };
    for &b in &buf[start..n] {
        if b.is_ascii_whitespace() {
            continue;
        }
        if b == b'{' || b == b'[' {
            return Ok(Some(Format::Jsonl));
        }
        break;
    }
    Ok(None)
}

fn classify_path(path: &Path) -> Result<(Format, Vec<std::path::PathBuf>)> {
    if path.is_file() {
        let format = sniff_format(path)?.ok_or_else(|| {
            anyhow::anyhow!(
                "Cannot determine format of {:?}: content not recognized as Parquet or JSONL",
                path
            )
        })?;
        return Ok((format, vec![path.to_path_buf()]));
    }
    if !path.is_dir() {
        anyhow::bail!("{:?} is not a file or directory", path);
    }

    let mut parquet_files = Vec::new();
    let mut jsonl_files = Vec::new();

    for entry in std::fs::read_dir(path)?.filter_map(|e| e.ok()) {
        let entry_path = entry.path();
        if !entry_path.is_file() {
            continue;
        }
        // Skip metadata/hidden files (e.g. _metadata, _SUCCESS, _dataset.json, .DS_Store)
        if entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('_') || n.starts_with('.'))
        {
            continue;
        }
        match sniff_format(&entry_path) {
            Ok(Some(Format::Parquet)) => parquet_files.push(entry_path),
            Ok(Some(Format::Jsonl)) => jsonl_files.push(entry_path),
            _ => {}
        }
    }

    match (!parquet_files.is_empty(), !jsonl_files.is_empty()) {
        (true, false) => {
            parquet_files.sort();
            Ok((Format::Parquet, parquet_files))
        }
        (false, true) => {
            jsonl_files.sort();
            Ok((Format::Jsonl, jsonl_files))
        }
        (true, true) => anyhow::bail!(
            "Directory {:?} contains mixed formats (parquet and jsonl)",
            path
        ),
        _ => anyhow::bail!(
            "No supported files in {:?} (content not recognized as Parquet or JSONL)",
            path
        ),
    }
}

/// Sniff format from content and open the appropriate source.
pub fn open(path: &Path) -> Result<Box<dyn DataSource>> {
    let (format, files) = classify_path(path)?;
    match format {
        Format::Parquet => Ok(Box::new(parquet::ParquetSource::open_files(files)?)),
        Format::Jsonl => Ok(Box::new(jsonl::JsonlSource::open_files(files)?)),
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    pub struct FakeDataSource {
        schema: Arc<Schema>,
        batch: RecordBatch,
    }

    impl FakeDataSource {
        pub fn from_batch(batch: RecordBatch) -> Self {
            let schema = batch.schema();
            FakeDataSource { schema, batch }
        }

        pub fn single_string_column(name: &str, values: &[&str]) -> Self {
            let array = StringArray::from(values.to_vec());
            let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
            Self::from_batch(batch)
        }

        pub fn two_columns(values: &[(&str, i32)]) -> Self {
            let names: Vec<&str> = values.iter().map(|(s, _)| *s).collect();
            let nums: Vec<i32> = values.iter().map(|(_, n)| *n).collect();
            let str_array = StringArray::from(names);
            let int_array = Int32Array::from(nums);
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(str_array), Arc::new(int_array)])
                    .unwrap();
            Self::from_batch(batch)
        }
    }

    impl DataSource for FakeDataSource {
        fn schema(&self) -> &Arc<Schema> {
            &self.schema
        }

        fn total_rows(&self) -> usize {
            self.batch.num_rows()
        }

        fn file_count(&self) -> usize {
            1
        }

        fn ensure_loaded(&mut self, _global_row: usize) -> Result<()> {
            Ok(())
        }

        fn get_row(&mut self, global_row: usize) -> (&RecordBatch, usize) {
            (&self.batch, global_row)
        }
    }
}
