pub mod parquet;

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
}

/// Detect format from path and open the appropriate source.
pub fn open(path: &Path) -> Result<Box<dyn DataSource>> {
    // For now, only parquet is supported.
    // Future: check extension / magic bytes for jsonl, orc, csv, etc.
    let source = parquet::ParquetSource::open(path)?;
    Ok(Box::new(source))
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
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(str_array), Arc::new(int_array)],
            ).unwrap();
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
