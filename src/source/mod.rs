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
