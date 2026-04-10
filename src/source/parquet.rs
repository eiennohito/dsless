use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use lru::LruCache;

use super::DataSource;

const ROW_GROUP_CACHE_SIZE: usize = 3;

struct RowGroupMeta {
    num_rows: usize,
    global_offset: usize,
}

struct FileEntry {
    path: PathBuf,
    row_groups: Vec<RowGroupMeta>,
}

pub struct ParquetSource {
    files: Vec<FileEntry>,
    total_rows: usize,
    schema: Arc<Schema>,
    rg_cache: LruCache<(usize, usize), RecordBatch>,
}

impl ParquetSource {
    pub fn open(path: &Path) -> Result<Self> {
        let file_paths = collect_parquet_files(path)?;
        if file_paths.is_empty() {
            anyhow::bail!("No parquet files found at {:?}", path);
        }

        let mut files = Vec::new();
        let mut total_rows = 0usize;
        let mut schema: Option<Arc<Schema>> = None;

        for file_path in &file_paths {
            let file = std::fs::File::open(file_path)
                .with_context(|| format!("Failed to open {:?}", file_path))?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .with_context(|| {
                        format!("Failed to read parquet metadata from {:?}", file_path)
                    })?;

            if schema.is_none() {
                schema = Some(builder.schema().clone());
            }

            let metadata = builder.metadata();
            let mut row_groups = Vec::new();
            for rg_idx in 0..metadata.num_row_groups() {
                let rg_meta = metadata.row_group(rg_idx);
                let num_rows = rg_meta.num_rows() as usize;
                row_groups.push(RowGroupMeta {
                    num_rows,
                    global_offset: total_rows,
                });
                total_rows += num_rows;
            }

            files.push(FileEntry {
                path: file_path.clone(),
                row_groups,
            });
        }

        Ok(ParquetSource {
            files,
            total_rows,
            schema: schema.unwrap(),
            rg_cache: LruCache::new(NonZeroUsize::new(ROW_GROUP_CACHE_SIZE).unwrap()),
        })
    }

    fn locate_row(&self, global_row: usize) -> (usize, usize, usize) {
        for (fi, file) in self.files.iter().enumerate() {
            for (ri, rg) in file.row_groups.iter().enumerate() {
                if global_row >= rg.global_offset
                    && global_row < rg.global_offset + rg.num_rows
                {
                    return (fi, ri, global_row - rg.global_offset);
                }
            }
        }
        panic!(
            "Row {} out of range (total: {})",
            global_row, self.total_rows
        );
    }
}

impl DataSource for ParquetSource {
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
        let (file_idx, rg_idx, _) = self.locate_row(global_row);
        let key = (file_idx, rg_idx);

        if self.rg_cache.contains(&key) {
            return Ok(());
        }

        let entry = &self.files[file_idx];
        let file = std::fs::File::open(&entry.path)
            .with_context(|| format!("Failed to open {:?}", entry.path))?;
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?
                .with_row_groups(vec![rg_idx])
                .build()?;

        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<_, _>>()
            .context("Failed to read row group")?;
        let batch = concat_batches(&self.schema, &batches)?;

        self.rg_cache.put(key, batch);
        Ok(())
    }

    fn get_row(&mut self, global_row: usize) -> (&RecordBatch, usize) {
        let (file_idx, rg_idx, local_row) = self.locate_row(global_row);
        let key = (file_idx, rg_idx);
        (self.rg_cache.get(&key).unwrap(), local_row)
    }
}

fn collect_parquet_files(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if path.is_dir() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(path)
            .with_context(|| format!("Failed to read directory {:?}", path))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "parquet"))
            .collect();
        files.sort();
        return Ok(files);
    }
    anyhow::bail!("{:?} is not a file or directory", path);
}
