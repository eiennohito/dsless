use arrow::array::*;
use arrow::datatypes::{Field, Schema};
use std::sync::Arc;

use crate::source::DataSource;

pub fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

pub struct MockSource {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

impl MockSource {
    pub fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl DataSource for MockSource {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
    fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
    fn file_count(&self) -> usize {
        1
    }
    fn ensure_loaded(&mut self, _global_row: usize) -> anyhow::Result<()> {
        Ok(())
    }
    fn get_row(&mut self, global_row: usize) -> (&RecordBatch, usize) {
        let mut offset = 0;
        for batch in &self.batches {
            if global_row < offset + batch.num_rows() {
                return (batch, global_row - offset);
            }
            offset += batch.num_rows();
        }
        panic!("row {} out of bounds", global_row);
    }
}
