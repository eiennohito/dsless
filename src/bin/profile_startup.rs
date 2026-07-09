use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use dsless::layout;
use dsless::render;
use dsless::source;
#[derive(Parser)]
struct Args {
    path: PathBuf,
    #[arg(short, default_value_t = 10)]
    iterations: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let _ = source::open(&args.path)?;

    let mut t_open = Vec::new();
    let mut t_layout = Vec::new();
    let mut t_resolve = Vec::new();
    let mut t_render = Vec::new();
    let mut t_total = Vec::new();

    let mut t_first_ensure = Duration::ZERO;
    let mut t_compute_cached = Duration::ZERO;

    // Test: read with limit vs full row group
    let mut t_limited_read = Duration::ZERO;
    let mut limited_rows = 0usize;
    let mut t_full_read = Duration::ZERO;
    let mut full_rows = 0usize;

    for iter in 0..args.iterations {
        let t0 = Instant::now();

        let source = source::open(&args.path)?;
        t_open.push(t0.elapsed());

        let mut source = source;

        let t1 = Instant::now();
        if iter == args.iterations - 1 {
            // Test: read just 200 rows with limit
            let file = std::fs::File::open(&args.path)?;
            let tl = Instant::now();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let schema: SchemaRef = builder.schema().clone();
            let reader = builder
                .with_row_groups(vec![0])
                .with_limit(200)
                .build()?;
            let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
            let batch = concat_batches(&schema, &batches)?;
            limited_rows = batch.num_rows();
            t_limited_read = tl.elapsed();

            // Test: read full row group (current behavior)
            let file = std::fs::File::open(&args.path)?;
            let tf = Instant::now();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let schema: SchemaRef = builder.schema().clone();
            let reader = builder
                .with_row_groups(vec![0])
                .build()?;
            let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
            let batch = concat_batches(&schema, &batches)?;
            full_rows = batch.num_rows();
            t_full_read = tf.elapsed();

            // Standard ensure_loaded (full RG)
            let te = Instant::now();
            let _ = source.ensure_loaded(0);
            t_first_ensure = te.elapsed();

            let tc = Instant::now();
            let _lo = layout::Layout::compute(source.as_mut());
            t_compute_cached = tc.elapsed();
        }
        let lo = layout::Layout::compute(source.as_mut());
        t_layout.push(t1.elapsed());

        let term_width = 120;
        let t2 = Instant::now();
        let spec = layout::RenderSpec::resolve(&lo, term_width);
        t_resolve.push(t2.elapsed());

        let t3 = Instant::now();
        let mut writer = render::LineWriter::new();
        let rows = 50.min(source.total_rows());
        for row in 0..rows {
            source.ensure_loaded(row)?;
            let (batch, local_row) = source.get_row(row);
            let _ = render::render_record(&spec, batch, local_row, row, &mut writer);
        }
        t_render.push(t3.elapsed());

        t_total.push(t0.elapsed());
    }

    let n = args.iterations;
    let median = |v: &mut Vec<Duration>| {
        v.sort();
        v[v.len() / 2]
    };

    eprintln!("=== dsless startup profile ({n} iterations, median) ===");
    eprintln!("  source::open          {:>10.2?}", median(&mut t_open));
    eprintln!("  Layout::compute       {:>10.2?}", median(&mut t_layout));
    eprintln!("    1st ensure_loaded   {:>10.2?}  (full RG, current behavior)", t_first_ensure);
    eprintln!("    compute (cached)    {:>10.2?}  (feed + resolve)", t_compute_cached);
    eprintln!("  RenderSpec::resolve   {:>10.2?}", median(&mut t_resolve));
    eprintln!("  render 50 rows        {:>10.2?}", median(&mut t_render));
    eprintln!("  ────────────────────────────────");
    eprintln!("  total                 {:>10.2?}", median(&mut t_total));
    eprintln!();
    eprintln!("=== row group read comparison ===");
    eprintln!("  with_limit(200)       {:>10.2?}  ({limited_rows} rows read)", t_limited_read);
    eprintln!("  full row group        {:>10.2?}  ({full_rows} rows read)", t_full_read);
    eprintln!("  speedup               {:>10.1}x", t_full_read.as_secs_f64() / t_limited_read.as_secs_f64());

    Ok(())
}
