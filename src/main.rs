mod cache;
mod layout;
mod render;
mod source;
mod tui;
mod unicode;
mod worker;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use source::DataSource;

#[derive(Parser)]
#[command(name = "dsless", about = "A pager for data-science formats")]
struct Cli {
    /// Path to a parquet file or directory of parquet files
    path: PathBuf,

    /// Maximum number of rows (default: unlimited in TUI, 1000 in pipe mode)
    #[arg(short = 'n', long)]
    max_rows: Option<usize>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let source = source::open(&cli.path)?;

    if std::io::IsTerminal::is_terminal(&std::io::stdout()) {
        tui::run_tui(source)
    } else {
        let max_rows = cli.max_rows.unwrap_or(1000);
        run_pipe(source, max_rows)
    }
}

fn run_pipe(mut source: Box<dyn DataSource>, max_rows: usize) -> Result<()> {
    let schema = source.schema().clone();
    let term_width = crossterm::terminal::size().map(|(w, _)| w as usize).unwrap_or(120);

    let lo = layout::Layout::compute(source.as_mut());
    let spec = layout::RenderSpec::resolve(&lo, term_width);
    let is_table = spec.is_table();

    if is_table {
        for line in &spec.render_table_header() {
            println!("{}", line);
        }
    } else {
        println!("Schema: {} columns", schema.fields().len());
        for field in schema.fields() {
            println!("  {} : {}", field.name(), field.data_type());
        }
        println!();
        if source.file_count() > 1 {
            println!("Files: {} files", source.file_count());
            println!();
        }
    }

    use std::fmt::Write;
    let mut writer = render::LineWriter::new();
    let total = source.total_rows().min(max_rows);
    for global_row in 0..total {
        writer.clear();
        if !is_table {
            let _ = write!(writer, "── Row {} ──", global_row);
            writer.newline();
        }
        source.ensure_loaded(global_row)?;
        let (batch, local_row) = source.get_row(global_row);
        spec.render_row(batch, local_row, &mut writer);
        let rendered = writer.finish();
        for line in rendered.lines() {
            println!("{}", line);
        }
    }

    if total >= max_rows && max_rows < source.total_rows() {
        println!(
            "... (stopped at {} rows, use -n to load more)",
            max_rows
        );
    } else {
        println!("Total: {} rows", total);
    }
    Ok(())
}
