use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "d2d")]
#[command(about = "D2D - High-performance database migration tool")]
#[command(version = env!("CARGO_PKG_VERSION"))]
pub struct Cli {
    /// Job configuration file (YAML)
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,

    /// Log level
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    /// Dry run (validate config without executing)
    #[arg(long)]
    pub dry_run: bool,
}
