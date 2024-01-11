//! Configuration constants and command line arguments.

use clap::Parser;

pub const PAGE_SIZE: usize = 8192;
pub const CACHE_SIZE: usize = 16384;

/// Command line arguments.
#[derive(Parser, Debug)]
#[clap(
    author = "abmfy",
    about = "YourSQL, a stupid relational database management system."
)]
pub struct Config {
    /// Batch mode.
    #[clap(short, long)]
    pub batch: bool,

    /// Specify database.
    #[clap(short, long)]
    pub database: Option<String>,
}
