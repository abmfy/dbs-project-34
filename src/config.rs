//! Configuration constants and command line arguments.

use std::path::PathBuf;

use clap::Parser;

pub const PAGE_SIZE: usize = 8192;
pub const CACHE_SIZE: usize = 16384;
/// Size of a link in a linked list.
pub const LINK_SIZE: usize = 4;

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

    /// Initialize the database.
    #[clap(short, long)]
    pub init: bool,

    /// Specify path to data directory.
    #[clap(short, long, default_value = "data")]
    pub path: PathBuf,

    /// Specify table to load data into.
    #[clap(short, long, requires("database"))]
    pub table: Option<String>,

    /// Specify path to data file to load.
    #[clap(short, long, requires("table"))]
    pub file: Option<PathBuf>,
}
