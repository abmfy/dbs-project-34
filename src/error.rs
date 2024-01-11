//! Error definitions.

use std::result;

use rustyline::error::ReadlineError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database {0} already exists")]
    DatabaseExists(String),
    #[error("Database {0} not found")]
    DatabaseNotFound(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Readline error: {0}")]
    ReadlineError(#[from] ReadlineError),
}

pub type Result<T> = result::Result<T, Error>;
