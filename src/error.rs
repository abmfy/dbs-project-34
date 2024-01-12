//! Error definitions.

use std::io::Error as IOError;
use std::result;

use csv::Error as CsvError;
use pest::error::Error as PestError;
use rustyline::error::ReadlineError;
use thiserror::Error;

use crate::parser::Rule;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database `{0}` already exists")]
    DatabaseExists(String),
    #[error("Database `{0}` not found")]
    DatabaseNotFound(String),

    #[error("CSV error: {0}")]
    Csv(#[from] CsvError),
    #[error("IO error: {0}")]
    IO(#[from] IOError),
    #[error("Readline error: {0}")]
    Readline(#[from] ReadlineError),
    #[error("Syntax error:\n{0}")]
    Syntax(#[from] Box<PestError<Rule>>),
}

pub type Result<T> = result::Result<T, Error>;
