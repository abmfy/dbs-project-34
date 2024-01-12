//! Error definitions.

use std::io::Error as IOError;
use std::result;

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

    #[error("IO error: {0}")]
    IoError(#[from] IOError),
    #[error("Readline error: {0}")]
    ReadlineError(#[from] ReadlineError),
    #[error("Syntax error: {0}")]
    SyntaxError(#[from] PestError<Rule>),
}

pub type Result<T> = result::Result<T, Error>;
