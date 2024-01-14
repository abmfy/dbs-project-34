//! Error definitions.

use std::io::Error as IOError;
use std::num::{ParseFloatError, ParseIntError};
use std::result;
use std::sync::{MutexGuard, PoisonError};

use csv::Error as CsvError;
use pest::error::Error as PestError;
use rustyline::error::ReadlineError;
use serde_json::error::Error as SerdeError;
use thiserror::Error;

use crate::file::PageCache;
use crate::parser::Rule;
use crate::schema::{Type, Value};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database `{0}` already exists")]
    DatabaseExists(String),
    #[error("Database `{0}` not found")]
    DatabaseNotFound(String),
    #[error("No database selected")]
    NoDatabaseSelected,

    #[error("Table `{0}` already exists")]
    TableExists(String),
    #[error("Table `{0}` not found")]
    TableNotFound(String),

    #[error("Value `{0}` does not match type `{1}`")]
    TypeMismatch(Value, Type),

    #[error("CSV error: {0}")]
    Csv(#[from] CsvError),
    #[error("IO error: {0}")]
    IO(#[from] IOError),
    #[error("Float parse error: {0}")]
    ParseFloat(#[from] ParseFloatError),
    #[error("Int parse error: {0}")]
    ParseInt(#[from] ParseIntError),
    #[error("Poison error: {0}")]
    Poison(#[from] PoisonError<MutexGuard<'static, PageCache>>),
    #[error("Readline error: {0}")]
    Readline(#[from] ReadlineError),
    #[error("Serialization error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Syntax error:\n{0}")]
    Syntax(#[from] Box<PestError<Rule>>),
}

pub type Result<T> = result::Result<T, Error>;
