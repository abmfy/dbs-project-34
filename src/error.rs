//! Error definitions.

use std::result;

use rustyline::error::ReadlineError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Readline error: {0}")]
    ReadlineError(ReadlineError),
}

impl From<ReadlineError> for Error {
    fn from(err: ReadlineError) -> Self {
        Self::ReadlineError(err)
    }
}

pub type Result<T> = result::Result<T, Error>;
