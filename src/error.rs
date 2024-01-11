//! Error definitions.

use std::result;
use std::string::FromUtf8Error;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    // #[error("IO error: {0}")]
    // Utf8Error(FromUtf8Error),
}

pub type Result<T> = result::Result<T, Error>;
