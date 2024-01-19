//! Error definitions.

use std::io::Error as IOError;
use std::num::{ParseFloatError, ParseIntError};
use std::result;
use std::sync::{MutexGuard, PoisonError};

use chrono::format::ParseError as ChronoParseError;
use csv::Error as CsvError;
use pest::error::Error as PestError;
use regex::Error as RegexError;
use rustyline::error::ReadlineError;
use serde_json::error::Error as SerdeError;
use thiserror::Error;

use crate::file::PageCache;
use crate::parser::Rule;
use crate::schema::{Type, Value};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0} not implemented")]
    NotImplemented(&'static str),

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
    #[error("Column `{0}` not found")]
    ColumnNotFound(String),
    #[error("Constraint `{0}` not found")]
    ConstraintNotFound(String),
    #[error("Inexact column name `{0}`")]
    InexactColumn(String),
    #[error("Index `{0}` on table `{1}` not found")]
    IndexNotFound(String, String),

    #[error("Duplicate column name `{0}`")]
    DuplicateColumn(String),
    #[error("Duplicate index on columns {0:?}")]
    DuplicateIndex(Vec<String>),
    #[error("Multiple primary keys on table `{0}`")]
    MultiplePrimaryKeys(String),
    #[error("No primary key on table `{0}`")]
    NoPrimaryKey(String),

    #[error("Field count mismatch: {0} provided but {1} expected")]
    FieldCountMismatch(usize, usize),
    #[error("Value `{0}` does not match type `{1}`")]
    TypeMismatch(Value, Type),
    #[error("Field `{0}` must not be null")]
    NotNullable(String),

    #[error("Constraint failed: types of foreign keys mismatch")]
    ForeignKeyTypeMismatch,
    #[error("Constraint failed: referenced fields not primary key")]
    ForeignKeyNotPrimaryKey,
    #[error("Constraint failed: duplicate value for constraint `{0}`")]
    DuplicateValue(String),
    #[error("Constraint failed: columns referenced by foreign key must be primary key")]
    ReferencedColumnsNotPrimaryKey,
    #[error("Constraint failed: fields referenced by foreign key `{0}` not exist")]
    ReferencedFieldsNotExist(String),
    #[error("Constraint failed: cannot update or delete row due to foreign key `{0}`")]
    RowReferencedByForeignKey(String),
    #[error("Constraint failed: cannot drop table due to foreign key `{0}`")]
    TableReferencedByForeignKey(String),

    #[error("There should be exactly one join condition")]
    JoinConditionCount,
    #[error("Only equal join is supported")]
    JoinOperation,
    #[error("Aggregation query mixed with non-aggregation query")]
    MixedAggregate,

    #[error("Date parse error: {0}")]
    ChronoParse(#[from] ChronoParseError),
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
    #[error("Regex error: {0}")]
    Regex(#[from] RegexError),
    #[error("Serialization error: {0}")]
    Serde(#[from] SerdeError),
    #[error("Syntax error:\n{0}")]
    Syntax(#[from] Box<PestError<Rule>>),
}

pub type Result<T> = result::Result<T, Error>;
