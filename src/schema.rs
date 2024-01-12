//! Table schema.

use std::{collections::HashMap, fmt::{Display, Formatter, self}};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// A type of a column.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Type {
    Int,
    Float,
    Varchar(usize),
}

impl Type {
    /// Get the size of a value.
    pub fn size(&self) -> usize {
        match self {
            Type::Int => 4,
            Type::Float => 8,
            Type::Varchar(len) => *len,
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Type::Int => write!(f, "INT"),
            Type::Float => write!(f, "FLOAT"),
            Type::Varchar(len) => write!(f, "VARCHAR({})", len),
        }
    }
}

/// A value of a column.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Value {
    Null,
    Int(i32),
    Float(f64),
    Varchar(String),
}

impl Value {
    /// Check if the value matches the type.
    pub fn check_type(&self, typ: &Type) -> bool {
        match (self, typ) {
            (Value::Null, _) => true,
            (Value::Int(_), Type::Int) => true,
            (Value::Float(_), Type::Float) => true,
            (Value::Varchar(_), Type::Varchar(_)) => true,
            _ => false,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Varchar(v) => write!(f, "{v}"),
        }
    }
}

/// A column in a table.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub typ: Type,
    pub nullable: bool,
    pub default: Option<Value>,
}

impl Column {
    pub fn new(name: String, typ: Type, nullable: bool, default: Option<Value>) -> Result<Self> {
        if let Some(value) = &default {
            if !value.check_type(&typ) {
                return Err(Error::TypeMismatch(value.clone(), typ));
            }
        }

        Ok(Self {
            name,
            typ,
            nullable,
            default,
        })
    }
}

/// A table schema. This type is for serialization.
#[derive(Deserialize, Serialize)]
pub struct Schema {
    pub columns: Vec<Column>,
}

/// A wrapped table schema.
pub struct TableSchema {
    schema: Schema,
    columns: Vec<Column>,
    offsets: Vec<usize>,
    /// Mapping from column name to index.
    column_map: HashMap<String, usize>,
}

impl TableSchema {
    /// Initialize schema information.
    pub fn new(schema: Schema) -> Self {
        let columns = schema.columns.clone();
        let offsets = columns
            .iter()
            .scan(0, |offset, c| {
                let ret = Some(*offset);
                *offset += c.typ.size();
                ret
            })
            .collect();
        let column_map: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self {
            schema,
            columns,
            offsets,
            column_map,
        }
    }

    /// Get the size of the null bitmap.
    pub fn null_bitmap_size(&self) -> usize {
        self.columns.len().div_ceil(8)
    }

    /// Get the length of a record.
    pub fn record_size(&self) -> usize {
        self.null_bitmap_size() + self.columns.iter().map(|c| c.typ.size()).sum::<usize>()
    }

    /// Return a reference to column information.
    pub fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    /// Get a column by its name.
    pub fn get_column(&self, name: &str) -> &Column {
        &self.columns[self.column_map[name]]
    }

    /// Get the offset of a column in a record.
    pub fn get_offset(&self, name: &str) -> usize {
        self.offsets[self.column_map[name]]
    }
}
