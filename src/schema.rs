//! Table schema.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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

/// A value of a column.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Value {
    Int(i32),
    Float(f64),
    Varchar(String),
}

/// A column in a table.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub typ: Type,
    pub nullable: bool,
    pub default: Option<Value>,
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
