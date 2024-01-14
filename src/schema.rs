//! Table schema.

use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    fs::File,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::{
    config::{LINK_SIZE, PAGE_SIZE},
    record::Record,
};

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

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Varchar(a), Value::Varchar(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Value {
    /// Parse value from string.
    pub fn from(s: &str, typ: &Type) -> Result<Self> {
        match typ {
            Type::Int => Ok(Value::Int(s.parse()?)),
            Type::Float => Ok(Value::Float(s.parse()?)),
            Type::Varchar(_) => Ok(Value::Varchar(s.to_owned())),
        }
    }

    /// Check if the value matches the type.
    pub fn check_type(&self, typ: &Type) -> bool {
        matches!(
            (self, typ),
            (Value::Null, _)
                | (Value::Int(_), Type::Int)
                | (Value::Float(_), Type::Float)
                | (Value::Varchar(_), Type::Varchar(_))
        )
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v:.2}"),
            Value::Varchar(v) => write!(f, "{}", v.trim_end_matches('\0')),
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

/// A constraint on a table.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Constraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
}

impl Display for Constraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::PrimaryKey { name, columns } => {
                write!(f, "PRIMARY KEY ")?;
                if let Some(name) = name {
                    write!(f, "{}", name)?;
                }
                write!(f, "({})", columns.join(", "))?;
            }
            Constraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
            } => {
                write!(f, "FOREIGN KEY ")?;
                if let Some(name) = name {
                    write!(f, "{}", name)?;
                }
                write!(
                    f,
                    "({}) REFERENCES {}({})",
                    columns.join(", "),
                    ref_table,
                    ref_columns.join(", ")
                )?;
            }
        }
        write!(f, ";")
    }
}

/// A field represents a column or a constraint.
pub enum Field {
    Column(Column),
    Constraint(Constraint),
}

/// Query selectors in a select statement.
pub enum Selectors {
    All,
    Some(Vec<Selector>),
}

/// Column selector in the form table.column,
/// where table part is optional
pub struct ColumnSelector(pub Option<String>, pub String);

/// Query selector.
pub enum Selector {
    Column(ColumnSelector),
}

impl Display for Selector {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Selector::Column(ColumnSelector(table, column)) => {
                if let Some(table) = table {
                    write!(f, "{}.", table)?;
                }
                write!(f, "{}", column)?;
            }
        }
        Ok(())
    }
}

/// SQL operator.
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// SQL expression.
pub enum Expression {
    Value(Value),
    Column(ColumnSelector),
}

/// Where clause.
pub enum WhereClause {
    OperatorExpression(ColumnSelector, Operator, Expression),
}

impl WhereClause {
    /// Check if the where clause matches a record.
    pub fn matches(&self, record: &Record, schema: &TableSchema) -> bool {
        match self {
            WhereClause::OperatorExpression(ColumnSelector(_, column), op, expr) => {
                let column = schema.get_column(column);
                let expr = match expr {
                    Expression::Value(v) => v,
                    Expression::Column(ColumnSelector(_, column)) => {
                        let column = schema.get_column(column);
                        &record.fields[schema.column_map[&column.name]]
                    }
                };
                let value = &record.fields[schema.column_map[&column.name]];
                match op {
                    Operator::Eq => value == expr,
                    Operator::Ne => value != expr,
                    Operator::Lt => value < expr,
                    Operator::Le => value <= expr,
                    Operator::Gt => value > expr,
                    Operator::Ge => value >= expr,
                }
            }
        }
    }
}

/// A table schema. This type is for serialization.
#[derive(Deserialize, Serialize)]
pub struct Schema {
    /// Count of pages in this table.
    pub pages: usize,
    /// Page number of the first free page of the table.
    pub free: Option<usize>,
    /// Page number of the first full page of the table.
    pub full: Option<usize>,
    /// Columns of the table.
    pub columns: Vec<Column>,
    /// Constraints on the table.
    pub constraints: Vec<Constraint>,
}

/// A wrapped table schema.
pub struct TableSchema {
    /// The schema.
    schema: Schema,
    /// Path to the schema, for serialization.
    path: PathBuf,
    /// Columns of the table.
    columns: Vec<Column>,
    /// Constraints on the table.
    constraints: Vec<Constraint>,
    /// Offsets of columns in a record.
    offsets: Vec<usize>,
    /// The size of the null bitmap.
    null_bitmap_size: usize,
    /// The length of a record.
    record_size: usize,
    /// Maximum count of records available in a page.
    max_records: usize,
    /// Size of free slot bitmap in bytes.
    free_bitmap_size: usize,
    /// Mapping from column name to index.
    column_map: HashMap<String, usize>,
}

impl TableSchema {
    /// Initialize schema information.
    pub fn new(schema: Schema, path: &Path) -> Result<Self> {
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
        let null_bitmap_size = columns.len().div_ceil(8);
        let record_size = null_bitmap_size + columns.iter().map(|c| c.typ.size()).sum::<usize>();

        // Allocate page space to fit as many records as possible.
        let mut max_records = PAGE_SIZE / record_size;
        let mut free_bitmap_size = max_records.div_ceil(8);
        let mut total_size = max_records * record_size + free_bitmap_size + 2 * LINK_SIZE;
        while total_size > PAGE_SIZE {
            max_records -= 1;
            free_bitmap_size = max_records.div_ceil(8);
            total_size = max_records * record_size + free_bitmap_size + 2 * LINK_SIZE;
        }
        log::info!("Max records {max_records} with {free_bitmap_size} bytes free bitmap");

        let constraints = schema.constraints.clone();

        Ok(Self {
            schema,
            path: path.to_owned(),
            columns,
            constraints,
            offsets,
            null_bitmap_size,
            record_size,
            max_records,
            free_bitmap_size,
            column_map,
        })
    }

    /// Save changes into the schema file.
    fn save(&self) -> Result<()> {
        log::info!("Saving schema to {}", self.path.display());
        let file = File::create(&self.path)?;
        serde_json::to_writer(file, &self.schema)?;
        Ok(())
    }

    /// Get the size of the null bitmap.
    pub fn get_null_bitmap_size(&self) -> usize {
        self.null_bitmap_size
    }

    /// Get the length of a record.
    pub fn get_record_size(&self) -> usize {
        self.record_size
    }

    /// Return a reference to column information.
    pub fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    /// Return a reference to table constraints.
    pub fn get_constraints(&self) -> &[Constraint] {
        &self.constraints
    }

    /// Get a column by its name.
    pub fn get_column(&self, name: &str) -> &Column {
        &self.columns[self.column_map[name]]
    }

    /// Get the index of a column by its name.
    pub fn get_column_index(&self, name: &str) -> usize {
        self.column_map[name]
    }

    /// Get the maximum count of records available in a page.
    pub fn get_max_records(&self) -> usize {
        self.max_records
    }

    /// Get the size of free slot bitmap in bytes.
    pub fn get_free_bitmap_size(&self) -> usize {
        self.free_bitmap_size
    }

    /// Get the offset of a column in a record.
    pub fn get_offset(&self, name: &str) -> usize {
        self.offsets[self.column_map[name]]
    }

    /// Get count of pages in the table.
    pub fn get_pages(&self) -> usize {
        self.schema.pages
    }

    /// Get the first free page in the table.
    pub fn get_free(&self) -> Option<usize> {
        self.schema.free
    }

    /// Set the first free page in the table.
    pub fn set_free(&mut self, free: Option<usize>) {
        self.schema.free = free;
    }

    /// Get the first full page in the table.
    pub fn get_full(&self) -> Option<usize> {
        self.schema.full
    }

    /// Set the first full page in the table.
    pub fn set_full(&mut self, full: Option<usize>) {
        self.schema.full = full;
    }

    /// Allocate a new page for the table.
    pub fn new_page(&mut self) -> Result<usize> {
        let page = self.schema.pages;
        self.schema.pages += 1;
        Ok(page)
    }
}

impl Drop for TableSchema {
    fn drop(&mut self) {
        if let Err(err) = self.save() {
            log::error!("Failed to save schema: {err}");
        }
    }
}
