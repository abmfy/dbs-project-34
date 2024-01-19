//! Table schema.

use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    fs::File,
    hash::{Hash, Hasher},
    ops::{Add, Div},
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};

use crate::config::{LINK_SIZE, PAGE_SIZE};
use crate::error::{Error, Result};
use crate::index::IndexSchema;
use crate::record::Record;
use crate::record::RecordSchema;

/// A type of a column.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Type {
    Int,
    Float,
    Varchar(usize),
    Date,
}

impl Type {
    /// Get the size of a value.
    pub fn size(&self) -> usize {
        match self {
            Type::Int => 4,
            Type::Float => 8,
            Type::Varchar(len) => *len,
            Type::Date => 10,
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Type::Int => write!(f, "INT"),
            Type::Float => write!(f, "FLOAT"),
            Type::Varchar(len) => write!(f, "VARCHAR({})", len),
            Type::Date => write!(f, "DATE"),
        }
    }
}

/// A value of a column.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Value {
    Null,
    Int(i32),
    Float(f64),
    Varchar(String),
    Date(NaiveDate),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Varchar(a), Value::Varchar(b)) => {
                a.trim_end_matches('\0') == b.trim_end_matches('\0')
            }
            (Value::Date(a), Value::Date(b)) => a == b,
            // Weak type: string ang date
            (Value::Varchar(a), Value::Date(b)) => a.trim_end_matches('\0') == b.to_string(),
            (Value::Date(a), Value::Varchar(b)) => a.to_string() == b.trim_end_matches('\0'),
            _ => false,
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0.hash(state),
            Value::Int(v) => v.hash(state),
            Value::Float(v) => v.to_bits().hash(state),
            Value::Varchar(v) => v.trim_end_matches('\0').hash(state),
            Value::Date(v) => v.hash(state),
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Varchar(a), Value::Varchar(b)) => a
                .trim_end_matches('\0')
                .partial_cmp(b.trim_end_matches('\0')),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            // Weak type: string ang date
            (Value::Varchar(a), Value::Date(b)) => {
                a.trim_end_matches('\0').partial_cmp(&b.to_string())
            }
            (Value::Date(a), Value::Varchar(b)) => a
                .to_string()
                .as_str()
                .partial_cmp(b.trim_end_matches('\0')),
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
            Type::Date => Ok(Value::Date(s.parse()?)),
        }
    }

    /// Check if the value matches the type.
    pub fn check_type(&self, typ: &Type) -> bool {
        matches!(
            (self, typ),
            (Value::Null, _)
                | (Value::Int(_), Type::Int)
                | (Value::Float(_), Type::Float)
                | (Value::Date(_), Type::Date)
        ) || matches!(
            (self, typ), (Value::Varchar(a), Type::Varchar(len)) if a.len() <= *len
        ) || matches!(
            (self, typ), (Value::Varchar(a) , Type::Date) if a.parse::<NaiveDate>().is_ok()
        )
    }

    /// Compare with other value, and return the smaller one.
    pub fn min<'a>(&'a self, other: &'a Self) -> &'a Self {
        match (self, other) {
            (Value::Null, _) => other,
            (_, Value::Null) => self,
            (Value::Int(a), Value::Int(b)) => {
                if a < b {
                    self
                } else {
                    other
                }
            }
            (Value::Float(a), Value::Float(b)) => {
                if a < b {
                    self
                } else {
                    other
                }
            }
            (Value::Varchar(a), Value::Varchar(b)) => {
                if a < b {
                    self
                } else {
                    other
                }
            }
            _ => self,
        }
    }

    /// Compare with other value, and return the larger one.
    pub fn max<'a>(&'a self, other: &'a Self) -> &'a Self {
        match (self, other) {
            (Value::Null, _) => other,
            (_, Value::Null) => self,
            (Value::Int(a), Value::Int(b)) => {
                if a > b {
                    self
                } else {
                    other
                }
            }
            (Value::Float(a), Value::Float(b)) => {
                if a > b {
                    self
                } else {
                    other
                }
            }
            (Value::Varchar(a), Value::Varchar(b)) => {
                if a > b {
                    self
                } else {
                    other
                }
            }
            _ => self,
        }
    }
}

impl Add for Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Varchar(a), Value::Varchar(b)) => Value::Varchar(a + &b),
            _ => Value::Null,
        }
    }
}

impl Div<usize> for Value {
    type Output = Value;

    fn div(self, rhs: usize) -> Self::Output {
        match self {
            Value::Int(v) => Value::Float(v as f64 / rhs as f64),
            Value::Float(v) => Value::Float(v / rhs as f64),
            _ => Value::Null,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v:.2}"),
            Value::Varchar(v) => write!(f, "{}", v.trim_end_matches('\0')),
            Value::Date(v) => write!(f, "{}", v),
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

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Column {}

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
        referrer: String,
        ref_table: String,
        ref_columns: Vec<String>,
    },
}

impl Constraint {
    /// Check the constraint against some table schemas.
    ///
    /// # Panics
    ///
    /// Panics if the number of schemas are incorrect.
    pub fn check(&self, schemas: &[&Schema]) -> Result<()> {
        match self {
            Self::PrimaryKey { columns, .. } => {
                let schema = schemas[0];
                for column in columns {
                    if !schema.has_column(column) {
                        return Err(Error::ColumnNotFound(column.clone()));
                    }
                }
            }
            Self::ForeignKey {
                columns,
                ref_columns,
                ..
            } => {
                if columns.len() != ref_columns.len() {
                    return Err(Error::FieldCountMismatch(columns.len(), ref_columns.len()));
                }

                let schema0 = schemas[0];
                let schema1 = schemas[1];

                for (column0, column1) in columns.iter().zip(ref_columns) {
                    if !schema0.has_column(column0) {
                        return Err(Error::ColumnNotFound(column0.clone()));
                    }
                    if !schema1.has_column(column1) {
                        return Err(Error::ColumnNotFound(column1.clone()));
                    }
                    let column0 = schema0.get_column(column0);
                    let column1 = schema1.get_column(column1);
                    if column0.typ != column1.typ {
                        return Err(Error::ForeignKeyTypeMismatch);
                    }
                }

                // Requires the referenced keys be primary key
                let pk = schema1
                    .constraints
                    .iter()
                    .find(|c| matches!(c, Constraint::PrimaryKey { .. }))
                    .ok_or(Error::ForeignKeyNotPrimaryKey)?;

                let pk_columns = pk.get_columns();

                if ref_columns.len() != pk_columns.len() {
                    return Err(Error::ForeignKeyNotPrimaryKey);
                }

                for column in ref_columns {
                    if !pk_columns.contains(column) {
                        return Err(Error::ForeignKeyNotPrimaryKey);
                    }
                }
            }
        }
        Ok(())
    }

    /// Get the name of this constraint.
    pub fn get_name(&self) -> Option<&str> {
        match self {
            Self::PrimaryKey { name, .. } => name.as_deref(),
            Self::ForeignKey { name, .. } => name.as_deref(),
        }
    }

    /// Get the columns of this constraint.
    pub fn get_columns(&self) -> &[String] {
        match self {
            Self::PrimaryKey { columns, .. } => columns,
            Self::ForeignKey { columns, .. } => columns,
        }
    }

    /// Get the display name of this constraint.
    pub fn get_display_name(&self) -> String {
        self.get_name().unwrap_or("<anonymous>").to_owned()
    }

    /// Get the referenced table name.
    ///
    /// # Panics
    ///
    /// Panics if the constraint is not a foreign key.
    pub fn get_ref_table(&self) -> &str {
        match self {
            Self::ForeignKey { ref_table, .. } => ref_table,
            _ => panic!("Constraint is not a foreign key"),
        }
    }

    /// Get the index name of this constraint.
    ///
    /// # Parameters
    ///
    /// - `referrer`: whether the index is on the referrer side.
    pub fn get_index_name(&self, referrer: bool) -> String {
        match self {
            Self::PrimaryKey { name, columns } => {
                String::from("pk.")
                    + &format!(
                        "{}.implicit",
                        if let Some(name) = name {
                            name.to_owned()
                        } else {
                            format!("annoy.{}", columns.join("_"))
                        }
                    )
            }
            Self::ForeignKey {
                name,
                columns,
                referrer: referrer_table,
                ref_columns,
                ..
            } => {
                let referrer_name = &format!("fk_referred.{referrer_table}");
                String::from(if referrer {
                    "fk_referrer"
                } else {
                    referrer_name
                }) + &format!(
                    ".{}.implicit",
                    if let Some(name) = name {
                        name.to_owned()
                    } else {
                        format!(
                            "annoy.{}",
                            if referrer {
                                columns.join("_")
                            } else {
                                ref_columns.join("_")
                            }
                        )
                    }
                )
            }
        }
    }
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
                ..
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
#[derive(Clone, Debug)]
pub enum Selectors {
    All,
    Some(Vec<Selector>),
}

impl Selectors {
    /// Check the selectors against a table schema.
    pub fn check(&self, schema: &TableSchema) -> Result<()> {
        match self {
            Selectors::All => Ok(()),
            Selectors::Some(selectors) => {
                for selector in selectors {
                    match selector {
                        Selector::Column(ColumnSelector(_, column)) => {
                            if !schema.has_column(column) {
                                return Err(Error::ColumnNotFound(column.clone()));
                            }
                        }
                        Selector::Aggregate(_, ColumnSelector(_, column)) => {
                            if !schema.has_column(column) {
                                return Err(Error::ColumnNotFound(column.clone()));
                            }
                        }
                        Selector::Count => {}
                    }
                }
                Ok(())
            }
        }
    }

    /// Check the selectors against some tables.
    ///
    /// # Error
    ///
    /// Unlike `check`, this function requires all selectors be explicit about tables.
    /// If a selector is not explicit about a table, an error will be returned.
    pub fn check_tables(&self, schemas: &[&TableSchema], tables: &[&str]) -> Result<()> {
        match self {
            Selectors::All => Ok(()),
            Selectors::Some(selectors) => {
                for selector in selectors {
                    match selector {
                        Selector::Column(column_selector) => {
                            column_selector.check_tables(schemas, tables)?;
                        }
                        Selector::Aggregate(_, column_selector) => {
                            column_selector.check_tables(schemas, tables)?;
                        }
                        Selector::Count => {}
                    }
                }
                Ok(())
            }
        }
    }
}

/// Column selector in the form table.column,
/// where table part is optional
#[derive(Clone, Debug)]
pub struct ColumnSelector(pub Option<String>, pub String);

impl PartialEq for ColumnSelector {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(table0), Some(table1)) = (&self.0, &other.0) {
            table0 == table1 && self.1 == other.1
        } else {
            self.1 == other.1
        }
    }
}

impl ColumnSelector {
    /// Check the column selector against some table schemas.
    ///
    /// # Error
    ///
    /// Return error when the column selector is not explicit about a table.
    pub fn check_tables(&self, schemas: &[&TableSchema], tables: &[&str]) -> Result<()> {
        let ColumnSelector(table_selector, column) = self;
        if let Some(table_selector) = table_selector {
            let mut found = false;
            for (&schema, &table) in schemas.iter().zip(tables) {
                if table == table_selector {
                    found = true;
                    if !schema.has_column(column) {
                        return Err(Error::ColumnNotFound(column.clone()));
                    }
                    break;
                }
            }
            if !found {
                return Err(Error::TableNotFound(table_selector.clone()));
            }
        } else {
            return Err(Error::InexactColumn(column.clone()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum Aggregator {
    Avg,
    Min,
    Max,
    Sum,
}

impl Aggregator {
    pub fn aggregate(&self, values: Vec<Value>) -> Value {
        match self {
            Aggregator::Avg => {
                let len = values.len();
                let sum = Self::Sum.aggregate(values);
                sum / len
            }
            Aggregator::Min => values
                .iter()
                .reduce(Value::min)
                .cloned()
                .unwrap_or(Value::Null),
            Aggregator::Max => values
                .iter()
                .reduce(Value::max)
                .cloned()
                .unwrap_or(Value::Null),
            Aggregator::Sum => values.into_iter().reduce(Add::add).unwrap_or(Value::Null),
        }
    }
}

impl Display for Aggregator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Aggregator::Avg => write!(f, "AVG"),
            Aggregator::Min => write!(f, "MIN"),
            Aggregator::Max => write!(f, "MAX"),
            Aggregator::Sum => write!(f, "SUM"),
        }
    }
}

/// Query selector.
#[derive(Clone, Debug)]
pub enum Selector {
    Column(ColumnSelector),
    Aggregate(Aggregator, ColumnSelector),
    Count,
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
            Selector::Aggregate(agg, ColumnSelector(table, column)) => {
                write!(f, "{}(", agg)?;
                if let Some(table) = table {
                    write!(f, "{}.", table)?;
                }
                write!(f, "{}", column)?;
                write!(f, ")")?;
            }
            Selector::Count => write!(f, "COUNT(*)")?,
        }
        Ok(())
    }
}

/// A key-value pair in set clause.
#[derive(Debug)]
pub struct SetPair(pub String, pub Value);

impl SetPair {
    /// Check the set pair against a table schema.
    pub fn check(&self, schema: &TableSchema) -> Result<()> {
        let SetPair(column, value) = &self;
        if !schema.has_column(column) {
            return Err(Error::ColumnNotFound(column.to_owned()));
        }
        let column = schema.get_column(column);
        let typ = &column.typ;
        if !value.check_type(typ) {
            return Err(Error::TypeMismatch(value.clone(), typ.clone()));
        }
        Ok(())
    }
}

/// SQL operator.
#[derive(Clone, Debug)]
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// SQL expression.
#[derive(Clone, Debug)]
pub enum Expression {
    Value(Value),
    Column(ColumnSelector),
}

/// Where clause.
#[derive(Clone, Debug)]
pub enum WhereClause {
    OperatorExpression(ColumnSelector, Operator, Expression),
    LikeString(ColumnSelector, String),
}

impl WhereClause {
    /// Check the where clause against a table schema.
    pub fn check(&self, schema: &TableSchema) -> Result<()> {
        match self {
            WhereClause::OperatorExpression(ColumnSelector(_, column), _, expr) => {
                if !schema.has_column(column) {
                    return Err(Error::ColumnNotFound(column.clone()));
                }
                match expr {
                    Expression::Value(_) => Ok(()),
                    Expression::Column(ColumnSelector(_, column)) => {
                        if !schema.has_column(column) {
                            return Err(Error::ColumnNotFound(column.clone()));
                        }
                        Ok(())
                    }
                }
            }
            WhereClause::LikeString(ColumnSelector(_, column), _) => {
                if !schema.has_column(column) {
                    return Err(Error::ColumnNotFound(column.clone()));
                }
                Ok(())
            }
        }
    }

    /// Check the where clause against some tables.
    ///
    /// # Error
    ///
    /// Unlike `check`, this function requires all selectors be explicit about tables.
    pub fn check_tables(&self, schemas: &[&TableSchema], tables: &[&str]) -> Result<()> {
        match self {
            WhereClause::OperatorExpression(column_selector, _, expr) => {
                column_selector.check_tables(schemas, tables)?;
                match expr {
                    Expression::Value(_) => Ok(()),
                    Expression::Column(column_selector) => {
                        column_selector.check_tables(schemas, tables)
                    }
                }
            }
            WhereClause::LikeString(column_selector, _) => {
                column_selector.check_tables(schemas, tables)
            }
        }
    }

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
            WhereClause::LikeString(ColumnSelector(_, column), pattern) => {
                let column = schema.get_column(column);
                let value = &record.fields[schema.column_map[&column.name]];
                if let Value::Varchar(v) = value {
                    let v = v.trim_end_matches('\0');
                    let pattern = regex::escape(pattern);
                    let pattern = pattern.replace('_', ".");
                    let pattern = pattern.replace('%', ".*");
                    let pattern = format!("^{pattern}$");
                    let re = RegexBuilder::new(&pattern)
                        .multi_line(true)
                        .build()
                        .expect("Failed to build regex");
                    re.is_match(v)
                } else {
                    false
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
    /// Referred constraints.
    pub referred_constraints: Vec<(String, Constraint)>,
    /// Indexes on the table.
    pub indexes: Vec<IndexSchema>,
}

impl Schema {
    /// Check if some column name is in the table.
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    /// Get a column in this schema by its name.
    ///
    /// # Panics
    ///
    /// Panics if the column is not found.
    pub fn get_column(&self, name: &str) -> &Column {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .expect("Column not found")
    }
}

/// A wrapped table schema.
pub struct TableSchema {
    /// The schema.
    schema: Schema,
    /// Path to the schema, for serialization.
    path: PathBuf,
    /// Columns of the table.
    columns: Vec<Column>,
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

        Ok(Self {
            schema,
            path: path.to_owned(),
            columns,
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

    /// Get the inner schema.
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the length of a record.
    pub fn get_record_size(&self) -> usize {
        self.record_size
    }

    /// Check whether a given column is in a table.
    pub fn has_column(&self, name: &str) -> bool {
        self.column_map.contains_key(name)
    }

    /// Return a reference to table constraints.
    pub fn get_constraints(&self) -> &[Constraint] {
        &self.schema.constraints
    }

    /// Return a reference to referred table constraints.
    pub fn get_referred_constraints(&self) -> &[(String, Constraint)] {
        &self.schema.referred_constraints
    }

    /// Return a reference to table indexes.
    pub fn get_indexes(&self) -> &[IndexSchema] {
        &self.schema.indexes
    }

    /// Check whether a given index is in a table.
    pub fn has_index(&self, name: &str) -> bool {
        self.schema.indexes.iter().any(|i| i.name == name)
    }

    /// Add an index to the table.
    pub fn add_index(&mut self, index: IndexSchema) {
        self.schema.indexes.push(index);
    }

    /// Remove an index from the table.
    pub fn remove_index(&mut self, name: &str) {
        log::info!("Dropping index {name}");
        log::info!("Current indexes: {:?}", self.schema.indexes);
        self.schema.indexes.retain(|i| i.name != name);
    }

    /// Get the primary key in the table.
    pub fn get_primary_key(&self) -> Option<&Constraint> {
        self.schema
            .constraints
            .iter()
            .find(|c| matches!(c, Constraint::PrimaryKey { .. }))
    }

    /// Get the foreign keys in the table.
    pub fn get_foreign_keys(&self) -> Vec<&Constraint> {
        self.schema
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::ForeignKey { .. }))
            .collect()
    }

    /// Add an constraint to the table.
    pub fn add_constraint(&mut self, constraint: Constraint) {
        self.schema.constraints.push(constraint);
    }

    /// Add an referred constraint to the table.
    pub fn add_referred_constraint(&mut self, table: String, constraint: Constraint) {
        self.schema.referred_constraints.push((table, constraint));
    }

    /// Remove the primary key on the table.
    pub fn remove_primary_key(&mut self) {
        log::info!("Dropping primary key");
        self.schema
            .constraints
            .retain(|c| !matches!(c, Constraint::PrimaryKey { .. }))
    }

    /// Remove an constraint from the table.
    pub fn remove_constraint(&mut self, name: &str) {
        log::info!("Dropping constraint {name}");
        log::info!("Current constraints: {:?}", self.schema.constraints);
        self.schema.constraints.retain(|c| match c {
            Constraint::PrimaryKey { name: n, .. } => n.as_deref() != Some(name),
            Constraint::ForeignKey { name: n, .. } => n.as_deref() != Some(name),
        });
    }

    /// Remove an referred constraint from the table.
    pub fn remove_referred_constraint(&mut self, table_name: &str, name: &str) {
        log::info!("Dropping constraint {name}");
        log::info!(
            "Current constraints: {:?}",
            self.schema.referred_constraints
        );
        self.schema.referred_constraints.retain(|(t, c)| {
            // Not the table we want to remove
            if t != table_name {
                return true;
            }
            match c {
                Constraint::PrimaryKey { name: n, .. } => n.as_deref() != Some(name),
                Constraint::ForeignKey { name: n, .. } => n.as_deref() != Some(name),
            }
        });
    }

    /// Remove referred constraints from a table.
    pub fn remove_referred_constraints_of_table(&mut self, table: &str) {
        self.schema.referred_constraints.retain(|(t, _)| t != table);
    }

    /// Get a column by its name.
    pub fn get_column(&self, name: &str) -> &Column {
        &self.columns[self.column_map[name]]
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
    pub fn new_page(&mut self) -> usize {
        let page = self.schema.pages;
        self.schema.pages += 1;
        page
    }
}

impl RecordSchema for TableSchema {
    /// Return a reference to column information.
    fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    /// Get the size of the null bitmap.
    fn get_null_bitmap_size(&self) -> usize {
        self.null_bitmap_size
    }

    /// Get the index of a column by its name.
    fn get_column_index(&self, name: &str) -> usize {
        self.column_map[name]
    }
}

impl Drop for TableSchema {
    fn drop(&mut self) {
        if let Err(err) = self.save() {
            log::error!("Failed to save schema: {err}");
        }
    }
}
