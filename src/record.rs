//! Data record management.

use bit_set::BitSet;

use crate::schema::{ColumnSelector, Selector, Selectors, TableSchema, Type, Value, SetPair};

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub fields: Vec<Value>,
}

impl Record {
    /// Create a new record.
    pub fn new(fields: Vec<Value>) -> Self {
        Self { fields }
    }

    /// Deserialize a record from a buffer.
    pub fn from(buf: &[u8], mut offset: usize, schema: &TableSchema) -> Self {
        let nulls = BitSet::from_bytes(&buf[offset..offset + schema.get_null_bitmap_size()]);
        offset += schema.get_null_bitmap_size();

        let mut fields = Vec::new();
        for (i, column) in schema.get_columns().iter().enumerate() {
            // Null field
            if nulls.contains(i) {
                fields.push(Value::Null);
                offset += column.typ.size();
                continue;
            }

            let value_buf = &buf[offset..offset + column.typ.size()];
            let value = match &column.typ {
                Type::Int => Value::Int(i32::from_le_bytes(value_buf.try_into().unwrap())),
                Type::Float => Value::Float(f64::from_le_bytes(value_buf.try_into().unwrap())),
                Type::Varchar(_) => {
                    let s = String::from_utf8_lossy(value_buf).to_string();
                    Value::Varchar(s)
                }
            };

            fields.push(value);
            offset += column.typ.size();
        }
        Self { fields }
    }

    /// Save a record into a buffer.
    pub fn save_into(&self, buf: &mut [u8], mut offset: usize, schema: &TableSchema) {
        let offset_orig = offset;
        offset += schema.get_null_bitmap_size();

        let mut nulls = BitSet::new();

        for (i, field) in self.fields.iter().enumerate() {
            let value = field;
            let value_buf = &mut buf[offset..offset + schema.get_columns()[i].typ.size()];
            match value {
                Value::Null => {
                    nulls.insert(i);
                }
                Value::Int(v) => {
                    value_buf.copy_from_slice(&v.to_le_bytes());
                }
                Value::Float(v) => {
                    value_buf.copy_from_slice(&v.to_le_bytes());
                }
                Value::Varchar(v) => {
                    // Fill the rest with zeros
                    value_buf[..v.len()].copy_from_slice(v.as_bytes());
                    value_buf[v.len()..].fill(0);
                }
            }

            offset += schema.get_columns()[i].typ.size();
        }

        let null_buf = &mut buf[offset_orig..offset_orig + schema.get_null_bitmap_size()];
        let null_bytes = &nulls.into_bit_vec().to_bytes();

        null_buf[..null_bytes.len()].copy_from_slice(null_bytes);
        null_buf[null_bytes.len()..].fill(0);
    }

    /// Select some fields in the record.
    pub fn select(&self, selectors: &Selectors, schema: &TableSchema) -> Record {
        match selectors {
            Selectors::All => self.clone(),
            Selectors::Some(selectors) => {
                let mut fields = vec![];
                for selector in selectors {
                    match selector {
                        Selector::Column(ColumnSelector(_, column)) => {
                            fields.push(self.fields[schema.get_column_index(column)].clone())
                        }
                    }
                }
                Record { fields }
            }
        }
    }

    /// Update some fields in the record.
    /// 
    /// # Returns
    /// 
    /// Return true if the record is updated.
    pub fn update(&mut self, set_pairs: &[SetPair], schema: &TableSchema) -> bool {
        let mut updated = false;
        for SetPair(column, value) in set_pairs {
            let index = schema.get_column_index(column);
            let old_value = &self.fields[index];
            if value != old_value {
                self.fields[index] = value.clone();
                updated = true;
            }
        }
        updated
    }
    
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::config::PAGE_SIZE;
    use crate::schema::{Column, Schema, TableSchema, Value};
    use crate::setup;

    use super::*;

    #[test]
    fn test_record() {
        setup::init_logging();

        let schema = TableSchema::new(
            Schema {
                pages: 0,
                free: None,
                full: None,
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        typ: Type::Int,
                        nullable: false,
                        default: None,
                    },
                    Column {
                        name: "name".to_string(),
                        typ: Type::Varchar(255),
                        nullable: false,
                        default: None,
                    },
                    Column {
                        name: "score".to_string(),
                        typ: Type::Float,
                        nullable: true,
                        default: None,
                    },
                ],
                constraints: vec![],
            },
            &PathBuf::new(),
        )
        .unwrap();

        let mut buf = [0u8; PAGE_SIZE];
        let record = Record {
            fields: vec![
                Value::Int(1),
                Value::Varchar("Alice".to_string()),
                Value::Float(100.0),
            ],
        };
        record.save_into(&mut buf, 0, &schema);

        log::info!("Test serializing. Buf: {:?}", &buf[..512]);

        let record = Record::from(&buf, 0, &schema);

        log::info!("Test deserializing. Record: {:?}", record);

        assert_eq!(record.fields[0], Value::Int(1));
        let name = "Alice";
        match &record.fields[1] {
            Value::Varchar(s) => assert_eq!(&s[..name.len()], name),
            _ => panic!("Wrong type"),
        }
        assert_eq!(record.fields[2], Value::Float(100.0));

        let record = Record {
            fields: vec![
                Value::Int(2),
                Value::Varchar("Bob".to_string()),
                Value::Null,
            ],
        };
        record.save_into(&mut buf, 0, &schema);

        log::info!("Test serializing. Buf: {:?}", &buf[..512]);

        let record = Record::from(&buf, 0, &schema);

        log::info!("Test deserializing. Record: {:?}", record);

        assert_eq!(record.fields[0], Value::Int(2));
        let name: &str = "Bob";
        match &record.fields[1] {
            Value::Varchar(s) => assert_eq!(&s[..name.len()], name),
            _ => panic!("Wrong type"),
        }
        assert_eq!(record.fields[2], Value::Null);
    }

    #[test]
    fn test_multiple_bytes_of_null_bitmap() {
        setup::init_logging();

        let schema = TableSchema::new(
            Schema {
                pages: 0,
                free: None,
                full: None,
                columns: vec![
                    Column {
                        name: "c0".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c1".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c2".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c3".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c4".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c5".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c6".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c7".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                    Column {
                        name: "c8".to_string(),
                        typ: Type::Int,
                        nullable: true,
                        default: None,
                    },
                ],
                constraints: vec![],
            },
            &PathBuf::new(),
        )
        .unwrap();

        let mut buf = [0u8; PAGE_SIZE];
        let record = Record {
            fields: vec![
                Value::Int(123),
                Value::Null,
                Value::Int(123),
                Value::Null,
                Value::Int(123),
                Value::Null,
                Value::Int(123),
                Value::Null,
                Value::Null,
            ],
        };

        record.save_into(&mut buf, 0, &schema);

        log::info!("Test serializing. Buf: {:?}", &buf[..512]);

        let record = Record::from(&buf, 0, &schema);

        log::info!("Test deserializing. Record: {:?}", record);

        assert_eq!(record.fields[0], Value::Int(123));
        assert_eq!(record.fields[1], Value::Null);
        assert_eq!(record.fields[2], Value::Int(123));
        assert_eq!(record.fields[3], Value::Null);
        assert_eq!(record.fields[4], Value::Int(123));
        assert_eq!(record.fields[5], Value::Null);
        assert_eq!(record.fields[6], Value::Int(123));
        assert_eq!(record.fields[7], Value::Null);
        assert_eq!(record.fields[8], Value::Null);
    }
}
