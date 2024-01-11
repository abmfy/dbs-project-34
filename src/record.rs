//! Data record management.

use bit_set::BitSet;

use crate::config::PAGE_SIZE;
use crate::error::Result;
use crate::schema::{TableSchema, Type, Value};

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    fields: Vec<Option<Value>>,
}

impl Record {
    /// Deserialize a record from a buffer.
    pub fn from_buf(buf: &[u8], mut offset: usize, schema: &TableSchema) -> Result<Self> {
        let nulls = BitSet::from_bytes(&buf[offset..offset + schema.null_bitmap_size()]);
        offset += schema.null_bitmap_size();

        let mut fields = Vec::new();
        for (i, column) in schema.get_columns().iter().enumerate() {
            // Null field
            if nulls.contains(i) {
                fields.push(None);
                offset += column.typ.size();
                continue;
            }

            let value_buf = &buf[offset..offset + column.typ.size()];
            let value = match &column.typ {
                Type::Int => Value::Int(i32::from_le_bytes(value_buf.try_into().unwrap())),
                Type::Float => Value::Float(f64::from_le_bytes(value_buf.try_into().unwrap())),
                Type::Varchar(size) => {
                    let s = String::from_utf8_lossy(value_buf).to_string();
                    Value::Varchar(s)
                }
            };

            fields.push(Some(value));
            offset += column.typ.size();
        }
        Ok(Self { fields })
    }

    /// Save a record into a buffer.
    pub fn into_buf(&self, buf: &mut [u8], mut offset: usize, schema: &TableSchema) -> Result<()> {
        let offset_orig = offset;
        offset += schema.null_bitmap_size();

        let mut nulls = BitSet::new();

        for (i, field) in self.fields.iter().enumerate() {
            if field.is_none() {
                nulls.insert(i);
                offset += schema.get_columns()[i].typ.size();
                continue;
            }

            let value = field.as_ref().unwrap();
            let value_buf = &mut buf[offset..offset + schema.get_columns()[i].typ.size()];
            match value {
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

        let null_buf = &mut buf[offset_orig..offset_orig + schema.null_bitmap_size()];
        let null_bytes = &nulls.into_bit_vec().to_bytes();

        null_buf[..null_bytes.len()].copy_from_slice(null_bytes);
        null_buf[null_bytes.len()..].fill(0);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{Column, Schema, TableSchema, Value};
    use crate::setup;

    use super::*;

    #[test]
    fn test_record() {
        setup::init_logging();

        let schema = TableSchema::new(Schema {
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
        });

        let mut buf = [0u8; PAGE_SIZE];
        let record = Record {
            fields: vec![
                Some(Value::Int(1)),
                Some(Value::Varchar("Alice".to_string())),
                Some(Value::Float(100.0)),
            ],
        };
        record.into_buf(&mut buf, 0, &schema).unwrap();

        log::info!("Test serializing. Buf: {:?}", &buf[..512]);

        let record = Record::from_buf(&buf, 0, &schema).unwrap();

        log::info!("Test deserializing. Record: {:?}", record);

        assert_eq!(record.fields[0].as_ref().unwrap(), &Value::Int(1));
        let name = "Alice";
        match &record.fields[1].as_ref().unwrap() {
            Value::Varchar(s) => assert_eq!(&s[..name.len()], name),
            _ => panic!("Wrong type"),
        }
        assert_eq!(record.fields[2].as_ref().unwrap(), &Value::Float(100.0));

        let mut record = Record {
            fields: vec![
                Some(Value::Int(2)),
                Some(Value::Varchar("Bob".to_string())),
                None,
            ],
        };
        record.into_buf(&mut buf, 0, &schema).unwrap();

        log::info!("Test serializing. Buf: {:?}", &buf[..512]);

        let record = Record::from_buf(&buf, 0, &schema).unwrap();

        log::info!("Test deserializing. Record: {:?}", record);

        assert_eq!(record.fields[0].as_ref().unwrap(), &Value::Int(2));
        let name: &str = "Bob";
        match &record.fields[1].as_ref().unwrap() {
            Value::Varchar(s) => assert_eq!(&s[..name.len()], name),
            _ => panic!("Wrong type"),
        }
        assert_eq!(record.fields[2].as_ref(), None);
    }

    #[test]
    fn test_multiple_bytes_of_null_bitmap() {
        setup::init_logging();

        let schema = TableSchema::new(Schema {
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
        });

        let mut buf = [0u8; PAGE_SIZE];
        let record = Record {
            fields: vec![
                Some(Value::Int(123)),
                None,
                Some(Value::Int(123)),
                None,
                Some(Value::Int(123)),
                None,
                Some(Value::Int(123)),
                None,
                None,
            ],
        };

        record.into_buf(&mut buf, 0, &schema).unwrap();

        log::info!("Test serializing. Buf: {:?}", &buf[..512]);

        let record = Record::from_buf(&buf, 0, &schema).unwrap();

        log::info!("Test deserializing. Record: {:?}", record);

        assert_eq!(record.fields[0].as_ref().unwrap(), &Value::Int(123));
        assert_eq!(record.fields[1].as_ref(), None);
        assert_eq!(record.fields[2].as_ref().unwrap(), &Value::Int(123));
        assert_eq!(record.fields[3].as_ref(), None);
        assert_eq!(record.fields[4].as_ref().unwrap(), &Value::Int(123));
        assert_eq!(record.fields[5].as_ref(), None);
        assert_eq!(record.fields[6].as_ref().unwrap(), &Value::Int(123));
        assert_eq!(record.fields[7].as_ref(), None);
        assert_eq!(record.fields[8].as_ref(), None);
    }
}
