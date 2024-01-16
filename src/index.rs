//! B+ Tree index management.
//!
//! Different from table pages, there are only one linked list
//! in a index storing empty pages.
//!
//! # Page Header Structure
//!
//! ## Internal Node
//!
//! | is_leaf | *align* | records | parent |
//! |---------|---------|---------|--------|
//! |      1B |      3B |      4B |     4B |
//!
//! ## Leaf Node
//!
//! | is_leaf | *align* | records | parent | prev | next |
//! |---------|---------|---------|--------|------|------|
//! |      1B |      3B |      4B |     4B |   4B |   4B |
//!
//! # Page record structure
//!
//! ## Internal Node
//!
//! |    key     | child |
//! |------------|-------|
//! | *key_size* |    4B |
//!
//! ## Leaf Node
//!
//! |    key     | page | slot |
//! |------------|------|------|
//! | *key_size* |   4B |   4B |
//!
//! # Reference
//!
//! Implementation adapted from [OI Wiki](https://oi-wiki.org/ds/bplus-tree/).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::{LINK_SIZE, PAGE_SIZE};
use crate::error::Result;
use crate::file::PageCache;
use crate::record::{Record, RecordSchema};
use crate::schema::{Column, TableSchema, Type};

const LEAF_OFFSET: usize = LINK_SIZE * 0;
const RECORDS_OFFSET: usize = LINK_SIZE * 1;
const PARENT_OFFSET: usize = LINK_SIZE * 2;
const PREV_OFFSET: usize = LINK_SIZE * 3;
const NEXT_OFFSET: usize = LINK_SIZE * 4;

/// Index schema.
#[derive(Deserialize, Serialize)]
pub struct IndexSchema {
    /// Count of pages in this index.
    pub pages: usize,
    /// Page number of the first free page in the index.
    pub free: Option<usize>,
    /// Is this a explicit index.
    pub explicit: bool,
    /// Name of the index.
    pub name: String,
    /// Name of the columns.
    pub column: Vec<String>,
    /// Root page id.
    pub root: Option<usize>,
}

/// An index.
pub struct Index<'a> {
    /// The index's fd.
    fd: Uuid,
    /// The index's schema.
    schema: &'a mut IndexSchema,
    /// Size of indexed columns.
    index_size: usize,
    /// Columns in the index.
    columns: Vec<Column>,
    /// Record schema for internal nodes.
    internal_schema: IndexRecordSchema,
    /// Record schema for leaf nodes.
    leaf_schema: IndexRecordSchema,
}

impl<'a> Index<'a> {
    /// Create a new index.
    ///
    /// # Panics
    ///
    /// Panics if column name not found in table schema.
    ///
    /// Please check the schema before creating an index.
    pub fn new(fd: Uuid, schema: &'a mut IndexSchema, table: &TableSchema) -> Self {
        let columns: Vec<Column> = schema
            .column
            .iter()
            .map(|col| table.get_column(col).clone())
            .collect();
        let index_size = columns.iter().map(|col| col.typ.size()).sum();
        let internal_schema = IndexRecordSchema::from(&columns, false);
        let leaf_schema = IndexRecordSchema::from(&columns, true);
        Self {
            fd,
            schema,
            index_size,
            columns,
            internal_schema,
            leaf_schema,
        }
    }

    /// Get the file descriptor of this index.
    pub fn get_fd(&self) -> Uuid {
        self.fd
    }

    /// Get the schema of this index.
    pub fn get_schema(&self) -> &IndexSchema {
        &self.schema
    }

    /// Get the columns in this index.
    pub fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    /// Get the next page in the linked list.
    fn get_next(buf: &[u8]) -> Option<usize> {
        let next = &buf[..LINK_SIZE];
        let next = u32::from_le_bytes(next.try_into().unwrap());
        if next == 0 {
            None
        } else {
            Some((next - 1) as usize)
        }
    }

    /// Set the next page in the linked list.
    fn set_next(buf: &mut [u8], next: Option<usize>) {
        let next = next.map(|n| n as u32);
        let next = next.map_or(0, |n| n + 1);
        let next = next.to_le_bytes();
        buf[..LINK_SIZE].copy_from_slice(&next);
    }

    /// Allocate an empty page for use.
    pub fn new_page(&mut self, fs: &mut PageCache) -> Result<usize> {
        let free_page_id = self.schema.free;

        if let Some(page_id) = free_page_id {
            let page = fs.get(self.fd, page_id)?;
            let next = Self::get_next(page);
            self.schema.free = next;
            Ok(page_id)
        } else {
            let page_id = self.schema.pages;
            self.schema.pages += 1;
            let page = fs.get_mut(self.fd, page_id)?;
            Self::set_next(page, None);
            Ok(page_id)
        }
    }

    /// Mark a page as free.
    pub fn free_page(&mut self, fs: &mut PageCache, page_id: usize) -> Result<()> {
        let page = fs.get(self.fd, page_id)?;
        let next = Self::get_next(page);
        self.schema.free = next;
        Ok(())
    }
}

/// Schema for internal nodes in an index.
pub struct IndexRecordSchema {
    columns: Vec<Column>,
}

impl IndexRecordSchema {
    pub fn from(columns: &[Column], is_leaf: bool) -> Self {
        let mut columns = columns.to_vec();

        if is_leaf {
            let page = Column::new("__page__".to_owned(), Type::Int, false, None).unwrap();
            let slot = Column::new("__slot__".to_owned(), Type::Int, false, None).unwrap();
            columns.extend([page, slot]);
        } else {
            let child = Column::new("__child__".to_owned(), Type::Int, false, None).unwrap();
            columns.push(child);
        }

        Self { columns }
    }
}

impl RecordSchema for IndexRecordSchema {
    fn get_columns(&self) -> &[Column] {
        &self.columns
    }
}

/// Common behaviors between IndexPage and IndexPageMut;
/// For code reuse.
pub trait LinkedIndexPage<'a> {
    /// Get the index.
    fn get_index(&self) -> &'a Index<'a>;

    /// Get the buffer.
    fn get_buf(&self) -> &[u8];

    /// Is this page a leaf node.
    fn is_leaf(&self) -> bool;

    /// Get the parent node of this node.
    fn get_parent(&self) -> Option<usize>;

    /// Get number of records in this page.
    fn get_records(&self) -> usize;

    /// Get the size of a record in this page.
    fn get_record_size(&self) -> usize;

    /// Get the maximum number of records in this page.
    fn get_max_records(&self) -> usize;

    /// Get the size of the header.
    fn get_header_size(&self) -> usize;

    /// Get the record schema of the index.
    fn get_record_schema(&'a self) -> &'a IndexRecordSchema {
        if self.is_leaf() {
            &self.get_index().leaf_schema
        } else {
            &self.get_index().internal_schema
        }
    }

    /// Get the next page number.
    fn get_next(&self) -> Option<usize> {
        let next = &self.get_buf()[..LINK_SIZE];
        let next = u32::from_le_bytes(next.try_into().unwrap());
        if next == 0 {
            None
        } else {
            Some((next - 1) as usize)
        }
    }
}

/// A read-only page in an index.
pub struct IndexPage<'a> {
    /// The index the page belongs to.
    index: &'a Index<'a>,
    /// The buffer of the index.
    buf: &'a [u8],
    /// Is this page a leaf node.
    leaf: bool,
    /// Parent node of this node.
    parent: Option<usize>,
    /// Count of records in this page.
    records: usize,
    /// The size of a record.
    record_size: usize,
    /// Maximum number of records in the page.
    max_records: usize,
    /// The size of the header.
    header_size: usize,
}

impl<'a> IndexPage<'a> {
    /// Read an index page from buffer.
    pub fn from_buf(index: &'a Index, buf: &'a [u8]) -> Self {
        let leaf = buf[LEAF_OFFSET] == 1;
        let records = u32::from_le_bytes(
            buf[RECORDS_OFFSET..RECORDS_OFFSET + LINK_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        let parent = u32::from_le_bytes(
            buf[PARENT_OFFSET..PARENT_OFFSET + LINK_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;
        let parent = if parent != 0 { Some(parent - 1) } else { None };

        let header_size = if leaf { 5 * LINK_SIZE } else { 3 * LINK_SIZE };

        let record_size = if leaf {
            // 2 for ids of page and slot
            index.index_size + 2 * LINK_SIZE
        } else {
            // 1 for id of page
            index.index_size + LINK_SIZE
        };

        let max_records = (PAGE_SIZE - header_size) / record_size;

        Self {
            index,
            buf,
            leaf,
            parent,
            records,
            record_size,
            max_records,
            header_size,
        }
    }
}

/// A writable page in an index.
pub struct IndexPageMut<'a> {
    /// The index the page belongs to.
    index: &'a Index<'a>,
    /// The buffer of the index.
    buf: &'a mut [u8],
    /// Is this page a leaf node.
    leaf: bool,
    /// Parent node of this node.
    parent: Option<usize>,
    /// Count of records in this page.
    records: usize,
    /// The size of a record.
    record_size: usize,
    /// Maximum number of records in the page.
    max_records: usize,
    /// The size of the header.
    header_size: usize,
}

impl<'a> IndexPageMut<'a> {
    /// Create an empty index page.
    pub fn new(index: &'a mut Index, buf: &'a mut [u8], parent: Option<usize>, leaf: bool) -> Self {
        let records = 0;

        let header_size = if leaf { 5 * LINK_SIZE } else { 3 * LINK_SIZE };
        let record_size = if leaf {
            // 2 for ids of page and slot
            index.index_size + 2 * LINK_SIZE
        } else {
            // 1 for id of page
            index.index_size + LINK_SIZE
        };

        let max_records = (PAGE_SIZE - header_size) / record_size;

        // Clear buffer
        buf.fill(0);

        if leaf {
            buf[LEAF_OFFSET] = 1;
        }

        if let Some(parent) = parent {
            buf[PARENT_OFFSET..PARENT_OFFSET + LINK_SIZE]
                .copy_from_slice(&((parent + 1) as u32).to_le_bytes());
        }

        Self {
            index,
            buf,
            leaf,
            parent,
            records,
            record_size,
            max_records,
            header_size,
        }
    }

    /// Read an index page from buffer.
    pub fn from_buf(index: &'a mut Index, buf: &'a mut [u8]) -> Self {
        let leaf = buf[LEAF_OFFSET] == 1;
        let records = u32::from_le_bytes(
            buf[RECORDS_OFFSET..RECORDS_OFFSET + LINK_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        let parent = u32::from_le_bytes(
            buf[PARENT_OFFSET..PARENT_OFFSET + LINK_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;
        let parent = if parent != 0 { Some(parent - 1) } else { None };

        let header_size = if leaf { 5 * LINK_SIZE } else { 3 * LINK_SIZE };

        let record_size = if leaf {
            // 2 for ids of page and slot
            index.index_size + 2 * LINK_SIZE
        } else {
            // 1 for id of page
            index.index_size + LINK_SIZE
        };

        let max_records = (PAGE_SIZE - header_size) / record_size;

        Self {
            index,
            buf,
            leaf,
            parent,
            records,
            record_size,
            max_records,
            header_size,
        }
    }
}

/// An iterator over records in a page.
///
/// # Iterated Items
///
/// Each item is a tuple of:
///
/// - The record.
/// - The slot number of the record.
/// - The offset of the record.
pub struct PageIterator<'a, T: LinkedIndexPage<'a>> {
    page: &'a T,
    slot: usize,
    offset: usize,
}

impl<'a, T: LinkedIndexPage<'a>> PageIterator<'a, T> {
    /// Create a new iterator over records in a page.
    pub fn new(page: &'a T) -> Self {
        Self {
            page,
            slot: 0,
            offset: page.get_header_size(),
        }
    }

    /// Increment the iterator.
    fn inc(&mut self) {
        self.slot += 1;
        self.offset += self.page.get_record_size();
    }
}

impl<'a, T: LinkedIndexPage<'a>> Iterator for PageIterator<'a, T> {
    type Item = (Record, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.slot < self.page.get_max_records() {
            let record = if self.page.is_leaf() {
                Record::from(
                    self.page.get_buf(),
                    self.offset,
                    &self.page.get_index().leaf_schema,
                )
            } else {
                Record::from(
                    self.page.get_buf(),
                    self.offset,
                    &self.page.get_index().internal_schema,
                )
            };
            let slot = self.slot;
            let offset = self.offset;
            self.inc();
            Some((record, slot, offset))
        } else {
            None
        }
    }
}
