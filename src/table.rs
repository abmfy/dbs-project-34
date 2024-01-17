//! Table management.
//!
//! The table pages are organized into two linked lists:
//! free and full. Free pages are those with free space,
//! and full pages are those without free space.
//!
//! The first 8 bytes of a page is the page number of
//! the previous and next pages in the linked list,
//! 4 bytes each. 0 stands for nil, and the rest numbers
//! are incremented by 1 to avoid confusion with nil.

use bit_set::BitSet;
use uuid::Uuid;

use crate::config::LINK_SIZE;
use crate::error::{Error, Result};
use crate::file::PageCache;
use crate::index::{Index, IndexSchema};
use crate::record::Record;
use crate::schema::{Selectors, SetPair, TableSchema, WhereClause};

/// Select result containing slot id.
pub type SelectResult = (Record, usize);

/// A table.
pub struct Table {
    /// The table's fd.
    fd: Uuid,
    /// The table's schema.
    schema: TableSchema,
}

impl Table {
    /// Create a new table.
    pub fn new(fd: Uuid, schema: TableSchema) -> Self {
        Self { fd, schema }
    }

    /// Get the file descriptor of the table.
    pub fn get_fd(&self) -> Uuid {
        self.fd
    }

    /// Get schema of the table.
    pub fn get_schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Allocate a new page.
    pub fn new_page<'a>(&'a mut self, fs: &'a mut PageCache) -> Result<TablePageMut> {
        let page_id = self.schema.new_page();
        log::debug!("Allocating new page {page_id}");

        if let Some(next_page_id) = self.schema.get_free() {
            let next_page_buf = fs.get_mut(self.fd, next_page_id)?;
            let mut next_page = TablePageMut::new(self, next_page_buf);
            next_page.set_prev(Some(page_id));
        }

        let free = self.schema.get_free();
        self.schema.set_free(Some(page_id));

        let page_buf = fs.get_mut(self.fd, page_id)?;
        let mut page = TablePageMut::new(self, page_buf);
        page.set_next(free);

        Ok(page)
    }

    /// Take a page off the linked list.
    fn take_page<'a>(
        &'a mut self,
        fs: &'a mut PageCache,
        page_id: usize,
    ) -> Result<(Option<usize>, Option<usize>)> {
        log::debug!("Taking page {page_id} off the linked list");

        let page_buf = fs.get_mut(self.fd, page_id)?;
        let page = TablePage::new(self, page_buf);

        let prev = page.get_prev();
        let next = page.get_next();

        drop(page);

        if let Some(prev_page_id) = prev {
            let prev_page_buf = fs.get_mut(self.fd, prev_page_id)?;
            let mut prev_page = TablePageMut::new(self, prev_page_buf);
            prev_page.set_next(next);
        }

        if let Some(next_page_id) = next {
            let next_page_buf = fs.get_mut(self.fd, next_page_id)?;
            let mut next_page = TablePageMut::new(self, next_page_buf);
            next_page.set_prev(prev);
        }

        Ok((prev, next))
    }

    /// Mark a page as free.
    pub fn free_page<'a>(&'a mut self, fs: &'a mut PageCache, page_id: usize) -> Result<()> {
        log::debug!("Marking page {page_id} as free");

        let (prev, next) = self.take_page(fs, page_id)?;

        if prev.is_none() {
            self.schema.set_full(next);
        }

        let free = self.schema.get_free();

        if let Some(free_page_id) = free {
            let free_page_buf = fs.get_mut(self.fd, free_page_id)?;
            let mut free_page = TablePageMut::new(self, free_page_buf);
            free_page.set_prev(Some(page_id));
        }

        let page_buf = fs.get_mut(self.fd, page_id)?;
        let mut page = TablePageMut::new(self, page_buf);

        page.set_prev(None);
        page.set_next(free);

        self.schema.set_free(Some(page_id));

        Ok(())
    }

    /// Mark a page as full.
    pub fn full_page<'a>(&'a mut self, fs: &'a mut PageCache, page_id: usize) -> Result<()> {
        log::debug!("Marking page {page_id} as full");

        let (prev, next) = self.take_page(fs, page_id)?;

        if prev.is_none() {
            self.schema.set_free(next);
        }

        let full = self.schema.get_full();

        if let Some(full_page_id) = full {
            let full_page_buf = fs.get_mut(self.fd, full_page_id)?;
            let mut full_page = TablePageMut::new(self, full_page_buf);
            full_page.set_prev(Some(page_id));
        }

        let page_buf = fs.get_mut(self.fd, page_id)?;
        let mut page = TablePageMut::new(self, page_buf);

        page.set_prev(None);
        page.set_next(full);

        self.schema.set_full(Some(page_id));

        Ok(())
    }

    /// Select from table using selector.
    pub fn select(
        &self,
        fs: &mut PageCache,
        selector: &Selectors,
        where_clauses: &[WhereClause],
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        for page_id in 0..self.schema.get_pages() {
            let page_buf = fs.get(self.fd, page_id)?;
            let page = TablePage::new(self, page_buf);

            for (record, _, _) in &page {
                if where_clauses
                    .iter()
                    .all(|clause| clause.matches(&record, &self.schema))
                {
                    // record_count += 1;
                    records.push(record.select(selector, &self.schema));
                }
            }
        }

        Ok(records)
    }

    /// Read a block of records out of the table.
    pub fn select_page(
        &self,
        fs: &mut PageCache,
        page_id: usize,
        selector: &Selectors,
    ) -> Result<Vec<SelectResult>> {
        let page_buf = fs.get(self.fd, page_id)?;
        let page = TablePage::new(self, page_buf);

        let mut ret = Vec::new();

        for (record, slot, _) in &page {
            ret.push((record.select(selector, &self.schema), slot));
        }

        Ok(ret)
    }

    /// Insert a record into the table.
    pub fn insert<'a>(&'a mut self, fs: &'a mut PageCache, record: Record) -> Result<()> {
        log::debug!("Inserting {record:?}");

        if self.schema.get_free().is_none() {
            log::debug!("No free page, allocating a new page");
            self.new_page(fs)?;
        }

        let page_id = self.schema.get_free().expect("No free page");
        let page_buf = fs.get_mut(self.fd, page_id)?;
        let mut page = TablePageMut::new(self, page_buf);

        if !page.insert(record, &self.schema) {
            log::debug!("A page is filled");
            self.full_page(fs, page_id)?;
        }

        Ok(())
    }

    /// Update records in the table.
    pub fn update<'a>(
        &'a mut self,
        fs: &'a mut PageCache,
        set_pairs: &[SetPair],
        where_clauses: &[WhereClause],
    ) -> Result<usize> {
        log::debug!("Updating {set_pairs:?} where {where_clauses:?}");

        let mut updated = 0usize;
        for page_id in 0..self.schema.get_pages() {
            let page_buf = fs.get_mut(self.fd, page_id)?;
            let mut page = TablePageMut::new(self, page_buf);

            let mut to_update = vec![];

            for (mut record, _, offset) in &page {
                if where_clauses
                    .iter()
                    .all(|clause| clause.matches(&record, &self.schema))
                    && record.update(set_pairs, &self.schema)
                {
                    updated += 1;
                    to_update.push((record, offset));
                }
            }

            for (record, offset) in to_update {
                page.update(record, offset, &self.schema);
            }
        }

        Ok(updated)
    }

    /// Delete records from the table.
    pub fn delete<'a>(
        &'a mut self,
        fs: &'a mut PageCache,
        where_clauses: &[WhereClause],
    ) -> Result<usize> {
        log::debug!("Deleting where {where_clauses:?}");

        let mut deleted = 0usize;

        let mut free_page_id = self.schema.get_free();
        while let Some(page_id) = free_page_id {
            let page_buf = fs.get_mut(self.fd, page_id)?;
            let mut page = TablePageMut::new(self, page_buf);

            let mut to_delete = vec![];

            for (record, slot, _) in &page {
                if where_clauses
                    .iter()
                    .all(|clause| clause.matches(&record, &self.schema))
                {
                    deleted += 1;
                    to_delete.push(slot);
                }
            }

            for slot in to_delete {
                page.free(slot);
            }

            free_page_id = page.get_next();
        }

        let mut full_page_id = self.schema.get_full();
        let mut to_free = vec![];
        while let Some(page_id) = full_page_id {
            let page_buf = fs.get_mut(self.fd, page_id)?;
            let mut page = TablePageMut::new(self, page_buf);

            let mut to_delete = vec![];

            for (record, slot, _) in &page {
                if where_clauses
                    .iter()
                    .all(|clause| clause.matches(&record, &self.schema))
                {
                    deleted += 1;
                    // If the page is full, it will be marked
                    // as having free space due to this deletion.
                    if to_delete.is_empty() {
                        to_free.push(page_id);
                    }
                    to_delete.push(slot);
                }
            }

            for slot in to_delete {
                page.free(slot);
            }

            full_page_id = page.get_next();
        }

        for page_id in to_free {
            self.free_page(fs, page_id)?;
        }

        Ok(deleted)
    }

    /// Save an index schema into the table.
    pub fn add_index(&mut self, schema: IndexSchema) {
        self.schema.add_index(schema);
    }

    /// Remove an index schema from the table.
    pub fn remove_index(&mut self, name: &str) {
        self.schema.remove_index(name);
    }
}

/// Common behaviors between TablePage and TablePageMut.
/// For code reuse.
pub trait LinkedPage<'a> {
    /// Get the table.
    fn get_table(&self) -> &'a Table;

    /// Get the buffer.
    fn get_buf(&self) -> &[u8];

    /// Get the size of a record.
    fn get_record_size(&self) -> usize;

    /// Get the maximum number of records in the page.
    fn get_max_records(&self) -> usize;

    /// Get the size of the free slot bitmap.
    fn get_free_bitmap_size(&self) -> usize;

    /// Get the occupied slots.
    fn get_occupied(&self) -> &BitSet;

    /// Get an iterator over records in the page.
    fn iter(&'a self) -> PageIterator<'a, Self>
    where
        Self: Sized,
    {
        PageIterator::new(self)
    }

    /// Get the previous page number.
    fn get_prev(&self) -> Option<usize> {
        let prev = &self.get_buf()[..LINK_SIZE];
        let prev = u32::from_le_bytes(prev.try_into().unwrap());
        if prev == 0 {
            None
        } else {
            Some((prev - 1) as usize)
        }
    }

    /// Get the next page number.
    fn get_next(&self) -> Option<usize> {
        let next = &self.get_buf()[LINK_SIZE..2 * LINK_SIZE];
        let next = u32::from_le_bytes(next.try_into().unwrap());
        if next == 0 {
            None
        } else {
            Some((next - 1) as usize)
        }
    }

    /// Check if the i-th slot is free.
    fn is_free(&self, i: usize) -> bool {
        !self.get_occupied().contains(i)
    }
}

/// A read-only page in a table.
pub struct TablePage<'a> {
    /// The table the page belongs to.
    table: &'a Table,
    /// The buffer of the page.
    buf: &'a [u8],
    /// The size of a record.
    record_size: usize,
    /// Maximum number of records in the page.
    max_records: usize,
    /// The size of the free slot bitmap.
    free_bitmap_size: usize,
    /// Free slot bitmap.
    occupied: BitSet,
}

impl<'a> TablePage<'a> {
    /// Create a new page object representing a page in a buffer.
    pub fn new(table: &'a Table, buf: &'a [u8]) -> Self {
        let record_size = table.schema.get_record_size();
        let max_records = table.schema.get_max_records();
        let free_bitmap_size = table.schema.get_free_bitmap_size();
        let occupied = BitSet::from_bytes(&buf[2 * LINK_SIZE..2 * LINK_SIZE + free_bitmap_size]);
        Self {
            table,
            buf,
            record_size,
            max_records,
            free_bitmap_size,
            occupied,
        }
    }
}

impl<'a> LinkedPage<'a> for TablePage<'a> {
    fn get_table(&self) -> &'a Table {
        self.table
    }

    fn get_buf(&self) -> &[u8] {
        self.buf
    }

    fn get_record_size(&self) -> usize {
        self.record_size
    }

    fn get_max_records(&self) -> usize {
        self.max_records
    }

    fn get_free_bitmap_size(&self) -> usize {
        self.free_bitmap_size
    }

    fn get_occupied(&self) -> &BitSet {
        &self.occupied
    }
}

impl<'a> IntoIterator for &'a TablePage<'a> {
    type Item = (Record, usize, usize);
    type IntoIter = PageIterator<'a, TablePage<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A writable page in a table.
pub struct TablePageMut<'a> {
    /// The table the page belongs to.
    table: &'a Table,
    /// The buffer of the page.
    buf: &'a mut [u8],
    /// The size of a record.
    record_size: usize,
    /// Maximum number of records in the page.
    max_records: usize,
    /// The size of the free slot bitmap.
    free_bitmap_size: usize,
    /// First free slot.
    free: Option<usize>,
    /// Free slot bitmap.
    occupied: BitSet,
}

impl<'a> TablePageMut<'a> {
    /// Create a new page object representing a page in a buffer.
    pub fn new(table: &'a Table, buf: &'a mut [u8]) -> Self {
        let record_size = table.schema.get_record_size();
        let max_records = table.schema.get_max_records();
        let free_bitmap_size = table.schema.get_free_bitmap_size();
        let occupied = BitSet::from_bytes(&buf[2 * LINK_SIZE..2 * LINK_SIZE + free_bitmap_size]);
        let free = (0..max_records).find(|i| !occupied.contains(*i)).or(None);
        Self {
            table,
            buf,
            record_size,
            max_records,
            free_bitmap_size,
            free,
            occupied,
        }
    }

    /// Set the previous page number.
    pub fn set_prev(&mut self, prev: Option<usize>) {
        let prev = prev.map(|p| p as u32);
        let prev = prev.map_or(0, |p| p + 1);
        let prev = prev.to_le_bytes();
        self.buf[..LINK_SIZE].copy_from_slice(&prev);
    }

    /// Set the next page number.
    pub fn set_next(&mut self, next: Option<usize>) {
        let next = next.map(|n| n as u32);
        let next = next.map_or(0, |n| n + 1);
        let next = next.to_le_bytes();
        self.buf[LINK_SIZE..2 * LINK_SIZE].copy_from_slice(&next);
    }

    /// Set the i-th slot to be free.
    pub fn free(&mut self, i: usize) {
        self.occupied.remove(i);
        self.buf[2 * LINK_SIZE + i / 8] &= !(1 << (7 - i % 8));
        if self.free.is_none() || self.free.unwrap() > i {
            self.free = Some(i);
        }
    }

    /// Set the i-th slot to be occupied.
    pub fn occupy(&mut self, i: usize) {
        self.occupied.insert(i);
        self.buf[2 * LINK_SIZE + i / 8] |= 1 << (7 - i % 8);
        self.free = (i + 1..self.max_records)
            .find(|i| !self.occupied.contains(*i))
            .or(None);
    }

    /// Insert a record into the page.
    ///
    /// # Returns
    ///
    /// Return `false` if the page becomes full after this insertion.
    pub fn insert(&mut self, record: Record, schema: &TableSchema) -> bool {
        let free = self.free.expect("Insert called on a full page");
        self.occupy(free);

        let offset =
            2 * LINK_SIZE + self.free_bitmap_size + free * self.table.schema.get_record_size();
        record.save_into(self.buf, offset, schema);

        self.free.is_some()
    }

    /// Update a record in the page.
    pub fn update(&mut self, record: Record, offset: usize, schema: &TableSchema) {
        record.save_into(self.buf, offset, schema);
    }
}

impl<'a> LinkedPage<'a> for TablePageMut<'a> {
    fn get_table(&self) -> &'a Table {
        self.table
    }

    fn get_buf(&self) -> &[u8] {
        self.buf
    }

    fn get_record_size(&self) -> usize {
        self.record_size
    }

    fn get_max_records(&self) -> usize {
        self.max_records
    }

    fn get_free_bitmap_size(&self) -> usize {
        self.free_bitmap_size
    }

    fn get_occupied(&self) -> &BitSet {
        &self.occupied
    }
}

impl<'a> IntoIterator for &'a TablePageMut<'a> {
    type Item = (Record, usize, usize);
    type IntoIter = PageIterator<'a, TablePageMut<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
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
pub struct PageIterator<'a, T: LinkedPage<'a>> {
    page: &'a T,
    slot: usize,
    offset: usize,
}

impl<'a, T: LinkedPage<'a>> PageIterator<'a, T> {
    /// Create a new iterator over records in a page.
    pub fn new(page: &'a T) -> Self {
        Self {
            page,
            slot: 0,
            offset: 2 * LINK_SIZE + page.get_free_bitmap_size(),
        }
    }

    /// Increment the iterator.
    fn inc(&mut self) {
        self.slot += 1;
        self.offset += self.page.get_record_size();
    }
}

impl<'a, T: LinkedPage<'a>> Iterator for PageIterator<'a, T> {
    type Item = (Record, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        while self.slot < self.page.get_max_records() {
            if self.page.is_free(self.slot) {
                self.inc();
                continue;
            }
            let record = Record::from(
                self.page.get_buf(),
                self.offset,
                &self.page.get_table().schema,
            );
            let slot = self.slot;
            let offset = self.offset;
            self.inc();
            return Some((record, slot, offset));
        }

        None
    }
}
