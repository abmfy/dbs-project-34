//! Table management.

use bit_set::BitSet;
use uuid::Uuid;

use crate::config::LINK_SIZE;
use crate::error::Result;
use crate::file::PageCache;
use crate::record::Record;
use crate::schema::TableSchema;

/// A table.
pub struct Table {
    /// The table's fd.
    fd: Uuid,
    /// The table's schema.
    schema: TableSchema,
    /// Maximum number of records in the page.
    max_records: usize,
    /// The size of the free slot bitmap.
    free_bitmap_size: usize,
}

impl Table {
    /// Create a new table.
    pub fn new(fd: Uuid, schema: TableSchema) -> Self {
        let max_records = schema.get_max_records();
        let free_bitmap_size = schema.get_free_bitmap_size();
        Self {
            fd,
            schema,
            max_records,
            free_bitmap_size,
        }
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
    pub fn new_page<'a>(&'a mut self, fs: &'a mut PageCache) -> Result<TablePage> {
        let page_id = self.schema.new_page()?;
        log::debug!("Allocating new page {page_id}");

        if let Some(next_page_id) = self.schema.get_free() {
            let next_page_buf = fs.get_mut(self.fd, next_page_id)?;
            let mut next_page =
                TablePage::new(self, next_page_buf, self.max_records, self.free_bitmap_size);
            next_page.set_prev(Some(page_id));
        }

        let free = self.schema.get_free();
        self.schema.set_free(Some(page_id));

        let page_buf = fs.get_mut(self.fd, page_id)?;
        let mut page = TablePage::new(self, page_buf, self.max_records, self.free_bitmap_size);
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
        let page = TablePage::new(self, page_buf, self.max_records, self.free_bitmap_size);

        let prev = page.get_prev();
        let next = page.get_next();

        drop(page);

        if let Some(prev_page_id) = prev {
            let prev_page_buf = fs.get_mut(self.fd, prev_page_id as usize)?;
            let mut prev_page =
                TablePage::new(self, prev_page_buf, self.max_records, self.free_bitmap_size);
            prev_page.set_next(next);
        }

        if let Some(next_page_id) = next {
            let next_page_buf = fs.get_mut(self.fd, next_page_id as usize)?;
            let mut next_page =
                TablePage::new(self, next_page_buf, self.max_records, self.free_bitmap_size);
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
            let free_page_buf = fs.get_mut(self.fd, free_page_id as usize)?;
            let mut free_page =
                TablePage::new(self, free_page_buf, self.max_records, self.free_bitmap_size);
            free_page.set_prev(Some(page_id));
        }

        let page_buf = fs.get_mut(self.fd, page_id)?;
        let mut page = TablePage::new(self, page_buf, self.max_records, self.free_bitmap_size);

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
            let full_page_buf = fs.get_mut(self.fd, full_page_id as usize)?;
            let mut full_page =
                TablePage::new(self, full_page_buf, self.max_records, self.free_bitmap_size);
            full_page.set_prev(Some(page_id));
        }

        let page_buf = fs.get_mut(self.fd, page_id)?;
        let mut page = TablePage::new(self, page_buf, self.max_records, self.free_bitmap_size);

        page.set_prev(None);
        page.set_next(full);

        self.schema.set_full(Some(page_id));

        Ok(())
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
        let mut page = TablePage::new(self, page_buf, self.max_records, self.free_bitmap_size);

        if !page.insert(record, &self.schema) {
            log::debug!("A page is filled");
            self.full_page(fs, page_id)?;
        }

        Ok(())
    }
}

/// A page in a table.
pub struct TablePage<'a> {
    /// The table the page belongs to.
    table: &'a Table,
    /// The buffer of the page.
    buf: &'a mut [u8],
    /// Maximum number of records in the page.
    max_records: usize,
    /// The size of the free slot bitmap.
    free_bitmap_size: usize,
    /// First free slot.
    free: Option<usize>,
    /// Free slot bitmap.
    occupied: BitSet,
}

impl<'a> TablePage<'a> {
    /// Create a new page object representing a page in a buffer.
    pub fn new(
        table: &'a Table,
        buf: &'a mut [u8],
        max_records: usize,
        free_bitmap_size: usize,
    ) -> Self {
        let occupied = BitSet::from_bytes(&buf[2 * LINK_SIZE..2 * LINK_SIZE + free_bitmap_size]);
        let free = (0..max_records)
            .find(|i| !occupied.contains(*i))
            .or_else(|| None);
        Self {
            table,
            buf,
            max_records,
            free_bitmap_size,
            free,
            occupied,
        }
    }

    /// Get the previous page number.
    pub fn get_prev(&self) -> Option<usize> {
        let prev = &self.buf[..LINK_SIZE];
        let prev = u32::from_le_bytes(prev.try_into().unwrap());
        if prev == 0 {
            None
        } else {
            Some((prev - 1) as usize)
        }
    }

    /// Set the previous page number.
    pub fn set_prev(&mut self, prev: Option<usize>) {
        let prev = prev.map(|p| p as u32);
        let prev = prev.map_or(0, |p| p + 1);
        let prev = prev.to_le_bytes();
        self.buf[..LINK_SIZE].copy_from_slice(&prev);
    }

    /// Get the next page number.
    pub fn get_next(&self) -> Option<usize> {
        let next = &self.buf[LINK_SIZE..2 * LINK_SIZE];
        let next = u32::from_le_bytes(next.try_into().unwrap());
        if next == 0 {
            None
        } else {
            Some((next - 1) as usize)
        }
    }

    /// Set the next page number.
    pub fn set_next(&mut self, next: Option<usize>) {
        let next = next.map(|n| n as u32);
        let next = next.map_or(0, |n| n + 1);
        let next = next.to_le_bytes();
        self.buf[LINK_SIZE..2 * LINK_SIZE].copy_from_slice(&next);
    }

    /// Check if the i-th slot is free.
    pub fn is_free(&self, i: usize) -> bool {
        !self.occupied.contains(i)
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
            .or_else(|| None);
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
        record.save_into(&mut self.buf, offset, schema);

        self.free.is_some()
    }
}
