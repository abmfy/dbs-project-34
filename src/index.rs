//! B+ Tree index management.
//!
//! Different from table pages, there are only one linked list
//! in a index storing empty pages.
//!
//! # Page Header Structure
//!
//! | leaf | *align* | size | prev | next | parent |
//! |------|---------|------|------|------|--------|
//! |   1B |      3B |   4B |   4B |   4B |     4B |
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

use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::{LINK_SIZE, PAGE_SIZE};
use crate::error::Result;
use crate::file::PageCache;
use crate::record::{Record, RecordSchema};
use crate::schema::{Column, TableSchema, Type};

const LEAF_OFFSET: usize = 0;
const SIZE_OFFSET: usize = LINK_SIZE;
const PREV_OFFSET: usize = LINK_SIZE * 2;
const NEXT_OFFSET: usize = LINK_SIZE * 3;
const PARENT_OFFSET: usize = LINK_SIZE * 4;
const HEADER_SIZE: usize = LINK_SIZE * 5;

// Utility functions for manipulating integers.

/// Deserialize an integer.
fn from_int(buf: &[u8]) -> usize {
    let int = u32::from_le_bytes(buf.try_into().unwrap());
    int as usize
}

/// Serialize an integer.
fn to_int(buf: &mut [u8], int: usize) {
    let int = int as u32;
    buf.copy_from_slice(&int.to_le_bytes());
}

/// Deserialize a nullable int, which utilizes 0 to represent null.
fn from_nullable_int(buf: &[u8]) -> Option<usize> {
    let int = u32::from_le_bytes(buf.try_into().unwrap());
    if int == 0 {
        None
    } else {
        Some((int - 1) as usize)
    }
}

/// Serialize a nullable int, which utilizes 0 to represent null.
fn to_nullable_int(buf: &mut [u8], int: Option<usize>) {
    let int = if let Some(int) = int { int + 1 } else { 0 } as u32;
    buf.copy_from_slice(&int.to_le_bytes());
}

/// Index schema.
#[derive(Clone, Debug, Deserialize, Serialize)]
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
    pub columns: Vec<String>,
    /// Root page id.
    pub root: Option<usize>,
}

impl IndexSchema {
    pub fn new(explicit: bool, prefix: Option<&str>, name: Option<&str>, columns: &[&str]) -> Self {
        let mut name = if let Some(name) = name {
            name.to_owned()
        } else {
            format!("annoy.{}", columns.join("_"))
        };
        if let Some(prefix) = prefix {
            name = format!("{}.{}", prefix, name);
        }
        if !explicit {
            name.push_str(".implicit");
        }
        Self {
            pages: 0,
            free: None,
            explicit,
            name,
            columns: columns.iter().map(|col| col.to_string()).collect(),
            root: None,
        }
    }
}

impl Display for IndexSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "INDEX {}({});", self.name, self.columns.join(", "))
    }
}

/// An iterator over leaf nodes.
///
/// Due to lifetime constraints, we couldn't implement
/// `Iterator` trait for this.
///
/// # Fields
///
/// `(page, slot)`
pub type LeafIterator = (usize, usize);

/// The result of an index search.
///
/// # Fields
///
/// `(keys, page, slot)`
pub type IndexResult = (Record, usize, usize);

/// An index.
pub struct Index {
    /// The index's fd.
    fd: Uuid,
    /// Path to the schema, for serialization.
    path: PathBuf,
    /// The index's schema.
    schema: IndexSchema,
    /// Size of indexed columns.
    index_size: usize,
    /// Columns in the index.
    columns: Vec<Column>,
    /// Record schema for internal nodes.
    internal_schema: IndexRecordSchema,
    /// Record schema for leaf nodes.
    leaf_schema: IndexRecordSchema,
}

impl Index {
    /// Create a new index.
    ///
    /// # Panics
    ///
    /// Panics if column name not found in table schema.
    ///
    /// Please check the schema before creating an index.
    pub fn new(fd: Uuid, schema: IndexSchema, path: &Path, table: &TableSchema) -> Self {
        let columns: Vec<Column> = schema
            .columns
            .iter()
            .map(|col| table.get_column(col).clone())
            .collect();
        let null_bitmap_size = columns.len().div_ceil(8);
        let index_size = columns.iter().map(|col| col.typ.size()).sum::<usize>() + null_bitmap_size;
        let internal_schema = IndexRecordSchema::from(&columns, false);
        let leaf_schema = IndexRecordSchema::from(&columns, true);
        Self {
            fd,
            path: path.to_owned(),
            schema,
            index_size,
            columns,
            internal_schema,
            leaf_schema,
        }
    }

    /// Save changes into the schema file.
    fn save(&self) -> Result<()> {
        log::debug!("Saving schema to {}", self.path.display());
        let file = File::create(&self.path)?;
        serde_json::to_writer(file, &self.schema)?;
        Ok(())
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

    /// Allocate an empty page for use.
    pub fn new_page(&mut self, fs: &mut PageCache) -> Result<usize> {
        let free_page_id = self.schema.free;
        if let Some(page_id) = free_page_id {
            let page_buf = fs.get(self.fd, page_id)?;
            let next = from_nullable_int(&page_buf[..LINK_SIZE]);
            self.schema.free = next;
            Ok(page_id)
        } else {
            let page_id = self.schema.pages;
            self.schema.pages += 1;
            Ok(page_id)
        }
    }

    /// Mark a page as free.
    pub fn free_page(&mut self, fs: &mut PageCache, page_id: usize) -> Result<()> {
        let free_page_id = self.schema.free;
        let page_buf = fs.get_mut(self.fd, page_id)?;
        to_nullable_int(&mut page_buf[..LINK_SIZE], free_page_id);
        self.schema.free = Some(page_id);
        Ok(())
    }

    /// Lookup the index of a children in its parent.
    fn lookup(
        &self,
        fs: &mut PageCache,
        parent_id: usize,
        child_id: usize,
    ) -> Result<Option<usize>> {
        log::info!("Looking up pos of {child_id} in {parent_id}");

        let buf = fs.get(self.fd, child_id)?;
        let page = IndexPage::from_buf(self, buf);
        let key = page.get_record(page.get_size() - 1);
        log::info!("Looking up for key {key:?}");

        let buf = fs.get(self.fd, parent_id)?;
        let page = IndexPage::from_buf(self, buf);

        let pos = self.find(&page, &key);
        // For case when deleted the max key
        let pos = if pos > 0 { pos - 1 } else { 0 };
        for i in pos..page.get_size() {
            let record = page.get_record(i);
            log::info!("Slot {i} in parent is {record:?}");
            if record.get_child() == child_id {
                return Ok(Some(i));
            }
            if record > key {
                break;
            }
        }
        Ok(None)
    }

    /// Find the first children with a key greater than or equal to the given key.
    ///
    /// Returns the last if no such children exists.
    ///
    /// This function uses binary search because otherwise it will cost too much time.
    fn find<'a, T: LinkedIndexPage<'a>>(&'a self, page: &'a T, key: &Record) -> usize {
        log::info!("Finding key {key:?}");

        let size = page.get_size();
        log::debug!("Size of this index record is {size}");
        let mut ret = size - 1;

        let mut l: i32 = 0;
        let mut r: i32 = size as i32 - 1;
        while l <= r {
            log::info!("Current range is [{l}, {r}]");
            let mid = (l + r) / 2;
            let record = page.get_record(mid as usize);
            log::info!("Comparing with {mid}: {record:?}");
            if &record < key {
                l = mid + 1;
            } else {
                r = mid - 1;
                ret = mid as usize;
            }
        }
        log::info!("Find results in {ret}");
        ret
    }

    /// Get the leaf node using a key.
    pub fn index(&self, fs: &mut PageCache, key: &Record) -> Result<Option<LeafIterator>> {
        log::debug!("Indexing {key:?}");

        let root_page_id = if let Some(page_id) = self.schema.root {
            page_id
        } else {
            // Tree empty
            return Ok(None);
        };

        log::debug!("Root is {root_page_id}");

        let mut page_id = root_page_id;
        let mut page_buf = fs.get(self.fd, page_id)?;
        let mut page = IndexPage::from_buf(self, page_buf);
        while !page.is_leaf() {
            let pos = self.find(&page, key);
            page_id = page.get_record(pos).get_child();
            log::debug!("Walk into {page_id}");
            page_buf = fs.get(self.fd, page_id)?;
            page = IndexPage::from_buf(self, page_buf);
        }

        log::debug!("Found leaf page {page_id}");

        // Find the correct position to insert
        let mut pos = self.find(&page, key);
        log::debug!("Position is  {pos}");
        while &page.get_record(pos) < key {
            pos += 1;
            for (record, slot, _) in page.iter().skip(pos) {
                pos = slot;
                if &record >= key {
                    break;
                }
            }
            if pos == page.get_size() {
                // Go along the linked list
                log::debug!("Go along the linked list");
                if let Some(next) = page.get_next() {
                    page_id = next;
                    log::debug!("Walk into {page_id}");
                    page_buf = fs.get(self.fd, page_id)?;
                    page = IndexPage::from_buf(self, page_buf);
                    pos = 0;
                } else {
                    // No more pages
                    log::debug!("No more pages");
                    break;
                }
            }
        }

        log::debug!("Found at {page_id} {pos}");

        Ok(Some((page_id, pos)))
    }

    /// Get the index record using a iterator.
    pub fn get_record(&self, fs: &mut PageCache, iter: LeafIterator) -> Result<IndexResult> {
        let (page_id, slot) = iter;
        let buf = fs.get(self.fd, page_id)?;
        let page = IndexPage::from_buf(self, buf);
        let record = page.get_record(slot);
        let (page, slot) = record.get_index();
        let record = Record::new(record.into_keys());
        Ok((record, page, slot))
    }

    /// Increment a leaf iterator.
    pub fn inc_iter(&self, fs: &mut PageCache, iter: LeafIterator) -> Result<Option<LeafIterator>> {
        let (page_id, slot) = iter;
        let buf = fs.get(self.fd, page_id)?;
        let page = IndexPage::from_buf(self, buf);
        if slot + 1 < page.get_size() {
            Ok(Some((page_id, slot + 1)))
        } else {
            let next = page.get_next();
            if let Some(next) = next {
                Ok(Some((next, 0)))
            } else {
                Ok(None)
            }
        }
    }

    /// Split one page into two.
    fn split(&mut self, fs: &mut PageCache, page_id: usize, new_page_id: usize) -> Result<()> {
        log::debug!("Splitting {page_id}, generating {new_page_id}");

        let buf = fs.get_mut(self.fd, page_id)?;
        let mut page = IndexPageMut::from_buf(self, buf);
        let size = page.get_size();
        let mid = size / 2;

        let records = page.remove_range(mid..size);

        let new_buf = fs.get_mut(self.fd, new_page_id)?;
        let mut new_page = IndexPageMut::from_buf(self, new_buf);
        new_page.insert_range(0, records);

        if !new_page.is_leaf() {
            // Update parent
            let records: Vec<_> = new_page.get_record_range(0..new_page.get_size());
            for record in records {
                let child = record.get_child();
                let child_buf = fs.get_mut(self.fd, child)?;
                let mut child_page = IndexPageMut::from_buf(self, child_buf);
                child_page.set_parent(Some(new_page_id));
            }
        }

        Ok(())
    }

    /// Insert a key into the index.
    pub fn insert(
        &mut self,
        fs: &mut PageCache,
        key: Record,
        page: usize,
        slot: usize,
    ) -> Result<()> {
        log::debug!("Adding ({key:?}, {page}, {slot}) into index");

        let record = Record::new_with_index(key.fields, page, slot);
        if self.schema.root.is_none() {
            // Tree empty
            let page_id = self.new_page(fs)?;
            self.schema.root = Some(page_id);
            let page_buf = fs.get_mut(self.fd, page_id)?;
            let mut page = IndexPageMut::new(self, page_buf, true);
            page.insert(0, record);
            return Ok(());
        };

        // Find the leaf page and slot to insert
        let (page_id, slot) = self.index(fs, &record)?.unwrap();
        let buf = fs.get_mut(self.fd, page_id)?;
        let mut page = IndexPageMut::from_buf(self, buf);
        page.insert(slot, record);

        // Split if overflow
        let mut curr_page_id = Some(page_id);
        while let Some(page_id) = curr_page_id {
            let buf = fs.get(self.fd, page_id)?;
            let page = IndexPage::from_buf(self, buf);
            let is_leaf = page.is_leaf();
            let curr_next = page.get_next();
            if page.is_overflow() {
                if page.get_parent().is_none() {
                    // Split root
                    let new_page_id = self.new_page(fs)?;
                    let new_buf = fs.get_mut(self.fd, new_page_id)?;
                    let mut new_page = IndexPageMut::new(self, new_buf, is_leaf);

                    new_page.set_prev(Some(page_id));
                    new_page.set_next(curr_next);

                    let buf = fs.get_mut(self.fd, page_id)?;
                    let mut page = IndexPageMut::from_buf(self, buf);

                    page.set_next(Some(new_page_id));

                    self.split(fs, page_id, new_page_id)?;

                    // Create a new root
                    let new_root_page_id = self.new_page(fs)?;
                    log::debug!("Splitting root, new root is {new_root_page_id}");
                    self.schema.root = Some(new_root_page_id);

                    // Update parent, and read max key
                    let buf = fs.get_mut(self.fd, page_id)?;
                    let mut page = IndexPageMut::from_buf(self, buf);
                    page.set_parent(Some(new_root_page_id));
                    let max_key = page.get_record(page.get_size() - 1).into_keys();
                    log::debug!("Set parent of {page_id} to {new_root_page_id}");

                    let new_buf = fs.get_mut(self.fd, new_page_id)?;
                    let mut new_page = IndexPageMut::from_buf(self, new_buf);
                    new_page.set_parent(Some(new_root_page_id));
                    let new_max_key = new_page.get_record(new_page.get_size() - 1).into_keys();
                    log::debug!("Set parent of {new_page_id} to {new_root_page_id}");

                    // Insert the split pages into the new root
                    let new_root_buf = fs.get_mut(self.fd, new_root_page_id)?;
                    let mut new_root_page = IndexPageMut::new(self, new_root_buf, false);

                    new_root_page.insert(0, Record::new_with_child(max_key, page_id));
                    new_root_page.insert(1, Record::new_with_child(new_max_key, new_page_id));
                } else {
                    // Split non-root
                    let parent_id = page.get_parent().unwrap();
                    let new_page_id = self.new_page(fs)?;
                    let new_buf = fs.get_mut(self.fd, new_page_id)?;
                    let mut new_page = IndexPageMut::new(self, new_buf, is_leaf);

                    new_page.set_prev(Some(page_id));
                    new_page.set_next(curr_next);

                    let buf = fs.get_mut(self.fd, page_id)?;
                    let mut page = IndexPageMut::from_buf(self, buf);

                    page.set_next(Some(new_page_id));

                    self.split(fs, page_id, new_page_id)?;

                    // Update parent, and read max key
                    let new_buf = fs.get_mut(self.fd, new_page_id)?;
                    let mut new_page = IndexPageMut::from_buf(self, new_buf);
                    new_page.set_parent(Some(parent_id));
                    let new_max_key = new_page.get_record(new_page.get_size() - 1).into_keys();

                    // Insert the new split page into the parent
                    let pos = self.lookup(fs, parent_id, page_id)?;
                    let pos = pos.map_or(0, |n| n + 1);

                    let parent_buf = fs.get_mut(self.fd, parent_id)?;
                    let mut parent_page = IndexPageMut::from_buf(self, parent_buf);
                    parent_page.insert(pos, Record::new_with_child(new_max_key, new_page_id));
                }
            }

            let buf = fs.get(self.fd, page_id)?;
            let page = IndexPage::from_buf(self, buf);
            curr_page_id = page.get_parent();

            // Update keys in parent page
            if let Some(parent_page_id) = page.get_parent() {
                let buf = fs.get_mut(self.fd, page_id)?;
                let page = IndexPage::from_buf(self, buf);
                let max_key = page.get_record(page.get_size() - 1).into_keys();

                let pos = self.lookup(fs, parent_page_id, page_id)?;
                let pos = pos.unwrap();

                let parent_buf = fs.get_mut(self.fd, parent_page_id)?;
                let mut parent_page = IndexPageMut::from_buf(self, parent_buf);
                parent_page.set_record(pos, Record::new_with_child(max_key, page_id));
            }
        }

        Ok(())
    }

    /// Remove a key from the index.
    pub fn remove(
        &mut self,
        fs: &mut PageCache,
        key: Record,
        page: usize,
        slot: usize,
    ) -> Result<()> {
        log::info!("Removing ({key:?}, {page}, {slot}) from index");

        assert_ne!(self.schema.root, None, "Removing from empty index");

        // Find the position to remove
        let mut iter = self.index(fs, &key)?.expect("Removing from empty index");
        loop {
            let (curr_key, curr_page, curr_slot) = self.get_record(fs, iter)?;
            log::info!("Current index record: ({curr_key:?}, {curr_page}, {curr_slot})");
            assert_eq!(curr_key, key, "Removing non-existing key");
            if curr_page == page && curr_slot == slot {
                let (page_id, slot) = iter;
                let buf = fs.get_mut(self.fd, page_id)?;
                let mut page = IndexPageMut::from_buf(self, buf);
                log::info!("Size of {page_id} is {} before removal", page.get_size());
                page.remove(slot);
                self.resolve(fs, page_id)?;
                return Ok(());
            }
            iter = self.inc_iter(fs, iter)?.expect("Removing non-existing key");
        }
    }

    /// Recursively update the max key of nodes.
    fn update_key(&mut self, fs: &mut PageCache, page_id: usize) -> Result<()> {
        let mut curr_page_id = page_id;
        loop {
            let buf = fs.get_mut(self.fd, curr_page_id)?;
            let page = IndexPage::from_buf(self, buf);
            if page.get_size() == 0 {
                log::info!("Page {page_id} is empty");
                break;
            }
            let max_key = page.get_record(page.get_size() - 1).into_keys();
            let parent_page_id = page.get_parent();

            if let Some(parent_page_id) = parent_page_id {
                let pos = self.lookup(fs, parent_page_id, curr_page_id)?.unwrap();

                let parent_buf = fs.get_mut(self.fd, parent_page_id)?;
                let mut parent_page = IndexPageMut::from_buf(self, parent_buf);
                parent_page.set_record(pos, Record::new_with_child(max_key, curr_page_id));

                curr_page_id = parent_page_id;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Unlink a node from its parent.
    fn unlink(&mut self, fs: &mut PageCache, parent_id: usize, child_id: usize) -> Result<()> {
        log::info!("Unlinking page {child_id} from {parent_id}");

        let pos = self.lookup(fs, parent_id, child_id)?.unwrap();

        let parent_buf = fs.get_mut(self.fd, parent_id).unwrap();
        let mut parent_page = IndexPageMut::from_buf(self, parent_buf);
        parent_page.remove(pos);

        Ok(())
    }

    /// Resolve underflow.
    fn resolve(&mut self, fs: &mut PageCache, mut page_id: usize) -> Result<()> {
        log::info!("Resolving underflow in page {page_id}");

        let buf = fs.get(self.fd, page_id)?;
        let page = IndexPage::from_buf(self, buf);
        let mut prev_id = page.get_prev();
        let mut next_id = page.get_next();
        let mut parent_id = page.get_parent();
        let mut is_underflow = page.is_underflow();
        log::info!("Size of page {page_id} is {}", page.get_size());
        while is_underflow {
            if self.borrow(fs, prev_id, Some(page_id))? {
                log::info!("Borrowing from the left sibling");
                self.update_key(fs, prev_id.unwrap())?;
                self.update_key(fs, page_id)?;
            } else if self.borrow(fs, Some(page_id), next_id)? {
                log::info!("Borrowing from the right sibling");
                self.update_key(fs, page_id)?;
                self.update_key(fs, next_id.unwrap())?;
            } else if self.merge(fs, prev_id, Some(page_id), true)? {
                log::info!("Merging into the left sibling");
                self.update_key(fs, prev_id.unwrap())?;

                // Free merged page
                self.unlink(fs, parent_id.unwrap(), page_id)?;
                self.free_page(fs, page_id)?;

                page_id = parent_id.expect("Root nodes will not underflow");
                let buf = fs.get(self.fd, page_id)?;
                let page = IndexPage::from_buf(self, buf);
                prev_id = page.get_prev();
                next_id = page.get_next();
                parent_id = page.get_parent();
                is_underflow = page.is_underflow();
            } else if self.merge(fs, Some(page_id), next_id, false)? {
                log::info!("Merging into the right sibling");
                self.update_key(fs, next_id.unwrap())?;

                // Free merged page
                self.unlink(fs, parent_id.unwrap(), page_id)?;
                self.free_page(fs, page_id)?;

                page_id = parent_id.expect("Root nodes will not underflow");
                let buf = fs.get(self.fd, page_id)?;
                let page = IndexPage::from_buf(self, buf);
                prev_id = page.get_prev();
                next_id = page.get_next();
                parent_id = page.get_parent();
                is_underflow = page.is_underflow();
            } else {
                unreachable!("Failed to resolve underflow");
            }
        }

        log::info!("Underflow resolved");
        self.update_key(fs, page_id)?;

        let root_id = self
            .schema
            .root
            .expect("Root node should exist at this point");

        let root_buf = fs.get(self.fd, root_id)?;
        let root_page = IndexPage::from_buf(self, root_buf);
        let root_size = root_page.get_size();

        // Delete the root node if it has only one child
        if !root_page.is_leaf() && root_size == 1 {
            log::info!("Deleting root node because it has only one child");

            let new_root_id = root_page.get_record(0).get_child();
            self.schema.root = Some(new_root_id);
            self.free_page(fs, root_id)?;

            let new_root_buf = fs.get_mut(self.fd, new_root_id)?;
            let mut new_root_page = IndexPageMut::from_buf(self, new_root_buf);
            new_root_page.set_parent(None);
        }

        // Delete the root node if it is empty
        if root_size == 0 {
            log::info!("Deleting root node because it is empty");

            self.schema.root = None;
            self.free_page(fs, root_id)?;
        }

        Ok(())
    }

    /// Try to borrow some records from a sibling.
    ///
    /// # Returns
    ///
    /// Returns false if failed to borrow.
    fn borrow(
        &mut self,
        fs: &mut PageCache,
        left_id: Option<usize>,
        right_id: Option<usize>,
    ) -> Result<bool> {
        if left_id.is_none() || right_id.is_none() {
            return Ok(false);
        }

        let (left_id, right_id) = (left_id.unwrap(), right_id.unwrap());

        let left_buf = fs.get(self.fd, left_id)?;
        let left_page = IndexPage::from_buf(self, left_buf);
        let left_size = left_page.get_size();

        let max_records = left_page.get_max_records();

        let right_buf = fs.get(self.fd, right_id)?;
        let right_page = IndexPage::from_buf(self, right_buf);
        let right_size = right_page.get_size();

        let total_size = left_size + right_size;

        // Can't borrow if total size is less than half of max size
        if total_size < (max_records + 1) / 2 * 2 {
            return Ok(false);
        }

        log::info!("Borrowing nodes between {left_id} and {right_id}");

        let left_new_size = total_size / 2;
        let right_new_size = total_size - left_new_size;

        if left_size > left_new_size {
            log::info!(
                "Moving {} records from {left_id} to {right_id}",
                left_size - left_new_size
            );

            let left_buf = fs.get_mut(self.fd, left_id)?;
            let mut left_page = IndexPageMut::from_buf(self, left_buf);
            let records = left_page.remove_range(left_new_size..left_size);

            let children: Option<Vec<_>> = if !left_page.is_leaf() {
                Some(records.iter().map(|r| r.get_child()).collect())
            } else {
                None
            };

            let right_buf = fs.get_mut(self.fd, right_id)?;
            let mut right_page = IndexPageMut::from_buf(self, right_buf);
            right_page.insert_range(0, records);

            // Update parent information of children
            if let Some(children) = children {
                for child in children {
                    let child_buf = fs.get_mut(self.fd, child)?;
                    let mut child_page = IndexPageMut::from_buf(self, child_buf);
                    child_page.set_parent(Some(right_id));
                }
            }
        }

        if right_size > right_new_size {
            log::info!(
                "Moving {} records from {right_id} to {left_id}",
                right_size - right_new_size
            );

            let right_buf = fs.get_mut(self.fd, right_id)?;
            let mut right_page = IndexPageMut::from_buf(self, right_buf);
            let records = right_page.remove_range(0..right_size - right_new_size);

            let children: Option<Vec<_>> = if !right_page.is_leaf() {
                Some(records.iter().map(|r| r.get_child()).collect())
            } else {
                None
            };

            let left_buf = fs.get_mut(self.fd, left_id)?;
            let mut left_page = IndexPageMut::from_buf(self, left_buf);
            left_page.insert_range(left_size, records);

            // Update parent information of children
            if let Some(children) = children {
                for child in children {
                    let child_buf = fs.get_mut(self.fd, child)?;
                    let mut child_page = IndexPageMut::from_buf(self, child_buf);
                    child_page.set_parent(Some(left_id));
                }
            }
        }

        Ok(true)
    }

    /// Merge two nodes into one.
    ///
    /// # Returns
    ///
    /// Returns false if failed to merge.
    fn merge(
        &mut self,
        fs: &mut PageCache,
        left_id: Option<usize>,
        right_id: Option<usize>,
        into_left: bool,
    ) -> Result<bool> {
        if left_id.is_none() || right_id.is_none() {
            return Ok(false);
        }

        let (left_id, right_id) = (left_id.unwrap(), right_id.unwrap());

        let left_buf = fs.get(self.fd, left_id)?;
        let left_page = IndexPage::from_buf(self, left_buf);
        let left_size = left_page.get_size();

        let max_records = left_page.get_max_records();

        let right_buf = fs.get(self.fd, right_id)?;
        let right_page = IndexPage::from_buf(self, right_buf);
        let right_size = right_page.get_size();

        let total_size = left_size + right_size;

        log::info!("Left size is {left_size}, right size is {right_size}");


        // Can't merge if total size is greater than max size
        if total_size > max_records {
            return Ok(false);
        }

        log::info!("Merging nodes between {left_id} and {right_id}");

        if into_left {
            log::info!("Merging {right_id} into {left_id}");

            let right_buf = fs.get(self.fd, right_id)?;
            let right_page = IndexPage::from_buf(self, right_buf);
            let records = right_page.get_record_range(0..right_size);
            let right_next = right_page.get_next();

            let children: Option<Vec<_>> = if !right_page.is_leaf() {
                Some(records.iter().map(|r| r.get_child()).collect())
            } else {
                None
            };

            let left_buf = fs.get_mut(self.fd, left_id)?;
            let mut left_page = IndexPageMut::from_buf(self, left_buf);
            left_page.insert_range(left_size, records);

            // Update link information
            left_page.set_next(right_next);
            if let Some(right_next_id) = right_next {
                let right_next_buf = fs.get_mut(self.fd, right_next_id)?;
                let mut right_next_page = IndexPageMut::from_buf(self, right_next_buf);
                right_next_page.set_prev(Some(left_id));
            }

            // Update parent information of children
            if let Some(children) = children {
                for child in children {
                    let child_buf = fs.get_mut(self.fd, child)?;
                    let mut child_page = IndexPageMut::from_buf(self, child_buf);
                    child_page.set_parent(Some(left_id));
                }
            }
        } else {
            log::info!("Merging {left_id} into {right_id}");

            let left_buf = fs.get(self.fd, left_id)?;
            let left_page = IndexPage::from_buf(self, left_buf);
            let records = left_page.get_record_range(0..left_size);
            let left_prev = left_page.get_prev();

            let children: Option<Vec<_>> = if !left_page.is_leaf() {
                Some(records.iter().map(|r| r.get_child()).collect())
            } else {
                None
            };

            let right_buf = fs.get_mut(self.fd, right_id)?;
            let mut right_page = IndexPageMut::from_buf(self, right_buf);
            right_page.insert_range(0, records);

            // Update link information
            right_page.set_prev(left_prev);
            if let Some(left_prev_id) = left_prev {
                let left_prev_buf = fs.get_mut(self.fd, left_prev_id)?;
                let mut left_prev_page = IndexPageMut::from_buf(self, left_prev_buf);
                left_prev_page.set_next(Some(right_id));
            }

            // Update parent information of children
            if let Some(children) = children {
                for child in children {
                    let child_buf = fs.get_mut(self.fd, child)?;
                    let mut child_page = IndexPageMut::from_buf(self, child_buf);
                    child_page.set_parent(Some(right_id));
                }
            }
        }

        Ok(true)
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        if let Err(err) = self.save() {
            log::error!("Failed to save index schema: {err}")
        }
    }
}

/// Schema for internal nodes in an index.
pub struct IndexRecordSchema {
    columns: Vec<Column>,
    cmp_keys: usize,
}

impl IndexRecordSchema {
    pub fn from(columns: &[Column], is_leaf: bool) -> Self {
        let mut columns = columns.to_vec();
        let cmp_keys = columns.len();

        if is_leaf {
            let page = Column::new("__page__".to_owned(), Type::Int, false, None).unwrap();
            let slot = Column::new("__slot__".to_owned(), Type::Int, false, None).unwrap();
            columns.extend([page, slot]);
        } else {
            let child = Column::new("__child__".to_owned(), Type::Int, false, None).unwrap();
            columns.push(child);
        }

        Self { columns, cmp_keys }
    }
}

impl RecordSchema for IndexRecordSchema {
    fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    fn get_cmp_keys(&self) -> usize {
        self.cmp_keys
    }
}

/// Common behaviors between IndexPage and IndexPageMut;
/// For code reuse.
pub trait LinkedIndexPage<'a> {
    /// Get the index.
    fn get_index(&self) -> &'a Index;

    /// Get the buffer.
    fn get_buf(&self) -> &[u8];

    /// Get the size of a record in this page.
    fn get_record_size(&self) -> usize;

    /// Get the maximum number of records in this page.
    fn get_max_records(&self) -> usize;

    /// Is this page a root node.
    fn is_root(&self) -> bool {
        self.get_parent().is_none()
    }

    /// Is this page a leaf node.
    fn is_leaf(&self) -> bool {
        self.get_buf()[LEAF_OFFSET] == 1
    }

    /// Get number of records in this page.
    fn get_size(&self) -> usize {
        from_int(&self.get_buf()[SIZE_OFFSET..SIZE_OFFSET + LINK_SIZE])
    }

    /// Get the previous node in the linked list for leaf nodes.
    fn get_prev(&self) -> Option<usize> {
        from_nullable_int(&self.get_buf()[PREV_OFFSET..PREV_OFFSET + LINK_SIZE])
    }

    /// Get the next node in the linked list for leaf nodes.
    fn get_next(&self) -> Option<usize> {
        from_nullable_int(&self.get_buf()[NEXT_OFFSET..NEXT_OFFSET + LINK_SIZE])
    }

    /// Get the parent node.
    fn get_parent(&self) -> Option<usize> {
        from_nullable_int(&self.get_buf()[PARENT_OFFSET..PARENT_OFFSET + LINK_SIZE])
    }

    /// Is this page full.
    fn is_full(&self) -> bool {
        self.get_size() == self.get_max_records()
    }

    /// Is this page overflow.
    fn is_overflow(&self) -> bool {
        self.get_size() > self.get_max_records()
    }

    /// Is this page underflow.
    fn is_underflow(&self) -> bool {
        !self.is_root() && self.get_size() < (self.get_max_records() + 1) / 2
    }

    /// Get a record from the page using a slot id.
    fn get_record(&'a self, slot: usize) -> Record {
        let offset = HEADER_SIZE + slot * self.get_record_size();
        Record::from(self.get_buf(), offset, self.get_record_schema())
    }

    /// Get records from the page using a range of slot id.
    fn get_record_range(&'a self, slots: Range<usize>) -> Vec<Record> {
        slots.map(|slot| self.get_record(slot)).collect()
    }

    /// Get the record schema of the index.
    fn get_record_schema(&'a self) -> &'a IndexRecordSchema {
        if self.is_leaf() {
            &self.get_index().leaf_schema
        } else {
            &self.get_index().internal_schema
        }
    }

    /// Get an iterator over records in the page.
    fn iter(&'a self) -> PageIterator<'a, Self>
    where
        Self: Sized,
    {
        PageIterator::new(self)
    }
}

/// A read-only page in an index.
struct IndexPage<'a> {
    /// The index the page belongs to.
    index: &'a Index,
    /// The buffer of the index.
    buf: &'a [u8],
    /// The size of a record.
    record_size: usize,
    /// Maximum number of records in the page.
    max_records: usize,
}

impl<'a> IndexPage<'a> {
    /// Read an index page from buffer.
    fn from_buf(index: &'a Index, buf: &'a [u8]) -> Self {
        let leaf = buf[LEAF_OFFSET] == 1;

        let record_size = if leaf {
            index.leaf_schema.get_record_size()
        } else {
            index.internal_schema.get_record_size()
        };

        // -1 for the record that will be inserted
        let max_records = (PAGE_SIZE - HEADER_SIZE) / record_size - 1;

        Self {
            index,
            buf,
            record_size,
            max_records,
        }
    }
}

impl<'a> LinkedIndexPage<'a> for IndexPage<'a> {
    fn get_index(&self) -> &'a Index {
        self.index
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
}

impl<'a> IntoIterator for &'a IndexPage<'a> {
    type Item = (Record, usize, usize);
    type IntoIter = PageIterator<'a, IndexPage<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A writable page in an index.
struct IndexPageMut<'a> {
    /// The index the page belongs to.
    index: &'a Index,
    /// The buffer of the index.
    buf: &'a mut [u8],
    /// The size of a record.
    record_size: usize,
    /// Maximum number of records in the page.
    max_records: usize,
}

impl<'a> IndexPageMut<'a> {
    /// Create an empty index page.
    fn new(index: &'a Index, buf: &'a mut [u8], leaf: bool) -> Self {
        let record_size = if leaf {
            index.leaf_schema.get_record_size()
        } else {
            index.internal_schema.get_record_size()
        };

        let max_records = (PAGE_SIZE - HEADER_SIZE) / record_size;

        // Clear buffer
        buf.fill(0);

        if leaf {
            buf[LEAF_OFFSET] = 1;
        }

        Self {
            index,
            buf,
            record_size,
            max_records,
        }
    }

    /// Read an index page from buffer.
    fn from_buf(index: &'a Index, buf: &'a mut [u8]) -> Self {
        let leaf = buf[LEAF_OFFSET] == 1;

        let record_size = if leaf {
            // 2 for ids of page and slot
            index.index_size + 2 * LINK_SIZE
        } else {
            // 1 for id of page
            index.index_size + LINK_SIZE
        };

        // -1 for the record that will be inserted
        let max_records = (PAGE_SIZE - HEADER_SIZE) / record_size - 1;

        Self {
            index,
            buf,
            record_size,
            max_records,
        }
    }

    /// Set number of records in this page.
    fn set_size(&mut self, size: usize) {
        to_int(&mut self.buf[SIZE_OFFSET..SIZE_OFFSET + LINK_SIZE], size);
    }

    /// Increment size.
    fn inc_size(&mut self) {
        self.set_size(self.get_size() + 1);
    }

    /// Decrement size.
    fn dec_size(&mut self) {
        self.set_size(self.get_size() - 1);
    }

    /// Set previous node.
    fn set_prev(&mut self, prev: Option<usize>) {
        to_nullable_int(&mut self.buf[PREV_OFFSET..PREV_OFFSET + LINK_SIZE], prev);
    }

    /// Set next node.
    fn set_next(&mut self, next: Option<usize>) {
        to_nullable_int(&mut self.buf[NEXT_OFFSET..NEXT_OFFSET + LINK_SIZE], next);
    }

    /// Set parent node.
    fn set_parent(&mut self, parent: Option<usize>) {
        to_nullable_int(
            &mut self.buf[PARENT_OFFSET..PARENT_OFFSET + LINK_SIZE],
            parent,
        );
    }

    /// Set a record in the page using a slot id.
    fn set_record(&mut self, slot: usize, record: Record) {
        log::debug!("Saving record {record:?} into slot {slot}");
        let offset = HEADER_SIZE + slot * self.record_size;
        let schema = if self.is_leaf() {
            &self.index.leaf_schema
        } else {
            &self.index.internal_schema
        };
        record.save_into(self.buf, offset, schema);
    }

    /// Shift records in the page to make room for a new record.
    fn shift(&mut self, slot: usize) {
        let begin = HEADER_SIZE + slot * self.record_size;
        let end = HEADER_SIZE + self.get_size() * self.record_size;
        let end = end.min(PAGE_SIZE - self.record_size);
        self.buf.copy_within(begin..end, begin + self.record_size)
    }

    /// Insert a record into the page.
    fn insert(&mut self, slot: usize, record: Record) {
        assert!(self.get_size() <= self.get_max_records());
        self.shift(slot);
        self.set_record(slot, record);
        self.inc_size();
    }

    /// Insert a range of records into the page.
    fn insert_range(&mut self, slot: usize, records: Vec<Record>) {
        assert!(self.get_size() + records.len() <= self.get_max_records() + 1);
        let begin = HEADER_SIZE + slot * self.record_size;
        let end = HEADER_SIZE + (slot + records.len()) * self.record_size;
        let end = end.min(PAGE_SIZE - records.len() * self.record_size);
        log::info!("Shifting range {begin}..{end}");
        log::info!("is_leaf: {}, end: {}", self.is_leaf(), end + records.len() * self.record_size);
        self.buf
            .copy_within(begin..end, begin + records.len() * self.record_size);
        for (i, record) in records.into_iter().enumerate() {
            self.set_record(slot + i, record);
            self.inc_size();
        }
    }

    /// Remove a record from the page.
    fn remove(&mut self, slot: usize) -> Record {
        assert!(self.get_size() > 0);
        let mut ret = self.get_record(slot);
        ret.index_keys = self.get_record_schema().get_cmp_keys();

        let begin = HEADER_SIZE + (slot + 1) * self.record_size;
        let end = HEADER_SIZE + self.get_size() * self.record_size;
        self.buf.copy_within(begin..end, begin - self.record_size);
        self.dec_size();

        ret
    }

    /// Remove a range of records from the page.
    fn remove_range(&mut self, slots: Range<usize>) -> Vec<Record> {
        log::debug!("Removing range {slots:?}");

        assert!(self.get_size() >= slots.len());
        let mut ret = self.get_record_range(slots.clone());
        for record in &mut ret {
            record.index_keys = self.get_record_schema().get_cmp_keys();
        }

        let begin = HEADER_SIZE + slots.end * self.record_size;
        let end = HEADER_SIZE + self.get_size() * self.record_size;
        self.buf
            .copy_within(begin..end, begin - slots.len() * self.record_size);
        self.set_size(self.get_size() - slots.len());

        ret
    }
}

impl<'a> LinkedIndexPage<'a> for IndexPageMut<'a> {
    fn get_index(&self) -> &'a Index {
        self.index
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
}

impl<'a> IntoIterator for &'a IndexPageMut<'a> {
    type Item = (Record, usize, usize);
    type IntoIter = PageIterator<'a, IndexPageMut<'a>>;

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
            offset: HEADER_SIZE,
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
        log::debug!("Page iterator, {}", self.slot);
        if self.slot < self.page.get_size() {
            let record = Record::from(
                self.page.get_buf(),
                self.offset,
                self.page.get_record_schema(),
            );
            let slot = self.slot;
            let offset = self.offset;
            self.inc();
            Some((record, slot, offset))
        } else {
            None
        }
    }
}
