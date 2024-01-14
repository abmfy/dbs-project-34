//! Paged file system, with LRU cache.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Mutex;

use lru::LruCache;
use once_cell::sync::Lazy;
use uuid::Uuid;

use crate::config::{CACHE_SIZE, PAGE_SIZE};

pub static FS: Lazy<Mutex<PageCache>> = Lazy::new(|| Mutex::new(PageCache::new()));

/// File wrapper providing a uuid for hashing.
pub struct File {
    id: Uuid,
    file: fs::File,
}

impl File {
    /// Open a file for read and write. If not exists, create it.
    pub fn open(name: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(name)?;
        let id = Uuid::new_v4();
        Ok(Self { id, file })
    }

    /// Read a given page on the file.
    pub fn read_page(&mut self, page: usize, buf: &mut [u8]) -> io::Result<()> {
        let offset = page * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64))?;

        let bytes_read = self.file.read(buf)?;
        log::debug!(
            "Read {} bytes from page {} on file {}",
            bytes_read,
            page,
            self.id
        );

        Ok(())
    }

    /// Write to a given page on the file.
    pub fn write_page(&mut self, page: usize, buf: &[u8]) -> io::Result<()> {
        let offset = page * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.write_all(buf)?;
        log::debug!("Write to page {} on file {}", page, self.id);
        Ok(())
    }
}

/// A page in the cache.
pub struct Page {
    dirty: bool,
    buf: [u8; PAGE_SIZE],
}

impl Page {
    /// Load contents from a file.
    fn new(file: &mut File, page: usize) -> io::Result<Self> {
        let mut buf = [0u8; PAGE_SIZE];
        file.read_page(page, &mut buf)?;
        Ok(Self { dirty: false, buf })
    }

    /// Borrow the buffer for read.
    fn as_buf(&self) -> &[u8] {
        &self.buf
    }

    /// Borrow the buffer for write.
    fn as_buf_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        &mut self.buf
    }

    /// Write back the page into disk.
    fn write_back(&mut self, file: &mut File, page: usize) -> io::Result<()> {
        log::debug!("Writing back page {} into file {}", page, file.id);
        if self.dirty {
            log::debug!("Page dirty, executing write");
            file.write_page(page, &self.buf)?;
            self.dirty = false;
        }
        Ok(())
    }
}

/// Page cache.
/// The index is file descriptor and page number.
pub struct PageCache {
    files: HashMap<Uuid, File>,
    /// Paged cache.
    cache: LruCache<(Uuid, usize), Page>,
}

impl PageCache {
    /// Create a new page buffer manager.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            cache: LruCache::new(NonZeroUsize::new(CACHE_SIZE).unwrap()),
        }
    }

    /// Open a file, and return the file descriptor.
    pub fn open(&mut self, name: &Path) -> io::Result<Uuid> {
        let file = File::open(name)?;
        let id = file.id;
        self.files.insert(file.id, file);
        Ok(id)
    }

    /// Close a file, while writing back dirty pages in the cache.
    pub fn close(&mut self, file: Uuid) -> io::Result<()> {
        let mut file = self.files.remove(&file).expect("File descriptor not found");

        let mut to_remove = Vec::new();

        self.cache.iter_mut().for_each(|(&(fd, page), page_buf)| {
            if fd == file.id {
                to_remove.push((fd, page));
                page_buf.write_back(&mut file, page).unwrap();
            }
        });

        to_remove.iter().for_each(|key| {
            self.cache.pop(key);
        });

        Ok(())
    }

    /// Close all files and clear the cache.
    pub fn clear(&mut self) {
        self.files.clear();
        self.cache.clear();
    }

    /// Write back cache into disk.
    pub fn write_back(&mut self) -> io::Result<()> {
        log::info!("Writing back page cache");
        for ((file, page), page_buf) in self.cache.iter_mut() {
            let file = self.files.get_mut(file).expect("File descriptor not found");
            page_buf.write_back(file, *page)?;
        }
        Ok(())
    }

    /// Probe the cache for a given page on a file.
    /// Reload if cache miss.
    fn cache_probe(&mut self, file: Uuid, page: usize) -> io::Result<()> {
        let file = self
            .files
            .get_mut(&file)
            .expect("File descriptor not found");

        let key = (file.id, page);

        // Cache miss
        if !self.cache.contains(&key) {
            log::debug!("Cache miss, file {}, page {}", file.id, page);

            // Reload the page from disk
            let page_buf = Page::new(file, page)?;

            // Insert the page into cache
            if let Some(((old_file, old_page), mut old_page_buf)) = self.cache.push(key, page_buf) {
                // LRUCache.push returns the hit entry or the evicted entry, so we need to check here
                if (old_file, old_page) != (file.id, page) {
                    log::debug!("Evicting page {} on file {}", old_page, old_file);
                    // Evict the least recently used page
                    let file = self
                        .files
                        .get_mut(&old_file)
                        .expect("File descriptor not found");
                    old_page_buf.write_back(file, old_page)?;
                }
            }
        } else {
            log::debug!("Cache hit, file {}, page {}", file.id, page);
        }

        Ok(())
    }

    /// Get a given page on a file for read.
    pub fn get(&mut self, file: Uuid, page: usize) -> io::Result<&[u8]> {
        log::debug!("Getting page {} on file {} for read", page, file);
        self.cache_probe(file, page)?;
        Ok(self.cache.get(&(file, page)).unwrap().as_buf())
    }

    /// Get a given page on a file for write.
    pub fn get_mut(&mut self, file: Uuid, page: usize) -> io::Result<&mut [u8]> {
        log::debug!("Getting page {} on file {} for write", page, file);
        self.cache_probe(file, page)?;
        Ok(self.cache.get_mut(&(file, page)).unwrap().as_buf_mut())
    }
}

#[cfg(test)]
mod tests {
    use crate::setup;

    use super::*;

    #[test]
    fn test_file() {
        setup::init_logging();

        {
            let mut text;
            let mut file = File::open(Path::new("test_file")).unwrap();
            let mut buf = [0u8; PAGE_SIZE];

            text = "Hello, world!".as_bytes();
            text.read(&mut buf).unwrap();
            file.write_page(1, &buf).unwrap();

            text = "Goodbye, world!".as_bytes();
            text.read(&mut buf).unwrap();
            file.write_page(5, &buf).unwrap();
        }

        {
            let mut text = [0u8; PAGE_SIZE].as_ref();
            let mut file = File::open(Path::new("test_file")).unwrap();
            let mut buf = [0u8; PAGE_SIZE];

            file.read_page(3, &mut buf).unwrap();
            assert_eq!(&buf[..text.len()], text);

            text = "Goodbye, world!".as_bytes();
            file.read_page(5, &mut buf).unwrap();
            assert_eq!(&buf[..text.len()], text);

            text = "Hello, world!".as_bytes();
            file.read_page(1, &mut buf).unwrap();
            assert_eq!(&buf[..text.len()], text);

            fs::remove_file("test_file").unwrap();
        }
    }

    #[test]
    fn test_page_cache() {
        setup::init_logging();

        let mut text = [0u8; PAGE_SIZE].as_ref();

        let mut cache = PageCache::new();
        let fd = cache.open(Path::new("test_page_cache")).unwrap();
        log::info!("Opening file with fd {fd}");

        let mut buf;
        let mut buf_mut;

        {
            buf = cache.get(fd, 3).unwrap();
            assert_eq!(&buf[..text.len()], text);
        }

        {
            buf_mut = cache.get_mut(fd, 5).unwrap();
            text = "Goodbye, world!".as_bytes();
            text.read(&mut buf_mut).unwrap();
        }

        {
            buf_mut = cache.get_mut(fd, 3).unwrap();
            text = "Hello, world!".as_bytes();
            text.read(&mut buf_mut).unwrap();
        }

        {
            buf = cache.get(fd, 3).unwrap();
            text = "Hello, world!".as_bytes();
            assert_eq!(&buf[..text.len()], text);
        }

        {
            buf_mut = cache.get_mut(fd, 1).unwrap();
            text = "NÓ∑¡".as_bytes();
            text.read(&mut buf_mut).unwrap();
        }

        // Force write back
        cache.write_back().unwrap();
        let mut cache = PageCache::new();
        let fd = cache.open(Path::new("test_page_cache")).unwrap();
        log::info!("Opening file with fd {fd}");

        {
            buf_mut = cache.get_mut(fd, 666).unwrap();
            text = "So dirty...".as_bytes();
            text.read(&mut buf_mut).unwrap();
        }

        {
            buf = cache.get(fd, 5).unwrap();
            text = "Goodbye, world!".as_bytes();
            assert_eq!(&buf[..text.len()], text);
        }

        // Fill the cache to test eviction; commented because it produces too much output
        // for i in 777..(CACHE_SIZE + 888) {
        //     if i % 2 == 0 {
        //         cache.get_mut(fd, i).unwrap();
        //     } else {
        //         cache.get(fd, i).unwrap();
        //     }
        // }

        // Test close
        cache.close(fd).unwrap();

        fs::remove_file("test_page_cache").unwrap();
    }
}
