//! Database system management.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::file::PageCache;
use crate::setup;

/// Database system manager.
pub struct System {
    /// Path to data directory.
    base: PathBuf,
    /// Current selected database.
    db: Option<PathBuf>,
    /// Cached paged filesystem.
    fs: PageCache,
}

impl System {
    /// Create a new database system manager.
    pub fn new(base: PathBuf) -> Self {
        Self {
            base,
            db: None,
            fs: PageCache::new(),
        }
    }

    /// Switch current database.
    ///
    /// # Cache Flushing
    ///
    /// When switching database, the cache is flushed.
    fn use_database(&mut self, name: &str) -> Result<()> {
        let path = self.base.join(name);
        if !path.exists() {
            log::error!("Database {} not found", name);
            return Err(Error::DatabaseNotFound(name.to_owned()));
        }

        if let Some(db) = &self.db {
            if &path == db {
                log::info!("Already using database {}", name);
                return Ok(());
            }
        }

        log::info!("Switching to database {}, flushing cache", name);
        self.fs.clear();

        log::info!("Using database {}", name);
        Ok(())
    }

    /// Create a fresh new database.
    /// Error when the name is used.
    fn create_database(&self, name: &str) -> Result<()> {
        let path = self.base.join(name);
        if path.exists() {
            log::error!("Database {} already exists", name);
            return Err(Error::DatabaseExists(name.to_owned()));
        }

        if let Err(err) = fs::create_dir(&path) {
            log::error!("Failed to create database {}: {}", name, err);
            return Err(err.into());
        }

        log::info!("Database {} created", name);
        Ok(())
    }

    /// Drop a database.
    /// Error when the name is not found.
    ///
    /// # Cache Flushing
    ///
    /// The cache is flushed when dropping current database.
    fn drop_database(&mut self, name: &str) -> Result<()> {
        let path = self.base.join(name);
        if !path.exists() {
            log::error!("Database {} not found", name);
            return Err(Error::DatabaseNotFound(name.to_owned()));
        }

        // Dropping current database. Flush cache.
        if let Some(db) = &self.db {
            if &path == db {
                log::info!("Dropping current database. Flushing cache.");
                self.db = None;
                self.fs.clear();
            }
        }

        if let Err(err) = fs::remove_dir_all(&path) {
            log::error!("Failed to drop database {}: {}", name, err);
            return Err(err.into());
        }

        log::info!("Database {} dropped", name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_create_database() {
        setup::init_logging();

        let base = PathBuf::from("test_create_database");
        fs::create_dir(&base).unwrap();
        let name = "test_create_database";
        System::new(base.clone()).create_database(name).unwrap();
        assert!(base.join(name).exists());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn test_drop_database() {
        setup::init_logging();

        let base = PathBuf::from("test_drop_database");
        fs::create_dir(&base).unwrap();

        let mut system = System::new(base.clone());

        let name = "test_drop_database";
        system.create_database(name).unwrap();
        system.drop_database(name).unwrap();
        assert!(!base.join(name).exists());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn test_dropping_current_database() {
        setup::init_logging();

        let base = PathBuf::from("test_dropping_current_database");
        fs::create_dir(&base).unwrap();

        let mut system = System::new(base.clone());

        let name = "test_dropping_current_database";
        system.create_database(name).unwrap();
        system.use_database(name).unwrap();
        system.drop_database(name).unwrap();
        assert!(!base.join(name).exists());
        assert!(system.db.is_none());
        fs::remove_dir_all(base).unwrap();
    }
}
