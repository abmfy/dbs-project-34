//! Database system management.

use std::fs;
use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::file::PageCache;
use crate::schema::Schema;

/// Database system manager.
pub struct System {
    /// Path to data directory.
    base: PathBuf,
    /// Current name of selected database.
    db_name: Option<String>,
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
            db_name: None,
            db: None,
            fs: PageCache::new(),
        }
    }

    /// Get current selected database.
    pub fn get_current_database(&self) -> &str {
        self.db_name.as_ref().map_or("∅", |name| name.as_str())
    }

    /// Switch current database.
    ///
    /// # Cache Flushing
    ///
    /// When switching database, the cache is flushed.
    pub fn use_database(&mut self, name: &str) -> Result<()> {
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

        self.db_name = Some(name.to_owned());
        self.db = Some(path);

        log::info!("Using database {}", name);
        Ok(())
    }

    /// Get a list of existing databases.
    pub fn get_databases(&self) -> Result<Vec<String>> {
        let mut ret = Vec::new();
        for entry in fs::read_dir(&self.base)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                ret.push(
                    path.file_name()
                        .expect("Unexpected database name")
                        .to_str()
                        .expect("Unexpected database name")
                        .to_owned(),
                );
            }
        }
        Ok(ret)
    }

    /// Create a fresh new database.
    /// Error when the name is used.
    pub fn create_database(&self, name: &str) -> Result<()> {
        let path = self.base.join(name);
        if path.exists() {
            log::error!("Database {} already exists", name);
            return Err(Error::DatabaseExists(name.to_owned()));
        }

        if let Err(err) = fs::create_dir_all(&path) {
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
    pub fn drop_database(&mut self, name: &str) -> Result<()> {
        let path = self.base.join(name);
        if !path.exists() {
            log::error!("Database {} not found", name);
            return Err(Error::DatabaseNotFound(name.to_owned()));
        }

        // Dropping current database. Flush cache.
        if let Some(db) = &self.db {
            if &path == db {
                log::info!("Dropping current database. Flushing cache.");
                self.db_name = None;
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

    /// Get a list of tables in current database.
    pub fn get_tables(&self) -> Result<Vec<String>> {
        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let mut ret = Vec::new();
        for entry in fs::read_dir(db)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                ret.push(
                    path.file_name()
                        .expect("Unexpected table name")
                        .to_str()
                        .expect("Unexpected table name")
                        .to_owned(),
                );
            }
        }
        Ok(ret)
    }

    /// Create a table.
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        log::info!("Creating table {}", name);

        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let table = db.join(name);

        if table.exists() {
            log::error!("Table {} already exists", name);
            return Err(Error::TableExists(name.to_owned()));
        }

        fs::create_dir(table.clone())?;

        let meta = table.join("meta.json");
        let mut file = fs::File::create(meta)?;
        serde_json::to_writer(&mut file, &schema)?;

        Ok(())
    }

    /// Drop a table.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        log::info!("Dropping table {}", name);

        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let table = db.join(name);

        if !table.exists() {
            log::error!("Table {} not found", name);
            return Err(Error::TableNotFound(name.to_owned()));
        }

        fs::remove_dir_all(table)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::setup;

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
