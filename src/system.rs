//! Database system management.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use csv::ReaderBuilder;

use crate::error::{Error, Result};
use crate::file::{PageCache, FS};
use crate::index::{Index, IndexSchema, LeafIterator};
use crate::record::{Record, RecordSchema};
use crate::schema::{
    ColumnSelector, Constraint, Expression, Operator, Schema, Selector, Selectors, SetPair,
    TableSchema, Value, WhereClause,
};
use crate::table::Table;

/// Database system manager.
pub struct System {
    /// Path to data directory.
    base: PathBuf,
    /// Current name of selected database.
    db_name: Option<String>,
    /// Current selected database.
    db: Option<PathBuf>,
    /// Mapping from table name to the table.
    tables: HashMap<String, Table>,
    /// Mapping from index name to the index.
    indexes: HashMap<(String, String), Index>,
}

impl System {
    /// Create a new database system manager.
    pub fn new(base: PathBuf) -> Self {
        Self {
            base,
            db_name: None,
            db: None,
            tables: HashMap::new(),
            indexes: HashMap::new(),
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
            if path.canonicalize()? == db.canonicalize()? {
                log::info!("Already using database {}", name);
                return Ok(());
            }
        }

        log::info!("Switching to database {}, flushing cache", name);
        FS.lock()?.clear()?;
        self.tables.clear();
        self.indexes.clear();

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
            if path.canonicalize()? == db.canonicalize()? {
                log::info!("Dropping current database. Flushing cache.");
                self.db_name = None;
                self.db = None;
                FS.lock()?.clear()?;
                self.tables.clear();
                self.indexes.clear();
            }
        }

        if let Err(err) = fs::remove_dir_all(&path) {
            log::error!("Failed to drop database {}: {}", name, err);
            return Err(err.into());
        }

        log::info!("Database {} dropped", name);
        Ok(())
    }

    /// Open a table, hold its file descriptor and schema.
    fn open_table(&mut self, name: &str) -> Result<()> {
        if self.tables.contains_key(name) {
            return Ok(());
        }

        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let table = db.join(name);

        if !table.exists() {
            log::error!("Table {} not found", name);
            return Err(Error::TableNotFound(name.to_owned()));
        }

        let mut fs = FS.lock()?;

        let fd = fs.open(&table.join("data.bin"))?;

        let meta = table.join("meta.json");
        let file = File::open(meta.clone())?;
        let schema = serde_json::from_reader(file)?;

        let table = Table::new(fd, TableSchema::new(schema, &meta)?);

        self.tables.insert(name.to_owned(), table);

        Ok(())
    }

    /// Get a table for read.
    fn get_table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or(Error::TableNotFound(name.to_owned()))
    }

    /// Get a table for write.
    fn get_table_mut(&mut self, name: &str) -> Result<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or(Error::TableNotFound(name.to_owned()))
    }

    /// Open all indexes on a table.
    ///
    /// # Returns
    ///
    /// Returns the name of indexes on the table.
    ///
    /// # Warning
    ///
    /// Please open the table before opening its indexes.
    fn open_indexes(&mut self, table_name: &str) -> Result<Vec<String>> {
        let table = self.get_table(table_name)?;
        let indexes: Vec<_> = table
            .get_schema()
            .get_indexes()
            .iter()
            .map(|index| index.name.clone())
            .collect();
        for index in &indexes {
            self.open_index(table_name, index)?;
        }
        Ok(indexes)
    }

    /// Open a index, hold its file descriptor and schema.
    fn open_index(&mut self, table_name: &str, name: &str) -> Result<()> {
        if self
            .indexes
            .contains_key(&(table_name.to_owned(), name.to_owned()))
        {
            return Ok(());
        }

        log::info!("Opening index {table_name}.{name}");
        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let table = db.join(table_name);

        let mut fs = FS.lock()?;

        let fd = fs.open(&table.join(format!("{name}.index.bin")))?;

        let meta = table.join(format!("{name}.index.json"));
        let file = File::open(meta.clone())?;
        let schema = serde_json::from_reader(file)?;

        let table = self.get_table(table_name)?;

        self.indexes.insert(
            (table_name.to_owned(), name.to_owned()),
            Index::new(fd, schema, &meta, table.get_schema()),
        );

        Ok(())
    }

    /// Get a index for read.
    fn get_index(&self, table: &str, name: &str) -> Result<&Index> {
        let key = (table.to_owned(), name.to_owned());
        self.indexes
            .get(&key)
            .ok_or(Error::IndexNotFound(name.to_owned()))
    }

    /// Get a index for write.
    fn get_index_mut(&mut self, table: &str, name: &str) -> Result<&mut Index> {
        let key = (table.to_owned(), name.to_owned());
        self.indexes
            .get_mut(&key)
            .ok_or(Error::IndexNotFound(name.to_owned()))
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

    /// Get the schema of a table.
    pub fn get_table_schema(&mut self, name: &str) -> Result<&TableSchema> {
        log::info!("Getting schema of table {}", name);

        self.open_table(name)?;
        let table = self.get_table(name)?;

        Ok(table.get_schema())
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

        let data = table.join("data.bin");
        fs::File::create(data)?;

        let meta = table.join("meta.json");
        let mut file = fs::File::create(meta)?;
        serde_json::to_writer(&mut file, &schema)?;

        self.open_table(name)?;

        let table_name = name;

        // Create indexes for constraints
        for constraint in schema.constraints {
            match constraint {
                Constraint::PrimaryKey { name, columns } => {
                    log::info!("Creating index for primary key {name:?}");
                    let name = name.as_deref();
                    let columns: Vec<_> = columns.iter().map(|c| c.as_str()).collect();
                    self.add_index(false, Some("pk"), table_name, name, columns.as_slice())?;
                }
                Constraint::ForeignKey {
                    name,
                    columns,
                    ref_table,
                    ref_columns,
                } => {
                    log::info!("Creating index for foreign key {name:?}");
                    let name = name.as_deref();
                    let columns: Vec<_> = columns.iter().map(|c| c.as_str()).collect();
                    self.add_index(false, Some("fk_to"), table_name, name, columns.as_slice())?;

                    log::info!("Creating index for foreign key referenced table {ref_table:?}");
                    let ref_columns: Vec<_> = ref_columns.iter().map(|c| c.as_str()).collect();
                    self.add_index(
                        false,
                        Some("fk_from"),
                        &ref_table,
                        None,
                        ref_columns.as_slice(),
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Drop a table.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        log::info!("Dropping table {}", name);

        // Writing back dirty pages in the cache.
        if let Some(table) = self.tables.remove(name) {
            let mut fs = FS.lock()?;
            fs.close(table.get_fd())?;
        }
        let keys: Vec<_> = self
            .indexes
            .keys()
            .filter(|(table_name, _)| table_name == name)
            .cloned()
            .collect();
        for index in keys {
            let index = self.indexes.remove(&index).unwrap();
            let mut fs = FS.lock()?;
            fs.close(index.get_fd())?;
        }

        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let table = db.join(name);

        if !table.exists() {
            log::error!("Table {} not found", name);
            return Err(Error::TableNotFound(name.to_owned()));
        }

        fs::remove_dir_all(table)?;

        Ok(())
    }

    /// Load batched data into a table.
    pub fn load_table(&mut self, name: &str, file: &Path) -> Result<usize> {
        log::info!("Loading data into table {}", name);

        self.open_table(name)?;
        let indexes = self.open_indexes(name)?;

        let mut count = 0;
        let mut reader = ReaderBuilder::new().has_headers(false).from_path(file)?;
        for result in reader.records() {
            let record = result?;
            log::debug!("Loading record {record:?}");
            let mut fields = vec![];
            let table = self.get_table_mut(name)?;
            for (field, column) in record.iter().zip(table.get_schema().get_columns()) {
                fields.push(Value::from(field, &column.typ)?);
            }
            let mut fs = FS.lock()?;
            let (page_id, slot) = table.insert(&mut fs, Record::new(fields.clone()))?;
            count += 1;

            // Insert into indexes
            for index_name in &indexes {
                let index = self.get_index(name, index_name)?;
                let table = self.get_table(name)?;

                let columns: Vec<_> = index
                    .get_columns()
                    .iter()
                    .cloned()
                    .map(|c| Selector::Column(ColumnSelector(None, c.name)))
                    .collect();
                let selector = Selectors::Some(columns);
                let key = Record::new(fields.clone()).select(&selector, table.get_schema());

                let index = self.get_index_mut(name, index_name)?;
                index.insert(&mut fs, key, page_id, slot)?;
            }
        }

        Ok(count)
    }

    /// Execute select statement.
    pub fn select(
        &mut self,
        selectors: &Selectors,
        tables: &[&str],
        where_clauses: Vec<WhereClause>,
    ) -> Result<Vec<Record>> {
        log::info!("Executing select statement");

        match tables.len() {
            0 => unreachable!(),
            1 => (),
            2 => return self.join_select(selectors, tables, where_clauses),
            _ => return Err(Error::NotImplemented("Join on multiple tables")),
        }

        assert_eq!(tables.len(), 1);

        let table_name = tables[0];
        self.open_table(tables[0])?;
        let table = self.get_table(tables[0])?;

        selectors.check(table.get_schema())?;
        for where_clause in &where_clauses {
            where_clause.check(table.get_schema())?
        }

        // Open all indexes of this table.
        self.open_indexes(table_name)?;

        let table = self.get_table(table_name)?;

        let mut fs = FS.lock()?;

        // Check index availability
        let index = self.match_index(&mut fs, tables[0], where_clauses.as_slice())?;
        if let Some((index_name, left_iter, right_key)) = index {
            log::info!("Using index {index_name}");

            // Use index
            let mut iter = left_iter;

            let mut ret = vec![];

            loop {
                let index = self.get_index(table_name, &index_name)?;
                let (record, page, slot) = index.get_record(&mut fs, iter)?;
                // Iteration ended
                if record > right_key {
                    return Ok(ret);
                }
                let table = self.get_table(table_name)?;
                if let Some(record) = table.select_page_slot(
                    &mut fs,
                    page,
                    slot,
                    selectors,
                    where_clauses.as_slice(),
                )? {
                    ret.push(record);
                }
                if let Some(new_iter) = index.inc_iter(&mut fs, iter)? {
                    iter = new_iter;
                } else {
                    return Ok(ret);
                }
            }
        } else {
            let ret = table.select(&mut fs, selectors, where_clauses.as_slice())?;
            Ok(ret)
        }
    }

    fn join_select(
        &mut self,
        selectors: &Selectors,
        tables: &[&str],
        where_clauses: Vec<WhereClause>,
    ) -> Result<Vec<Record>> {
        log::info!("Executing join select statement");

        assert_eq!(tables.len(), 2);

        let (mut table0_name, mut table1_name) = (tables[0], tables[1]);

        self.open_table(table0_name)?;
        self.open_table(table1_name)?;
        let indexes0 = self.open_indexes(table0_name)?;
        let indexes1 = self.open_indexes(table1_name)?;

        // Check selectors and where clauses
        let table0 = self.get_table(table0_name)?;
        let table1 = self.get_table(table1_name)?;
        let schemas = [table0.get_schema(), table1.get_schema()];
        let tables = [table0_name, table1_name];
        selectors.check_tables(&schemas, &tables)?;
        for where_clause in &where_clauses {
            where_clause.check_tables(&schemas, &tables)?;
        }

        // Find out equal join condition
        let mut cond = None;
        let mut real_where_clauses = vec![];
        for where_clause in &where_clauses {
            if let WhereClause::OperatorExpression(
                ColumnSelector(Some(table0), column0),
                operator,
                Expression::Column(ColumnSelector(Some(table1), column1)),
            ) = where_clause
            {
                // Not a join condition
                if table0 == table1 {
                    real_where_clauses.push(where_clause.clone());
                    continue;
                }

                if !matches!(operator, Operator::Eq) {
                    Err(Error::JoinOperation)?;
                }

                if cond.is_some() {
                    Err(Error::JoinConditionCount)?;
                }

                cond = Some(if table0 == table0_name {
                    (column0, column1)
                } else {
                    (column1, column0)
                })
            } else {
                // Not a join condition
                real_where_clauses.push(where_clause.clone());
            }
        }
        if cond.is_none() {
            return Err(Error::JoinConditionCount);
        }
        let mut cond = cond.unwrap();
        log::info!(
            "Join condition is {table0_name}.{} = {table1_name}.{}",
            cond.0,
            cond.1
        );

        let mut index_to_use = None;

        // Try to use index
        for index in &indexes0 {
            let index = self.get_index(table0_name, index)?;
            if index.get_columns().len() == 1 && &index.get_columns()[0].name == cond.0 {
                log::info!("Use index of {} on table {table0_name}", cond.0);
                index_to_use = Some(index);
                break;
            }
        }

        if index_to_use.is_none() {
            // Swap tables
            (table0_name, table1_name) = (table1_name, table0_name);
            cond = (cond.1, cond.0);

            for index in &indexes1 {
                let index = self.get_index(table0_name, index)?;
                if index.get_columns().len() == 1 && &index.get_columns()[0].name == cond.0 {
                    log::info!("Use index of {} on table {table0_name}", cond.0);
                    index_to_use = Some(index);
                    break;
                }
            }
        }

        // Now, table0 will have index if possible, so we use table1 as outer table
        // and table0 as inner table.

        let (inner_table_name, outer_table_name) = (table0_name, table1_name);
        let (inner_cond, outer_cond) = cond;
        let outer_table = self.get_table(outer_table_name)?;
        let inner_table = self.get_table(inner_table_name)?;

        let outer_cond_index = outer_table.get_schema().get_column_index(outer_cond);

        fn match_where_clauses(
            where_clauses: &[WhereClause],
            table_name: &str,
        ) -> Vec<WhereClause> {
            where_clauses
                .iter()
                .filter(|&where_clause| match where_clause {
                    WhereClause::OperatorExpression(ColumnSelector(table_selector, _), _, _) => {
                        table_selector.as_ref().unwrap() == table_name
                    }
                })
                .cloned()
                .collect()
        }

        let outer_where_clauses = match_where_clauses(&real_where_clauses, table1_name);
        let mut inner_where_clauses = match_where_clauses(&real_where_clauses, table0_name);

        let schemas = [outer_table.get_schema(), inner_table.get_schema()];
        let tables = [outer_table_name, inner_table_name];

        let mut ret = vec![];

        let mut fs = FS.lock()?;

        if let Some(index) = index_to_use {
            log::info!("Use index on join select");

            let outer_table_pages = outer_table.get_schema().get_pages();

            for page_id in 0..outer_table_pages {
                log::info!("Iterating on page {page_id} of outer table");
                let block = outer_table.select_page(
                    &mut fs,
                    page_id,
                    &Selectors::All,
                    outer_where_clauses.as_slice(),
                )?;
                for (outer_record, _) in block {
                    // Query index
                    let join_cond = outer_record.fields[outer_cond_index].clone();
                    let key = Record::new(vec![join_cond]);
                    let iter = index.index(&mut fs, &key)?;
                    if iter.is_none() {
                        continue;
                    }

                    let mut iter = iter.unwrap();
                    loop {
                        let (index_record, page_id, slot) = index.get_record(&mut fs, iter)?;
                        // Iteration ended
                        if index_record > key {
                            break;
                        }
                        if let Some(inner_record) = inner_table.select_page_slot(
                            &mut fs,
                            page_id,
                            slot,
                            &Selectors::All,
                            inner_where_clauses.as_slice(),
                        )? {
                            ret.push(Record::select_tables(
                                &[&outer_record, &inner_record],
                                selectors,
                                &schemas,
                                &tables,
                            )?);
                        }

                        // Increment iterator
                        if let Some(new_iter) = index.inc_iter(&mut fs, iter)? {
                            iter = new_iter;
                        } else {
                            break;
                        }
                    }
                }
            }
        } else {
            log::info!("Fallback to nested loop");

            let outer_table_pages = outer_table.get_schema().get_pages();

            for page_id in 0..outer_table_pages {
                log::info!("Iterating on page {page_id} of outer table");
                let block = outer_table.select_page(
                    &mut fs,
                    page_id,
                    &Selectors::All,
                    outer_where_clauses.as_slice(),
                )?;
                for (outer_record, _) in block {
                    let join_cond = outer_record.fields[outer_cond_index].clone();

                    inner_where_clauses.push(WhereClause::OperatorExpression(
                        ColumnSelector(None, inner_cond.to_owned()),
                        Operator::Eq,
                        Expression::Value(join_cond),
                    ));

                    let inner_records =
                        inner_table.select(&mut fs, &Selectors::All, &inner_where_clauses)?;
                    for inner_record in inner_records {
                        ret.push(Record::select_tables(
                            &[&outer_record, &inner_record],
                            selectors,
                            &schemas,
                            &tables,
                        )?);
                    }

                    inner_where_clauses.pop();
                }
            }
        }

        Ok(ret)
    }

    /// Execute insert statement.
    pub fn insert(&mut self, table: &str, records: Vec<Record>) -> Result<()> {
        log::info!("Executing insert statement");

        let table_name = table;

        self.open_table(table)?;
        // Open all indexes of this table.
        let indexes = self.open_indexes(table_name)?;

        let table = self.get_table(table)?;

        let schema = table.get_schema();
        for record in &records {
            record.check(schema)?;
        }

        let mut fs = FS.lock()?;

        for record in records {
            let table = self.get_table(table_name)?;
            let schema = table.get_schema();

            // Check primary key.
            for constraint in schema.get_constraints() {
                match constraint {
                    Constraint::PrimaryKey { name, .. } => {
                        let index_name = constraint.get_index_name(false);

                        let index = self.get_index(table_name, &index_name)?;
                        let table = self.get_table(table_name)?;

                        let selector = index.get_selector();
                        let key = record.select(&selector, table.get_schema());

                        if index.contains(&mut fs, &key)? {
                            Err(Error::DuplicatePrimaryKey(
                                name.clone().unwrap_or("<anonymous>".to_string()),
                            ))?;
                        }
                    }
                    Constraint::ForeignKey {
                        name,
                        columns,
                        ref_table,
                        ref_columns,
                    } => todo!(),
                }
            }

            let table = self.get_table_mut(table_name)?;
            let (page_id, slot) = table.insert(&mut fs, record.clone())?;

            let name = table_name;

            // Insert into indexes
            for index_name in &indexes {
                let index = self.get_index(name, index_name)?;
                let table = self.get_table(name)?;

                let columns: Vec<_> = index
                    .get_columns()
                    .iter()
                    .cloned()
                    .map(|c| Selector::Column(ColumnSelector(None, c.name)))
                    .collect();
                let selector = Selectors::Some(columns);
                let key = record.select(&selector, table.get_schema());

                let index = self.get_index_mut(name, index_name)?;
                index.insert(&mut fs, key, page_id, slot)?;
            }
        }

        Ok(())
    }

    /// Execute update statement.
    pub fn update(
        &mut self,
        table: &str,
        set_pairs: &[SetPair],
        where_clauses: &[WhereClause],
    ) -> Result<usize> {
        log::info!("Executing update statement");

        let name = table;

        self.open_table(table)?;
        let table = self.get_table(table)?;
        for set_pair in set_pairs {
            set_pair.check(table.get_schema())?;
        }
        for where_clause in where_clauses {
            where_clause.check(table.get_schema())?
        }

        // Open all indexes of this table.
        let indexes = self.open_indexes(name)?;

        let mut fs = FS.lock()?;

        let mut updated = vec![];

        // Check index availability
        let index = self.match_index(&mut fs, name, where_clauses)?;
        if let Some((index_name, left_iter, right_key)) = index {
            log::info!("Using index {index_name}");

            let table_name = name;

            // Use index
            let mut iter = left_iter;

            loop {
                let index = self.get_index(table_name, &index_name)?;
                let (record, page, slot) = index.get_record(&mut fs, iter)?;
                // Iteration ended
                if record > right_key {
                    break;
                }
                let table = self.get_table_mut(table_name)?;
                if let Some((record_old, record_new)) =
                    table.update_page_slot(&mut fs, page, slot, set_pairs, where_clauses)?
                {
                    updated.push((record_old, record_new, page, slot));
                }
                let index = self.get_index(table_name, &index_name)?;
                if let Some(new_iter) = index.inc_iter(&mut fs, iter)? {
                    iter = new_iter;
                } else {
                    break;
                }
            }
        } else {
            let table = self.get_table_mut(name)?;
            updated = table.update(&mut fs, set_pairs, where_clauses)?;
        }

        let updated_count = updated.len();

        for (record_old, record_new, page, slot) in updated {
            // Update indexes
            for index_name in &indexes {
                let index = self.get_index(name, index_name)?;
                let table = self.get_table(name)?;

                let columns: Vec<_> = index
                    .get_columns()
                    .iter()
                    .cloned()
                    .map(|c| Selector::Column(ColumnSelector(None, c.name)))
                    .collect();
                let selector = Selectors::Some(columns);

                let key_old = record_old.select(&selector, table.get_schema());
                let key_new = record_new.select(&selector, table.get_schema());

                let index = self.get_index_mut(name, index_name)?;
                index.remove(&mut fs, key_old, page, slot)?;
                index.insert(&mut fs, key_new, page, slot)?;
            }
        }

        Ok(updated_count)
    }

    /// Execute delete statement.
    pub fn delete(&mut self, table: &str, where_clauses: &[WhereClause]) -> Result<usize> {
        log::info!("Executing delete statement");

        let name = table;

        self.open_table(table)?;
        let table = self.get_table(table)?;

        for where_clause in where_clauses {
            where_clause.check(table.get_schema())?
        }

        // Open all indexes of this table.
        let indexes = self.open_indexes(name)?;

        let mut deleted = vec![];

        let mut fs = FS.lock()?;

        // Check index availability
        let index = self.match_index(&mut fs, name, where_clauses)?;
        if let Some((index_name, left_iter, right_key)) = index {
            log::info!("Using index {index_name}");

            let table_name = name;

            // Use index
            let mut iter = left_iter;

            loop {
                let index = self.get_index(table_name, &index_name)?;
                let (record, page, slot) = index.get_record(&mut fs, iter)?;
                // Iteration ended
                if record > right_key {
                    break;
                }
                let table = self.get_table_mut(table_name)?;
                if let Some(record) = table.delete_page_slot(&mut fs, page, slot, where_clauses)? {
                    deleted.push((record, page, slot));
                }
                let index = self.get_index(table_name, &index_name)?;
                if let Some(new_iter) = index.inc_iter(&mut fs, iter)? {
                    iter = new_iter;
                } else {
                    break;
                }
            }
        } else {
            let table = self.get_table_mut(name)?;
            deleted = table.delete(&mut fs, where_clauses)?;
        }

        let deleted_count = deleted.len();

        for (record, page, slot) in deleted {
            // Delete from indexes
            for index_name in &indexes {
                let index = self.get_index(name, index_name)?;
                let table = self.get_table(name)?;

                let columns: Vec<_> = index
                    .get_columns()
                    .iter()
                    .cloned()
                    .map(|c| Selector::Column(ColumnSelector(None, c.name)))
                    .collect();
                let selector = Selectors::Some(columns);
                let key = record.select(&selector, table.get_schema());

                let index = self.get_index_mut(name, index_name)?;
                index.remove(&mut fs, key, page, slot)?;
            }
        }

        Ok(deleted_count)
    }

    /// Match the condition against the index, and return the index leaf iterator
    /// if the query can be speeded up by the index.
    fn match_index(
        &self,
        fs: &mut PageCache,
        table_name: &str,
        where_clauses: &[WhereClause],
    ) -> Result<Option<(String, LeafIterator, Record)>> {
        log::info!("Matching index for table {}", table_name);

        let table = self.get_table(table_name)?;

        // Left and right bounds for the condition.
        let mut left: HashMap<String, Vec<i32>> = HashMap::new();
        let mut right: HashMap<String, Vec<i32>> = HashMap::new();

        let mut known_columns: HashSet<String> = Default::default();
        for where_clause in where_clauses {
            match where_clause {
                WhereClause::OperatorExpression(column, operator, expression) => {
                    match expression {
                        Expression::Column(_) => return Ok(None),
                        Expression::Value(v) => {
                            let column_name = column.1.clone();
                            // Only index on int supported yet
                            if let Value::Int(value) = v {
                                match operator {
                                    Operator::Eq => {
                                        known_columns.insert(column_name.clone());
                                        left.entry(column_name.clone()).or_default().push(*value);
                                        right.entry(column_name).or_default().push(*value);
                                    }
                                    Operator::Ne => {
                                        // Ne is ignored
                                    }
                                    Operator::Lt => {
                                        known_columns.insert(column_name.clone());
                                        right.entry(column_name).or_default().push(*value - 1);
                                    }
                                    Operator::Le => {
                                        known_columns.insert(column_name.clone());
                                        right.entry(column_name).or_default().push(*value);
                                    }
                                    Operator::Gt => {
                                        known_columns.insert(column_name.clone());
                                        left.entry(column_name).or_default().push(*value + 1);
                                    }
                                    Operator::Ge => {
                                        known_columns.insert(column_name.clone());
                                        left.entry(column_name).or_default().push(*value);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if known_columns.is_empty() {
            return Ok(None);
        }

        log::info!("Known columns in condition: {known_columns:?}");

        // The conditions are only on one column, and the comparisons are all values
        for index in table.get_schema().get_indexes() {
            if index.columns.len() == 1 && known_columns.contains(&index.columns[0]) {
                let left = left.remove(&index.columns[0]).unwrap_or_default();
                let right = right.remove(&index.columns[0]).unwrap_or_default();

                // Use this index
                let index = self.get_index(table_name, &index.name)?;

                // Filter conditions
                let left = left.iter().max().unwrap_or(&i32::MIN);
                let right = right.iter().min().unwrap_or(&i32::MAX);

                let left_key = Record::new(vec![Value::Int(*left)]);
                let right_key = Record::new(vec![Value::Int(*right)]);

                let left_iter = index.index(fs, &left_key)?;
                let right_iter = index.index(fs, &right_key)?;

                if left_iter.is_none() {
                    return Ok(None);
                }
                if right_iter.is_none() {
                    return Ok(None);
                }

                let left_iter = left_iter.unwrap();

                return Ok(Some((
                    index.get_schema().name.clone(),
                    left_iter,
                    right_key,
                )));
            }
        }

        Ok(None)
    }

    /// Initialize index, adding all existing records into the index.
    fn init_index(&mut self, table_name: &str, index_name: &str, columns: &[&str]) -> Result<()> {
        log::info!("Initializing index {table_name}.{index_name}");

        let table = self.get_table(table_name)?;
        let columns: Vec<_> = columns
            .iter()
            .map(|&s| Selector::Column(ColumnSelector(None, s.to_owned())))
            .collect();
        let selectors = Selectors::Some(columns);

        let mut fs = FS.lock()?;

        let pages = table.get_schema().get_pages();
        for i in 0..pages {
            log::info!("Adding index for page {i}");
            let table = self.get_table(table_name)?;
            let keys = table.select_page(&mut fs, i, &selectors, &[])?;
            let index = self.get_index_mut(table_name, index_name)?;
            for (key, slot) in keys {
                index.insert(&mut fs, key, i, slot)?;
            }
        }

        Ok(())
    }

    /// Execute add index statement.
    pub fn add_index(
        &mut self,
        explicit: bool,
        prefix: Option<&str>,
        table_name: &str,
        index_name: Option<&str>,
        columns: &[&str],
    ) -> Result<()> {
        log::info!("Executing add index statement");

        self.open_table(table_name)?;
        let table = self.get_table(table_name)?;

        let schema = table.get_schema();
        for &column in columns {
            if !schema.has_column(column) {
                return Err(Error::ColumnNotFound(column.to_owned()));
            }
        }

        // Duplicate index is only checked on explicit indexes.
        if explicit {
            for index in schema.get_indexes() {
                if index.columns == columns {
                    return Err(Error::DuplicateIndex(
                        columns.iter().map(|&s| s.to_owned()).collect(),
                    ));
                }
            }
        }

        let schema = IndexSchema::new(explicit, prefix, index_name, columns);
        let index_name = schema.name.clone();

        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let table = db.join(table_name);

        let filename = format!("{}.index.bin", index_name);
        let data = table.join(filename);
        fs::File::create(data)?;

        let filename = format!("{}.index.json", index_name);
        let meta = table.join(filename);
        let mut file = fs::File::create(meta)?;
        serde_json::to_writer(&mut file, &schema)?;

        self.open_table(table_name)?;
        let table = self.get_table_mut(table_name)?;
        table.add_index(schema);

        self.open_index(table_name, &index_name)?;
        self.init_index(table_name, &index_name, columns)?;

        Ok(())
    }

    /// Execute drop index statement.
    pub fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()> {
        log::info!("Executing drop index statement");

        // Writing back dirty pages in the cache.
        if let Some(index) = self
            .indexes
            .remove(&(table_name.to_owned(), index_name.to_owned()))
        {
            let mut fs = FS.lock()?;
            fs.close(index.get_fd())?;
        }

        self.open_table(table_name)?;
        let table = self.get_table(table_name)?;

        let schema = table.get_schema();
        if !schema.has_index(index_name) {
            return Err(Error::IndexNotFound(index_name.to_owned()));
        }

        let db = self.db.as_ref().ok_or(Error::NoDatabaseSelected)?;
        let table = db.join(table_name);

        let filename = format!("{}.index.bin", index_name);
        let data = table.join(filename);
        fs::remove_file(data)?;

        let filename = format!("{}.index.json", index_name);
        let meta = table.join(filename);
        fs::remove_file(meta)?;

        self.open_table(table_name)?;
        let table = self.get_table_mut(table_name)?;
        table.remove_index(index_name);

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
