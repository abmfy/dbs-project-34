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
use crate::table::{SelectResult, Table};

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
            .ok_or(Error::IndexNotFound(name.to_owned(), table.to_owned()))
    }

    /// Get a index for write.
    fn get_index_mut(&mut self, table: &str, name: &str) -> Result<&mut Index> {
        let key = (table.to_owned(), name.to_owned());
        self.indexes
            .get_mut(&key)
            .ok_or(Error::IndexNotFound(name.to_owned(), table.to_owned()))
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

        // Check constraint schemas
        for constraint in &schema.constraints {
            match constraint {
                Constraint::PrimaryKey { .. } => {
                    constraint.check(&[&schema])?;
                }
                Constraint::ForeignKey { ref_table, .. } => {
                    self.open_table(ref_table)?;
                    let schema0 = &schema;
                    let schema1 = self.get_table(ref_table)?.get_schema().get_schema();
                    constraint.check(&[schema0, schema1])?;
                }
            }
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
        for constraint in &schema.constraints {
            match constraint {
                Constraint::PrimaryKey { name, columns } => {
                    log::info!("Creating index for primary key {name:?}");
                    let name = name.as_deref();
                    let columns: Vec<_> = columns.iter().map(|c| c.as_str()).collect();
                    self.add_index(
                        false,
                        Some("pk"),
                        table_name,
                        name,
                        columns.as_slice(),
                        true,
                    )?;
                }
                Constraint::ForeignKey {
                    name,
                    columns,
                    ref_table,
                    ref_columns,
                    ..
                } => {
                    log::info!("Creating index for foreign key {name:?}");
                    let name = name.as_deref();
                    let columns: Vec<_> = columns.iter().map(|c| c.as_str()).collect();
                    self.add_index(
                        false,
                        Some("fk_referrer"),
                        table_name,
                        name,
                        columns.as_slice(),
                        true,
                    )?;

                    log::info!("Creating index for foreign key referenced table {ref_table:?}");
                    let ref_columns: Vec<_> = ref_columns.iter().map(|c| c.as_str()).collect();
                    let prefix = format!("fk_referred.{}", table_name);
                    self.add_index(
                        false,
                        Some(&prefix),
                        ref_table,
                        name,
                        ref_columns.as_slice(),
                        true,
                    )?;

                    log::info!("Adding referred constraint to referenced table {ref_table:?}");
                    let ref_table = self.get_table_mut(ref_table)?;
                    ref_table.add_referred_constraint(table_name.to_owned(), constraint.clone());
                }
            }
        }

        Ok(())
    }

    /// Drop a table.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        log::info!("Dropping table {}", name);

        // Check foreign key.
        self.open_table(name)?;
        let table = self.get_table(name)?;
        if !table.get_schema().get_referred_constraints().is_empty() {
            let some_fk = &table.get_schema().get_referred_constraints()[0];
            return Err(Error::TableReferencedByForeignKey(
                some_fk.1.get_name().unwrap_or("<anonymous>").to_owned(),
            ));
        }

        // Removed foreign keys to other tables.
        let foreign_keys: Vec<_> = table
            .get_schema()
            .get_foreign_keys()
            .into_iter()
            .cloned()
            .collect();
        let mut fk_indexes = vec![];
        for fk in foreign_keys {
            let ref_table = fk.get_ref_table();
            let ref_table = self.get_table_mut(ref_table)?;
            ref_table.remove_referred_constraint_of_table(name);
            fk_indexes.push((fk.get_ref_table().to_owned(), fk.get_index_name(false)));
        }

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

        for (table_name, index_name) in fk_indexes {
            self.drop_index(&table_name, &index_name)?;
        }

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

    /// Perform grouping on some query results.
    pub fn group(
        &self,
        selectors: &[Selector],
        results: Vec<SelectResult>,
        group_by: &ColumnSelector,
    ) -> Vec<Vec<SelectResult>> {
        log::info!("Grouping on {group_by:?}");

        let mut ret = vec![];
        let mut group = HashMap::new();
        let mut group_by_index = None;

        for (i, selector) in selectors.iter().enumerate() {
            match selector {
                Selector::Column(c) => {
                    if c == group_by {
                        group_by_index = Some(i);
                    }
                }
                _ => (),
            }
        }

        for (record, page, slot) in results {
            let group_by_value = record.fields[group_by_index.unwrap()].clone();
            let group = group.entry(group_by_value).or_insert_with(|| vec![]);
            group.push((record, page, slot));
        }

        for (_, group) in group {
            ret.push(group);
        }

        ret
    }

    /// Perform aggregation on some query results.
    pub fn aggregate(
        &self,
        selectors: &[Selector],
        results: Vec<Vec<SelectResult>>,
        grouped: bool,
    ) -> Vec<SelectResult> {
        let mut ret = vec![];

        for group in results {
            let mut fields = vec![];

            for (i, selector) in selectors.iter().enumerate() {
                match selector {
                    Selector::Aggregate(aggregator, _) => {
                        let mut values = vec![];
                        for (record, _, _) in &group {
                            values.push(record.fields[i].clone());
                        }
                        fields.push(aggregator.aggregate(values));
                    }
                    Selector::Count => {
                        fields.push(Value::Int(group.len() as i32));
                    }
                    _ => {
                        fields.push(group[0].0.fields[i].clone());
                    }
                }
            }

            ret.push((Record::new(fields), 0, 0));
        }

        if grouped {
            // Remove the added group column
            for (record, _, _) in &mut ret {
                record.fields.pop();
            }
        }

        ret
    }

    /// Execute select statement.
    pub fn select(
        &mut self,
        selectors: &Selectors,
        tables: &[&str],
        where_clauses: Vec<WhereClause>,
        group_by: Option<ColumnSelector>,
    ) -> Result<Vec<SelectResult>> {
        log::info!("Executing select statement");

        // Add group as last column
        let selectors = if let Some(group_by) = &group_by {
            match selectors {
                Selectors::All => Selectors::All,
                Selectors::Some(selectors) => {
                    let mut selectors = selectors.clone();
                    selectors.push(Selector::Column(group_by.clone()));
                    Selectors::Some(selectors)
                }
            }
        } else {
            selectors.clone()
        };
        let selectors = &selectors;

        let ret = match tables.len() {
            0 => unreachable!(),
            1 => {
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
                            break ret;
                        }
                        let table = self.get_table(table_name)?;
                        if let Some(record) = table.select_page_slot(
                            &mut fs,
                            page,
                            slot,
                            selectors,
                            where_clauses.as_slice(),
                        )? {
                            ret.push((record, page, slot));
                        }
                        if let Some(new_iter) = index.inc_iter(&mut fs, iter)? {
                            iter = new_iter;
                        } else {
                            break ret;
                        }
                    }
                } else {
                    table.select(&mut fs, selectors, where_clauses.as_slice())?
                }
            }
            2 => self.join_select(selectors, tables, where_clauses)?,
            _ => return Err(Error::NotImplemented("Join on multiple tables")),
        };

        // Perform aggregation
        match selectors {
            Selectors::All => Ok(ret),
            Selectors::Some(selectors) => {
                // Whether aggregation is needed
                let mut aggregate = false;
                for selector in selectors {
                    match selector {
                        Selector::Aggregate { .. } | Selector::Count => {
                            aggregate = true;
                        }
                        _ => (),
                    }
                }

                let mut ret = if let Some(group_by) = &group_by {
                    self.group(selectors, ret, group_by)
                } else {
                    vec![ret]
                };

                Ok(if aggregate {
                    self.aggregate(selectors.as_slice(), ret, group_by.is_some())
                } else {
                    ret.pop().unwrap_or_default()
                })
            }
        }
    }

    fn join_select(
        &mut self,
        selectors: &Selectors,
        tables: &[&str],
        where_clauses: Vec<WhereClause>,
    ) -> Result<Vec<SelectResult>> {
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
                for (outer_record, _, _) in block {
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
                            ret.push((
                                Record::select_tables(
                                    &[&outer_record, &inner_record],
                                    selectors,
                                    &schemas,
                                    &tables,
                                )?,
                                page_id,
                                slot,
                            ));
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
                for (outer_record, _, _) in block {
                    let join_cond = outer_record.fields[outer_cond_index].clone();

                    inner_where_clauses.push(WhereClause::OperatorExpression(
                        ColumnSelector(None, inner_cond.to_owned()),
                        Operator::Eq,
                        Expression::Value(join_cond),
                    ));

                    let inner_records =
                        inner_table.select(&mut fs, &Selectors::All, &inner_where_clauses)?;
                    for (inner_record, page_id, slot) in inner_records {
                        ret.push((
                            Record::select_tables(
                                &[&outer_record, &inner_record],
                                selectors,
                                &schemas,
                                &tables,
                            )?,
                            page_id,
                            slot,
                        ));
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

        for record in records {
            let table = self.get_table(table_name)?;
            let schema = table.get_schema();
            let constraints = schema.get_constraints().to_owned();

            // Check constraints.
            for constraint in &constraints {
                match constraint {
                    Constraint::PrimaryKey { .. } => {
                        let index_name = constraint.get_index_name(false);

                        let index = self.get_index(table_name, &index_name)?;
                        let table = self.get_table(table_name)?;

                        let selector = index.get_selector();
                        let key = record.select(&selector, table.get_schema());

                        let mut fs = FS.lock()?;
                        if index.contains(&mut fs, &key)? {
                            Err(Error::DuplicateValue(constraint.get_display_name()))?;
                        }
                    }
                    Constraint::ForeignKey { ref_table, .. } => {
                        self.open_table(ref_table)?;
                        self.open_indexes(ref_table)?;

                        let index_name = constraint.get_index_name(true);
                        let table = self.get_table(table_name)?;
                        let index = self.get_index(table_name, &index_name)?;
                        let selector = index.get_selector();
                        let key = record.select(&selector, table.get_schema());

                        let index_name = constraint.get_index_name(false);
                        let index = self.get_index(ref_table, &index_name)?;

                        log::info!("Checking fk: indexing {key:?} in {ref_table}");

                        let mut fs = FS.lock()?;
                        if !index.contains(&mut fs, &key)? {
                            Err(Error::ReferencedFieldsNotExist(
                                constraint.get_display_name(),
                            ))?;
                        }
                    }
                }
            }

            let mut fs = FS.lock()?;

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
        let table_name = table;

        self.open_table(table)?;
        let table = self.get_table(table)?;
        let mut set_columns = HashSet::new();
        for set_pair in set_pairs {
            set_pair.check(table.get_schema())?;
            // Check duplicate column names.
            if set_columns.contains(&set_pair.0) {
                Err(Error::DuplicateColumn(set_pair.0.to_owned()))?;
            } else {
                set_columns.insert(set_pair.0.to_owned());
            }
        }
        for where_clause in where_clauses {
            where_clause.check(table.get_schema())?
        }

        // Open all indexes of this table.
        let indexes = self.open_indexes(name)?;

        let table = self.get_table(table_name)?;
        let schema = table.get_schema();
        let primary_key = schema.get_primary_key();
        let foreign_keys = schema.get_foreign_keys();
        let referred_constraints = schema.get_referred_constraints();

        // Find out constraints that will be affected.
        let primary_key = if let Some(primary_key) = primary_key {
            if let Constraint::PrimaryKey { columns, .. } = primary_key {
                if columns.iter().any(|column| set_columns.contains(column)) {
                    Some(primary_key.clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let foreign_keys = foreign_keys
            .iter()
            .filter(|fk| {
                if let Constraint::ForeignKey { columns, .. } = fk {
                    columns.iter().any(|column| set_columns.contains(column))
                } else {
                    false
                }
            })
            .cloned()
            .cloned()
            .collect::<Vec<_>>();
        let referred_constraints = referred_constraints
            .iter()
            .filter(|(_, fk)| {
                if let Constraint::ForeignKey { ref_columns, .. } = fk {
                    ref_columns
                        .iter()
                        .any(|column| set_columns.contains(column))
                } else {
                    false
                }
            })
            .cloned()
            .collect::<Vec<_>>();

        log::info!("Constraints affected by this update: {primary_key:?}, {foreign_keys:?}, {referred_constraints:?}");

        // Check constraints.
        if primary_key.is_some() || !foreign_keys.is_empty() || !referred_constraints.is_empty() {
            log::info!("Checking constraints in update");

            // Peek records to be updated.
            let records = self.select(&Selectors::All, &[name], where_clauses.to_vec(), None)?;

            // Open table and indexes of constraints.
            for fk in &foreign_keys {
                let ref_table = fk.get_ref_table();

                self.open_table(ref_table)?;
                self.open_indexes(ref_table)?;
            }

            for (referrer, _) in &referred_constraints {
                self.open_table(referrer)?;
                self.open_indexes(referrer)?;
            }

            let mut fs = FS.lock()?;
            let mut updated_count = 0;

            for (record, page_id, slot) in &records {
                let table = self.get_table(table_name)?;
                let schema = table.get_schema();

                let mut record_updated = record.clone();
                let updated = record_updated.update(set_pairs, schema);

                if !updated {
                    continue;
                }

                // Check primary key constraint.
                if let Some(primary_key) = &primary_key {
                    log::info!("Checking primary key");

                    let index_name = primary_key.get_index_name(true);

                    let index = self.get_index(table_name, &index_name)?;
                    let table = self.get_table(table_name)?;

                    let selector = index.get_selector();
                    let key = record.select(&selector, table.get_schema());
                    let key_updated = record_updated.select(&selector, table.get_schema());

                    // Key not updated
                    if key == key_updated {
                        continue;
                    }

                    log::info!("Checking pk: {key:?}");

                    if index.contains(&mut fs, &key_updated)? {
                        Err(Error::DuplicateValue(primary_key.get_display_name()))?;
                    }
                }

                // Check foreign key constraints.
                for fk in &foreign_keys {
                    let ref_table = fk.get_ref_table();
                    let index_name = fk.get_index_name(false);

                    log::info!("Checking foreign key {}", &index_name);

                    let index_name = fk.get_index_name(true);
                    let table = self.get_table(table_name)?;
                    let index = self.get_index(table_name, &index_name)?;
                    let selector = index.get_selector();
                    let key = record.select(&selector, table.get_schema());
                    let key_updated = record_updated.select(&selector, table.get_schema());

                    let index_name = fk.get_index_name(false);
                    let index = self.get_index(ref_table, &index_name)?;

                    log::info!("Key before update: {key:?}");
                    log::info!("Key after update: {key_updated:?}");

                    // Key not updated
                    if key == key_updated {
                        continue;
                    }

                    log::info!("Checking fk: indexing {key:?} in {ref_table}");

                    if !index.contains(&mut fs, &key_updated)? {
                        Err(Error::ReferencedFieldsNotExist(fk.get_display_name()))?;
                    }
                }

                // Check referred foreign key constraints.
                for (referrer, fk) in &referred_constraints {
                    let index_name = fk.get_index_name(true);

                    log::info!("Checking foreign key {}", &index_name);

                    let index_name = fk.get_index_name(false);
                    let table = self.get_table(table_name)?;
                    let index = self.get_index(table_name, &index_name)?;
                    let selector = index.get_selector();
                    let key = record.select(&selector, table.get_schema());
                    let key_updated = record_updated.select(&selector, table.get_schema());

                    let index_name = fk.get_index_name(true);
                    let index = self.get_index(referrer, &index_name)?;

                    // Key not updated
                    if key == key_updated {
                        continue;
                    }

                    log::info!("Checking fk: indexing {key:?} in {referrer}");

                    if index.contains(&mut fs, &key)? {
                        Err(Error::RowReferencedByForeignKey(fk.get_display_name()))?;
                    }
                }

                log::info!("Constraint check OK, perform update");

                let table = self.get_table_mut(table_name)?;
                if let Some((record_old, record_new)) =
                    table.update_page_slot(&mut fs, *page_id, *slot, set_pairs, where_clauses)?
                {
                    updated_count += 1;

                    // Update index
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
                        index.remove(&mut fs, key_old, *page_id, *slot)?;
                        index.insert(&mut fs, key_new, *page_id, *slot)?;
                    }
                }
            }

            return Ok(updated_count);
        }

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
        let table_name = table;

        self.open_table(table)?;
        let table = self.get_table(table)?;

        for where_clause in where_clauses {
            where_clause.check(table.get_schema())?
        }

        // Open all indexes of this table.
        let indexes = self.open_indexes(name)?;

        let table = self.get_table(name)?;
        let referred_constraints = table.get_schema().get_referred_constraints().to_owned();

        // Open tables and indexes of referred constraints.
        for (ref referrer, _) in referred_constraints {
            self.open_table(referrer)?;
            self.open_indexes(referrer)?;
        }

        let table = self.get_table(name)?;
        let referred_constraints = table.get_schema().get_referred_constraints();

        // Check foreign key constraints.
        if !referred_constraints.is_empty() {
            // Peek records to be deleted.
            let records = self.select(&Selectors::All, &[name], where_clauses.to_vec(), None)?;

            let mut fs = FS.lock()?;

            let table = self.get_table(name)?;
            let referred_constraints = table.get_schema().get_referred_constraints();
            for (referrer, fk) in referred_constraints {
                if let Constraint::ForeignKey { .. } = fk {
                    let index_name = fk.get_index_name(false);
                    let index = self.get_index(table_name, &index_name)?;
                    let selector = index.get_selector();

                    let index_name = fk.get_index_name(true);
                    let index = self.get_index(referrer, &index_name)?;

                    for (record, _, _) in &records {
                        let key = record.select(&selector, table.get_schema());

                        if index.contains(&mut fs, &key)? {
                            Err(Error::RowReferencedByForeignKey(fk.get_display_name()))?;
                        }
                    }
                }
            }
        }

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
            log::info!("Checking index {}", index.name);
            if index.columns.len() == 1 && known_columns.contains(&index.columns[0]) {
                let left = left.remove(&index.columns[0]).unwrap_or_default();
                let right = right.remove(&index.columns[0]).unwrap_or_default();

                // Use this index
                let index = self.get_index(table_name, &index.name)?;

                // Filter conditions
                let left = left.iter().max().unwrap_or(&i32::MIN);
                let right = right.iter().min().unwrap_or(&i32::MAX);

                log::info!("Left bound: {left}, right bound: {right}");

                let left_key = Record::new(vec![Value::Int(*left)]);
                let right_key = Record::new(vec![Value::Int(*right)]);

                let left_iter = index.index(fs, &left_key)?;
                let right_iter = index.index(fs, &right_key)?;

                log::info!("Left iter: {left_iter:?}, right iter: {right_iter:?}");

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
            for (key, _, slot) in keys {
                index.insert(&mut fs, key, i, slot)?;
            }
        }

        Ok(())
    }

    /// Execute add index statement.
    ///
    /// # Parameters
    ///
    /// - `init`: whether to initialize the index.
    pub fn add_index(
        &mut self,
        explicit: bool,
        prefix: Option<&str>,
        table_name: &str,
        index_name: Option<&str>,
        columns: &[&str],
        init: bool,
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
        if init {
            self.init_index(table_name, &index_name, columns)?;
        }

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
            return Err(Error::IndexNotFound(
                index_name.to_owned(),
                table_name.to_owned(),
            ));
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

    /// Execute add primary key statement.
    pub fn add_primary_key(
        &mut self,
        table_name: &str,
        constraint_name: Option<&str>,
        columns: &[&str],
    ) -> Result<()> {
        log::info!("Executing add primary key statement");

        self.open_table(table_name)?;
        let table = self.get_table(table_name)?;

        let schema = table.get_schema();
        for &column in columns {
            if !schema.has_column(column) {
                return Err(Error::ColumnNotFound(column.to_owned()));
            }
        }

        for constraint in schema.get_constraints() {
            if let Constraint::PrimaryKey { .. } = constraint {
                Err(Error::MultiplePrimaryKeys(table_name.to_owned()))?;
            }
        }

        let constraint = Constraint::PrimaryKey {
            name: constraint_name.map(|s| s.to_owned()),
            columns: columns.iter().map(|&s| s.to_owned()).collect(),
        };

        log::info!("Creating index for primary key {constraint_name:?}");
        self.add_index(
            false,
            Some("pk"),
            table_name,
            constraint_name,
            columns,
            false,
        )?;

        // Initialize the index, while checking for duplicate primary key.
        let index_name = constraint.get_index_name(false);
        let index = self.get_index(table_name, &index_name)?;
        let selector = index.get_selector();

        let mut fs = FS.lock()?;

        let table = self.get_table(table_name)?;
        let pages = table.get_schema().get_pages();
        for i in 0..pages {
            log::info!("Adding index for page {i}");
            let table = self.get_table(table_name)?;
            let keys = table.select_page(&mut fs, i, &selector, &[])?;

            let mut failed = false;
            let index = self.get_index_mut(table_name, &index_name)?;
            for (key, _, slot) in keys {
                log::info!("Checking primary key {key:?}");
                if index.contains(&mut fs, &key)? {
                    failed = true;
                    break;
                } else {
                    index.insert(&mut fs, key, i, slot)?;
                }
            }

            if failed {
                drop(fs);
                self.drop_index(table_name, &index_name)?;
                return Err(Error::DuplicateValue(
                    constraint_name.unwrap_or("<anonymous>").to_string(),
                ));
            }
        }

        let table = self.get_table_mut(table_name)?;
        table.add_constraint(constraint);

        Ok(())
    }

    /// Execute drop primary key statement.
    pub fn drop_primary_key(
        &mut self,
        table_name: &str,
        constraint_name: Option<&str>,
    ) -> Result<()> {
        log::info!("Executing drop primary key statement");

        self.open_table(table_name)?;
        let table = self.get_table(table_name)?;

        let schema = table.get_schema();
        let pk = schema.get_primary_key();
        if pk.is_none() {
            return Err(Error::NoPrimaryKey(table_name.to_owned()));
        }
        let constraint = pk.unwrap();
        if let Constraint::PrimaryKey { name, .. } = constraint {
            if let (Some(name), Some(constraint_name)) = (name.as_deref(), constraint_name) {
                if name != constraint_name {
                    return Err(Error::ConstraintNotFound(constraint_name.to_owned()));
                }
            }
        } else {
            unreachable!();
        }

        let index_name = constraint.get_index_name(false);

        self.drop_index(table_name, &index_name)?;

        let table = self.get_table_mut(table_name)?;
        table.remove_primary_key();

        Ok(())
    }

    /// Execute add foreign key statement.
    pub fn add_foreign_key(
        &mut self,
        table_name: &str,
        constraint_name: Option<&str>,
        columns: &[&str],
        ref_table_name: &str,
        ref_columns: &[&str],
    ) -> Result<()> {
        log::info!("Executing add foreign key statement");

        self.open_table(table_name)?;
        self.open_table(ref_table_name)?;

        let constraint = Constraint::ForeignKey {
            name: constraint_name.map(|s| s.to_owned()),
            columns: columns.iter().map(|&s| s.to_owned()).collect(),
            referrer: table_name.to_owned(),
            ref_table: ref_table_name.to_owned(),
            ref_columns: ref_columns.iter().map(|&s| s.to_owned()).collect(),
        };

        // Check constraint schemas
        let table = self.get_table(table_name)?;
        let ref_table = self.get_table(ref_table_name)?;
        let schema0 = table.get_schema().get_schema();
        let schema1 = ref_table.get_schema().get_schema();
        constraint.check(&[schema0, schema1])?;

        log::info!("Creating index for foreign key {constraint_name:?}");
        self.add_index(
            false,
            Some("fk_referrer"),
            table_name,
            constraint_name,
            columns,
            false,
        )?;

        let prefix = format!("fk_referred.{}", table_name);
        self.add_index(
            false,
            Some(&prefix),
            ref_table_name,
            constraint_name,
            ref_columns,
            true,
        )?;

        // Initialize the index, while checking for foreign key existence.
        let index_name = constraint.get_index_name(true);
        let index_name_referred = constraint.get_index_name(false);

        let index = self.get_index(table_name, &index_name)?;
        let selector = index.get_selector();

        let mut fs = FS.lock()?;

        let table = self.get_table(table_name)?;
        let pages = table.get_schema().get_pages();
        for i in 0..pages {
            log::info!("Adding index for page {i}");
            let table = self.get_table(table_name)?;
            let keys = table.select_page(&mut fs, i, &selector, &[])?;

            // Check foreign key constraint
            let mut failed = false;
            let index_referred = self.get_index(ref_table_name, &index_name_referred)?;
            for (key, _, _) in &keys {
                log::info!("Checking foreign key {key:?}");
                if !index_referred.contains(&mut fs, key)? {
                    failed = true;
                    break;
                }
            }

            if failed {
                drop(fs);
                self.drop_index(table_name, &index_name)?;
                self.drop_index(ref_table_name, &index_name_referred)?;
                return Err(Error::ReferencedFieldsNotExist(
                    constraint.get_display_name(),
                ));
            }

            log::info!("Foreign key check ok, inserting index");

            let index = self.get_index_mut(table_name, &index_name)?;
            for (key, _, slot) in keys {
                index.insert(&mut fs, key, i, slot)?;
            }
        }

        let table = self.get_table_mut(table_name)?;
        table.add_constraint(constraint.clone());

        let ref_table = self.get_table_mut(ref_table_name)?;
        ref_table.add_referred_constraint(table_name.to_owned(), constraint);

        Ok(())
    }

    /// Execute drop foreign key statement.
    pub fn drop_foreign_key(&mut self, table_name: &str, constraint_name: &str) -> Result<()> {
        log::info!("Executing drop foreign key statement");

        self.open_table(table_name)?;
        let table = self.get_table(table_name)?;

        let schema = table.get_schema();
        let fks = schema.get_foreign_keys();

        let mut constraint = None;
        for fk in fks {
            if let Constraint::ForeignKey { name, .. } = fk {
                if let Some(name) = name.as_deref() {
                    if name == constraint_name {
                        constraint = Some(fk.clone());
                        break;
                    }
                }
            }
        }
        if constraint.is_none() {
            return Err(Error::ConstraintNotFound(constraint_name.to_owned()));
        }
        let constraint = constraint.unwrap();

        let index_name = constraint.get_index_name(true);
        self.drop_index(table_name, &index_name)?;

        let index_name = constraint.get_index_name(false);
        let ref_table_name = constraint.get_ref_table();
        self.drop_index(ref_table_name, &index_name)?;

        let table = self.get_table_mut(table_name)?;
        table.remove_constraint(constraint_name);

        let ref_table = self.get_table_mut(ref_table_name)?;
        ref_table.remove_referred_constraint_of_table(table_name);

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
