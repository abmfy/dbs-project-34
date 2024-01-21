# 数据库系统概论 课程项目报告

在本课程项目中，我们基于 Rust 语言完成了一个简单的单用户关系型数据库管理系统 (RDBMS)，能够实现基本的增删查改、索引加速等功能。

## 依赖

本项目使用 [Cargo](https://github.com/rust-lang/cargo) 管理依赖，使用到的第三方依赖库如下：

```toml
bit-set = "0.5.3"
chrono = { version = "0.4.31", features = ["serde"] }
clap = { version = "4.4.14", features = ["derive"] }
console = "0.15.8"
csv = "1.3.0"
env_logger = "0.10.1"
log = "0.4.20"
lru = "0.12.1"
once_cell = "1.19.0"
pest = "2.7.6"
pest_derive = "2.7.6"
prettytable-rs = "0.10.0"
regex = "1.10.2"
rustyline = "13.0.0"
serde = { version = "1.0.195", features = ["derive"] }
serde_json = "1.0.111"
thiserror = "1.0.56"
uuid = { version = "1.6.1", features = ["v4", "fast-rng"] }
```

## 运行

```sh
cargo run
```

默认会通过日志打印一些信息，可以通过 `RUST_LOG=0` 环境变量关闭。

## 系统架构设计

整个系统分为文件管理、记录管理、索引管理、解析器、系统管理与查询处理六个模块。

### 文件管理

主要位于 `file.rs` 模块中。实现了基于缓存的页式文件系统，是整个系统与底层文件系统交互的接口。文件管理模块会维护一个缓存池，用于缓存文件中的页，以减少磁盘 IO 次数，其中缓存的基本单位为页。

### 记录管理

主要位于 `record.rs` 模块中。这一模块用于处理记录的序列化及反序列化，以及单条记录的字段选择、更新等操作。

### 索引管理

主要位于 `index.rs` 模块中。这一模块实现了基于 B+ 树的索引，用于处理索引的建立、插入、删除、查找等操作。

### 解析器

主要位于 `parser.rs` 模块中。这一模块实现了基于 PEG (Parsing Expression Grammar) 的语法解析器，能够将 SQL 语句解析为抽象语法树 (AST)。

### 系统管理

主要位于 `schema.rs` 与 `table.rs` 模块中。这一模块实现了表的数据表的管理，包括格式、约束、索引等信息。

### 查询处理

主要位于 `system.rs` 模块中。这一模块负责在解析器解析完成语法后，将 AST 转化为实际的查询计划执行。

## 各模块详细设计

### 文件管理

页式文件缓存 `PageCache` 保存有文件池和页面池，其中文件池保存了所有打开的文件，页面池保存了所有缓存的页面。文件池基于哈希表实现，在打开一个文件时，将会为其分配一个 UUIDv4 并作为哈希表的键。页面池是一个最近最少使用 (LRU) 缓存，每次尝试读写一个文件号、页面号组合时，缓存将首先在页面池中查找，如果命中则直接返回该页面，否则将从对应的文件中获取该页。

由于 Rust 中的 `std::fs::File` 结构是对操作系统文件描述符的封装，并不能直接克隆，我们在其之上进行了一层新的封装，增加了一个 `Uuid` 作为文件管理模块及系统其他模块之间使用的文件描述符，好处是它可以通过简单的拷贝进行克隆从而代表这个文件。

结构 `Page` 代表了页式文件系统缓存中的一页，可以通过 `as_buf` 或 `as_buf_mut` 函数获取该页的数据缓存块。基于 Rust 的可变借用机制，一旦需要获取一页的可变引用，该页面会被自动标记为脏页。在将页面从缓存池中排除时，如果该页为脏，将会将内容实际写回磁盘。

在本项目中，我们采用了针对每个数据库新建一个目录、针对每个表新建一个子目录的文件结构。同时，每个表以及每个索引都对应两个文件，一个为二进制的数据文件，一个为存储元信息的 JSON 文件。

### 记录管理

我们采用定长记录结构，每条记录的开始是一个空位图，其中每个位对应于后续一个定长列是否为空。整数和浮点数采用二进制表示存储，字符串采用空字符结尾填充的 UTF-8 表示存储，日期采用字符串表示存储。

索引结构中的记录同样也采用记录管理模块中的结构。不同的是，数据记录采用所有列进行比较运算，而索引记录只会采用实际的数据列进行比较。

结构 `Record` 封装了一系列值，并提供 `from`、`save_into` 等函数处理结构的序列化与反序列化。

### 索引管理

基于 B+ 树的索引管理模块。每个 B+ 树结点为一页，结点分为内部结点和叶结点两种。此外，我们在叶结点中维护了一个双向链表，用于加速范围查询。

页头结构：

| leaf | *align* | size | prev | next | parent |
|------|---------|------|------|------|--------|
|   1B |      3B |   4B |   4B |   4B |     4B |

其中 `leaf` 记录当前页是否为叶结点，`size` 记录当前页中的记录数，`prev` 和 `next` 记录当前页在双向链表中的前驱和后继，`parent` 记录当前页的父结点。

内部结点记录结构：

|    key     | child |
|------------|-------|
| *key_size* |    4B |

其中 `key` 记录当前记录的索引键，`child` 记录当前记录的子结点页号。

叶结点记录结构：

|    key     | page | slot |
|------------|------|------|
| *key_size* |   4B |   4B |

其中 `key` 记录当前记录的索引键，`page` 记录当前记录的数据页号，`slot` 记录当前记录的数据槽号。

`IndexSchema` 结构记录了索引的元信息，包括索引的名称、索引的列、索引是否为显式索引，以及当前索引的空页链表的首结点、B+ 树根节点、总页数等信息。

`Index` 结构是一个索引的实例，由 `IndexSchema` 以及一个打开的文件描述符组成。通过 `index`、`contains`、`insert`、`remove` 等函数可以对索引进行查询、插入、删除等操作。

我们定义了 `IndexPage` 与 `IndexPageMut` 结构作为对索引页的封装，以提供方便地访问页头以及对内部结点信息的访问与修改的接口。

### 解析器

基于解析表达文法 (Parsing Expression Grammar) 的 SQL 语法解析器。我们使用 [pest](https://pest.rs) 解析器将 SQL 语句解析为抽象语法树 (AST)，然后交给查询处理模块进行执行。

函数对外提供了 `parse` 接口，可以将 SQL 语句解析执行。

### 系统管理

`schema` 模块定义了 AST 相关结构，以及与表的元数据相关的结构。

`Type` 枚举定义了数据的几种类型，可以通过 `size` 函数获得类型的大小。

`Value` 枚举定义了数据值，可以通过 `from` 函数将字符串解析为 `Value`。

`Column` 结构定义了列的描述，包括列的名称、类型、是否为主键、是否为非空、默认值等。

`Constraint` 枚举定义了主键、外键、唯一等约束，记录了约束名、约束列等信息。

`Selectors` 枚举定义了 SELECT 语句中的选择器，可以为 `*` 或是一系列 `Selector`，其中 `Selector` 可以为列选择器或是聚合选择器。

`SetPair` 定义了 UPDATE 语句中的 SET 子句，包括列选择器和值。

`WhereClause` 定义了 WHERE 子句，包括列选择器、操作符和值。

`Schema` 结构记录了表的元信息，包括表中的列、约束、索引，以及空闲及满页链表的首结点、总页数等信息。

`TableSchema` 结构是对 `Schema` 的封装，提供了对表的元信息的访问与修改的接口。

`table` 模块定义了表的数据结构 `Table`，由一个 `Schema` 以及一个表的数据文件的文件描述符组成。`TablePage` 与 `TablePageMut` 结构是对表页的封装，提供了方便地访问页头以及对记录的访问与修改的接口。一个表中维护了空闲与满页两个链表，当插入一条记录时将会直接从空闲页中选取，当一页的槽位全部占满时将会转为满页，当从满页中移除一条记录时将会转为空闲页。

`Table` 提供了 `insert`、`remove`、`update`、`select` 等函数，用于对表进行插入、删除、更新、查询等操作。部分操作拥有 `*_page_slot` 变种，用于使用索引已查询出将要操作记录所在的页面和槽位时直接进行操作，避免了对整个表进行扫描。

### 查询处理

`system` 模块用于执行实际的查询处理，主要为结构 `System`，记录了当前数据目录、当前数据库以及打开的表和索引。在使用表和索引前，需要通过 `open_table` 和 `open_index` 函数将其对应文件打开。

主要的查询处理函数有以下几类：

- 系统管理：切换数据库 `use_database`、获取数据库列表 `get_databases`、创建数据库 `create_database`、删除数据库 `drop_database`。
- 表管理：创建表 `create_table`、删除表 `drop_table`、创建索引 `add_index`、删除索引 `drop_index`、创建主键约束 `add_primary_key`、删除主键约束 `drop_primary_key`、创建外键约束 `add_foreign_key`、删除外键约束 `drop_foreign_key`、创建唯一约束 `add_unique`、删除唯一约束 `drop_unique`。
- 数据操作：插入 `insert`、删除 `delete`、更新 `update`、查询 `select`。

在对表进行约束的增删前，会首先进行检查，如果约束不满足，将会抛出错误。

在进行数据操作前，会首先检查条件是否满足索引的使用要求。`match_index` 由于匹配 WHERE 子句中可用于索引的条件，并返回索引的起止位置。如果有索引可用，将会利用索引直接进行数据操作，否则将会对整个表进行扫描。接下来，在进行需要修改数据的操作前，会先检查相关约束是否满足，只有约束满足才会执行操作。

对于较复杂的选择查询，还有一些辅助函数用于对数据进行处理，例如 `join_select` 用于连接查询、`aggregate` 用于对查询结果进行聚合、`group` 用于对查询结果进行分组、`order` 用于对查询结果进行排序。投影操作在选取完数据后立刻进行，因此对于聚合、分组、排序等操作，如果所需要的列在查询的选择器中不存在，我们会加上这些列，并在进行完操作后将其删除。

## 主要接口说明

以下根据项目中的各个模块说明主要的接口。

### `mod config`

该模块定义了项目中的常量以及命令行参数。

- `const PAGE_SIZE: usize`: 页面大小。
- `const CACHE_SIZE: usize`: 缓存页面数。
- `const LINK_SIZE: usize`: 链表指针大小。
- `const SHELL_HISTORY: &str`: 命令行历史文件名。

#### `struct Config`

命令行参数。

- `batch: bool`: 是否为批处理模式。
- `database: Option<String>`: 启动数据库。
- `init: bool`: 初始化系统。
- `path: PathBuf`: 指定数据目录。
- `table: Option<String>`: 指定加载数据的目标表。
- `file: Option<PathBuf>`: 加载某一文件中的数据。

### `mod error`

该模块定义了项目中使用的错误，`Error` 枚举定义了所有可能的错误，`Result` 类型为 `std::result::Result` 的错误类型为 `Error` 的别名。

### `mod file`

文件管理模块。

#### `struct PageCache`

页式文件缓存。

- `fn new() -> Self`: 新建一个页式文件缓存。
- `fn open(&mut self, name: &Path) -> io::Result<Uuid>`: 打开一个文件，返回文件描述符。
- `fn close(&mut self, file: Uuid) -> io::Result<()>`: 关闭一个文件。
- `fn clear(&mut self) -> io::Result<()>`: 关闭所有文件并写回缓存。
- `fn get(&mut self, file: Uuid, page: usize) -> io::Result<&[u8]>`: 根据文件描述符和页号获取一块页面的只读引用。
- `fn get_mut(&mut self, file: Uuid, page: usize) -> io::Result<&mut [u8]>`: 根据文件描述符和页号获取一块页面的可写引用。

### `mod index`

索引管理模块。

#### `struct IndexSchema`

索引元数据。

- `pages: usize`: 当前索引的总页数。
- `free: Option<usize>`: 空闲页链表的首结点。
- `explicit: bool`: 是否为显式索引。
- `name: String`: 索引名。
- `columns: Vec<String>`: 索引所在的列。
- `root: Option<usize>`: B+ 树根节点。
- `fn new(explicit: bool, prefix: Option<&str>, name: Option<&str>, columns: &[&str]) -> Self`: 创建一个新的索引元数据，其中 `prefix` 为索引名的前缀。
- `impl Display`: 用于输出时展示索引信息。
- `#[derive(Clone, Debug, Deserialize, Serialize)]`: 用于序列化和反序列化。

#### `type LeafIterator`

`(usize, usize)`

基于双向链表的叶结点迭代器，用于加速范围查询。

#### `type IndexResult`

`(Record, usize, usize)`

索引查询结果，包含索引记录以及查询所得的页面号和槽位号。

#### `struct Index`

索引实例，由 `IndexSchema` 以及一个打开的文件描述符组成。

- `fn new(fd: Uuid, schema: IndexSchema, path: &Path, table: &TableSchema) -> Self`: 从文件描述符、元数据、元数据路径以及表的元数据创建一个索引实例。
- `fn get_fd(&self) -> Uuid`: 获取文件描述符。
- `fn get_schema(&self) -> &IndexSchema`: 获取元数据。
- `fn get_columns(&self) -> &[Column]`: 获取索引列。
- `fn get_selector(&self) -> Selectors`: 获取索引对应的选择器。
- `fn index(&self, fs: &mut PageCache, key: &Record) -> Result<Option<LeafIterator>>`: 根据索引键查询索引，返回叶结点迭代器。
- `fn contains(&self, fs: &mut PageCache, key: &Record) -> Result<bool>`: 根据索引键查询索引，返回是否存在。
- `fn get_record(&self, fs: &mut PageCache, iter: LeafIterator) -> Result<IndexResult>`: 根据叶结点迭代器获取索引记录。
- `fn inc_iter(&self, fs: &mut PageCache, iter: LeafIterator) -> Result<Option<LeafIterator>>`: 步进叶结点迭代器。
- `fn insert(&mut self, fs: &mut PageCache, key: Record, page: usize, slot: usize) -> Result<()>`: 插入索引记录。
- `fn remove(&mut self, fs: &mut PageCache, key: Record, page: usize, slot: usize,)`: 移除索引记录。
- `impl Drop`: 用于自动保存索引元信息。

### `mod parser`

查询解析模块。

#### `enum QueryStat`

查询结果的统计信息。

- `Query(usize)`: 查询结果的行数。
- `Update(usize)`: 操作影响的行数。
- `Desc(Vec<Constraint>, Vec<IndexSchema>)`: 描述表的约束和索引信息。

#### `fn parse`

`fn parse<'a>(system: &mut System, command: &'a str) -> Vec<(&'a str, Result<(Table, QueryStat)>)>`

解析并执行一条 SQL 语句，返回每条语句的执行结果和统计信息。

### `mod record`

记录管理模块。

#### `trait RecordSchema`

记录元数据特性，由表和索引分别实现，提供对记录结构的描述。

- `fn get_columns(&self) -> &[Column]`: 获取记录的列。
- `fn get_null_bitmap_size(&self) -> usize`: 获取记录的空位图大小。
- `fn get_column_index(&self, name: &str) -> usize`: 获取某一列的位置。
- `fn get_cmp_keys(&self) -> usize`: 获取用于排序的列数。
- `fn get_record_size(&self) -> usize`: 获取一条记录的大小。

#### `struct Record`

一条记录，由一系列值组成。

- `fields: Vec<Value>`: 记录的值。
- `index_keys: usize`: 用于索引的列数。
- `fn new(fields: Vec<Value>) -> Self`: 创建一条数据记录。
- `fn new_with_index(mut fields: Vec<Value>, page: usize, slot: usize) -> Self`: 创建一条索引叶记录。
- `fn new_with_child(mut fields: Vec<Value>, child: usize) -> Self`: 创建一条索引内部记录。
- `fn into_keys(self) -> Vec<Value>`: 获取记录的索引键。
- `fn has_null(&self) -> bool`: 检查记录是否含有空值。
- `fn get_child(&self) -> usize`: 获取索引内部记录的子结点列。
- `fn get_index(&self) -> (usize, usize)`: 获取索引叶记录的页号和槽位列。
- `fn set_child(&mut self, child: usize)`: 设置索引内部记录的子结点列。
- `fn set_index(&mut self, page: usize, slot: usize)`: 设置索引叶记录的页号和槽位列。
- `fn check<S: RecordSchema>(&self, schema: &S) -> Result<()>`: 检查记录是否符合所给出的结构。
- `fn from<S: RecordSchema>(buf: &[u8], mut offset: usize, schema: &S) -> Self`: 从二进制数据中解析出一条记录。
- `fn save_into<S: RecordSchema>(&self, buf: &mut [u8], mut offset: usize, schema: &S)`: 将一条记录序列化为二进制数据。
- `fn select<S: RecordSchema>(&self, selectors: &Selectors, schema: &S) -> Self`: 根据选择器选择记录的部分列。
- `fn select_tables<S: RecordSchema>(records: &[&Self], selectors: &Selectors, schemas: &[&S], tables: &[&str]) -> Result<Self>`: 根据选择器从多个表中选取记录的部分列。
- `fn update<S: RecordSchema>(&mut self, set_pairs: &[SetPair], schema: &S) -> bool`: 更新一条记录，返回记录是否被改变。
- `impl PartialEq`: 用于记录之间的判等。
- `impl PartialOrd`: 用于记录之间的比较。

### `mod schema`

表的元数据，以及各种语法树结构。

#### `enum Type`

数据类型。

- `Int`: 有符号整数。
- `Float`: 双精度浮点数。
- `Varchar(usize)`: 字符串。
- `Date`: 日期。
- `fn size(&self) -> usize`: 获取数据类型的大小。
- `impl Display`: 用于输出时展示数据类型。
- `#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]`: 用于序列化于反序列化，以及类型之间的比较。

#### `enum Value`

数据值。

- `Null`: 空值。
- `Int(i32)`: 有符号整数。
- `Float(f64)`: 双精度浮点数。
- `Varchar(String)`: 字符串。
- `Date(NaiveDate)`: 日期。
- `fn from(s: &str, typ: &Type) -> Result<Self>`: 从字符串解析数据值。
- `fn check_type(&self, typ: &Type) -> bool`: 检查值是否符合对应类型。
- `fn min<'a>(&'a self, other: &'a Self) -> &'a Self`: 将两个值进行比较，返回较小的值。
- `fn max<'a>(&'a self, other: &'a Self) -> &'a Self`: 将两个值进行比较，返回较大的值。
- `impl Display`: 用于输出时展示数据值。
- `impl Add`: 实现加法运算。
- `impl Div<usize>`: 实现对于整数的除法运算。
- `impl PartialEq`: 用于数据值之间的判等。
- `impl PartialOrd`: 用于数据值之间的比较。
- `impl Hash`: 用于数据值的哈希。

#### `struct Column`

表中的列。

- `name: String`: 列名。
- `typ: Type`: 列类型。
- `nullable: bool`: 列是否可空。
- `default: Option<Value>`: 列的默认值。
- `fn new(name: String, typ: Type, nullable: bool, default: Option<Value>) -> Result<Self>`: 创建一列。
- `impl PartialEq`: 进行列名的判等。
- `impl Eq`: 进行列名的判等。
- `#[derive(Clone, Debug, Deserialize, Serialize)]`: 用于序列化与反序列化。

#### `enum Constraint`

表中的约束。

- `PrimaryKey { name: Option<String>, columns: Vec<String> }`: 主键约束。
- `ForeignKey { name: Option<String>, columns: Vec<String>, referrer: String, ref_table: String, ref_columns: Vec<String> }`: 外键约束。
- `Unique { name: Option<String>, columns: Vec<String> }`: 唯一约束。
- `fn check(&self, schemas: &[&Schema]) -> Result<()>`: 检查约束是否符合表的结构。
- `fn get_name(&self) -> Option<&str>`: 获取约束名。
- `fn get_columns(&self) -> &[String]`: 获取约束所在的列。
- `fn get_display_name(&self) -> String`: 获取约束用于展示的名字。
- `fn get_ref_table(&self) -> &str`: 获取约束引用的列。
- `fn get_index_name(&self, referrer: bool) -> String`: 获取约束所绑定的索引名。
- `impl Display`: 用于约束的显示。
- `#[derive(Clone, Debug, Deserialize, Serialize)]`: 用于序列化与反序列化。

#### `enum Field`

表示一列或者一个约束。

- `Column(Column)`: 一列。
- `Constraint(Constraint)`: 一个约束。

#### `enum Selectors`

选择器，用于从表中选取部分列。

- `All`: `*` 选择器，选取所有列。
- `Some(Vec<Selector>)`: 选取一些指定列。
- `fn check(&self, schema: &TableSchema) -> Result<()>`: 检查选择器是否符合表的结构。
- `fn check_tables(&self, schemas: &[&TableSchema], tables: &[&str]) -> Result<()>`: 检查选择器是否符合多个表的结构。
- `#[derive(Clone, Debug)]`: 用于克隆与调试输出。

#### `struct ColumnSelector`

列选择器，用于选择指定表的指定列。

- `(Option<String>, String)`: 表名和列名，其中表名在查询只涉及一表时可以省略。
- `fn check_tables(&self, schemas: &[&TableSchema], tables: &[&str]) -> Result<()>`: 用于检查选择器是否符合表的结构。
- `impl PartialEq`: 用于列选择器的判等。
- `#[derive(Clone, Debug)]`: 用于克隆与调试输出。

#### `enum Aggregator`

聚合器。

- `Avg`: 平均值。
- `Min`: 最小值。
- `Max`: 最大值。
- `Sum`: 求和。
- `aggregate(&self, values: Vec<Value>) -> Value`: 用于聚合一系列值，返回一个新值。
- `impl Display`: 用于显示输出。
- `#[derive(Clone, Debug)]`: 用于克隆与调试输出。

#### `enum Selector`

查询选择器。

- `Column(ColumnSelector)`: 列选择器。
- `Aggregate(Aggregator, ColumnSelector)`: 聚合选择器。
- `Count`: `COUNT(*)` 选择器。
- `impl Display`: 用于显示输出。
- `#[derive(Clone, Debug)]`: 用于克隆与调试输出。

#### `struct SetPair`

UPDATE 语句中的 SET 子句。

- `(String, Value)`: 列名与值。
- `fn check(&self, schema: &TableSchema) -> Result<()>`: 检查一个 SET 子句是否符合表的结构。
- `#[derive(Debug)]`: 用于调试输出。

#### `enum Operator`

WHERE 子句中的运算符。

- `Eq`: 等于。
- `Ne`: 不等于。
- `Lt`: 小于。
- `Le`: 小于等于。
- `Gt`: 大于。
- `Ge`: 大于等于。
- `#[derive(Clone, Debug)]`: 用于克隆与调试输出。

#### `enum Expression`

WHERE 子句中的表达式。

- `Value(Value)`: 字面量。
- `Column(ColumnSelector)`: 列选择器。
- `#[derive(Clone, Debug)]`: 用于克隆与调试输出。

#### `enum WhereClause`

WHERE 子句。

- `OperatorExpression(ColumnSelector, Operator, Expression)`: 与表达式进行比较。
- `LikeString(ColumnSelector, String)`: 字符串模糊匹配。
- `IsNull(ColumnSelector, bool)`: 空值检查。
- `fn check(&self, schema: &TableSchema) -> Result<()>`: 检查 WHERE 子句是否符合表的结构。
- `fn check_tables(&self, schemas: &[&TableSchema], tables: &[&str]) -> Result<()>`: 检查 WHERE 子句是否符合一些表的结构。
- `fn matches(&self, record: &Record, schema: &TableSchema) -> bool`: 检查一条记录是否满足 WHERE 子句的条件。
- `#[derive(Clone, Debug)]`: 用于克隆与调试输出。

#### `struct Schema`

表的元数据。

- `pages: usize`: 表的总页数。
- `free: Option<usize>`: 表的空闲页链表的首结点。
- `full: Option<usize>`: 表的满页链表的首结点。
- `columns: Vec<Column>`: 表中的列。
- `constraints: Vec<Constraint>`: 表中的约束。
- `referred_constraints: Vec<(String, Constraint)>`: 目标为当前表的外键约束。
- `indexes: Vec<IndexSchema>`: 表中的索引。
- `fn has_column(&self, name: &str) -> bool`: 检查表中是否含有指定名称的列。
- `fn get_column(&self, name: &str) -> &Column`: 获取指定名称的列。
- `#[derive(Deserialize, Serialize)]`: 用于序列化与反序列化。

## `struct TableSchema`

封装的表元数据，提供了更多操作。

- `fn new(schema: Schema, path: &Path) -> Result<Self>`: 新建一个表元数据。
- `fn get_schema(&self) -> &Schema`: 获取内部的元数据结构。
- `fn get_record_size(&self) -> usize`: 获取一条记录的大小。
- `fn has_column(&self, name: &str) -> bool`: 是否含有指定名称的列。
- `fn get_constraints(&self) -> &[Constraint]`: 获取约束列表。
- `fn get_referred_constraints(&self) -> &[(String, Constraint)]`: 获取被引用的约束列表。
- `fn get_indexes(&self) -> &[IndexSchema]`: 获取索引列表。
- `fn has_index(&self, name: &str) -> bool`: 检查是否存在指定名称的索引。
- `fn add_index(&mut self, index: IndexSchema)`: 将索引添加到表中。
- `fn remove_index(&mut self, name: &str)`: 从表中移除索引。
- `fn get_primary_key(&self) -> Option<&Constraint>`: 获取主键约束。
- `fn get_foreign_keys(&self) -> Vec<&Constraint>`: 获取外键约束。
- `fn add_constraint(&mut self, constraint: Constraint)`: 增加约束。
- `fn add_referred_constraint(&mut self, table: String, constraint: Constraint)`: 增加被引用约束。
- `fn remove_primary_key(&mut self)`: 移除主键约束。
- `fn remove_constraint(&mut self, name: &str)`: 移除约束。
- `fn remove_referred_constraint(&mut self, table_name: &str, name: &str)`: 移除被引用约束。
- `fn remove_referred_constraints_of_table(&mut self, table: &str)`: 移除来自某个表的所有被引用约束。
- `fn get_column(&self, name: &str) -> &Column`: 获取指定名称的列。
- `fn get_max_records(&self) -> usize`: 获取一页中最大的记录数。
- `fn get_free_bitmap_size(&self) -> usize`: 获取一页中空闲位图的大小。
- `fn get_offset(&self, name: &str) -> usize`: 获取一条记录中指定列的偏移量。
- `fn get_pages(&self) -> usize`: 获取表的总页数。
- `fn get_free(&self) -> Option<usize>`: 获取表的空闲页链表的首结点。
- `fn set_free(&mut self, free: Option<usize>)`: 设置表的空闲页链表的首结点。
- `fn get_full(&self) -> Option<usize>`: 获取表的满页链表的首结点。
- `fn set_full(&mut self, free: Option<usize>)`: 设置表的满页链表的首结点。
- `fn new_page(&mut self) -> usize`: 为表分配新的一页。
- `impl RecordSchema`: 用于提供对记录结构的描述。
- `impl Drop`: 用于自动保存表元信息。

### `mod setup`

启动初始化。

- `fn init_logging()`: 初始化日志。
- `fn init_config() -> Config`: 解析命令行参数。

### `mod system`

系统管理与查询处理模块。

#### `struct System`

数据库管理系统。

- `fn new(base: PathBuf) -> Self`: 新建一个系统。
- `fn get_current_database(&self) -> &str`: 获取当前数据库。
- `fn use_database(&mut self, name: &str) -> Result<()>`: 切换数据库。
- `fn get_databases(&self) -> Result<Vec<String>>`: 获取所有数据库。
- `fn create_database(&self, name: &str) -> Result<()>`: 创建数据库。
- `fn drop_database(&mut self, name: &str) -> Result<()>`: 删除数据库。
- `fn get_tables(&self) -> Result<Vec<String>>`: 获取所有表。
- `fn get_table_schema(&mut self, name: &str) -> Result<&TableSchema>`: 获取一个表的结构。
- `fn create_table(&mut self, name: &str, schema: Schema) -> Result<()>`: 创建一个表。
- `fn drop_table(&mut self, name: &str) -> Result<()>`: 删除一个表。
- `fn load_table(&mut self, name: &str, file: &Path) -> Result<usize>`: 将数据装入指定表。
- `fn select(&mut self, selectors: &Selectors, tables: &[&str], where_clauses: Vec<WhereClause>, group_by: Option<ColumnSelector>, order_by: Option<(ColumnSelector, bool)>) -> Result<Vec<SelectResult>>`: 执行 SELECT 语句。
- `fn insert(&mut self, table: &str, records: Vec<Record>) -> Result<()>`: 执行 INSERT 语句。
- `fn update(&mut self, table: &str, set_pairs: &[SetPair], where_clauses: &[WhereClause]) -> Result<usize>`: 执行 UPDATE 语句。
- `fn delete(&mut self, table: &str, where_clauses: &[WhereClause]) -> Result<usize>`: 执行 DELETE 语句。
- `fn add_index(&mut self, explicit: bool, prefix: Option<&str>, table_name: &str, index_name: Option<&str>, columns: &[&str], init: bool) -> Result<()>`: 在指定表上创建索引。
- `fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()>`: 删除指定表上的一个索引。
- `fn add_primary_key(&mut self, table_name: &str, constraint_name: Option<&str>, columns: &[&str]) -> Result<()>`: 在指定表上创建主键约束。
- `fn drop_primary_key(&mut self, table_name: &str, constraint_name: Option<&str>) -> Result<()>`: 删除指定表上的主键约束。
- `fn add_foreign_key(&mut self, table_name: &str, constraint_name: Option<&str>, columns: &[&str], ref_table_name: &str, ref_columns: &[&str]) -> Result<()>`: 在指定表上创建外键约束。
- `fn drop_foreign_key(&mut self, table_name: &str, constraint_name: &str) -> Result<()>`: 删除指定表上的外键约束。
- `fn add_unique(&mut self, table_name: &str, constraint_name: Option<&str>, columns: &[&str]) -> Result<()>`: 在指定表上创建唯一约束。

### `mod table`

表管理模块。

#### `type SelectResult`

`(Record, usize, usize)`

选择结果，以及对应记录的页号与槽位号。

#### `struct Table`

表实例，由元数据以及数据文件的文件描述符组成。

- `fn new(fd: Uuid, schema: TableSchema) -> Self`: 创建一个新的表实例。
- `fn get_fd(&self) -> Uuid`: 获取文件描述符。
- `fn get_schema(&self) -> &TableSchema`: 获取表的元数据。
- `fn select(&self, fs: &mut PageCache, selector: &Selectors, where_clauses: &[WhereClause]) -> Result<Vec<SelectResult>>`: 根据选择器和条件从表中选取记录。
- `fn select_page_slot(&self, fs: &mut PageCache, page_id: usize, slot: usize, selector: &Selectors, where_clauses: &[WhereClause]) -> Result<Option<Record>>`: 根据选择器和条件直接从指定的页号和槽位号选取记录。
- `fn select_page(&self, fs: &mut PageCache, page_id: usize, selector: &Selectors, where_clauses: &[WhereClause]) -> Result<Vec<SelectResult>>`: 根据选择器和条件选取指定页面的记录。
- `fn insert<'a>(&'a mut self, fs: &'a mut PageCache, record: Record) -> Result<(usize, usize)>`: 将一条记录插入到表中，返回插入位置。
- `fn update<'a>(&'a mut self, fs: &'a mut PageCache, set_pairs: &[SetPair], where_clauses: &[WhereClause]) -> Result<Vec<(Record, Record, usize, usize)>>`: 更新表中符合条件的记录，返回更新前后的记录及对应位置。
- `fn update_page_slot(&mut self, fs: &mut PageCache, page_id: usize, slot: usize, set_pairs: &[SetPair], where_clauses: &[WhereClause]) -> Result<Option<(Record, Record)>>`: 如果符合条件，更新指定位置的记录，返回更新前后的记录。
- `fn delete<'a>(&'a mut self, fs: &'a mut PageCache, where_clauses: &[WhereClause]) -> Result<Vec<(Record, usize, usize)>>`: 删除符合条件的记录，返回删除的记录及位置。
- `fn delete_page_slot(&mut self, fs: &mut PageCache, page_id: usize, slot: usize, where_clauses: &[WhereClause]) -> Result<Option<Record>>`: 如果符合条件，删除指定位置的记录，返回删除的记录。
- `fn add_index(&mut self, schema: IndexSchema)`: 增加索引。
- `fn remove_index(&mut self, name: &str)`: 删除索引。
- `fn add_constraint(&mut self, schema: Constraint)`: 增加约束。
- `fn remove_constraint(&mut self, name: &str)`: 删除约束。
- `fn add_referred_constraint(&mut self, table: String, schema: Constraint)`: 增加被引用约束。
- `fn remove_primary_key(&mut self)`: 删除主键。
- `fn remove_referred_constraint(&mut self, table: &str, name: &str)`: 删除被引用约束。
- `fn remove_referred_constraint_of_table(&mut self, table: &str)`: 删除来自指定表的所有被引用约束。

## 实验结果

实现了全部必做功能，包括：

- 基本运行
- 系统管理
- 查询解析
- 完整性约束
- 模式管理
- 索引模块

实现了以下选做功能：

- 模糊查询：`LIKE % _`
- 聚合查询：`MAX`、`SUM`、`COUNT` 等
- 分组查询：`GROUP BY`
- 排序分页：`LIMIT`、`OFFSET`、`ORDER BY`
- 日期：`DATE`
- `UNIQUE` 约束: Schema 增删，唯一性约束
- `NULL`: `WHERE` 比较，主外键完整性约束，插入 `NULL` 判定等

## 小组分工

王博文：全部

## 参考文献

- [数据库系统概论实验文档](https://thu-db.github.io/dbs-tutorial/)
- [B+ 树 - OI Wiki](https://oiwiki.org/ds/bplus-tree/)
