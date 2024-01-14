//! SQL parser.

use std::collections::HashSet;
use std::path::Path;

use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use pest_derive::Parser;
use prettytable::{format::consts::FORMAT_NO_LINESEP_WITH_TITLE, row, Table};

use crate::{
    error::{Error, Result},
    schema::{Column, Constraint, Field, Schema, Type, Value},
    stat::QueryStat,
    system::System,
};

#[derive(Parser)]
#[grammar = "sql.pest"]
enum SqlParser {}

/// Create an empty table with default format.
fn fresh_table() -> Table {
    let mut ret = Table::new();
    ret.set_format(*FORMAT_NO_LINESEP_WITH_TITLE);
    ret
}

/// Parse a complete string.
///
/// # Returns
///
/// Returns a vector of command-result pairs, in which the result
/// contains a result table and query statistics.
pub fn parse<'a>(
    system: &mut System,
    command: &'a str,
) -> Vec<(&'a str, Result<(Table, QueryStat)>)> {
    log::info!("Parsing command: {command}");

    let sql = SqlParser::parse(Rule::program, command);
    if let Err(err) = sql {
        return vec![(command, Err(Box::new(err).into()))];
    }

    let sql = sql.unwrap();
    let mut ret = vec![];

    for statement in sql {
        let command = statement.as_str();
        match statement.as_rule() {
            Rule::db_statement => {
                let result = parse_db_statement(system, statement.into_inner());
                ret.push((command, result));
            }
            Rule::table_statement => {
                let result = parse_table_statement(system, statement.into_inner());
                ret.push((command, result));
            }
            _ => continue,
        }
    }

    // Empty statement
    ret
}

fn parse_db_statement(system: &mut System, statement: Pairs<Rule>) -> Result<(Table, QueryStat)> {
    log::info!("Parsing db statement: {statement:?}");

    let pair = statement.into_iter().next().unwrap();
    match pair.as_rule() {
        Rule::create_db_statement => parse_create_db_statement(system, pair.into_inner()),
        Rule::drop_db_statement => parse_drop_db_statement(system, pair.into_inner()),
        Rule::show_dbs_statement => parse_show_dbs_statement(system, pair.into_inner()),
        Rule::use_db_statement => parse_use_db_statement(system, pair.into_inner()),
        Rule::show_tables_statement => parse_show_tables_statement(system, pair.into_inner()),
        _ => unimplemented!(),
    }
}

fn parse_create_db_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing create db statement: {statement:?}");

    let name = statement.into_iter().next().unwrap().as_str();

    system.create_database(name)?;

    Ok((fresh_table(), QueryStat::Update(1)))
}

fn parse_drop_db_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing drop db statement: {statement:?}");

    let name = statement.into_iter().next().unwrap().as_str();

    system.drop_database(name)?;

    Ok((fresh_table(), QueryStat::Update(1)))
}

fn parse_show_dbs_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing show dbs statement: {statement:?}");

    let mut ret = fresh_table();
    ret.set_titles(row!["DATABASES"]);

    let dbs = system.get_databases()?;

    dbs.iter().for_each(|db| {
        ret.add_row(row![db]);
    });

    Ok((ret, QueryStat::Query(dbs.len())))
}

fn parse_use_db_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing use db statement: {statement:?}");

    let name = statement.into_iter().next().unwrap().as_str();

    system.use_database(name)?;

    Ok((fresh_table(), QueryStat::Update(0)))
}

fn parse_show_tables_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing show tables statement: {statement:?}");

    let mut ret = fresh_table();
    ret.set_titles(row!["TABLES"]);

    let tables = system.get_tables()?;

    tables.iter().for_each(|table| {
        ret.add_row(row![table]);
    });

    Ok((ret, QueryStat::Query(tables.len())))
}

fn parse_table_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    let pair = statement.into_iter().next().unwrap();
    match pair.as_rule() {
        Rule::create_table_statement => parse_create_table_statement(system, pair.into_inner()),
        Rule::drop_table_statement => parse_drop_table_statement(system, pair.into_inner()),
        Rule::desc_statement => parse_desc_statement(system, pair.into_inner()),
        Rule::load_statement => parse_load_statement(system, pair.into_inner()),
        _ => unimplemented!(),
    }
}

fn parse_value(value: Pair<Rule>) -> Result<Value> {
    let ret;

    match value.as_rule() {
        Rule::integer => {
            ret = Value::Int(value.as_str().parse()?);
        }
        Rule::float => {
            ret = Value::Float(value.as_str().parse()?);
        }
        Rule::string => {
            ret = Value::Varchar(value.into_inner().next().unwrap().as_str().to_owned());
        }
        Rule::null => ret = Value::Null,
        _ => panic!("Invalid value: {value:?}"),
    }

    Ok(ret)
}

fn parse_column(pairs: Pairs<Rule>) -> Result<Column> {
    let mut name = None;
    let mut typ = None;
    let mut not_null = false;
    let mut default = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::identifier => {
                name = Some(pair.as_str());
            }
            Rule::typ => {
                let pair = pair.into_inner().next().unwrap();
                match pair.as_rule() {
                    Rule::int_t => {
                        typ = Some(Type::Int);
                    }
                    Rule::float_t => {
                        typ = Some(Type::Float);
                    }
                    Rule::varchar_t => {
                        let mut size = None;
                        for pair in pair.into_inner() {
                            match pair.as_rule() {
                                Rule::integer => {
                                    size = Some(pair.as_str().parse().unwrap());
                                }
                                _ => continue,
                            }
                        }
                        let size = size.unwrap();
                        typ = Some(Type::Varchar(size));
                    }
                    _ => panic!("Invalid type: {pair:?}"),
                }
            }
            Rule::not_null_clause => {
                not_null = true;
            }
            Rule::value => {
                default = Some(parse_value(pair.into_inner().next().unwrap())?);
            }
            _ => continue,
        }
    }

    // These value are guaranteed to be Some by the grammar.
    let name = name.unwrap();
    let typ = typ.unwrap();

    Column::new(name.to_string(), typ, !not_null, default)
}

fn parse_primary_key(pairs: Pairs<Rule>) -> Result<Constraint> {
    let mut name = None;
    let mut columns = vec![];

    for pair in pairs {
        match pair.as_rule() {
            Rule::identifier => {
                name = Some(pair.as_str().to_owned());
            }
            Rule::identifiers => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::identifier => {
                            columns.push(pair.as_str().to_owned());
                        }
                        _ => continue,
                    }
                }
            }
            _ => continue,
        }
    }

    Ok(Constraint::PrimaryKey { name, columns })
}

fn parse_foreign_key(pairs: Pairs<Rule>) -> Result<Constraint> {
    let mut name = None;
    let mut columns = vec![];
    let mut ref_table = None;
    let mut ref_columns = vec![];

    for pair in pairs {
        match pair.as_rule() {
            Rule::identifier => {
                name = Some(pair.as_str().to_owned());
            }
            Rule::identifiers => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::identifier => {
                            columns.push(pair.as_str().to_owned());
                        }
                        _ => continue,
                    }
                }
            }
            Rule::references_clause => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::identifier => {
                            ref_table = Some(pair.as_str().to_owned());
                        }
                        Rule::identifiers => {
                            for pair in pair.into_inner() {
                                match pair.as_rule() {
                                    Rule::identifier => {
                                        ref_columns.push(pair.as_str().to_owned());
                                    }
                                    _ => continue,
                                }
                            }
                        }
                        _ => continue,
                    }
                }
            }
            _ => continue,
        }
    }

    let ref_table = ref_table.unwrap();

    Ok(Constraint::ForeignKey {
        name,
        columns,
        ref_table,
        ref_columns,
    })
}

fn parse_field_list(field_list: Pairs<Rule>) -> Result<Vec<Field>> {
    let mut ret = vec![];

    for field in field_list {
        match field.as_rule() {
            Rule::field_def => ret.push(Field::Column(parse_column(field.into_inner())?)),
            Rule::primary_key => {
                ret.push(Field::Constraint(parse_primary_key(field.into_inner())?))
            }
            Rule::foreign_key => {
                ret.push(Field::Constraint(parse_foreign_key(field.into_inner())?))
            }
            _ => continue,
        }
    }

    Ok(ret)
}

fn parse_create_table_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing create table statement: {statement:?}");

    let mut name = None;
    let mut fields = None;

    for pair in statement {
        match pair.as_rule() {
            Rule::identifier => {
                name = Some(pair.as_str());
            }
            Rule::field_list => {
                fields = Some(parse_field_list(pair.into_inner())?);
            }
            _ => continue,
        }
    }

    // Guaranteed to be Some by the grammar.
    let name = name.unwrap();
    let fields = fields.unwrap();

    let (columns, constraints): (Vec<Field>, Vec<Field>) =
        fields.into_iter().partition(|field| match field {
            Field::Column(_) => true,
            Field::Constraint(_) => false,
        });

    let mut primary_key_count = 0;
    let mut primary_key_columns = HashSet::new();
    let constraints = constraints
        .into_iter()
        .map(|field| match field {
            Field::Constraint(constraint) => {
                match &constraint {
                    Constraint::PrimaryKey { columns, .. } => {
                        primary_key_count += 1;
                        primary_key_columns.extend(columns.clone());
                    }
                    _ => {}
                }
                constraint
            }
            _ => unreachable!(),
        })
        .collect();

    if primary_key_count > 1 {
        return Err(Error::MultiplePrimaryKeys(name.to_owned()));
    }

    let mut duplicate_column_name = None;
    let mut column_names = HashSet::new();
    let columns = columns
        .into_iter()
        .map(|field| match field {
            Field::Column(mut column) => {
                if column_names.contains(&column.name) {
                    duplicate_column_name = Some(column.name.clone());
                }
                // It's implied that the primary keys are not null.
                if primary_key_columns.contains(&column.name) {
                    column.nullable = false;
                }
                column_names.insert(column.name.clone());
                column
            }
            _ => unreachable!(),
        })
        .collect();

    if let Some(name) = duplicate_column_name {
        return Err(Error::DuplicateColumn(name));
    }

    system.create_table(
        name,
        Schema {
            pages: 0,
            free: None,
            full: None,
            columns,
            constraints,
        },
    )?;

    Ok((fresh_table(), QueryStat::Update(0)))
}

fn parse_drop_table_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing drop table statement: {statement:?}");

    let name = statement.into_iter().next().unwrap().as_str();

    system.drop_table(name)?;

    Ok((fresh_table(), QueryStat::Update(0)))
}

fn parse_desc_statement(system: &mut System, statement: Pairs<Rule>) -> Result<(Table, QueryStat)> {
    log::info!("Parsing desc statement: {statement:?}");

    let name = statement.into_iter().next().unwrap().as_str();

    let schema = system.get_table_schema(name)?;

    let mut ret = fresh_table();
    ret.set_titles(row!["Field", "Type", "Null", "Default"]);

    schema.get_columns().iter().for_each(|column| {
        let default = match &column.default {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        };
        let nullable = if column.nullable { "YES" } else { "NO" };
        ret.add_row(row![column.name, column.typ, nullable, default,]);
    });

    let constraints = schema.get_constraints().into();

    Ok((ret, QueryStat::Desc(constraints)))
}

fn parse_load_statement(system: &mut System, statement: Pairs<Rule>) -> Result<(Table, QueryStat)> {
    log::info!("Parsing load statement: {statement:?}");

    let mut ret = fresh_table();
    ret.set_titles(row!["rows"]);

    let mut file = None;
    let mut name = None;

    for pair in statement {
        match pair.as_rule() {
            Rule::string => {
                if file.is_none() {
                    file = Some(pair.into_inner().next().unwrap().as_str());
                }
            }
            Rule::identifier => {
                name = Some(pair.as_str());
            }
            _ => continue,
        }
    }

    let file = file.unwrap();
    let name = name.unwrap();

    let rows = system.load_table(name, Path::new(file))?;
    ret.add_row(row![rows]);

    Ok((ret, QueryStat::Update(rows)))
}
