//! SQL parser.

use pest::{iterators::Pairs, Parser};
use pest_derive::Parser;
use prettytable::{format::consts::FORMAT_NO_LINESEP_WITH_TITLE, row, Table};

use crate::{error::Result, stat::QueryStat, system::System};

#[derive(Parser)]
#[grammar = "sql.pest"]
enum SqlParser {}

pub fn parse(system: &mut System, command: &str) -> Result<Option<(Table, QueryStat)>> {
    log::info!("Parsing command: {command}");

    let sql = SqlParser::parse(Rule::program, command)?;

    for statement in sql {
        match statement.as_rule() {
            Rule::db_statement => {
                return Ok(Some(parse_db_statement(system, statement.into_inner())?));
            }
            _ => continue,
        }
    }

    // Empty statement
    Ok(None)
}

fn parse_db_statement(system: &mut System, statement: Pairs<Rule>) -> Result<(Table, QueryStat)> {
    log::info!("Parsing db statement: {statement:?}");

    let pair = statement.into_iter().next().unwrap();
    match pair.as_rule() {
        Rule::create_db_statement => parse_create_db_statement(system, pair.into_inner()),
        Rule::drop_db_statement => parse_drop_db_statement(system, pair.into_inner()),
        Rule::show_dbs_statement => parse_show_dbs_statement(system, pair.into_inner()),
        Rule::use_db_statement => parse_use_db_statement(system, pair.into_inner()),
        _ => unimplemented!(),
    }
}

fn parse_create_db_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing create db statement: {statement:?}");

    let mut ret = Table::new();
    ret.set_format(*FORMAT_NO_LINESEP_WITH_TITLE);

    let name = statement.into_iter().next().unwrap().as_str();

    system.create_database(name)?;

    Ok((ret, QueryStat::Update(1)))
}

fn parse_drop_db_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing drop db statement: {statement:?}");

    let mut ret = Table::new();
    ret.set_format(*FORMAT_NO_LINESEP_WITH_TITLE);

    let name = statement.into_iter().next().unwrap().as_str();

    system.drop_database(name)?;

    Ok((ret, QueryStat::Update(1)))
}

fn parse_show_dbs_statement(
    system: &mut System,
    statement: Pairs<Rule>,
) -> Result<(Table, QueryStat)> {
    log::info!("Parsing show dbs statement: {statement:?}");

    let mut ret = Table::new();
    ret.set_format(*FORMAT_NO_LINESEP_WITH_TITLE);
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

    let mut ret = Table::new();
    ret.set_format(*FORMAT_NO_LINESEP_WITH_TITLE);

    let name = statement.into_iter().next().unwrap().as_str();

    system.use_database(name)?;

    Ok((ret, QueryStat::Update(0)))
}
