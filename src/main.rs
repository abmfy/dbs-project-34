mod config;
mod error;
mod file;
mod parser;
mod record;
mod schema;
mod setup;
mod stat;
mod system;
mod table;

use std::io;
use std::time::Instant;

use rustyline::{config::Configurer, error::ReadlineError, DefaultEditor};

use config::SHELL_HISTORY;
use error::Result;
use file::FS;
use parser::parse;
use system::System;

use crate::stat::QueryStat;

/// Write back page cache and shutdown.
fn exit() -> Result<()> {
    log::info!("Shutting down");
    let mut fs = FS.lock()?;
    fs.write_back()?;
    Ok(())
}

fn batch_main(mut system: System) -> Result<()> {
    let mut buf = String::new();

    loop {
        buf.clear();
        let size = io::stdin().read_line(&mut buf)?;
        // EOF reached
        if size == 0 {
            break;
        }
        buf = buf.trim().to_string();
        log::info!("Read line: {buf}");

        if buf == "exit" {
            break;
        }

        for (command, result) in parse(&mut system, &buf) {
            match result {
                Ok((table, stat)) => {
                    table.to_csv(io::stdout())?;
                    if let QueryStat::Desc(constraints) = stat {
                        println!();
                        for constraint in constraints {
                            println!("{constraint}");
                        }
                    }
                }
                Err(err) => {
                    println!("!ERROR");
                    println!("{err}");
                }
            }
            println!("@{command}");
        }
    }

    exit()
}

fn shell_main(mut system: System) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    rl.set_auto_add_history(true);
    rl.load_history(SHELL_HISTORY).ok();

    println!("{}", console::style("Welcome to YourSQL!").green().bold());

    // Multi-line buffer
    let mut buf: Option<String> = None;

    loop {
        let db_name = system.get_current_database();
        let prompt = if buf.is_none() {
            format!("yoursql {db_name}> ")
        } else {
            format!("        {}-> ", " ".repeat(db_name.chars().count() - 1))
        };

        let prompt = console::style(prompt).blue().to_string();

        // Set some color on prompt
        match rl.readline(&prompt) {
            Ok(line) => {
                // Skip empty lines
                if line.trim().is_empty() {
                    continue;
                }

                if line.trim_end().ends_with(';') {
                    let command = buf.unwrap_or_default() + &line;
                    let start_time = Instant::now();
                    for (_, result) in parse(&mut system, &command) {
                        match result {
                            Ok((table, stat)) => {
                                if !table.is_empty() {
                                    table.printstd();
                                }
                                match stat {
                                    QueryStat::Query(size) if size > 1 => {
                                        print!("{size} rows in set");
                                    }
                                    QueryStat::Query(1) => {
                                        print!("1 row in set");
                                    }
                                    QueryStat::Query(_) => {
                                        print!("Empty set");
                                    }
                                    QueryStat::Update(size) => {
                                        print!("Query OK, ");
                                        if size == 1 {
                                            print!("1 row affected");
                                        } else {
                                            print!("{size} rows affected");
                                        }
                                    }
                                    QueryStat::Desc(constraints) => {
                                        for constraint in constraints {
                                            println!("{constraint}");
                                        }
                                        print!("Desc OK");
                                    }
                                }
                                let elapsed = start_time.elapsed();
                                println!(" ({:.2} sec)", elapsed.as_secs_f64());
                            }
                            Err(err) => {
                                println!("{} {err}", console::style("Error:").bold().red());
                            }
                        }
                    }
                    buf = None;
                    continue;
                } else {
                    // Buffer multi-line input
                    let buf = buf.get_or_insert_with(String::new);
                    buf.push_str(&line);
                    buf.push('\n');
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => {
                println!("Good morning, and in case I don't see you,\nGood afternoon,\nGood evening,\nAnd good night.");
                break;
            }
            Err(err) => {
                println!("Terminal error: {:?}", err);
                return Err(err.into());
            }
        }
    }

    rl.save_history(SHELL_HISTORY)?;

    exit()
}

fn main() -> Result<()> {
    setup::init_logging();
    let config = setup::init_config();

    // Remove the database directory if it exists.
    if config.init {
        if config.path.exists() {
            log::info!("Removing database directory");
            std::fs::remove_dir_all(&config.path)?;
        }
        return Ok(());
    }

    // Create database directory if it doesn't exist.
    if !config.path.exists() {
        log::info!("Creating database directory");
        std::fs::create_dir_all(&config.path)?;
    }

    let mut system = system::System::new(config.path.clone());
    if let Some(db) = config.database {
        system.use_database(&db)?;
    }

    // Load data into a table.
    if let Some(file) = config.file {
        if let Some(table) = config.table {
            let count = system.load_table(&table, &file)?;
            log::info!("Loaded {} rows into table {}", count, table);
        }
    }

    if config.batch {
        batch_main(system)
    } else {
        shell_main(system)
    }
}
