mod config;
mod error;
mod file;
mod parser;
mod record;
mod schema;
mod setup;
mod stat;
mod system;

use std::time::Instant;

use rustyline::{config::Configurer, error::ReadlineError, DefaultEditor};

use error::Result;
use parser::parse;
use system::System;

use crate::stat::QueryStat;

fn batch_main(system: System) -> Result<()> {
    Ok(())
}

fn shell_main(mut system: System) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    rl.set_auto_add_history(true);

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
                    match parse(&mut system, &command) {
                        Ok(ret) => {
                            if let Some((table, stat)) = ret {
                                if !table.is_empty() {
                                    table.printstd();
                                }
                                match stat {
                                    QueryStat::Query(size) => {
                                        if size > 1 {
                                            print!("{size} rows in set");
                                        } else if size == 1 {
                                            print!("1 row in set");
                                        } else {
                                            print!("Empty set");
                                        }
                                    }
                                    QueryStat::Update(size) => {
                                        print!("Query OK, ");
                                        if size == 1 {
                                            print!("1 row affected");
                                        } else {
                                            print!("{size} rows affected");
                                        }
                                    }
                                }
                            }
                            let elapsed = start_time.elapsed();
                            println!(" ({:.2} sec)", elapsed.as_secs_f64());
                        }
                        Err(err) => {
                            println!("{} {err}", console::style("Error:").bold().red());
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
                break Ok(());
            }
            Err(err) => {
                println!("Terminal error: {:?}", err);
                break Err(err.into());
            }
        }
    }
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
        return Ok(())
    }

    let mut system = system::System::new(config.path.clone());
    if let Some(db) = config.database {
        system.use_database(&db)?;
    }

    if config.batch {
        batch_main(system)
    } else {
        shell_main(system)
    }
}
