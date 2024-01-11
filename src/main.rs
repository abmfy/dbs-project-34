mod config;
mod error;
mod file;
mod record;
mod schema;
mod setup;
mod system;

use rustyline::{config::Configurer, error::ReadlineError, DefaultEditor};

use error::Result;
use system::System;

fn batch_main(system: System) -> Result<()> {
    Ok(())
}

fn shell_main(system: System) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    rl.set_auto_add_history(true);

    loop {
        // Set some color on prompt
        match rl.readline(&console::style("yoursql> ").blue().to_string()) {
            Ok(line) => {
                println!("Line: {}", line);
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => {
                println!("Good morning, and in case I don't see you,\nGood afternoon,\nGood evening,\nAnd good night.");
                break Ok(());
            }
            Err(err) => {
                println!("Terminal error: {:?}", err);
                break Ok(());
            }
        }
    }
}

fn main() -> Result<()> {
    setup::init_logging();
    let config = setup::init_config();

    let system = system::System::new(config.path.clone());

    if config.batch {
        batch_main(system)
    } else {
        shell_main(system)
    }
}
