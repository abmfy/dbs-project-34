mod config;
mod error;
mod file;
mod record;
mod schema;
mod setup;

use rustyline::{config::Configurer, error::ReadlineError, DefaultEditor};

use error::Result;

fn batch_main() -> Result<()> {
    Ok(())
}

fn shell_main() -> Result<()> {
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

    if config.batch {
        batch_main()
    } else {
        shell_main()
    }
}
