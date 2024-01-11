//! Basic setup functions.

use env_logger::{self, Env};

pub fn init_logging() {
    let _ = env_logger::try_init_from_env(Env::new().default_filter_or("info"));
    log::info!("Logging initialized");
}
