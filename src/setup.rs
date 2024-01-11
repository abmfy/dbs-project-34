//! Basic setup functions.

use clap::Parser;
use env_logger::{self, Env};

use crate::config::Config;

pub fn init_logging() {
    let _ = env_logger::try_init_from_env(Env::new().default_filter_or("info"));
    log::info!("Logging initialized");
}

pub fn init_config() -> Config {
    let config = Config::parse();
    log::info!("Config: {:?}", config);
    config
}
