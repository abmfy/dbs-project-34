mod config;
mod error;
mod file;
mod record;
mod schema;
mod setup;

fn main() {
    setup::init_logging();
}
