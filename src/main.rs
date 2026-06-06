mod cli;
mod config;
mod db;
mod output;
mod parser;
mod schema;

use anyhow::Result;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    cli::run().await
}
