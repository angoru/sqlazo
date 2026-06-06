use std::io::{self, Read};
use std::time::Instant;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

use crate::config::ConnectionConfig;
use crate::db;
use crate::output::{ConnectionMetadata, JsonMetaOutput, QueryMetadata};
use crate::parser;
use crate::schema;

#[derive(Parser)]
#[command(name = "sqlazo", version, about = "Run SQL queries for sqlazo.nvim")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Query {
        #[arg(short = 'f', long = "format", default_value = "json-meta")]
        format: String,
        #[arg(long)]
        schema: bool,
        file: String,
    },
}

pub async fn run() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Query {
            format,
            schema,
            file,
        } => run_query(&file, &format, schema).await,
    }
}

async fn run_query(file: &str, format: &str, schema_mode: bool) -> Result<()> {
    if file != "-" {
        bail!("Rust sqlazo currently supports stdin only; use '-' as the query file");
    }
    if !schema_mode && format != "json-meta" {
        bail!("Rust sqlazo currently supports only '-f json-meta'");
    }

    let mut content = String::new();
    io::stdin().read_to_string(&mut content)?;
    let parsed = parser::parse_file(&content)?;
    if parsed.query.trim().is_empty() {
        bail!("No query found in file.");
    }

    let config = ConnectionConfig::from_env()?.merge(parsed.connection);
    config.validate()?;

    if schema_mode {
        let value = schema::load_schema(&config).await?;
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    let started_at = Instant::now();
    let result = db::execute_query(&config, &parsed.query).await?;
    let duration_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let output = JsonMetaOutput {
        columns: result.columns,
        row_count: result.rows.len(),
        rows: result.rows,
        is_select: result.is_select,
        affected_rows: result.affected_rows,
        last_insert_id: result.last_insert_id,
        metadata: QueryMetadata {
            duration_ms,
            query: parsed.query,
            connection: ConnectionMetadata::from(&config),
        },
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
