use anyhow::{bail, Result};
use serde_json::{json, Value};
use sqlx::Row;

use crate::config::ConnectionConfig;
use crate::db;

pub async fn load_schema(config: &ConnectionConfig) -> Result<Value> {
    match config.db_type() {
        "sqlite" => sqlite_schema(config).await,
        "postgresql" => postgres_schema(config).await,
        "mysql" | "mariadb" => mysql_schema(config).await,
        other => bail!("Unknown database type: {other}"),
    }
}

async fn sqlite_schema(config: &ConnectionConfig) -> Result<Value> {
    let pool = db::sqlite_pool(config).await?;
    let table_rows = sqlx::query(
        "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await?;

    let mut tables = Vec::new();
    let mut columns = serde_json::Map::new();
    for row in table_rows {
        let table_name: String = row.try_get("name")?;
        tables.push(table_name.clone());
        let escaped = table_name.replace('\'', "''");
        let pragma = format!("PRAGMA table_info('{escaped}')");
        let column_rows = sqlx::query(&pragma).fetch_all(&pool).await?;
        let table_columns: Vec<Value> = column_rows
            .into_iter()
            .map(|column| {
                let name: String = column.try_get("name").unwrap_or_default();
                let data_type: String = column.try_get("type").unwrap_or_default();
                let pk: i64 = column.try_get("pk").unwrap_or(0);
                json!({ "name": name, "type": data_type, "key": if pk != 0 { "PRI" } else { "" } })
            })
            .collect();
        columns.insert(table_name, Value::Array(table_columns));
    }

    Ok(json!({
        "database": config.database(),
        "tables": tables,
        "columns": columns,
    }))
}

async fn postgres_schema(config: &ConnectionConfig) -> Result<Value> {
    let pool = db::postgres_pool(config).await?;
    let table_rows = sqlx::query(
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await?;

    let mut tables = Vec::new();
    let mut columns = serde_json::Map::new();
    for row in table_rows {
        let table_name: String = row.try_get("table_name")?;
        tables.push(table_name.clone());
        let column_rows = sqlx::query(
            r#"
            SELECT
                c.column_name,
                c.data_type,
                CASE
                    WHEN pk.column_name IS NOT NULL THEN 'PRI'
                    WHEN uq.column_name IS NOT NULL THEN 'UNI'
                    ELSE ''
                END as column_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = 'public'
                    AND tc.table_name = $1
            ) pk ON c.column_name = pk.column_name
            LEFT JOIN (
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'UNIQUE'
                    AND tc.table_schema = 'public'
                    AND tc.table_name = $2
            ) uq ON c.column_name = uq.column_name
            WHERE c.table_schema = 'public' AND c.table_name = $3
            ORDER BY c.ordinal_position
            "#,
        )
        .bind(&table_name)
        .bind(&table_name)
        .bind(&table_name)
        .fetch_all(&pool)
        .await?;
        columns.insert(
            table_name,
            Value::Array(column_values(
                column_rows,
                "column_name",
                "data_type",
                "column_key",
            )),
        );
    }

    Ok(json!({
        "database": config.database(),
        "tables": tables,
        "columns": columns,
    }))
}

async fn mysql_schema(config: &ConnectionConfig) -> Result<Value> {
    let pool = db::mysql_pool(config).await?;
    let table_rows = sqlx::query(
        "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
    )
    .bind(config.database())
    .fetch_all(&pool)
    .await?;

    let mut tables = Vec::new();
    let mut columns = serde_json::Map::new();
    for row in table_rows {
        let table_name: String = row.try_get("TABLE_NAME")?;
        tables.push(table_name.clone());
        let column_rows = sqlx::query(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        )
        .bind(config.database())
        .bind(&table_name)
        .fetch_all(&pool)
        .await?;
        columns.insert(
            table_name,
            Value::Array(column_values(
                column_rows,
                "COLUMN_NAME",
                "DATA_TYPE",
                "COLUMN_KEY",
            )),
        );
    }

    Ok(json!({
        "database": config.database(),
        "tables": tables,
        "columns": columns,
    }))
}

fn column_values<R>(rows: Vec<R>, name_key: &str, type_key: &str, key_key: &str) -> Vec<Value>
where
    R: Row,
    for<'r> &'r str: sqlx::ColumnIndex<R>,
    String: for<'r> sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    rows.into_iter()
        .map(|row| {
            let name: String = row.try_get(name_key).unwrap_or_default();
            let data_type: String = row.try_get(type_key).unwrap_or_default();
            let key: String = row.try_get(key_key).unwrap_or_default();
            json!({ "name": name, "type": data_type, "key": key })
        })
        .collect()
}
