use anyhow::{bail, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use serde_json::Value;
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Executor, MySqlPool, PgPool, Row, SqlitePool, TypeInfo, ValueRef};

use crate::config::ConnectionConfig;
use crate::output::QueryResult;

pub async fn execute_query(config: &ConnectionConfig, query: &str) -> Result<QueryResult> {
    match config.db_type() {
        "sqlite" => execute_sqlite(config, query).await,
        "postgresql" => execute_postgres(config, query).await,
        "mysql" | "mariadb" => execute_mysql(config, query).await,
        other => bail!("Unknown database type: {other}"),
    }
}

pub async fn sqlite_pool(config: &ConnectionConfig) -> Result<SqlitePool> {
    if config.database() == ":memory:" {
        return Ok(SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?);
    }
    let options = SqliteConnectOptions::new()
        .filename(config.database())
        .create_if_missing(false);
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?)
}

pub async fn postgres_pool(config: &ConnectionConfig) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(5)
        .connect(&network_url(config, "postgresql"))
        .await?)
}

pub async fn mysql_pool(config: &ConnectionConfig) -> Result<MySqlPool> {
    Ok(MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&network_url(config, config.db_type()))
        .await?)
}

async fn execute_sqlite(config: &ConnectionConfig, query: &str) -> Result<QueryResult> {
    let pool = sqlite_pool(config).await?;
    if returns_rows(query) {
        let columns = describe_columns(&pool, query).await?;
        let rows = sqlx::query(query).fetch_all(&pool).await?;
        Ok(rows_to_result(rows, columns, sqlite_value, true, 0, None))
    } else {
        let result = sqlx::query(query).execute(&pool).await?;
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: result.rows_affected(),
            last_insert_id: Some(result.last_insert_rowid()),
            is_select: false,
        })
    }
}

async fn execute_postgres(config: &ConnectionConfig, query: &str) -> Result<QueryResult> {
    let pool = postgres_pool(config).await?;
    if returns_rows(query) {
        let columns = describe_columns(&pool, query).await?;
        let rows = sqlx::query(query).fetch_all(&pool).await?;
        Ok(rows_to_result(rows, columns, pg_value, true, 0, None))
    } else {
        let result = sqlx::query(query).execute(&pool).await?;
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: result.rows_affected(),
            last_insert_id: None,
            is_select: false,
        })
    }
}

async fn execute_mysql(config: &ConnectionConfig, query: &str) -> Result<QueryResult> {
    let pool = mysql_pool(config).await?;
    if returns_rows(query) {
        let columns = describe_columns(&pool, query).await?;
        let rows = sqlx::query(query).fetch_all(&pool).await?;
        Ok(rows_to_result(rows, columns, mysql_value, true, 0, None))
    } else {
        let result = sqlx::query(query).execute(&pool).await?;
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: result.rows_affected(),
            last_insert_id: Some(result.last_insert_id() as i64),
            is_select: false,
        })
    }
}

fn rows_to_result<R, F>(
    rows: Vec<R>,
    columns: Vec<String>,
    value_fn: F,
    is_select: bool,
    affected_rows: u64,
    last_insert_id: Option<i64>,
) -> QueryResult
where
    R: Row,
    F: Fn(&R, usize) -> Value,
{
    let rows = rows
        .iter()
        .map(|row| {
            (0..row.columns().len())
                .map(|index| value_fn(row, index))
                .collect()
        })
        .collect();
    QueryResult {
        columns,
        rows,
        affected_rows,
        last_insert_id,
        is_select,
    }
}

async fn describe_columns<'e, E>(executor: E, query: &str) -> Result<Vec<String>>
where
    E: Executor<'e>,
{
    Ok(executor
        .describe(query)
        .await?
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect())
}

fn sqlite_value(row: &SqliteRow, index: usize) -> Value {
    if row.try_get_raw(index).is_ok_and(|raw| raw.is_null()) {
        return Value::Null;
    }
    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Value::from(value);
    }
    if let Ok(value) = row.try_get::<f64, _>(index) {
        return Value::from(value);
    }
    if let Ok(value) = row.try_get::<String, _>(index) {
        return Value::from(value);
    }
    row.try_get::<Vec<u8>, _>(index)
        .map(|bytes| Value::from(format!("{bytes:?}")))
        .unwrap_or(Value::Null)
}

fn pg_value(row: &PgRow, index: usize) -> Value {
    if row.try_get_raw(index).is_ok_and(|raw| raw.is_null()) {
        return Value::Null;
    }
    match row.column(index).type_info().name() {
        "BOOL" => row
            .try_get::<bool, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null),
        "INT2" => row
            .try_get::<i16, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null),
        "INT4" => row
            .try_get::<i32, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null),
        "INT8" => row
            .try_get::<i64, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null),
        "FLOAT4" => row
            .try_get::<f32, _>(index)
            .map(|value| Value::from(value as f64))
            .unwrap_or(Value::Null),
        "FLOAT8" => row
            .try_get::<f64, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null),
        "DATE" => row
            .try_get::<NaiveDate, _>(index)
            .map(|value| Value::from(format_date(value)))
            .unwrap_or(Value::Null),
        "TIME" => row
            .try_get::<NaiveTime, _>(index)
            .map(|value| Value::from(format_time(value)))
            .unwrap_or(Value::Null),
        "TIMESTAMP" => row
            .try_get::<NaiveDateTime, _>(index)
            .map(|value| Value::from(format_datetime(value)))
            .unwrap_or(Value::Null),
        "TIMESTAMPTZ" => row
            .try_get::<DateTime<Utc>, _>(index)
            .map(|value| Value::from(format_datetime_utc(value)))
            .unwrap_or(Value::Null),
        _ => row
            .try_get::<String, _>(index)
            .map(Value::from)
            .unwrap_or_else(|_| Value::from(format!("<{}>", row.column(index).type_info().name()))),
    }
}

fn mysql_value(row: &MySqlRow, index: usize) -> Value {
    if row.try_get_raw(index).is_ok_and(|raw| raw.is_null()) {
        return Value::Null;
    }
    let type_name = row.column(index).type_info().name().to_ascii_lowercase();
    if type_name.contains("bool") {
        return row
            .try_get::<bool, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null);
    }
    if type_name.contains("int") {
        return row
            .try_get::<i64, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null);
    }
    if type_name.contains("float") || type_name.contains("double") || type_name.contains("decimal")
    {
        return row
            .try_get::<f64, _>(index)
            .map(Value::from)
            .unwrap_or(Value::Null);
    }
    if type_name == "date" {
        return row
            .try_get::<NaiveDate, _>(index)
            .map(|value| Value::from(format_date(value)))
            .unwrap_or(Value::Null);
    }
    if type_name == "time" {
        return row
            .try_get::<NaiveTime, _>(index)
            .map(|value| Value::from(format_time(value)))
            .unwrap_or(Value::Null);
    }
    if type_name == "datetime" || type_name == "timestamp" {
        return row
            .try_get::<NaiveDateTime, _>(index)
            .map(|value| Value::from(format_datetime(value)))
            .unwrap_or(Value::Null);
    }
    row.try_get::<String, _>(index)
        .map(Value::from)
        .unwrap_or_else(|_| Value::from(format!("<{}>", row.column(index).type_info().name())))
}

fn format_date(value: NaiveDate) -> String {
    value.format("%Y-%m-%d").to_string()
}

fn format_time(value: NaiveTime) -> String {
    if value.nanosecond() == 0 {
        value.format("%H:%M:%S").to_string()
    } else {
        value.format("%H:%M:%S%.6f").to_string()
    }
}

fn format_datetime(value: NaiveDateTime) -> String {
    if value.and_utc().timestamp_subsec_micros() == 0 {
        value.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        value.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}

fn format_datetime_utc(value: DateTime<Utc>) -> String {
    format_datetime(value.naive_utc())
}

fn returns_rows(query: &str) -> bool {
    let first = executable_sql(query)
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('(')
        .to_ascii_uppercase();
    matches!(
        first.as_str(),
        "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN"
    )
}

fn executable_sql(mut query: &str) -> &str {
    loop {
        query = query.trim_start();
        if let Some(rest) = query.strip_prefix("--") {
            query = rest.split_once('\n').map(|(_, rest)| rest).unwrap_or("");
        } else if let Some(rest) = query.strip_prefix('#') {
            query = rest.split_once('\n').map(|(_, rest)| rest).unwrap_or("");
        } else if let Some(rest) = query.strip_prefix("/*") {
            query = rest.split_once("*/").map(|(_, rest)| rest).unwrap_or("");
        } else {
            return query;
        }
    }
}

fn network_url(config: &ConnectionConfig, scheme: &str) -> String {
    let user = urlencoding::encode(config.user.as_deref().unwrap_or(""));
    let password = urlencoding::encode(config.password.as_deref().unwrap_or(""));
    let host = config.host.as_deref().unwrap_or("localhost");
    let port = config
        .port
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    let database = config.database.as_deref().unwrap_or("");
    format!("{scheme}://{user}:{password}@{host}{port}/{database}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_rows_skips_leading_line_comments() {
        assert!(returns_rows(
            "-- Search mappings in rol_permits.\nSELECT * FROM auth_user;"
        ));
    }

    #[test]
    fn returns_rows_skips_leading_block_comments() {
        assert!(returns_rows("/* context */\nSELECT 1;"));
    }

    #[test]
    fn formats_temporal_values_without_fraction_when_zero() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 3).unwrap();
        let time = NaiveTime::from_hms_opt(14, 5, 9).unwrap();
        let datetime = date.and_time(time);

        assert_eq!(format_date(date), "2026-06-03");
        assert_eq!(format_time(time), "14:05:09");
        assert_eq!(format_datetime(datetime), "2026-06-03 14:05:09");
    }

    #[test]
    fn formats_temporal_values_with_microseconds() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 3).unwrap();
        let time = NaiveTime::from_hms_micro_opt(14, 5, 9, 123456).unwrap();
        let datetime = date.and_time(time);

        assert_eq!(format_time(time), "14:05:09.123456");
        assert_eq!(format_datetime(datetime), "2026-06-03 14:05:09.123456");
    }
}
