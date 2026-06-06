use std::io::Write;
use std::process::{Command, Stdio};

use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Row;

#[tokio::test]
async fn query_outputs_json_meta_for_sqlite() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let db_path = temp.path().to_string_lossy().to_string();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO users (name) VALUES ('Alice'), ('Bob')")
        .execute(&pool)
        .await
        .unwrap();
    drop(pool);

    let output = run_sqlazo(format!(
        "-- engine: sqlite\n-- db: {db_path}\n\nSELECT id, name FROM users ORDER BY id;"
    ));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["columns"], serde_json::json!(["id", "name"]));
    assert_eq!(data["row_count"], 2);
    assert_eq!(data["rows"][0], serde_json::json!([1, "Alice"]));
    assert_eq!(data["is_select"], true);
}

#[tokio::test]
async fn schema_outputs_sqlite_tables_and_columns() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let db_path = temp.path().to_string_lossy().to_string();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM sqlite_master")
        .fetch_one(&pool)
        .await
        .unwrap()
        .try_get(0)
        .unwrap();
    assert!(count > 0);
    drop(pool);

    let output = run_sqlazo_with_args(
        &["query", "--schema", "-"],
        format!("-- engine: sqlite\n-- db: {db_path}\n\nSELECT 1;"),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["tables"], serde_json::json!(["users"]));
    assert_eq!(data["columns"]["users"][0]["name"], "id");
    assert_eq!(data["columns"]["users"][0]["key"], "PRI");
}

#[tokio::test]
async fn empty_select_keeps_columns() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let db_path = temp.path().to_string_lossy().to_string();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&db_path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    drop(pool);

    let output = run_sqlazo(format!(
        "-- engine: sqlite\n-- db: {db_path}\n\nSELECT id, name FROM users WHERE id = -1;"
    ));
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["columns"], serde_json::json!(["id", "name"]));
    assert_eq!(data["row_count"], 0);
}

fn run_sqlazo(input: String) -> std::process::Output {
    run_sqlazo_with_args(&["query", "-f", "json-meta", "-"], input)
}

fn run_sqlazo_with_args(args: &[&str], input: String) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sqlazo"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}
