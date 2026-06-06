use serde::Serialize;
use serde_json::Value;

use crate::config::ConnectionConfig;

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub affected_rows: u64,
    pub last_insert_id: Option<i64>,
    pub is_select: bool,
}

#[derive(Serialize)]
pub struct JsonMetaOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
    pub is_select: bool,
    pub affected_rows: u64,
    pub last_insert_id: Option<i64>,
    pub metadata: QueryMetadata,
}

#[derive(Serialize)]
pub struct QueryMetadata {
    pub duration_ms: f64,
    pub query: String,
    pub connection: ConnectionMetadata,
}

#[derive(Serialize)]
pub struct ConnectionMetadata {
    pub db_type: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
}

impl From<&ConnectionConfig> for ConnectionMetadata {
    fn from(config: &ConnectionConfig) -> Self {
        Self {
            db_type: config.db_type.clone(),
            host: config.host.clone(),
            port: config.port,
            database: config.database.clone(),
            user: config.user.clone(),
        }
    }
}
