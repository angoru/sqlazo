use anyhow::{bail, Result};
use url::Url;

use crate::config::ConnectionConfig;

#[derive(Debug, PartialEq, Eq)]
pub struct ParsedFile {
    pub connection: ConnectionConfig,
    pub query: String,
}

pub fn parse_file(content: &str) -> Result<ParsedFile> {
    let mut connection = ConnectionConfig::default();
    let mut query_lines = Vec::new();
    let mut header_ended = false;

    for line in content.lines() {
        if header_ended {
            query_lines.push(line);
            continue;
        }

        let stripped = line.trim();
        if let Some((key, value)) = parse_header(stripped) {
            apply_header(&mut connection, &key, &value)?;
        } else if stripped.is_empty() {
            header_ended = true;
        } else {
            header_ended = true;
            query_lines.push(line);
        }
    }

    let mut query = query_lines.join("\n").trim().to_string();
    if query.starts_with("```") {
        let mut lines: Vec<&str> = query.lines().collect();
        if lines
            .first()
            .is_some_and(|line| line.trim().starts_with("```"))
        {
            lines.remove(0);
        }
        if lines
            .last()
            .is_some_and(|line| line.trim().starts_with("```"))
        {
            lines.pop();
        }
        query = lines.join("\n").trim().to_string();
    }
    if is_comment_only(&query) {
        query.clear();
    }

    Ok(ParsedFile { connection, query })
}

fn parse_header(line: &str) -> Option<(String, String)> {
    let body = line.strip_prefix("--")?.trim();
    let (key, value) = body.split_once(':')?;
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Some((key.trim().to_lowercase(), value.to_string()))
}

fn apply_header(config: &mut ConnectionConfig, key: &str, value: &str) -> Result<()> {
    match key {
        "url" => {
            *config = config.clone().merge(parse_url(value)?);
        }
        "host" | "server" => config.host = Some(value.to_string()),
        "port" => config.port = value.parse::<u16>().ok(),
        "user" | "username" => config.user = Some(value.to_string()),
        "password" | "pass" => config.password = Some(value.to_string()),
        "db" | "database" | "schema" => config.database = Some(value.to_string()),
        "db_type" | "dbtype" | "engine" => config.db_type = Some(value.to_lowercase()),
        _ => {}
    }
    Ok(())
}

fn parse_url(value: &str) -> Result<ConnectionConfig> {
    let url = Url::parse(value)?;
    let mut config = ConnectionConfig::default();
    match url.scheme().to_lowercase().as_str() {
        "sqlite" => {
            config.db_type = Some("sqlite".to_string());
            config.database = Some(parse_sqlite_database(value, &url));
        }
        "postgresql" | "postgres" => {
            config.db_type = Some("postgresql".to_string());
            fill_network_url(&mut config, &url);
        }
        "mysql" | "mariadb" => {
            config.db_type = Some(url.scheme().to_lowercase());
            fill_network_url(&mut config, &url);
        }
        scheme => bail!("Unsupported database URL scheme: {scheme}"),
    }
    Ok(config)
}

fn fill_network_url(config: &mut ConnectionConfig, url: &Url) {
    if let Some(host) = url.host_str() {
        config.host = Some(host.to_string());
    }
    config.port = url.port();
    let user = url.username();
    if !user.is_empty() {
        config.user = Some(decode(user));
    }
    if let Some(password) = url.password() {
        config.password = Some(decode(password));
    }
    let path = url.path().trim_start_matches('/');
    if !path.is_empty() {
        config.database = Some(path.to_string());
    }
}

fn parse_sqlite_database(raw: &str, url: &Url) -> String {
    if raw == "sqlite://:memory:" || url.path() == "/:memory:" {
        return ":memory:".to_string();
    }
    if raw.starts_with("sqlite:////") {
        return format!("/{}", url.path().trim_start_matches('/'));
    }
    if !url.host_str().unwrap_or("").is_empty() {
        return url.host_str().unwrap().to_string();
    }
    url.path().trim_start_matches('/').to_string()
}

fn decode(value: &str) -> String {
    urlencoding::decode(value)
        .map(|value| value.into_owned())
        .unwrap_or_else(|_| value.to_string())
}

fn is_comment_only(query: &str) -> bool {
    query
        .lines()
        .all(|line| line.trim().is_empty() || line.trim().starts_with("--"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_headers_and_query() {
        let parsed = parse_file("-- engine: sqlite\n-- db: /tmp/app.db\n\nSELECT 1;").unwrap();
        assert_eq!(parsed.connection.db_type.as_deref(), Some("sqlite"));
        assert_eq!(parsed.connection.database.as_deref(), Some("/tmp/app.db"));
        assert_eq!(parsed.query, "SELECT 1;");
    }

    #[test]
    fn parses_postgres_url_alias() {
        let parsed =
            parse_file("-- url: postgres://user:p%40ss@localhost/app\n\nSELECT 1;").unwrap();
        assert_eq!(parsed.connection.db_type.as_deref(), Some("postgresql"));
        assert_eq!(parsed.connection.user.as_deref(), Some("user"));
        assert_eq!(parsed.connection.password.as_deref(), Some("p@ss"));
        assert_eq!(parsed.connection.database.as_deref(), Some("app"));
    }

    #[test]
    fn parses_sqlite_absolute_url() {
        let parsed = parse_file("-- url: sqlite:////tmp/my.db\n\nSELECT 1;").unwrap();
        assert_eq!(parsed.connection.database.as_deref(), Some("/tmp/my.db"));
    }
}
