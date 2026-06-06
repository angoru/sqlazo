use std::env;

use anyhow::{bail, Result};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub db_type: Option<String>,
}

impl ConnectionConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        let db_type = env::var("DB_TYPE").ok().map(|value| value.to_lowercase());
        let mut config = Self {
            host: env::var("DB_HOST")
                .ok()
                .or_else(|| Some("localhost".to_string())),
            port: env::var("DB_PORT")
                .ok()
                .and_then(|value| value.parse::<u16>().ok()),
            user: env::var("DB_USER").ok(),
            password: env::var("DB_PASSWORD").ok(),
            database: env::var("DB_DATABASE").ok(),
            db_type,
        };
        config.apply_default_port();
        Ok(config)
    }

    pub fn merge(mut self, file: Self) -> Self {
        let old_db_type = self.db_type.clone();
        if file.host.is_some() {
            self.host = file.host;
        }
        if file.port.is_some() {
            self.port = file.port;
        }
        if file.user.is_some() {
            self.user = file.user;
        }
        if file.password.is_some() {
            self.password = file.password;
        }
        if file.database.is_some() {
            self.database = file.database;
        }
        if file.db_type.is_some() {
            self.db_type = file.db_type.map(|value| value.to_lowercase());
            if self.db_type != old_db_type && file.port.is_none() {
                self.port = None;
                self.apply_default_port();
            }
        }
        self
    }

    pub fn validate(&self) -> Result<()> {
        let db_type = self.db_type.as_deref().unwrap_or("");
        if db_type.is_empty() {
            bail!("Database type not specified. Set DB_TYPE, add a header like '-- db_type: postgresql', or use a URL header.");
        }
        match db_type {
            "sqlite" => {
                if self.database.as_deref().unwrap_or("").is_empty() {
                    bail!("Database not specified. Set DB_DATABASE or add '-- db: xxx' or use URL format.");
                }
            }
            "postgresql" | "mysql" | "mariadb" => {
                if self.user.as_deref().unwrap_or("").is_empty() {
                    bail!("User not specified. Set DB_USER or add '-- user: xxx' to file header.");
                }
                if self.database.as_deref().unwrap_or("").is_empty() {
                    bail!("Database not specified. Set DB_DATABASE or add '-- db: xxx' to file header.");
                }
                if self.password.as_deref().unwrap_or("").is_empty() {
                    bail!("Password not specified. Set DB_PASSWORD environment variable.");
                }
            }
            other => bail!("Unknown database type: {other}"),
        }
        Ok(())
    }

    pub fn db_type(&self) -> &str {
        self.db_type.as_deref().unwrap_or("")
    }

    pub fn database(&self) -> &str {
        self.database.as_deref().unwrap_or("")
    }

    fn apply_default_port(&mut self) {
        if self.port.is_some() {
            return;
        }
        self.port = match self.db_type.as_deref() {
            Some("postgresql") => Some(5432),
            Some("mysql" | "mariadb") => Some(3306),
            _ => None,
        };
    }
}
