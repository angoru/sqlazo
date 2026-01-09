"""Database connection management with config priority."""

import os
from dataclasses import dataclass
from typing import Optional, Any

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.errors import Error as MySQLError

import psycopg2
from psycopg2.extensions import connection as PostgreSQLConnection


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    
    host: str = "localhost"
    port: Optional[int] = None  # Will default based on db_type
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    db_type: str = "mysql"  # "mysql" or "postgresql"
    
    def __post_init__(self):
        """Set default port based on database type if not specified."""
        if self.port is None:
            self.port = 5432 if self.db_type == "postgresql" else 3306
    
    @classmethod
    def from_env(cls) -> "ConnectionConfig":
        """Create config from environment variables."""
        config = cls()
        
        if env_host := os.environ.get("SQLAZO_HOST"):
            config.host = env_host
        if env_port := os.environ.get("SQLAZO_PORT"):
            try:
                config.port = int(env_port)
            except ValueError:
                pass
        if env_user := os.environ.get("SQLAZO_USER"):
            config.user = env_user
        if env_password := os.environ.get("SQLAZO_PASSWORD"):
            config.password = env_password
        if env_db := os.environ.get("SQLAZO_DB"):
            config.database = env_db
        if env_db_type := os.environ.get("SQLAZO_DB_TYPE"):
            config.db_type = env_db_type.lower()
            # Update port if not explicitly set
            if not os.environ.get("SQLAZO_PORT"):
                config.port = 5432 if config.db_type == "postgresql" else 3306
            
        return config
    
    def merge_with(self, file_params: dict) -> "ConnectionConfig":
        """
        Merge with file header params. File params take priority.
        
        Args:
            file_params: Dict from ParsedFile.get_connection_params()
            
        Returns:
            New ConnectionConfig with merged values.
        """
        new_db_type = file_params.get("db_type", self.db_type)
        new_port = file_params.get("port", self.port)
        
        # If db_type changed and port wasn't explicitly set, use default for new db_type
        if new_db_type != self.db_type and "port" not in file_params:
            new_port = 5432 if new_db_type == "postgresql" else 3306
        
        return ConnectionConfig(
            host=file_params.get("host", self.host),
            port=new_port,
            user=file_params.get("user", self.user),
            password=file_params.get("password", self.password),
            database=file_params.get("database", self.database),
            db_type=new_db_type,
        )
    
    def validate(self) -> list[str]:
        """
        Validate the configuration.
        
        Returns:
            List of error messages. Empty if valid.
        """
        errors = []
        if not self.user:
            errors.append("User not specified. Set SQLAZO_USER or add '-- user: xxx' to file header.")
        if not self.database:
            errors.append("Database not specified. Set SQLAZO_DB or add '-- db: xxx' to file header.")
        if not self.password:
            errors.append("Password not specified. Set SQLAZO_PASSWORD environment variable.")
        return errors
    
    def to_mysql_kwargs(self) -> dict:
        """Convert to kwargs for mysql.connector.connect()."""
        kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.user:
            kwargs["user"] = self.user
        if self.password:
            kwargs["password"] = self.password
        if self.database:
            kwargs["database"] = self.database
        return kwargs
    
    def to_psycopg_kwargs(self) -> dict:
        """Convert to kwargs for psycopg2.connect()."""
        kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.user:
            kwargs["user"] = self.user
        if self.password:
            kwargs["password"] = self.password
        if self.database:
            kwargs["dbname"] = self.database  # psycopg2 uses 'dbname' not 'database'
        return kwargs


def get_connection(config: ConnectionConfig) -> Any:
    """
    Create a database connection.
    
    Args:
        config: Connection configuration.
        
    Returns:
        Database connection object (MySQL or PostgreSQL).
        
    Raises:
        MySQLError: If MySQL connection fails.
        psycopg2.Error: If PostgreSQL connection fails.
    """
    if config.db_type == "postgresql":
        return psycopg2.connect(**config.to_psycopg_kwargs())
    else:
        return mysql.connector.connect(**config.to_mysql_kwargs())
