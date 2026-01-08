"""Database connection management with config priority."""

import os
from dataclasses import dataclass, field
from typing import Optional

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.errors import Error as MySQLError


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    
    host: str = "localhost"
    port: int = 3306
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    
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
            
        return config
    
    def merge_with(self, file_params: dict) -> "ConnectionConfig":
        """
        Merge with file header params. File params take priority.
        
        Args:
            file_params: Dict from ParsedFile.get_connection_params()
            
        Returns:
            New ConnectionConfig with merged values.
        """
        return ConnectionConfig(
            host=file_params.get("host", self.host),
            port=file_params.get("port", self.port),
            user=file_params.get("user", self.user),
            password=file_params.get("password", self.password),
            database=file_params.get("database", self.database),
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
    
    def to_connect_kwargs(self) -> dict:
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


def get_connection(config: ConnectionConfig) -> MySQLConnection:
    """
    Create a database connection.
    
    Args:
        config: Connection configuration.
        
    Returns:
        MySQL connection object.
        
    Raises:
        MySQLError: If connection fails.
    """
    return mysql.connector.connect(**config.to_connect_kwargs())
