"""Database connection management with config priority."""

import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Any

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.errors import Error as MySQLError

import psycopg2
from psycopg2.extensions import connection as PostgreSQLConnection

from pymongo import MongoClient


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    
    host: str = "localhost"
    port: Optional[int] = None  # Will default based on db_type
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    db_type: str = "mysql"  # "mysql", "postgresql", "sqlite", or "mongodb"
    connection_string: Optional[str] = None  # For MongoDB connection strings
    
    def __post_init__(self):
        """Set default port based on database type if not specified."""
        if self.port is None and self.db_type not in ("sqlite", "mongodb"):
            self.port = 5432 if self.db_type == "postgresql" else 3306
        elif self.port is None and self.db_type == "mongodb":
            self.port = 27017
    
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
            # Update port if not explicitly set (SQLite doesn't use ports)
            if not os.environ.get("SQLAZO_PORT") and config.db_type not in ("sqlite",):
                if config.db_type == "postgresql":
                    config.port = 5432
                elif config.db_type == "mongodb":
                    config.port = 27017
                else:
                    config.port = 3306
            
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
        if new_db_type != self.db_type and "port" not in file_params and new_db_type not in ("sqlite",):
            if new_db_type == "postgresql":
                new_port = 5432
            elif new_db_type == "mongodb":
                new_port = 27017
            else:
                new_port = 3306
        
        return ConnectionConfig(
            host=file_params.get("host", self.host),
            port=new_port,
            user=file_params.get("user", self.user),
            password=file_params.get("password", self.password),
            database=file_params.get("database", self.database),
            db_type=new_db_type,
            connection_string=file_params.get("connection_string", self.connection_string),
        )
    
    def validate(self) -> list[str]:
        """
        Validate the configuration.
        
        Returns:
            List of error messages. Empty if valid.
        """
        errors = []
        # SQLite only needs database path, no user/password
        if self.db_type == "sqlite":
            if not self.database:
                errors.append("Database not specified. Set SQLAZO_DB or add '-- db: xxx' or use URL format.")
            return errors
        
        # MongoDB can use connection string or individual params
        if self.db_type == "mongodb":
            if not self.connection_string and not self.host:
                errors.append("MongoDB host or connection string not specified. Use URL format like 'mongodb://localhost:27017/mydb'.")
            if not self.database:
                errors.append("Database not specified. Add database name to your MongoDB URL.")
            return errors
        
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
    
    def to_mongo_kwargs(self) -> dict:
        """Convert to kwargs for pymongo.MongoClient()."""
        # If we have a connection string, prefer that
        if self.connection_string:
            return {"host": self.connection_string}
        
        # Otherwise build connection from individual params
        kwargs = {
            "host": self.host,
            "port": self.port,
        }
        if self.user:
            kwargs["username"] = self.user
        if self.password:
            kwargs["password"] = self.password
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
    if config.db_type == "sqlite":
        return sqlite3.connect(config.database)
    elif config.db_type == "postgresql":
        return psycopg2.connect(**config.to_psycopg_kwargs())
    elif config.db_type == "mongodb":
        client = MongoClient(**config.to_mongo_kwargs())
        # Return a tuple of (client, database) for MongoDB
        return (client, client[config.database])
    else:
        return mysql.connector.connect(**config.to_mysql_kwargs())
