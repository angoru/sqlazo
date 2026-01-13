"""Database connection management with config priority."""

import os
from dataclasses import dataclass
from typing import Optional, Any

from sqlazo.databases import get_handler_for_db_type


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    
    host: str = "localhost"
    port: Optional[int] = None  # Will default based on db_type
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    db_type: str = "mysql"
    connection_string: Optional[str] = None  # For MongoDB connection strings
    
    def __post_init__(self):
        """Set default port based on database type if not specified."""
        if self.port is None:
            handler = get_handler_for_db_type(self.db_type)
            if handler and handler.default_port:
                self.port = handler.default_port
    
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
            # Update port based on handler default if not explicitly set
            if not os.environ.get("SQLAZO_PORT"):
                handler = get_handler_for_db_type(config.db_type)
                if handler and handler.default_port:
                    config.port = handler.default_port
            
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
            handler = get_handler_for_db_type(new_db_type)
            if handler and handler.default_port:
                new_port = handler.default_port
        
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
        Validate the configuration using the appropriate handler.
        
        Returns:
            List of error messages. Empty if valid.
        """
        handler = get_handler_for_db_type(self.db_type)
        if handler:
            return handler.validate_config(self)
        return ["Unknown database type: " + self.db_type]


def get_connection(config: ConnectionConfig) -> Any:
    """
    Create a database connection using the appropriate handler.
    
    Args:
        config: Connection configuration.
        
    Returns:
        Database connection object.
    """
    handler = get_handler_for_db_type(config.db_type)
    if handler:
        return handler.get_connection(config)
    raise ValueError(f"Unknown database type: {config.db_type}")
