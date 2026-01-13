"""Base class for database handlers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import ParseResult


@dataclass
class QueryResult:
    """Result of executing a query."""
    
    # For SELECT queries
    columns: list[str] = None
    rows: list[tuple] = None
    
    # For INSERT/UPDATE/DELETE
    affected_rows: int = 0
    last_insert_id: Optional[int] = None
    
    # Query metadata
    is_select: bool = True
    
    def __post_init__(self):
        if self.columns is None:
            self.columns = []
        if self.rows is None:
            self.rows = []


class DatabaseHandler(ABC):
    """Abstract base class for database handlers."""
    
    # URL schemes this handler supports (e.g., ["redis"], ["mongodb", "mongodb+srv"])
    schemes: list[str] = []
    
    # Default port for this database
    default_port: int = None
    
    # Comment prefixes for header parsing (e.g., ["--"] for SQL, ["#"] for Redis)
    comment_prefixes: list[str] = ["--"]
    
    # Whether this database requires user/password
    requires_auth: bool = True
    
    # Whether this database requires a database name
    requires_database: bool = True
    
    @abstractmethod
    def parse_url(self, parsed: ParseResult, url: str) -> dict:
        """
        Parse a database connection URL.
        
        Args:
            parsed: Parsed URL from urlparse.
            url: Original URL string.
            
        Returns:
            Dict with connection parameters.
        """
        pass
    
    @abstractmethod
    def validate_config(self, config: "ConnectionConfig") -> list[str]:
        """
        Validate the connection configuration.
        
        Args:
            config: Connection configuration to validate.
            
        Returns:
            List of error messages. Empty if valid.
        """
        pass
    
    @abstractmethod
    def get_connection(self, config: "ConnectionConfig") -> Any:
        """
        Create a database connection.
        
        Args:
            config: Connection configuration.
            
        Returns:
            Database connection object.
        """
        pass
    
    @abstractmethod
    def execute_query(self, connection: Any, query: str) -> QueryResult:
        """
        Execute a query and return the result.
        
        Args:
            connection: Active database connection.
            query: Query string to execute.
            
        Returns:
            QueryResult with columns/rows or affected_rows.
        """
        pass
    
    def close_connection(self, connection: Any) -> None:
        """
        Close the database connection.
        
        Args:
            connection: Database connection to close.
        """
        if hasattr(connection, 'close'):
            connection.close()
    
    def get_schema(self, connection: Any, database: str) -> Optional[dict]:
        """
        Get database schema (optional, for autocomplete).
        
        Args:
            connection: Active database connection.
            database: Database name.
            
        Returns:
            Schema dict or None if not supported.
        """
        return None


# Type hint for ConnectionConfig (defined in connection.py to avoid circular import)
ConnectionConfig = Any
