"""SQLite database handler."""

import sqlite3
from typing import Any
from urllib.parse import ParseResult

from sqlazo.databases.base import DatabaseHandler, QueryResult


class SQLiteHandler(DatabaseHandler):
    """Handler for SQLite databases."""
    
    schemes = ["sqlite"]
    default_port = None  # SQLite doesn't use ports
    comment_prefixes = ["--"]
    requires_auth = False
    requires_database = True
    
    def parse_url(self, parsed: ParseResult, url: str) -> dict:
        """Parse SQLite connection URL."""
        params = {"db_type": "sqlite"}
        
        # Handle :memory: special case
        if parsed.netloc == ":memory:" or parsed.path == "/:memory:":
            params["database"] = ":memory:"
        else:
            # For sqlite:///path/to/db, the path is the database file
            params["database"] = parsed.path if parsed.path else parsed.netloc
        
        return params
    
    def validate_config(self, config) -> list[str]:
        """Validate SQLite configuration."""
        errors = []
        if not config.database:
            errors.append("Database not specified. Set DB_DATABASE or add '-- db: xxx' or use URL format.")
        return errors
    
    def get_connection(self, config) -> Any:
        """Create SQLite connection."""
        return sqlite3.connect(config.database)
    
    def execute_query(self, connection: Any, query: str) -> QueryResult:
        """Execute SQLite query."""
        cursor = connection.cursor()
        
        try:
            cursor.execute(query)
            
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                return QueryResult(
                    columns=columns,
                    rows=rows,
                    is_select=True,
                )
            else:
                connection.commit()
                return QueryResult(
                    affected_rows=cursor.rowcount,
                    last_insert_id=cursor.lastrowid,
                    is_select=False,
                )
        finally:
            cursor.close()
