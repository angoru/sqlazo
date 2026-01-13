"""PostgreSQL database handler."""

from typing import Any
from urllib.parse import ParseResult, unquote

import psycopg2
from psycopg2.extensions import connection as PostgreSQLConnection

from sqlazo.databases.base import DatabaseHandler, QueryResult


class PostgreSQLHandler(DatabaseHandler):
    """Handler for PostgreSQL databases."""
    
    schemes = ["postgresql", "postgres"]
    default_port = 5432
    comment_prefixes = ["--"]
    requires_auth = True
    requires_database = True
    
    def parse_url(self, parsed: ParseResult, url: str) -> dict:
        """Parse PostgreSQL connection URL."""
        params = {"db_type": "postgresql"}
        
        if parsed.hostname:
            params["host"] = parsed.hostname
        if parsed.port:
            params["port"] = parsed.port
        if parsed.username:
            params["user"] = unquote(parsed.username)
        if parsed.password:
            params["password"] = unquote(parsed.password)
        if parsed.path and parsed.path != "/":
            params["database"] = parsed.path.lstrip("/")
        
        return params
    
    def validate_config(self, config) -> list[str]:
        """Validate PostgreSQL configuration."""
        errors = []
        if not config.user:
            errors.append("User not specified. Set SQLAZO_USER or add '-- user: xxx' to file header.")
        if not config.database:
            errors.append("Database not specified. Set SQLAZO_DB or add '-- db: xxx' to file header.")
        if not config.password:
            errors.append("Password not specified. Set SQLAZO_PASSWORD environment variable.")
        return errors
    
    def get_connection(self, config) -> Any:
        """Create PostgreSQL connection."""
        kwargs = {
            "host": config.host,
            "port": config.port or self.default_port,
        }
        if config.user:
            kwargs["user"] = config.user
        if config.password:
            kwargs["password"] = config.password
        if config.database:
            kwargs["dbname"] = config.database  # psycopg2 uses 'dbname'
        
        return psycopg2.connect(**kwargs)
    
    def execute_query(self, connection: Any, query: str) -> QueryResult:
        """Execute PostgreSQL query."""
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
                    is_select=False,
                )
        finally:
            cursor.close()
