"""MySQL database handler."""

from typing import Any
from urllib.parse import ParseResult, unquote

import mysql.connector
from mysql.connector.errors import Error as MySQLError

from sqlazo.databases.base import DatabaseHandler, QueryResult


class MySQLHandler(DatabaseHandler):
    """Handler for MySQL databases."""
    
    schemes = ["mysql"]
    default_port = 3306
    comment_prefixes = ["--"]
    requires_auth = True
    requires_database = True
    
    def parse_url(self, parsed: ParseResult, url: str) -> dict:
        """Parse MySQL connection URL."""
        params = {"db_type": "mysql"}
        
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
        """Validate MySQL configuration."""
        errors = []
        if not config.user:
            errors.append("User not specified. Set DB_USER or add '-- user: xxx' to file header.")
        if not config.database:
            errors.append("Database not specified. Set DB_DATABASE or add '-- db: xxx' to file header.")
        if not config.password:
            errors.append("Password not specified. Set DB_PASSWORD environment variable.")
        return errors
    
    def get_connection(self, config) -> Any:
        """Create MySQL connection."""
        kwargs = {
            "host": config.host,
            "port": config.port or self.default_port,
        }
        if config.user:
            kwargs["user"] = config.user
        if config.password:
            kwargs["password"] = config.password
        if config.database:
            kwargs["database"] = config.database
        
        return mysql.connector.connect(**kwargs)
    
    def execute_query(self, connection: Any, query: str) -> QueryResult:
        """Execute MySQL query."""
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
                    last_insert_id=getattr(cursor, 'lastrowid', None),
                    is_select=False,
                )
        finally:
            cursor.close()
