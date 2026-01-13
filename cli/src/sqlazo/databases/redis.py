"""Redis database handler."""

from typing import Any
from urllib.parse import ParseResult, unquote

import redis as redis_client

from sqlazo.databases.base import DatabaseHandler, QueryResult


class RedisHandler(DatabaseHandler):
    """Handler for Redis databases."""
    
    schemes = ["redis"]
    default_port = 6379
    comment_prefixes = ["#"]
    requires_auth = False
    requires_database = False
    
    def parse_url(self, parsed: ParseResult, url: str) -> dict:
        """Parse Redis connection URL."""
        params = {"db_type": "redis"}
        
        if parsed.hostname:
            params["host"] = parsed.hostname
        if parsed.port:
            params["port"] = parsed.port
        if parsed.username:
            params["user"] = unquote(parsed.username)
        if parsed.password:
            params["password"] = unquote(parsed.password)
        # Redis uses path for database number (e.g., redis://localhost:6379/0)
        if parsed.path and parsed.path != "/":
            db_num = parsed.path.lstrip("/")
            if db_num.isdigit():
                params["database"] = int(db_num)
        
        return params
    
    def validate_config(self, config) -> list[str]:
        """Validate Redis configuration."""
        errors = []
        if not config.host:
            errors.append("Redis host not specified. Use URL format like 'redis://localhost:6379/0'.")
        return errors
    
    def get_connection(self, config) -> Any:
        """Create Redis connection."""
        kwargs = {
            "host": config.host,
            "port": config.port or self.default_port,
            "decode_responses": True,
        }
        if config.user:
            kwargs["username"] = config.user
        if config.password:
            kwargs["password"] = config.password
        if config.database is not None:
            kwargs["db"] = config.database if isinstance(config.database, int) else 0
        
        return redis_client.Redis(**kwargs)
    
    def execute_query(self, connection: Any, query: str) -> QueryResult:
        """Execute Redis commands."""
        lines = query.strip().splitlines()
        results = []
        
        for line in lines:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue
            
            parts = self._parse_command(line)
            if not parts:
                continue
            
            command = parts[0].upper()
            args = parts[1:]
            
            try:
                result = connection.execute_command(command, *args)
                results.append((command, args, result))
            except Exception as e:
                results.append((command, args, f"ERROR: {e}"))
        
        # Format results for display
        if len(results) == 1:
            command, args, result = results[0]
            return self._result_to_query_result(command, result)
        else:
            rows = []
            for command, args, result in results:
                args_str = " ".join(str(a) for a in args) if args else ""
                result_str = self._format_value(result)
                rows.append((command, args_str, result_str))
            
            return QueryResult(
                columns=["command", "args", "result"],
                rows=rows,
                is_select=True,
            )
    
    def _parse_command(self, line: str) -> list:
        """Parse a Redis command line into command and arguments."""
        parts = []
        current = ""
        in_quotes = False
        quote_char = None
        
        for char in line:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
            elif char == " " and not in_quotes:
                if current:
                    parts.append(current)
                    current = ""
            else:
                current += char
        
        if current:
            parts.append(current)
        
        return parts
    
    def _format_value(self, value: Any) -> str:
        """Format a Redis value for display."""
        if value is None:
            return "(nil)"
        elif isinstance(value, bool):
            return "OK" if value else "(error)"
        elif isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        elif isinstance(value, list):
            if not value:
                return "(empty list)"
            formatted = []
            for i, item in enumerate(value, 1):
                formatted.append(f"{i}) {self._format_value(item)}")
            return "\n".join(formatted)
        elif isinstance(value, dict):
            if not value:
                return "(empty hash)"
            formatted = []
            for k, v in value.items():
                formatted.append(f"{k}: {self._format_value(v)}")
            return "\n".join(formatted)
        elif isinstance(value, set):
            if not value:
                return "(empty set)"
            return ", ".join(str(v) for v in value)
        else:
            return str(value)
    
    def _result_to_query_result(self, command: str, result: Any) -> QueryResult:
        """Convert a Redis command result to QueryResult."""
        # Commands that return key-value pairs (hashes)
        if command in ("HGETALL", "CONFIG"):
            if isinstance(result, dict):
                return QueryResult(
                    columns=["field", "value"],
                    rows=[(k, self._format_value(v)) for k, v in result.items()],
                    is_select=True,
                )
        
        # Commands that return lists
        if isinstance(result, list):
            return QueryResult(
                columns=["index", "value"],
                rows=[(i, self._format_value(v)) for i, v in enumerate(result)],
                is_select=True,
            )
        
        # Commands that return sets
        if isinstance(result, set):
            return QueryResult(
                columns=["member"],
                rows=[(m,) for m in result],
                is_select=True,
            )
        
        # Single value result
        return QueryResult(
            columns=["result"],
            rows=[(self._format_value(result),)],
            is_select=True,
        )
