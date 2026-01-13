"""Parse SQL files to extract connection headers and queries."""

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from sqlazo.databases import get_handler, get_all_comment_prefixes


@dataclass
class ParsedFile:
    """Result of parsing a SQL file."""
    
    # Connection parameters from header
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    db_type: str = "mysql"
    connection_string: Optional[str] = None  # For MongoDB connection strings
    
    # The SQL query content
    query: str = ""
    
    def get_connection_params(self) -> dict:
        """Return non-None connection parameters as a dict."""
        params = {}
        if self.host:
            params["host"] = self.host
        if self.port:
            params["port"] = self.port
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password
        if self.database is not None:
            params["database"] = self.database
        if self.db_type:
            params["db_type"] = self.db_type
        if self.connection_string:
            params["connection_string"] = self.connection_string
        return params


# Supported header keys and their ParsedFile attribute names
HEADER_KEYS = {
    "host": "host",
    "server": "host",
    "port": "port",
    "user": "user",
    "username": "user",
    "password": "password",
    "pass": "password",
    "db": "database",
    "database": "database",
    "schema": "database",
}


def parse_url(url: str) -> dict:
    """
    Parse a database connection URL using the appropriate handler.
    
    Args:
        url: Connection URL string.
        
    Returns:
        Dict with connection parameters including db_type.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    
    # Handle scheme aliases (postgres -> postgresql)
    handler = get_handler(scheme)
    
    if handler:
        return handler.parse_url(parsed, url)
    
    # Default to MySQL for unknown schemes
    from sqlazo.databases.mysql import MySQLHandler
    return MySQLHandler().parse_url(parsed, url)


def parse_file(content: str) -> ParsedFile:
    """
    Parse a SQL file content to extract connection header and query.
    
    Header format (in comments):
        -- host: localhost
        -- user: myuser
        -- db: mydb
        -- port: 3306
    
    Or URL format:
        -- url: mysql://user:pass@host:port/database
    
    Args:
        content: The full content of the SQL file.
        
    Returns:
        ParsedFile with extracted connection params and query.
    """
    result = ParsedFile()
    lines = content.splitlines()
    query_lines = []
    header_ended = False
    
    # Build pattern for all comment prefixes from handlers
    comment_prefixes = get_all_comment_prefixes()
    # Escape special regex characters
    escaped_prefixes = [re.escape(p) for p in comment_prefixes]
    prefix_pattern = "|".join(escaped_prefixes)
    
    # Pattern to match header comments: -- key: value OR // key: value OR # key: value
    header_pattern = re.compile(rf"^(?:{prefix_pattern})\s*(\w+)\s*:\s*(.+?)\s*$")
    
    for line in lines:
        if header_ended:
            query_lines.append(line)
            continue
            
        stripped = line.strip()
        match = header_pattern.match(stripped)
        
        if match:
            key = match.group(1).lower()
            value = match.group(2)
            
            # Handle URL connection string
            if key == "url":
                url_params = parse_url(value)
                for param_key, param_value in url_params.items():
                    setattr(result, param_key, param_value)
            elif key in HEADER_KEYS:
                attr_name = HEADER_KEYS[key]
                if attr_name == "port":
                    try:
                        setattr(result, attr_name, int(value))
                    except ValueError:
                        pass  # Ignore invalid port
                else:
                    setattr(result, attr_name, value)
            else:
                # Comment with key:value format but not a known header key
                header_ended = True
                query_lines.append(line)
        elif any(stripped.startswith(p) for p in comment_prefixes):
            # Other comment (no key:value format) - part of query
            header_ended = True
            query_lines.append(line)
        elif stripped == "":
            # Empty line - marks end of header section
            header_ended = True
        else:
            # First non-comment, non-empty line marks end of header
            header_ended = True
            query_lines.append(line)
    
    # Join query lines, strip leading/trailing whitespace
    result.query = "\n".join(query_lines).strip()
    
    return result


def parse_file_path(file_path: str) -> ParsedFile:
    """
    Parse a SQL file from a file path.
    
    Args:
        file_path: Path to the SQL file.
        
    Returns:
        ParsedFile with extracted connection params and query.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    return parse_file(content)
