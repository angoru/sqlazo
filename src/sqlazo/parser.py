"""Parse SQL files to extract connection headers and queries."""

import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse, unquote


@dataclass
class ParsedFile:
    """Result of parsing a SQL file."""
    
    # Connection parameters from header
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    db_type: str = "mysql"  # "mysql", "postgresql", "sqlite", or "mongodb"
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
        if self.database:
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
    Parse a database connection URL.
    
    Format: mysql://user:password@host:port/database
            postgresql://user:password@host:port/database
            sqlite:///path/to/database.db
            sqlite://:memory:
    
    Args:
        url: Connection URL string.
        
    Returns:
        Dict with connection parameters including db_type.
    """
    parsed = urlparse(url)
    params = {}
    
    # Detect database type from scheme
    scheme = parsed.scheme.lower()
    if scheme in ("postgresql", "postgres"):
        params["db_type"] = "postgresql"
    elif scheme == "sqlite":
        params["db_type"] = "sqlite"
        # SQLite uses file path as database, not host/user/password
        # Handle :memory: special case
        if parsed.netloc == ":memory:" or parsed.path == "/:memory:":
            params["database"] = ":memory:"
        else:
            # For sqlite:///path/to/db, the path is the database file
            # netloc is empty, path contains the full path
            params["database"] = parsed.path if parsed.path else parsed.netloc
        return params
    elif scheme in ("mongodb", "mongodb+srv"):
        params["db_type"] = "mongodb"
        # MongoDB uses same host/port/user/password as SQL databases
        # but also supports mongodb+srv:// for DNS seedlist
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
        # Store the full URL for MongoDB (pymongo prefers connection strings)
        params["connection_string"] = url
        return params
    else:
        params["db_type"] = "mysql"
    
    if parsed.hostname:
        params["host"] = parsed.hostname
    if parsed.port:
        params["port"] = parsed.port
    if parsed.username:
        params["user"] = unquote(parsed.username)
    if parsed.password:
        params["password"] = unquote(parsed.password)
    if parsed.path and parsed.path != "/":
        # Remove leading slash
        params["database"] = parsed.path.lstrip("/")
    
    return params


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
    
    # Patterns to match header comments: -- key: value OR // key: value (for MongoDB)
    sql_header_pattern = re.compile(r"^--\s*(\w+)\s*:\s*(.+?)\s*$")
    js_header_pattern = re.compile(r"^//\s*(\w+)\s*:\s*(.+?)\s*$")
    
    for line in lines:
        if header_ended:
            query_lines.append(line)
            continue
            
        # Check if this is a header comment (SQL or JS style)
        stripped = line.strip()
        match = sql_header_pattern.match(stripped) or js_header_pattern.match(stripped)
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
                # This could be part of the query, so end header
                header_ended = True
                query_lines.append(line)
        elif stripped.startswith("--") or stripped.startswith("//"):
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

