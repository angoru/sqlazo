"""Tests for the parser module."""

import pytest
from sqlazo.parser import parse_file, ParsedFile


class TestParseFile:
    """Tests for parse_file function."""
    
    def test_basic_header_parsing(self):
        """Test parsing a file with all header fields."""
        content = """-- host: localhost
-- user: myuser
-- db: testdb
-- port: 3306

SELECT * FROM users;
"""
        result = parse_file(content)
        
        assert result.host == "localhost"
        assert result.user == "myuser"
        assert result.database == "testdb"
        assert result.port == 3306
        assert result.query == "SELECT * FROM users;"
    
    def test_alternative_key_names(self):
        """Test parsing with alternative key names."""
        content = """-- server: myserver
-- username: admin
-- schema: myschema

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.host == "myserver"
        assert result.user == "admin"
        assert result.database == "myschema"
    
    def test_no_header(self):
        """Test parsing a file with no header."""
        content = "SELECT * FROM users;"
        result = parse_file(content)
        
        assert result.host is None
        assert result.user is None
        assert result.database is None
        assert result.port is None
        assert result.query == "SELECT * FROM users;"
    
    def test_partial_header(self):
        """Test parsing with only some header fields."""
        content = """-- host: remotehost
-- db: production

SELECT COUNT(*) FROM orders;
"""
        result = parse_file(content)
        
        assert result.host == "remotehost"
        assert result.database == "production"
        assert result.user is None
        assert result.port is None
    
    def test_multiline_query(self):
        """Test parsing a multiline query."""
        content = """-- db: test

SELECT 
    id,
    name,
    email
FROM users
WHERE active = 1
ORDER BY name;
"""
        result = parse_file(content)
        
        expected_query = """SELECT 
    id,
    name,
    email
FROM users
WHERE active = 1
ORDER BY name;"""
        assert result.query == expected_query
    
    def test_comment_in_query(self):
        """Test that comments after header are part of query."""
        content = """-- db: test

-- This is a comment in the query
SELECT * FROM users;
"""
        result = parse_file(content)
        
        assert "-- This is a comment in the query" in result.query
    
    def test_whitespace_variations(self):
        """Test parsing with various whitespace in header."""
        content = """--host:localhost
-- user:  myuser  
--  db :mydb

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.host == "localhost"
        assert result.user == "myuser"
        assert result.database == "mydb"
    
    def test_invalid_port(self):
        """Test that invalid port is ignored."""
        content = """-- port: invalid

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.port is None
    
    def test_get_connection_params(self):
        """Test the get_connection_params method."""
        result = ParsedFile(
            host="localhost",
            port=3306,
            user="admin",
            database="mydb",
        )
        
        params = result.get_connection_params()
        
        assert params == {
            "host": "localhost",
            "port": 3306,
            "user": "admin",
            "database": "mydb",
            "db_type": "mysql",
        }
    
    def test_get_connection_params_partial(self):
        """Test get_connection_params with partial data."""
        result = ParsedFile(host="localhost")
        
        params = result.get_connection_params()
        
        assert params == {"host": "localhost", "db_type": "mysql"}
    
    def test_url_format_parsing(self):
        """Test parsing URL format connection string."""
        content = """-- url: mysql://root:secret@localhost:3306/mydb

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.host == "localhost"
        assert result.port == 3306
        assert result.user == "root"
        assert result.password == "secret"
        assert result.database == "mydb"
        assert result.query == "SELECT 1;"
    
    def test_url_format_minimal(self):
        """Test parsing URL with minimal info."""
        content = """-- url: mysql://localhost/mydb

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.host == "localhost"
        assert result.database == "mydb"
        assert result.user is None
        assert result.password is None
    
    def test_url_with_special_characters(self):
        """Test URL with URL-encoded password."""
        content = """-- url: mysql://user:p%40ssword@localhost/db

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.password == "p@ssword"
    
    def test_postgresql_url_format(self):
        """Test parsing PostgreSQL URL format connection string."""
        content = """-- url: postgresql://admin:secret@pghost:5432/pgdb

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.host == "pghost"
        assert result.port == 5432
        assert result.user == "admin"
        assert result.password == "secret"
        assert result.database == "pgdb"
        assert result.db_type == "postgresql"
    
    def test_postgres_url_shorthand(self):
        """Test parsing 'postgres://' shorthand URL."""
        content = """-- url: postgres://user:pass@localhost/mydb

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.db_type == "postgresql"
        assert result.host == "localhost"
        assert result.database == "mydb"
    
    def test_mysql_url_sets_db_type(self):
        """Test that MySQL URL explicitly sets db_type."""
        content = """-- url: mysql://root:pass@localhost/testdb

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.db_type == "mysql"
    
    def test_default_db_type_is_mysql(self):
        """Test that default db_type is mysql when using key-value format."""
        content = """-- host: localhost
-- db: testdb

SELECT 1;
"""
        result = parse_file(content)
        
        assert result.db_type == "mysql"

