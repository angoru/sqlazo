"""Tests for database handlers with mocked connections."""

import pytest
from unittest.mock import Mock, patch, MagicMock


class TestMySQLHandler:
    """Tests for MySQLHandler."""
    
    def test_validate_config_missing_user(self):
        from sqlazo.databases.mysql import MySQLHandler
        handler = MySQLHandler()
        config = Mock(user=None, database="test", password="pass")
        errors = handler.validate_config(config)
        assert len(errors) == 1
        assert "User not specified" in errors[0]
    
    def test_validate_config_missing_database(self):
        from sqlazo.databases.mysql import MySQLHandler
        handler = MySQLHandler()
        config = Mock(user="test", database=None, password="pass")
        errors = handler.validate_config(config)
        assert len(errors) == 1
        assert "Database not specified" in errors[0]
    
    def test_validate_config_missing_password(self):
        from sqlazo.databases.mysql import MySQLHandler
        handler = MySQLHandler()
        config = Mock(user="test", database="db", password=None)
        errors = handler.validate_config(config)
        assert len(errors) == 1
        assert "Password not specified" in errors[0]
    
    def test_validate_config_valid(self):
        from sqlazo.databases.mysql import MySQLHandler
        handler = MySQLHandler()
        config = Mock(user="test", database="db", password="pass")
        errors = handler.validate_config(config)
        assert errors == []
    
    @patch("sqlazo.databases.mysql.mysql.connector.connect")
    def test_get_connection(self, mock_connect):
        from sqlazo.databases.mysql import MySQLHandler
        handler = MySQLHandler()
        config = Mock(host="localhost", port=3306, user="test", password="pass", database="db")
        handler.get_connection(config)
        mock_connect.assert_called_once()
    
    @patch("sqlazo.databases.mysql.mysql.connector.connect")
    def test_execute_query_select(self, mock_connect):
        from sqlazo.databases.mysql import MySQLHandler
        handler = MySQLHandler()
        
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        result = handler.execute_query(mock_conn, "SELECT * FROM users")
        
        assert result.columns == ["id", "name"]
        assert result.rows == [(1, "Alice"), (2, "Bob")]
        assert result.is_select == True
    
    @patch("sqlazo.databases.mysql.mysql.connector.connect")
    def test_execute_query_insert(self, mock_connect):
        from sqlazo.databases.mysql import MySQLHandler
        handler = MySQLHandler()
        
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.rowcount = 1
        mock_cursor.lastrowid = 42
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        result = handler.execute_query(mock_conn, "INSERT INTO users VALUES (1, 'Test')")
        
        assert result.affected_rows == 1
        assert result.last_insert_id == 42
        assert result.is_select == False


class TestPostgreSQLHandler:
    """Tests for PostgreSQLHandler."""
    
    def test_validate_config_valid(self):
        from sqlazo.databases.postgresql import PostgreSQLHandler
        handler = PostgreSQLHandler()
        config = Mock(user="test", database="db", password="pass")
        errors = handler.validate_config(config)
        assert errors == []
    
    @patch("sqlazo.databases.postgresql.psycopg2.connect")
    def test_get_connection(self, mock_connect):
        from sqlazo.databases.postgresql import PostgreSQLHandler
        handler = PostgreSQLHandler()
        config = Mock(host="localhost", port=5432, user="test", password="pass", database="db")
        handler.get_connection(config)
        mock_connect.assert_called_once()
    
    @patch("sqlazo.databases.postgresql.psycopg2.connect")
    def test_execute_query_select(self, mock_connect):
        from sqlazo.databases.postgresql import PostgreSQLHandler
        handler = PostgreSQLHandler()
        
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("email",)]
        mock_cursor.fetchall.return_value = [(1, "a@b.com")]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        result = handler.execute_query(mock_conn, "SELECT * FROM users")
        
        assert result.columns == ["id", "email"]
        assert result.is_select == True


class TestSQLiteHandler:
    """Tests for SQLiteHandler."""
    
    def test_validate_config_missing_database(self):
        from sqlazo.databases.sqlite import SQLiteHandler
        handler = SQLiteHandler()
        config = Mock(database=None)
        errors = handler.validate_config(config)
        assert len(errors) == 1
        assert "Database not specified" in errors[0]
    
    def test_validate_config_valid(self):
        from sqlazo.databases.sqlite import SQLiteHandler
        handler = SQLiteHandler()
        config = Mock(database=":memory:")
        errors = handler.validate_config(config)
        assert errors == []
    
    @patch("sqlazo.databases.sqlite.sqlite3.connect")
    def test_get_connection(self, mock_connect):
        from sqlazo.databases.sqlite import SQLiteHandler
        handler = SQLiteHandler()
        config = Mock(database=":memory:")
        handler.get_connection(config)
        mock_connect.assert_called_once_with(":memory:")
    
    @patch("sqlazo.databases.sqlite.sqlite3.connect")
    def test_execute_query_select(self, mock_connect):
        from sqlazo.databases.sqlite import SQLiteHandler
        handler = SQLiteHandler()
        
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall.return_value = [("a", "b")]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        result = handler.execute_query(mock_conn, "SELECT * FROM t")
        
        assert result.columns == ["col1", "col2"]
        assert result.is_select == True


class TestRedisHandler:
    """Tests for RedisHandler."""
    
    def test_validate_config_missing_host(self):
        from sqlazo.databases.redis import RedisHandler
        handler = RedisHandler()
        config = Mock(host=None)
        errors = handler.validate_config(config)
        assert len(errors) == 1
        assert "host not specified" in errors[0]
    
    def test_validate_config_valid(self):
        from sqlazo.databases.redis import RedisHandler
        handler = RedisHandler()
        config = Mock(host="localhost")
        errors = handler.validate_config(config)
        assert errors == []
    
    @patch("sqlazo.databases.redis.redis_client.Redis")
    def test_get_connection(self, mock_redis):
        from sqlazo.databases.redis import RedisHandler
        handler = RedisHandler()
        config = Mock(host="localhost", port=6379, user=None, password=None, database=0)
        handler.get_connection(config)
        mock_redis.assert_called_once()
    
    @patch("sqlazo.databases.redis.redis_client.Redis")
    def test_execute_query_ping(self, mock_redis):
        from sqlazo.databases.redis import RedisHandler
        handler = RedisHandler()
        
        mock_conn = MagicMock()
        mock_conn.execute_command.return_value = True
        
        result = handler.execute_query(mock_conn, "PING")
        
        mock_conn.execute_command.assert_called_with("PING")
        assert result.is_select == True
    
    def test_parse_command(self):
        from sqlazo.databases.redis import RedisHandler
        handler = RedisHandler()
        
        parts = handler._parse_command('SET foo "hello world"')
        assert parts == ["SET", "foo", "hello world"]
    
    def test_format_value_nil(self):
        from sqlazo.databases.redis import RedisHandler
        handler = RedisHandler()
        assert handler._format_value(None) == "(nil)"
    
    def test_format_value_list(self):
        from sqlazo.databases.redis import RedisHandler
        handler = RedisHandler()
        result = handler._format_value(["a", "b"])
        assert "1) a" in result
        assert "2) b" in result


class TestMongoDBHandler:
    """Tests for MongoDBHandler."""
    
    def test_validate_config_missing_host(self):
        from sqlazo.databases.mongodb import MongoDBHandler
        handler = MongoDBHandler()
        config = Mock(connection_string=None, host=None, database="test")
        errors = handler.validate_config(config)
        assert len(errors) == 1
        assert "host or connection string" in errors[0]
    
    def test_validate_config_missing_database(self):
        from sqlazo.databases.mongodb import MongoDBHandler
        handler = MongoDBHandler()
        config = Mock(connection_string=None, host="localhost", database=None)
        errors = handler.validate_config(config)
        assert len(errors) == 1
        assert "Database not specified" in errors[0]
    
    def test_validate_config_valid(self):
        from sqlazo.databases.mongodb import MongoDBHandler
        handler = MongoDBHandler()
        config = Mock(connection_string="mongodb://localhost:27017/test", host="localhost", database="test")
        errors = handler.validate_config(config)
        assert errors == []
