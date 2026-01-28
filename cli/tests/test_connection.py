"""Tests for connection module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import os


class TestConnectionConfig:
    """Tests for ConnectionConfig class."""
    
    def test_default_port_mysql(self):
        from sqlazo.connection import ConnectionConfig
        config = ConnectionConfig(db_type="mysql")
        assert config.port == 3306
    
    def test_default_port_postgresql(self):
        from sqlazo.connection import ConnectionConfig
        config = ConnectionConfig(db_type="postgresql")
        assert config.port == 5432
    
    def test_default_port_redis(self):
        from sqlazo.connection import ConnectionConfig
        config = ConnectionConfig(db_type="redis")
        assert config.port == 6379
    
    def test_default_port_mongodb(self):
        from sqlazo.connection import ConnectionConfig
        config = ConnectionConfig(db_type="mongodb")
        assert config.port == 27017
    
    def test_default_port_sqlite_none(self):
        from sqlazo.connection import ConnectionConfig
        config = ConnectionConfig(db_type="sqlite")
        assert config.port is None
    
    def test_from_env(self):
        from sqlazo.connection import ConnectionConfig
        with patch.dict(os.environ, {
            "DB_HOST": "testhost",
            "DB_PORT": "9999",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpass",
            "DB_DATABASE": "testdb",
        }):
            config = ConnectionConfig.from_env()
            assert config.host == "testhost"
            assert config.port == 9999
            assert config.user == "testuser"
            assert config.password == "testpass"
            assert config.database == "testdb"
    
    def test_from_env_db_type(self):
        from sqlazo.connection import ConnectionConfig
        with patch.dict(os.environ, {
            "DB_TYPE": "postgresql",
        }, clear=True):
            config = ConnectionConfig.from_env()
            assert config.db_type == "postgresql"
            assert config.port == 5432

    def test_from_env_dotenv_support(self):
        """Test that .env files are loaded when they exist."""
        import tempfile
        import os
        from unittest.mock import patch
        from sqlazo.connection import ConnectionConfig
        
        # Create a temporary .env file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("DB_HOST=env.example.com\n")
            f.write("DB_PORT=9999\n")
            f.write("DB_USER=envuser\n")
            env_file = f.name
        
        try:
            # Clear environment and test with explicit dotenv path
            with patch.dict(os.environ, {}, clear=True):
                config = ConnectionConfig.from_env(dotenv_path=env_file)
                assert config.host == "env.example.com"
                assert config.port == 9999
                assert config.user == "envuser"
                assert config.db_type == "mysql"  # default
        finally:
            os.unlink(env_file)

    def test_env_vars_override_dotenv(self):
        """Test that environment variables take precedence over .env files."""
        import tempfile
        import os
        from unittest.mock import patch
        from sqlazo.connection import ConnectionConfig
        
        # Create a temporary .env file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("DB_HOST=env.example.com\n")
            f.write("DB_PORT=9999\n")
            env_file = f.name
        
        try:
            # Test environment variables override .env
            with patch.dict(os.environ, {
                "DB_HOST": "override.example.com",
            }, clear=True):
                config = ConnectionConfig.from_env(dotenv_path=env_file)
                assert config.host == "override.example.com"  # env var wins
                assert config.port == 9999  # from .env file
        finally:
            os.unlink(env_file)
    
    def test_merge_with_file_params(self):
        from sqlazo.connection import ConnectionConfig
        base = ConnectionConfig(host="default", user="base_user")
        file_params = {"host": "file_host", "database": "file_db"}
        merged = base.merge_with(file_params)
        
        assert merged.host == "file_host"
        assert merged.user == "base_user"
        assert merged.database == "file_db"
    
    def test_merge_with_db_type_change(self):
        from sqlazo.connection import ConnectionConfig
        base = ConnectionConfig(db_type="mysql", port=3306)
        file_params = {"db_type": "postgresql"}
        merged = base.merge_with(file_params)
        
        assert merged.db_type == "postgresql"
        assert merged.port == 5432
    
    def test_validate_delegates_to_handler(self):
        from sqlazo.connection import ConnectionConfig
        config = ConnectionConfig(db_type="mysql", user="u", password="p", database="d")
        errors = config.validate()
        assert errors == []


class TestGetConnection:
    """Tests for get_connection function."""
    
    @patch("sqlazo.databases.mysql.mysql.connector.connect")
    def test_get_connection_mysql(self, mock_connect):
        from sqlazo.connection import ConnectionConfig, get_connection
        config = ConnectionConfig(
            db_type="mysql",
            host="localhost",
            port=3306,
            user="test",
            password="pass",
            database="db"
        )
        get_connection(config)
        mock_connect.assert_called_once()
    
    @patch("sqlazo.databases.sqlite.sqlite3.connect")
    def test_get_connection_sqlite(self, mock_connect):
        from sqlazo.connection import ConnectionConfig, get_connection
        config = ConnectionConfig(db_type="sqlite", database=":memory:")
        get_connection(config)
        mock_connect.assert_called_once_with(":memory:")
    
    def test_get_connection_unknown_type(self):
        from sqlazo.connection import ConnectionConfig, get_connection
        config = ConnectionConfig(db_type="unknown_db")
        with pytest.raises(ValueError, match="Unknown database type"):
            get_connection(config)
