"""Tests for the schema introspection module."""

import pytest
from unittest.mock import MagicMock, patch

from sqlazo.schema import (
    ColumnInfo,
    TableInfo,
    SchemaInfo,
    get_schema,
)


class TestSchemaInfo:
    """Tests for SchemaInfo dataclass."""
    
    def test_to_dict_empty(self):
        """Test to_dict with no tables."""
        schema = SchemaInfo(database="testdb")
        
        result = schema.to_dict()
        
        assert result == {
            "database": "testdb",
            "tables": [],
            "columns": {},
        }
    
    def test_to_dict_with_tables(self):
        """Test to_dict with tables and columns."""
        schema = SchemaInfo(
            database="testdb",
            tables=[
                TableInfo(
                    name="users",
                    table_type="BASE TABLE",
                    columns=[
                        ColumnInfo(name="id", data_type="int", column_key="PRI"),
                        ColumnInfo(name="name", data_type="varchar"),
                    ],
                ),
                TableInfo(
                    name="orders",
                    table_type="BASE TABLE",
                    columns=[
                        ColumnInfo(name="id", data_type="int", column_key="PRI"),
                        ColumnInfo(name="user_id", data_type="int", column_key="MUL"),
                    ],
                ),
            ],
        )
        
        result = schema.to_dict()
        
        assert result["database"] == "testdb"
        assert result["tables"] == ["users", "orders"]
        assert result["columns"]["users"] == [
            {"name": "id", "type": "int", "key": "PRI"},
            {"name": "name", "type": "varchar", "key": ""},
        ]
        assert result["columns"]["orders"] == [
            {"name": "id", "type": "int", "key": "PRI"},
            {"name": "user_id", "type": "int", "key": "MUL"},
        ]


class TestGetSchema:
    """Tests for get_schema function."""
    
    def test_get_schema_basic(self):
        """Test basic schema introspection with mocked connection."""
        # Mock cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            # First call: tables query
            [("users", "BASE TABLE"), ("orders", "BASE TABLE")],
            # Second call: columns for users
            [
                ("id", "int", "NO", "PRI"),
                ("name", "varchar", "YES", ""),
            ],
            # Third call: columns for orders
            [
                ("id", "int", "NO", "PRI"),
                ("user_id", "int", "YES", "MUL"),
            ],
        ]
        
        # Mock connection
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Call function
        result = get_schema(mock_connection, "testdb")
        
        # Verify
        assert result.database == "testdb"
        assert len(result.tables) == 2
        assert result.tables[0].name == "users"
        assert len(result.tables[0].columns) == 2
        assert result.tables[0].columns[0].name == "id"
        assert result.tables[0].columns[0].column_key == "PRI"
    
    def test_get_schema_empty_database(self):
        """Test schema introspection on empty database."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        result = get_schema(mock_connection, "emptydb")
        
        assert result.database == "emptydb"
        assert len(result.tables) == 0
    
    def test_get_schema_includes_views(self):
        """Test that views are included in schema."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("users", "BASE TABLE"), ("user_summary", "VIEW")],
            [("id", "int", "NO", "PRI")],
            [("user_count", "bigint", "YES", "")],
        ]
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        result = get_schema(mock_connection, "testdb")
        
        assert len(result.tables) == 2
        assert result.tables[0].table_type == "BASE TABLE"
        assert result.tables[1].table_type == "VIEW"
