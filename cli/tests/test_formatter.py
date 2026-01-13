"""Tests for the formatter module."""

import json
import pytest
from sqlazo.executor import QueryResult
from sqlazo.formatter import format_result


class TestFormatResult:
    """Tests for format_result function."""
    
    def test_table_format_basic(self):
        """Test basic table formatting."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[(1, "Alice"), (2, "Bob")],
            is_select=True,
        )
        
        output = format_result(result, "table")
        
        assert "+----+-------+" in output
        assert "| id | name  |" in output
        assert "| 1  | Alice |" in output
        assert "| 2  | Bob   |" in output
        assert "(2 rows)" in output
    
    def test_table_format_single_row(self):
        """Test table format with single row."""
        result = QueryResult(
            columns=["value"],
            rows=[(42,)],
            is_select=True,
        )
        
        output = format_result(result, "table")
        
        assert "(1 row)" in output
    
    def test_table_format_null_values(self):
        """Test table format with NULL values."""
        result = QueryResult(
            columns=["name"],
            rows=[(None,)],
            is_select=True,
        )
        
        output = format_result(result, "table")
        
        assert "NULL" in output
    
    def test_csv_format(self):
        """Test CSV formatting."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[(1, "Alice"), (2, "Bob")],
            is_select=True,
        )
        
        output = format_result(result, "csv")
        
        lines = output.split("\n")
        assert lines[0] == "id,name"
        assert lines[1] == "1,Alice"
        assert lines[2] == "2,Bob"
    
    def test_json_format(self):
        """Test JSON formatting."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[(1, "Alice"), (2, "Bob")],
            is_select=True,
        )
        
        output = format_result(result, "json")
        data = json.loads(output)
        
        assert len(data) == 2
        assert data[0] == {"id": 1, "name": "Alice"}
        assert data[1] == {"id": 2, "name": "Bob"}
    
    def test_affected_rows_format(self):
        """Test formatting for non-SELECT queries."""
        result = QueryResult(
            affected_rows=5,
            is_select=False,
        )
        
        output = format_result(result, "table")
        
        assert "Affected rows: 5" in output
    
    def test_affected_rows_with_insert_id(self):
        """Test formatting for INSERT with last_insert_id."""
        result = QueryResult(
            affected_rows=1,
            last_insert_id=42,
            is_select=False,
        )
        
        output = format_result(result, "table")
        
        assert "Affected rows: 1" in output
        assert "Last insert ID: 42" in output
    
    def test_empty_result(self):
        """Test formatting empty result."""
        result = QueryResult(
            columns=[],
            rows=[],
            is_select=True,
        )
        
        output = format_result(result, "table")
        
        assert output == "(No results)"
