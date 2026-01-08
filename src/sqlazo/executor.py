"""Execute SQL queries and return results."""

from dataclasses import dataclass
from typing import Any, Optional

from mysql.connector import MySQLConnection


@dataclass
class QueryResult:
    """Result of executing a SQL query."""
    
    # For SELECT queries
    columns: list[str] = None
    rows: list[tuple] = None
    
    # For INSERT/UPDATE/DELETE
    affected_rows: int = 0
    last_insert_id: Optional[int] = None
    
    # Query metadata
    is_select: bool = True
    
    def __post_init__(self):
        if self.columns is None:
            self.columns = []
        if self.rows is None:
            self.rows = []


def execute_query(connection: MySQLConnection, query: str) -> QueryResult:
    """
    Execute a SQL query and return the result.
    
    Args:
        connection: Active MySQL connection.
        query: SQL query to execute.
        
    Returns:
        QueryResult with columns/rows for SELECT, or affected_rows for others.
    """
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        
        # Check if this is a SELECT-like query (has results)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return QueryResult(
                columns=columns,
                rows=rows,
                is_select=True,
            )
        else:
            # INSERT/UPDATE/DELETE
            connection.commit()
            return QueryResult(
                affected_rows=cursor.rowcount,
                last_insert_id=cursor.lastrowid,
                is_select=False,
            )
    finally:
        cursor.close()
