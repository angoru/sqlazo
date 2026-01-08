"""Database schema introspection for autocomplete."""

from dataclasses import dataclass, field
from typing import Optional

from mysql.connector import MySQLConnection


@dataclass
class ColumnInfo:
    """Information about a database column."""
    
    name: str
    data_type: str
    is_nullable: bool = True
    column_key: str = ""  # PRI, UNI, MUL, or empty
    

@dataclass
class TableInfo:
    """Information about a database table."""
    
    name: str
    table_type: str = "BASE TABLE"  # BASE TABLE or VIEW
    columns: list[ColumnInfo] = field(default_factory=list)


@dataclass
class SchemaInfo:
    """Complete schema information for a database."""
    
    database: str
    tables: list[TableInfo] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "database": self.database,
            "tables": [t.name for t in self.tables],
            "columns": {
                t.name: [
                    {
                        "name": c.name,
                        "type": c.data_type,
                        "key": c.column_key,
                    }
                    for c in t.columns
                ]
                for t in self.tables
            },
        }


def get_schema(connection: MySQLConnection, database: str) -> SchemaInfo:
    """
    Introspect database schema to get tables and columns.
    
    Args:
        connection: Active MySQL connection.
        database: Database name to introspect.
        
    Returns:
        SchemaInfo with tables and their columns.
    """
    schema = SchemaInfo(database=database)
    cursor = connection.cursor()
    
    try:
        # Get all tables and views
        cursor.execute("""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
        """, (database,))
        
        tables_data = cursor.fetchall()
        
        for table_name, table_type in tables_data:
            table = TableInfo(name=table_name, table_type=table_type)
            
            # Get columns for this table
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table_name))
            
            for col_name, data_type, is_nullable, column_key in cursor.fetchall():
                column = ColumnInfo(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=(is_nullable == "YES"),
                    column_key=column_key or "",
                )
                table.columns.append(column)
            
            schema.tables.append(table)
        
        return schema
        
    finally:
        cursor.close()
