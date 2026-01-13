"""Database schema introspection for autocomplete."""

from dataclasses import dataclass, field
from typing import Any


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


def get_schema(connection: Any, database: str, db_type: str = "mysql") -> SchemaInfo:
    """
    Introspect database schema to get tables and columns.
    
    Args:
        connection: Active database connection.
        database: Database name to introspect.
        db_type: Database type ("mysql", "postgresql", "sqlite", or "mongodb").
        
    Returns:
        SchemaInfo with tables and their columns.
    """
    if db_type == "sqlite":
        return _get_schema_sqlite(connection, database)
    elif db_type == "postgresql":
        return _get_schema_postgresql(connection, database)
    elif db_type == "mongodb":
        return _get_schema_mongodb(connection, database)
    else:
        return _get_schema_mysql(connection, database)


def _get_schema_mysql(connection: Any, database: str) -> SchemaInfo:
    """MySQL-specific schema introspection."""
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


def _get_schema_postgresql(connection: Any, database: str) -> SchemaInfo:
    """PostgreSQL-specific schema introspection."""
    schema = SchemaInfo(database=database)
    cursor = connection.cursor()
    
    try:
        # Get all tables and views from public schema
        cursor.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        tables_data = cursor.fetchall()
        
        for table_name, table_type in tables_data:
            table = TableInfo(name=table_name, table_type=table_type)
            
            # Get columns for this table with constraint info
            cursor.execute("""
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    CASE 
                        WHEN pk.column_name IS NOT NULL THEN 'PRI'
                        WHEN uq.column_name IS NOT NULL THEN 'UNI'
                        ELSE ''
                    END as column_key
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                        AND tc.table_schema = 'public'
                        AND tc.table_name = %s
                ) pk ON c.column_name = pk.column_name
                LEFT JOIN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'UNIQUE'
                        AND tc.table_schema = 'public'
                        AND tc.table_name = %s
                ) uq ON c.column_name = uq.column_name
                WHERE c.table_schema = 'public' AND c.table_name = %s
                ORDER BY c.ordinal_position
            """, (table_name, table_name, table_name))
            
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


def _get_schema_sqlite(connection: Any, database: str) -> SchemaInfo:
    """SQLite-specific schema introspection."""
    schema = SchemaInfo(database=database)
    cursor = connection.cursor()
    
    try:
        # Get all tables (exclude sqlite internal tables)
        cursor.execute("""
            SELECT name, type
            FROM sqlite_master
            WHERE type IN ('table', 'view')
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        
        tables_data = cursor.fetchall()
        
        for table_name, table_type in tables_data:
            normalized_type = "BASE TABLE" if table_type == "table" else "VIEW"
            table = TableInfo(name=table_name, table_type=normalized_type)
            
            # Get columns for this table using PRAGMA
            # PRAGMA returns: cid, name, type, notnull, dflt_value, pk
            cursor.execute(f"PRAGMA table_info('{table_name}')")
            
            for row in cursor.fetchall():
                col_name = row[1]
                data_type = row[2]
                is_not_null = row[3]
                is_pk = row[5]
                
                column = ColumnInfo(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=(is_not_null == 0),
                    column_key="PRI" if is_pk else "",
                )
                table.columns.append(column)
            
            schema.tables.append(table)
        
        return schema
        
    finally:
        cursor.close()


def _get_schema_mongodb(connection: Any, database: str) -> SchemaInfo:
    """
    MongoDB-specific schema introspection.
    
    Args:
        connection: MongoDB database object (from MongoClient).
        database: Database name (for labeling).
        
    Note:
        MongoDB is schemaless, so we sample documents to infer field structure.
    """
    schema = SchemaInfo(database=database)
    
    # Get list of collections (MongoDB equivalent of tables)
    collection_names = connection.list_collection_names()
    
    for coll_name in sorted(collection_names):
        # Skip system collections
        if coll_name.startswith('system.'):
            continue
        
        table = TableInfo(name=coll_name, table_type="COLLECTION")
        
        # Sample a few documents to infer field structure
        # We sample up to 10 documents to get a good picture of fields
        collection = connection[coll_name]
        sample_docs = list(collection.find().limit(10))
        
        # Collect all field names and their types
        field_types = {}
        for doc in sample_docs:
            for key, value in doc.items():
                type_name = _mongo_type_name(value)
                if key not in field_types:
                    field_types[key] = type_name
                elif field_types[key] != type_name and type_name != "null":
                    # Multiple types detected, mark as mixed
                    if field_types[key] != "mixed":
                        field_types[key] = "mixed"
        
        # Convert to ColumnInfo
        for field_name, field_type in field_types.items():
            column = ColumnInfo(
                name=field_name,
                data_type=field_type,
                is_nullable=True,  # MongoDB fields are always nullable
                column_key="PRI" if field_name == "_id" else "",
            )
            table.columns.append(column)
        
        schema.tables.append(table)
    
    return schema


def _mongo_type_name(value: Any) -> str:
    """Get a type name for a MongoDB value."""
    if value is None:
        return "null"
    
    type_map = {
        str: "string",
        int: "int",
        float: "double",
        bool: "bool",
        list: "array",
        dict: "object",
    }
    
    for py_type, mongo_type in type_map.items():
        if isinstance(value, py_type):
            return mongo_type
    
    # Handle ObjectId and other BSON types
    type_name = type(value).__name__
    return type_name.lower()
