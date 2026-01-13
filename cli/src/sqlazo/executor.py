"""Execute SQL queries and return results."""

# Re-export QueryResult from base for backward compatibility
from sqlazo.databases.base import QueryResult
from sqlazo.databases import get_handler_for_db_type


def execute_query(connection, query: str, db_type: str = "mysql") -> QueryResult:
    """
    Execute a query and return the result.
    
    This function delegates to the appropriate handler based on db_type.
    For backward compatibility, it defaults to MySQL-style execution.
    
    Args:
        connection: Active database connection.
        query: Query string to execute.
        db_type: Database type (mysql, postgresql, sqlite, mongodb, redis).
        
    Returns:
        QueryResult with columns/rows for SELECT, or affected_rows for others.
    """
    handler = get_handler_for_db_type(db_type)
    if handler:
        return handler.execute_query(connection, query)
    
    # Fallback to basic SQL execution for unknown types
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return QueryResult(
                columns=columns,
                rows=rows,
                is_select=True,
            )
        else:
            connection.commit()
            return QueryResult(
                affected_rows=cursor.rowcount,
                last_insert_id=getattr(cursor, 'lastrowid', None),
                is_select=False,
            )
    finally:
        cursor.close()


# Backward compatibility exports - these now delegate to handlers
def execute_mongo_query(db, query: str) -> QueryResult:
    """Execute a MongoDB query. Deprecated: use handler directly."""
    from sqlazo.databases.mongodb import MongoDBHandler
    handler = MongoDBHandler()
    return handler._execute_mongo_query(db, query)


def execute_redis_query(client, query: str) -> QueryResult:
    """Execute Redis commands. Deprecated: use handler directly."""
    from sqlazo.databases.redis import RedisHandler
    handler = RedisHandler()
    return handler.execute_query(client, query)
