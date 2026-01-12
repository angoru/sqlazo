"""Execute SQL queries and return results."""

import re
import json
from dataclasses import dataclass
from typing import Any, Optional
from bson import ObjectId


def mongo_json_encoder(obj):
    """Custom JSON encoder for MongoDB types."""
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


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


def execute_query(connection: Any, query: str) -> QueryResult:
    """
    Execute a SQL query and return the result.
    
    Args:
        connection: Active database connection (MySQL or PostgreSQL).
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
            # lastrowid is MySQL-specific, PostgreSQL needs RETURNING clause
            last_id = getattr(cursor, 'lastrowid', None)
            return QueryResult(
                affected_rows=cursor.rowcount,
                last_insert_id=last_id,
                is_select=False,
            )
    finally:
        cursor.close()


def execute_mongo_query(db: Any, query: str) -> QueryResult:
    """
    Execute a MongoDB query and return the result.
    
    Supports:
        db.collection.find({query})
        db.collection.findOne({query})
        db.collection.insertOne({doc})
        db.collection.insertMany([docs])
        db.collection.updateOne({filter}, {update})
        db.collection.updateMany({filter}, {update})
        db.collection.deleteOne({filter})
        db.collection.deleteMany({filter})
        db.collection.countDocuments({filter})
        db.collection.aggregate([pipeline])
    
    Args:
        db: MongoDB database object (from MongoClient).
        query: MongoDB query string in JavaScript-like format.
        
    Returns:
        QueryResult with columns/rows for find operations, or affected_rows for others.
    """
    # Parse the query: db.collection.method(args)
    query = query.strip()
    
    # Pattern to match db.collection.method(args)
    pattern = r'^db\.(\w+)\.(\w+)\s*\((.*)\)\s*;?\s*$'
    match = re.match(pattern, query, re.DOTALL)
    
    if not match:
        raise ValueError(f"Invalid MongoDB query format. Expected: db.collection.method(args)\nGot: {query}")
    
    collection_name = match.group(1)
    method = match.group(2)
    args_str = match.group(3).strip()
    
    collection = db[collection_name]
    
    # Parse arguments (JSON-like)
    def parse_args(args_str: str) -> list:
        """Parse comma-separated JSON arguments."""
        if not args_str:
            return []
        
        # Handle multiple arguments (e.g., for updateOne)
        # This is a simplified parser - real implementation would need better JSON parsing
        args = []
        depth = 0
        current = ""
        in_string = False
        escape_next = False
        
        for char in args_str:
            if escape_next:
                current += char
                escape_next = False
                continue
            
            if char == '\\':
                current += char
                escape_next = True
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                current += char
                continue
            
            if not in_string:
                if char in '{[':
                    depth += 1
                elif char in '}]':
                    depth -= 1
                elif char == ',' and depth == 0:
                    if current.strip():
                        args.append(json.loads(current.strip()))
                    current = ""
                    continue
            
            current += char
        
        if current.strip():
            args.append(json.loads(current.strip()))
        
        return args
    
    try:
        args = parse_args(args_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in query arguments: {e}")
    
    # Execute based on method
    if method == "find":
        filter_doc = args[0] if args else {}
        projection = args[1] if len(args) > 1 else None
        cursor = collection.find(filter_doc, projection)
        return _cursor_to_result(cursor)
    
    elif method == "findOne":
        filter_doc = args[0] if args else {}
        projection = args[1] if len(args) > 1 else None
        doc = collection.find_one(filter_doc, projection)
        if doc:
            return _doc_to_result(doc)
        return QueryResult(columns=[], rows=[], is_select=True)
    
    elif method == "insertOne":
        if not args:
            raise ValueError("insertOne requires a document argument")
        result = collection.insert_one(args[0])
        return QueryResult(
            affected_rows=1,
            last_insert_id=str(result.inserted_id),
            is_select=False,
        )
    
    elif method == "insertMany":
        if not args:
            raise ValueError("insertMany requires an array of documents")
        docs = args[0] if isinstance(args[0], list) else args
        result = collection.insert_many(docs)
        return QueryResult(
            affected_rows=len(result.inserted_ids),
            is_select=False,
        )
    
    elif method in ("updateOne", "updateMany"):
        if len(args) < 2:
            raise ValueError(f"{method} requires filter and update arguments")
        filter_doc = args[0]
        update_doc = args[1]
        if method == "updateOne":
            result = collection.update_one(filter_doc, update_doc)
        else:
            result = collection.update_many(filter_doc, update_doc)
        return QueryResult(
            affected_rows=result.modified_count,
            is_select=False,
        )
    
    elif method in ("deleteOne", "deleteMany"):
        filter_doc = args[0] if args else {}
        if method == "deleteOne":
            result = collection.delete_one(filter_doc)
        else:
            result = collection.delete_many(filter_doc)
        return QueryResult(
            affected_rows=result.deleted_count,
            is_select=False,
        )
    
    elif method == "countDocuments":
        filter_doc = args[0] if args else {}
        count = collection.count_documents(filter_doc)
        return QueryResult(
            columns=["count"],
            rows=[(count,)],
            is_select=True,
        )
    
    elif method == "aggregate":
        if not args:
            raise ValueError("aggregate requires a pipeline argument")
        pipeline = args[0] if isinstance(args[0], list) else args
        cursor = collection.aggregate(pipeline)
        return _cursor_to_result(cursor)
    
    else:
        raise ValueError(f"Unsupported MongoDB method: {method}")


def _cursor_to_result(cursor) -> QueryResult:
    """Convert a MongoDB cursor to QueryResult."""
    docs = list(cursor)
    if not docs:
        return QueryResult(columns=[], rows=[], is_select=True)
    
    # Get all unique keys across all documents
    all_keys = []
    seen_keys = set()
    for doc in docs:
        for key in doc.keys():
            if key not in seen_keys:
                all_keys.append(key)
                seen_keys.add(key)
    
    # Convert documents to rows
    rows = []
    for doc in docs:
        row = []
        for key in all_keys:
            value = doc.get(key, None)
            # Convert ObjectId and other BSON types to string
            if isinstance(value, ObjectId):
                value = str(value)
            elif isinstance(value, (dict, list)):
                value = json.dumps(value, default=mongo_json_encoder)
            row.append(value)
        rows.append(tuple(row))
    
    return QueryResult(
        columns=all_keys,
        rows=rows,
        is_select=True,
    )


def _doc_to_result(doc: dict) -> QueryResult:
    """Convert a single MongoDB document to QueryResult."""
    columns = list(doc.keys())
    row = []
    for key in columns:
        value = doc[key]
        if isinstance(value, ObjectId):
            value = str(value)
        elif isinstance(value, (dict, list)):
            value = json.dumps(value, default=mongo_json_encoder)
        row.append(value)
    
    return QueryResult(
        columns=columns,
        rows=[tuple(row)],
        is_select=True,
    )

