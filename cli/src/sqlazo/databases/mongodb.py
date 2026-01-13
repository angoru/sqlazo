"""MongoDB database handler."""

import re
import json
from typing import Any
from urllib.parse import ParseResult, unquote

from pymongo import MongoClient
from bson import ObjectId

from sqlazo.databases.base import DatabaseHandler, QueryResult


def _mongo_json_encoder(obj):
    """Custom JSON encoder for MongoDB types."""
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class MongoDBHandler(DatabaseHandler):
    """Handler for MongoDB databases."""
    
    schemes = ["mongodb", "mongodb+srv"]
    default_port = 27017
    comment_prefixes = ["//"]
    requires_auth = False
    requires_database = True
    
    def parse_url(self, parsed: ParseResult, url: str) -> dict:
        """Parse MongoDB connection URL."""
        params = {"db_type": "mongodb"}
        
        if parsed.hostname:
            params["host"] = parsed.hostname
        if parsed.port:
            params["port"] = parsed.port
        if parsed.username:
            params["user"] = unquote(parsed.username)
        if parsed.password:
            params["password"] = unquote(parsed.password)
        if parsed.path and parsed.path != "/":
            params["database"] = parsed.path.lstrip("/")
        # Store the full URL for MongoDB (pymongo prefers connection strings)
        params["connection_string"] = url
        
        return params
    
    def validate_config(self, config) -> list[str]:
        """Validate MongoDB configuration."""
        errors = []
        if not config.connection_string and not config.host:
            errors.append("MongoDB host or connection string not specified. Use URL format like 'mongodb://localhost:27017/mydb'.")
        if not config.database:
            errors.append("Database not specified. Add database name to your MongoDB URL.")
        return errors
    
    def get_connection(self, config) -> Any:
        """Create MongoDB connection."""
        if config.connection_string:
            kwargs = {"host": config.connection_string}
        else:
            kwargs = {
                "host": config.host,
                "port": config.port or self.default_port,
            }
            if config.user:
                kwargs["username"] = config.user
            if config.password:
                kwargs["password"] = config.password
        
        client = MongoClient(**kwargs)
        # Return tuple of (client, database) for MongoDB
        return (client, client[config.database])
    
    def close_connection(self, connection: Any) -> None:
        """Close MongoDB connection."""
        # connection is (client, db) tuple
        connection[0].close()
    
    def execute_query(self, connection: Any, query: str) -> QueryResult:
        """Execute MongoDB query."""
        # connection is (client, db) tuple
        _, db = connection
        return self._execute_mongo_query(db, query)
    
    def _execute_mongo_query(self, db: Any, query: str) -> QueryResult:
        """Execute a MongoDB query string."""
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
        
        try:
            args = self._parse_args(args_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in query arguments: {e}")
        
        # Execute based on method
        if method == "find":
            filter_doc = args[0] if args else {}
            projection = args[1] if len(args) > 1 else None
            cursor = collection.find(filter_doc, projection)
            return self._cursor_to_result(cursor)
        
        elif method == "findOne":
            filter_doc = args[0] if args else {}
            projection = args[1] if len(args) > 1 else None
            doc = collection.find_one(filter_doc, projection)
            if doc:
                return self._doc_to_result(doc)
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
            return self._cursor_to_result(cursor)
        
        else:
            raise ValueError(f"Unsupported MongoDB method: {method}")
    
    def _parse_args(self, args_str: str) -> list:
        """Parse comma-separated JSON arguments."""
        if not args_str:
            return []
        
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
    
    def _cursor_to_result(self, cursor) -> QueryResult:
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
                if isinstance(value, ObjectId):
                    value = str(value)
                elif isinstance(value, (dict, list)):
                    value = json.dumps(value, default=_mongo_json_encoder)
                row.append(value)
            rows.append(tuple(row))
        
        return QueryResult(
            columns=all_keys,
            rows=rows,
            is_select=True,
        )
    
    def _doc_to_result(self, doc: dict) -> QueryResult:
        """Convert a single MongoDB document to QueryResult."""
        columns = list(doc.keys())
        row = []
        for key in columns:
            value = doc[key]
            if isinstance(value, ObjectId):
                value = str(value)
            elif isinstance(value, (dict, list)):
                value = json.dumps(value, default=_mongo_json_encoder)
            row.append(value)
        
        return QueryResult(
            columns=columns,
            rows=[tuple(row)],
            is_select=True,
        )
