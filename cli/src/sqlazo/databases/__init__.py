"""Database handler registry."""

from typing import Dict, List, Optional, Type
from sqlazo.databases.base import DatabaseHandler


# Registry of handlers by scheme
_handlers: Dict[str, DatabaseHandler] = {}
_handler_instances: Dict[str, DatabaseHandler] = {}


def register_handler(handler_class: Type[DatabaseHandler]) -> None:
    """Register a database handler for its schemes."""
    instance = handler_class()
    for scheme in handler_class.schemes:
        _handlers[scheme.lower()] = handler_class
        _handler_instances[scheme.lower()] = instance


def get_handler(scheme: str) -> Optional[DatabaseHandler]:
    """Get handler instance for a URL scheme."""
    return _handler_instances.get(scheme.lower())


def get_handler_for_db_type(db_type: str) -> Optional[DatabaseHandler]:
    """Get handler instance for a database type."""
    # db_type is typically the same as scheme, but handle aliases
    return get_handler(db_type)


def get_all_handlers() -> List[DatabaseHandler]:
    """Get all registered handler instances (deduplicated)."""
    seen = set()
    handlers = []
    for instance in _handler_instances.values():
        if id(instance) not in seen:
            seen.add(id(instance))
            handlers.append(instance)
    return handlers


def get_all_comment_prefixes() -> List[str]:
    """Get all comment prefixes from all handlers."""
    prefixes = set()
    for handler in get_all_handlers():
        prefixes.update(handler.comment_prefixes)
    return list(prefixes)


def get_all_schemes() -> List[str]:
    """Get all registered URL schemes."""
    return list(_handlers.keys())


# Import and register all handlers
from sqlazo.databases.mysql import MySQLHandler
from sqlazo.databases.postgresql import PostgreSQLHandler
from sqlazo.databases.sqlite import SQLiteHandler
from sqlazo.databases.mongodb import MongoDBHandler
from sqlazo.databases.redis import RedisHandler

register_handler(MySQLHandler)
register_handler(PostgreSQLHandler)
register_handler(SQLiteHandler)
register_handler(MongoDBHandler)
register_handler(RedisHandler)
