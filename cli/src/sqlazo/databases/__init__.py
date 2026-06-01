"""SQL database handler registry."""

from importlib import import_module
from typing import Optional

from sqlazo.databases.base import DatabaseHandler

_HANDLER_CLASSES = {
    "mysql": ("sqlazo.databases.mysql", "MySQLHandler"),
    "mariadb": ("sqlazo.databases.mysql", "MySQLHandler"),
    "postgresql": ("sqlazo.databases.postgresql", "PostgreSQLHandler"),
    "postgres": ("sqlazo.databases.postgresql", "PostgreSQLHandler"),
    "sqlite": ("sqlazo.databases.sqlite", "SQLiteHandler"),
}
_HANDLERS: dict[str, DatabaseHandler] = {}


def get_handler(scheme: str) -> Optional[DatabaseHandler]:
    """Return the SQL handler for a URL scheme."""
    key = scheme.lower()
    if key in _HANDLERS:
        return _HANDLERS[key]

    handler_ref = _HANDLER_CLASSES.get(key)
    if not handler_ref:
        return None

    module_name, class_name = handler_ref
    module = import_module(module_name)
    handler = getattr(module, class_name)()
    _HANDLERS[key] = handler
    return handler


def get_handler_for_db_type(db_type: str) -> Optional[DatabaseHandler]:
    """Return the SQL handler for a configured database type."""
    return get_handler(db_type)


def get_all_comment_prefixes() -> list[str]:
    """Return supported header comment prefixes."""
    return ["--"]


def get_all_schemes() -> list[str]:
    """Return supported URL schemes."""
    return sorted(_HANDLER_CLASSES)
