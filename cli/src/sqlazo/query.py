"""Run parsed SQL files through the configured database handler."""

import json
import sys
import time

from sqlazo.connection import ConnectionConfig, get_connection
from sqlazo.formatter import OutputFormat, format_result
from sqlazo.parser import ParsedFile, parse_file, parse_file_path
from sqlazo.schema import get_schema
from sqlazo.databases import get_handler_for_db_type


def load_parsed_file(file_path: str) -> ParsedFile:
    """Load SQL content from a file path or stdin."""
    if file_path == "-":
        return parse_file(sys.stdin.read())
    return parse_file_path(file_path)


def build_connection_config(parsed: ParsedFile) -> ConnectionConfig:
    """Merge environment config with file header config."""
    env_config = ConnectionConfig.from_env()
    return env_config.merge_with(parsed.get_connection_params())


def output_schema(connection, config: ConnectionConfig, handler) -> None:
    """Print schema JSON for autocomplete integrations."""
    schema = handler.get_schema(connection, config.database)
    if schema is None and config.db_type in ("mysql", "mariadb", "postgresql", "sqlite"):
        schema = get_schema(connection, config.database, config.db_type)

    if hasattr(schema, "to_dict"):
        schema = schema.to_dict()
    if schema is None:
        schema = {"database": config.database or "", "tables": [], "columns": {}}

    print(json.dumps(schema, indent=2))


def run_query_command(file_path: str, fmt: OutputFormat, verbose: bool, schema: bool) -> None:
    """Run the sqlazo query command."""
    parsed = load_parsed_file(file_path)
    if not parsed.query.strip():
        raise ValueError("No query found in file.")

    config = build_connection_config(parsed)
    errors = config.validate()
    if errors:
        raise ValueError("\n".join(errors))

    handler = get_handler_for_db_type(config.db_type)
    if not handler:
        raise ValueError(f"Unknown database type: {config.db_type}")

    if verbose:
        db_info = f"{config.db_type.upper()}: {config.host}"
        if config.port:
            db_info += f":{config.port}"
        if config.database is not None:
            db_info += f"/{config.database}"
        print(f"-- Connecting to {db_info}", file=sys.stderr)
        if config.user:
            print(f"-- User: {config.user}", file=sys.stderr)
        print("-- Executing query...", file=sys.stderr)

    connection = get_connection(config)
    try:
        if schema:
            output_schema(connection, config, handler)
        else:
            started_at = time.perf_counter()
            result = handler.execute_query(connection, parsed.query)
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            metadata = {
                "duration_ms": duration_ms,
                "query": parsed.query,
                "connection": {
                    "db_type": config.db_type,
                    "host": config.host,
                    "port": config.port,
                    "database": config.database,
                    "user": config.user,
                },
            }
            print(format_result(result, fmt, metadata=metadata))
    finally:
        handler.close_connection(connection)
