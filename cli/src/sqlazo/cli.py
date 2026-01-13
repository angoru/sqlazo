"""CLI interface for sqlazo."""

import argparse
import json
import sys

from sqlazo.connection import ConnectionConfig, get_connection
from sqlazo.formatter import OutputFormat, format_result
from sqlazo.parser import parse_file, parse_file_path
from sqlazo.schema import get_schema
from sqlazo.databases import get_handler_for_db_type


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="sqlazo",
        description="Execute SQL queries from files with connection headers.",
        epilog="Connection priority: file header > environment variables > defaults",
    )
    
    parser.add_argument(
        "file",
        help="SQL file to execute. Use '-' to read from stdin.",
    )
    
    parser.add_argument(
        "-f", "--format",
        choices=["table", "csv", "json", "record"],
        default="table",
        help="Output format (default: table)",
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show connection info before executing",
    )
    
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Output database schema as JSON (for autocomplete)",
    )
    
    args = parser.parse_args()
    
    try:
        # Parse the SQL file
        if args.file == "-":
            content = sys.stdin.read()
            parsed = parse_file(content)
        else:
            parsed = parse_file_path(args.file)
        
        if not parsed.query.strip():
            print("Error: No query found in file.", file=sys.stderr)
            sys.exit(1)
        
        # Build connection config
        env_config = ConnectionConfig.from_env()
        file_params = parsed.get_connection_params()
        config = env_config.merge_with(file_params)
        
        # Validate config
        errors = config.validate()
        if errors:
            for error in errors:
                print(f"Error: {error}", file=sys.stderr)
            sys.exit(1)
        
        # Verbose output
        if args.verbose:
            db_info = f"{config.db_type.upper()}: {config.host}"
            if config.port:
                db_info += f":{config.port}"
            if config.database is not None:
                db_info += f"/{config.database}"
            print(f"-- Connecting to {db_info}", file=sys.stderr)
            if config.user:
                print(f"-- User: {config.user}", file=sys.stderr)
            print(f"-- Executing query...", file=sys.stderr)
        
        # Get handler for this database type
        handler = get_handler_for_db_type(config.db_type)
        if not handler:
            print(f"Error: Unknown database type: {config.db_type}", file=sys.stderr)
            sys.exit(1)
        
        # Connect and execute
        connection = get_connection(config)
        
        try:
            if args.schema:
                # Schema handling
                schema = handler.get_schema(connection, config.database)
                if schema:
                    print(json.dumps(schema, indent=2))
                else:
                    # Fallback to old schema module for SQL databases
                    schema = get_schema(connection, config.database, config.db_type)
                    print(json.dumps(schema.to_dict(), indent=2))
            else:
                # Execute query
                result = handler.execute_query(connection, parsed.query)
                output = format_result(result, args.format)
                print(output)
            
        except Exception as e:
            handler.close_connection(connection)
            raise
        finally:
            handler.close_connection(connection)
            
    except FileNotFoundError:
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
