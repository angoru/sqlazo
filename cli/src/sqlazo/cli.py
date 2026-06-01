"""CLI interface for sqlazo."""

import argparse
import json
import os
import sys
import time

from sqlazo import __version__
from sqlazo.connection import ConnectionConfig, get_connection, load_connection_from_profile
from sqlazo.formatter import OutputFormat, format_result
from sqlazo.parser import parse_file, parse_file_path
from sqlazo.schema import get_schema
from sqlazo.databases import get_handler_for_db_type
from sqlazo.secure_credentials import SecureCredentialManager


def _normalize_argv(argv: list[str]) -> list[str]:
    """Preserve the legacy `sqlazo file.sql` query form."""
    if not argv:
        return argv

    if argv[0] in {"query", "cred", "-h", "--help", "--version"}:
        return argv

    return ["query", *argv]


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="sqlazo",
        description="Execute SQL queries from files with connection headers.",
        epilog="Connection priority: file header > environment variables (DB_*) > .env file > defaults",
    )

    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Main query execution command
    query_parser = subparsers.add_parser('query', help='Execute SQL queries')
    query_parser.add_argument(
        "file",
        help="SQL file to execute. Use '-' to read from stdin.",
    )
    query_parser.add_argument(
        "-f", "--format",
        choices=["table", "csv", "json", "record", "json-meta"],
        default="table",
        help="Output format (default: table)",
    )
    query_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show connection info before executing",
    )
    query_parser.add_argument(
        "--schema",
        action="store_true",
        help="Output database schema as JSON (for autocomplete)",
    )
    query_parser.add_argument(
        "--profile",
        help="Use credentials from stored profile",
    )

    # Credential management commands
    cred_parser = subparsers.add_parser('cred', help='Manage encrypted credentials')
    cred_subparsers = cred_parser.add_subparsers(dest='cred_action', help='Credential actions')

    # Store credentials
    store_parser = cred_subparsers.add_parser('store', help='Store encrypted credentials')
    store_parser.add_argument('profile', help='Profile name for the credentials')
    store_parser.add_argument('--host', help='Database host')
    store_parser.add_argument('--port', type=int, help='Database port')
    store_parser.add_argument('--user', help='Database user')
    store_parser.add_argument('--password', help='Database password')
    store_parser.add_argument('--password-env', help='Environment variable containing the database password')
    store_parser.add_argument('--database', help='Database name')
    store_parser.add_argument('--db-type', help='Database type (mysql, postgresql, etc.)')

    # List credentials
    list_parser = cred_subparsers.add_parser('list', help='List stored credential profiles')

    # Retrieve credentials
    retrieve_parser = cred_subparsers.add_parser('retrieve', help='Retrieve stored credentials')
    retrieve_parser.add_argument('profile', help='Profile name to retrieve')

    # Delete credentials
    delete_parser = cred_subparsers.add_parser('delete', help='Delete stored credentials')
    delete_parser.add_argument('profile', help='Profile name to delete')

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show version information and exit",
    )

    args = parser.parse_args(_normalize_argv(sys.argv[1:]))
    
    try:
        # Handle credential management commands
        if args.command == 'cred':
            cred_manager = SecureCredentialManager()

            if args.cred_action == 'store':
                # Collect credentials from arguments
                credentials = {}
                if args.host:
                    credentials['host'] = args.host
                if args.port:
                    credentials['port'] = args.port
                if args.user:
                    credentials['user'] = args.user
                if args.password:
                    credentials['password'] = args.password
                elif args.password_env:
                    env_password = os.environ.get(args.password_env)
                    if env_password is not None:
                        credentials['password'] = env_password
                if args.database:
                    credentials['database'] = args.database
                if args.db_type:
                    credentials['db_type'] = args.db_type

                success = cred_manager.store_credentials(args.profile, credentials)
                if success:
                    print(f"Credentials for profile '{args.profile}' stored successfully.")
                else:
                    print(f"Failed to store credentials for profile '{args.profile}'.", file=sys.stderr)
                    sys.exit(1)

            elif args.cred_action == 'list':
                profiles = cred_manager.list_profiles()
                if profiles:
                    print("Stored credential profiles:")
                    for profile in profiles:
                        print(f"  - {profile}")
                else:
                    print("No stored credential profiles found.")

            elif args.cred_action == 'retrieve':
                credentials = cred_manager.retrieve_credentials(args.profile)
                if credentials:
                    print(f"Credential profile '{args.profile}':")
                    for key, value in credentials.items():
                        print(f"  {key}: {value}")
                else:
                    print(f"No credentials found for profile '{args.profile}' or incorrect password.", file=sys.stderr)
                    sys.exit(1)

            elif args.cred_action == 'delete':
                success = cred_manager.delete_profile(args.profile)
                if success:
                    print(f"Credential profile '{args.profile}' deleted successfully.")
                else:
                    print(f"Failed to delete profile '{args.profile}' or profile not found.", file=sys.stderr)
                    sys.exit(1)
            else:
                parser.print_help()
                sys.exit(1)

        # Handle query execution (default behavior)
        elif args.command == 'query' or args.command is None:
            # Handle backward compatibility - if no subcommand but file is provided
            file_arg = getattr(args, 'file', None)
            if file_arg is None and args.command is None:
                # This means no arguments were provided
                parser.print_help()
                sys.exit(1)
            elif args.command is None:  # Backward compatibility
                # Re-parse to get the file argument without subcommand
                args = argparse.Namespace(
                    file=file_arg,
                    format=args.format if hasattr(args, 'format') else 'table',
                    verbose=args.verbose if hasattr(args, 'verbose') else False,
                    schema=args.schema if hasattr(args, 'schema') else False,
                    profile=getattr(args, 'profile', None)
                )

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
            if args.profile:
                # Load from stored profile
                try:
                    config = load_connection_from_profile(args.profile)
                    # Override with file params if any
                    file_params = parsed.get_connection_params()
                    if file_params:
                        config = config.merge_with(file_params)
                except ValueError as e:
                    print(f"Error: {e}", file=sys.stderr)
                    sys.exit(1)
            else:
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
                    if schema is not None:
                        if hasattr(schema, "to_dict"):
                            schema = schema.to_dict()
                        print(json.dumps(schema, indent=2))
                    else:
                        # Fallback to old schema module for SQL databases
                        if config.db_type in ("mysql", "mariadb", "postgresql", "sqlite"):
                            schema = get_schema(connection, config.database, config.db_type)
                            print(json.dumps(schema.to_dict(), indent=2))
                        else:
                            empty_schema = {
                                "database": config.database if config.database is not None else "",
                                "tables": [],
                                "columns": {},
                            }
                            print(json.dumps(empty_schema, indent=2))
                else:
                    # Execute query
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
                    output = format_result(result, args.format, metadata=metadata)
                    print(output)
            finally:
                handler.close_connection(connection)

    except FileNotFoundError:
        print(f"Error: File not found: {getattr(args, 'file', 'unknown')}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
