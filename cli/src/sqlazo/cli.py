"""CLI interface for sqlazo."""

import argparse
import sys

from sqlazo import __version__
from sqlazo.query import run_query_command


def _normalize_argv(argv: list[str]) -> list[str]:
    """Preserve the legacy `sqlazo file.sql` query form."""
    if not argv:
        return argv
    if argv[0] in {"query", "-h", "--help", "--version"}:
        return argv
    return ["query", *argv]


def build_parser() -> argparse.ArgumentParser:
    """Build the sqlazo argument parser."""
    parser = argparse.ArgumentParser(
        prog="sqlazo",
        description="Execute SQL queries from files with connection headers.",
        epilog="Connection priority: file header > environment variables (DB_*) > .env file > defaults",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show version information and exit",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    query_parser = subparsers.add_parser("query", help="Execute SQL queries")
    query_parser.add_argument("file", help="SQL file to execute. Use '-' to read from stdin.")
    query_parser.add_argument(
        "-f",
        "--format",
        choices=["table", "csv", "json", "record", "json-meta"],
        default="table",
        help="Output format (default: table)",
    )
    query_parser.add_argument("-v", "--verbose", action="store_true", help="Show connection info before executing")
    query_parser.add_argument("--schema", action="store_true", help="Output database schema as JSON")
    return parser


def main() -> None:
    """Main entry point."""
    parser = build_parser()
    args = parser.parse_args(_normalize_argv(sys.argv[1:]))

    if args.command != "query":
        parser.print_help()
        sys.exit(1)

    try:
        run_query_command(args.file, args.format, args.verbose, args.schema)
    except FileNotFoundError:
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
