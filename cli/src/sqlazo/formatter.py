"""Format query results for output."""

import csv
import io
import json
from typing import Literal

from sqlazo.executor import QueryResult


OutputFormat = Literal["table", "csv", "json", "record"]


def format_result(result: QueryResult, fmt: OutputFormat = "table") -> str:
    """
    Format a query result for output.
    
    Args:
        result: The query result to format.
        fmt: Output format - 'table', 'csv', 'json', or 'record'.
        
    Returns:
        Formatted string representation.
    """
    if not result.is_select:
        if result.last_insert_id:
            return f"Affected rows: {result.affected_rows}, Last insert ID: {result.last_insert_id}"
        return f"Affected rows: {result.affected_rows}"
    
    if fmt == "table":
        return _format_table(result)
    elif fmt == "csv":
        return _format_csv(result)
    elif fmt == "json":
        return _format_json(result)
    elif fmt == "record":
        return _format_record(result)
    else:
        raise ValueError(f"Unknown format: {fmt}")


def _format_table(result: QueryResult) -> str:
    """Format result as ASCII table."""
    if not result.columns:
        return "(No results)"
    
    # Calculate column widths
    widths = [len(col) for col in result.columns]
    for row in result.rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val) if val is not None else "NULL"))
    
    # Build table
    lines = []
    
    # Header separator
    separator = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    
    # Header row
    header = "|" + "|".join(f" {col:<{widths[i]}} " for i, col in enumerate(result.columns)) + "|"
    
    lines.append(separator)
    lines.append(header)
    lines.append(separator)
    
    # Data rows
    for row in result.rows:
        formatted_vals = []
        for i, val in enumerate(row):
            str_val = str(val) if val is not None else "NULL"
            formatted_vals.append(f" {str_val:<{widths[i]}} ")
        lines.append("|" + "|".join(formatted_vals) + "|")
    
    lines.append(separator)
    
    # Row count
    lines.append(f"({len(result.rows)} row{'s' if len(result.rows) != 1 else ''})")
    
    return "\n".join(lines)


def _format_record(result: QueryResult) -> str:
    """Format result as records (one field per line)."""
    if not result.columns:
        return "(No results)"
    
    if not result.rows:
        return "(No results)"
    
    lines = []
    
    # Find max column name length for alignment
    max_col_len = max(len(col) for col in result.columns)
    
    for row_num, row in enumerate(result.rows, 1):
        lines.append(f"*************************** {row_num}. row ***************************")
        for i, col in enumerate(result.columns):
            val = row[i]
            str_val = str(val) if val is not None else "NULL"
            lines.append(f"{col:>{max_col_len}}: {str_val}")
    
    # Row count
    lines.append(f"({len(result.rows)} row{'s' if len(result.rows) != 1 else ''})")
    
    return "\n".join(lines)


def _format_csv(result: QueryResult) -> str:
    """Format result as CSV."""
    if not result.columns:
        return ""
    
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(result.columns)
    
    for row in result.rows:
        writer.writerow([val if val is not None else "" for val in row])
    
    return output.getvalue().rstrip()


def _format_json(result: QueryResult) -> str:
    """Format result as JSON array of objects."""
    if not result.columns:
        return "[]"
    
    rows_as_dicts = []
    for row in result.rows:
        row_dict = {}
        for i, col in enumerate(result.columns):
            val = row[i]
            # Handle special types that JSON can't serialize
            if val is not None and not isinstance(val, (str, int, float, bool)):
                val = str(val)
            row_dict[col] = val
        rows_as_dicts.append(row_dict)
    
    return json.dumps(rows_as_dicts, indent=2, ensure_ascii=False)

