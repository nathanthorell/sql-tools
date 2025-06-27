import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple

import numpy as np
import pandas as pd
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from data_compare.data_compare_types import ComparisonConfig, ComparisonResult, QueryResult
from utils import Connection
from utils.rich_utils import COLORS, console


def execute_sql_query(
    conn: Connection, sql_query: str, params: Optional[Tuple[Any, ...]] = None
) -> Tuple[pd.DataFrame, float]:
    """Execute a SQL query and return results with execution duration"""
    start_time = datetime.now()

    query_preview = sql_query[:50].replace("\n", " ") + ("..." if len(sql_query) > 50 else "")
    console.print(f"[dim]Executing query:[/] [blue]{query_preview}[/]", end="\r")

    try:
        engine = conn.get_sqlalchemy_engine()
        df = pd.read_sql_query(sql_query, engine, params=params)

        duration = (datetime.now() - start_time).total_seconds()
        console.print(f"[green]Query completed in {duration:.2f}s[/]       ")
        return df, duration

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        console.print(f"[red]Query failed after {duration:.2f}s[/]       ")
        raise Exception(f"Query failed after {duration:.2f}s: {str(e)}") from e


def compare_sql(
    left_conn: Connection,
    right_conn: Connection,
    left_query: str,
    right_query: str,
    *,
    left_params: Optional[Tuple[Any, ...]] = None,
    right_params: Optional[Tuple[Any, ...]] = None,
) -> ComparisonResult:
    """Compare the results of two SQL queries"""
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(complete_style="green"),
        TimeElapsedColumn(),
        console=console,
        expand=False,
    ) as progress:
        # Left Query Execution
        task_left = progress.add_task("Executing left query...", total=1)
        left_results, left_duration = execute_sql_query(
            conn=left_conn, sql_query=left_query, params=left_params
        )
        left_result = QueryResult(results=left_results, duration=left_duration)
        progress.update(task_left, completed=1)

        # Right Query Execution
        task_right = progress.add_task("Executing right query...", total=1)
        right_results, right_duration = execute_sql_query(
            conn=right_conn, sql_query=right_query, params=right_params
        )
        right_result = QueryResult(results=right_results, duration=right_duration)
        progress.update(task_right, completed=1)

    comparison = ComparisonResult(left_result, right_result)
    comparison.rich_display()

    return comparison


def run_comparisons(config: ComparisonConfig) -> bool:
    """Run all SQL comparisons from config"""
    success = True

    console.print()
    console.rule("[bold]SQL Data Comparison[/]")
    console.print("[italic]Comparing queries across database systems[/]", justify="center")
    console.print()

    # Get output settings
    output_type = config.config.get("output_type", None)
    output_dir = config.config.get("output_file_path", "./output/")
    output_format = config.config.get("output_format", "csv")
    timestamp_file = config.config.get("timestamp_file", False)
    max_sql_in_values = config.config.get("max_sql_in_values", 1000)

    for i, comparison in enumerate(config.comparisons):
        name = comparison.name
        color = COLORS[i % len(COLORS)]
        console.print()
        console.rule(f"[bold {color}]{name}[/]")

        output_table_name = comparison.table_name

        left_conn = comparison.left_connection
        right_conn = comparison.right_connection

        try:
            console.print(f"Left database type:  [{color}]{comparison.left_db_type}[/]")
            console.print(f"Right database type: [{color}]{comparison.right_db_type}[/]")
            console.print()

            result = compare_sql(
                left_conn=left_conn,
                right_conn=right_conn,
                left_query=comparison.left_query,
                right_query=comparison.right_query,
            )

            # Handle output file generation if configured
            if output_type and output_dir:
                match output_type:
                    case "left_only" | "both":
                        if not result.left_only.empty:
                            generate_output_file(
                                name=name,
                                output_type=output_type,
                                dataset=result.left_only,
                                output_dir=output_dir,
                                table_name=output_table_name,
                                format=output_format,
                                timestamp_file=timestamp_file,
                                max_sql_in_values=max_sql_in_values,
                            )
                    case "right_only" | "both":
                        if not result.right_only.empty:
                            generate_output_file(
                                name=name,
                                output_type=output_type,
                                dataset=result.right_only,
                                output_dir=output_dir,
                                table_name=output_table_name,
                                format=output_format,
                                timestamp_file=timestamp_file,
                                max_sql_in_values=max_sql_in_values,
                            )
                    case _:
                        console.print(f"[yellow]Unknown output type: {output_type}[/]")

            if not result.is_equal:
                success = False

        except Exception as e:
            success = False
            console.print(f"[bold red]Error in comparison {name}:[/] {e}")

    console.print()
    if success:
        console.rule("[bold green]All comparisons successful[/]")
    else:
        console.rule("[bold red]Some comparisons failed[/]")
    console.print()

    return success


def load_sql_file(file_path: str) -> str:
    """Load SQL query from a file"""
    from pathlib import Path

    sql_path = Path(file_path)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path) as f:
        return f.read()


def format_value_for_sql_in(value: Any) -> str:
    """Format a single value for use in SQL IN statement"""
    if pd.isna(value) or value is None:
        return "NULL"
    elif isinstance(value, str):
        # Escape single quotes by doubling them
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    elif isinstance(value, (bool, np.bool_)):
        return "1" if value else "0"
    elif isinstance(value, (int, float, np.integer, np.floating)):
        # Handle all numeric types (including numpy types)
        return str(value)
    else:
        # For other types (datetime, etc.), convert to string and quote
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"


def generate_sql_statement(
    dataset: pd.DataFrame, table_name: str, max_values: Optional[int] = None
) -> str:
    """Generate a complete SQL SELECT statement from a dataset"""
    if dataset.empty:
        return "-- No data to generate SELECT statement"

    # Get unique values from the first column (typically the key column)
    column_name = dataset.columns[0]
    values = dataset.iloc[:, 0].unique()

    # Limit the number of values if max_values is specified
    if max_values is not None and len(values) > max_values:
        values = values[:max_values]
        console.print(
            f"[yellow]Warning: Truncated to {max_values} values for SQL SELECT statement[/]"
        )

    # Format values for SQL
    formatted_values = [format_value_for_sql_in(val) for val in values]

    # Build the WHERE clause
    if len(formatted_values) == 1:
        where_clause = f"WHERE {column_name} = {formatted_values[0]}"
    else:
        if len(formatted_values) <= 10:
            # For small lists, put all on one line
            in_clause = f"IN ({', '.join(formatted_values)})"
        else:
            # For larger lists, format with line breaks for readability
            values_str = ",\n        ".join(formatted_values)
            in_clause = f"IN (\n        {values_str}\n    )"
        where_clause = f"WHERE {column_name} {in_clause}"

    # Build the complete SELECT statement
    select_statement = f"SELECT *\nFROM [{table_name}]\n{where_clause};"

    return select_statement


def generate_output_file(
    name: str,
    output_type: str,
    dataset: pd.DataFrame,
    output_dir: str,
    table_name: str,
    format: str = "csv",
    timestamp_file: bool = False,
    max_sql_in_values: Optional[int] = None,
) -> str:
    """Generate an output file from a dataset."""
    output_path = Path(output_dir)
    os.makedirs(output_path, exist_ok=True)

    clean_name = name.lower()
    clean_name = clean_name.replace(" ", "_")
    clean_name = re.sub(r"_+", "_", clean_name)
    clean_name = re.sub(r"[^a-z0-9_-]", "", clean_name)

    # Determine filename based on timestamp preference
    if timestamp_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{output_type}_{timestamp}"
    else:
        filename = f"{clean_name}_{output_type}"

    file_path = Path()
    if format.lower() == "csv":
        file_path = output_path / f"{filename}.csv"
        dataset.to_csv(file_path, index=False)
    elif format.lower() == "json":
        file_path = output_path / f"{filename}.json"
        dataset.to_json(file_path, orient="records", lines=True)
    elif format.lower() == "sql":
        file_path = output_path / f"{filename}.sql"
        sql_statement = generate_sql_statement(
            dataset, table_name=table_name, max_values=max_sql_in_values
        )
        unique_count = len(dataset.iloc[:, 0].unique()) if not dataset.empty else 0
        sql_content = f"""-- SQL SELECT Statement for "{name}"
-- Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
-- Total records: {len(dataset)}
-- Unique values: {unique_count}
-- Key column: {dataset.columns[0] if not dataset.empty else "N/A"}
-- Usage: Copy this query and modify the table name as needed

{sql_statement}
"""
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(sql_content)
    else:
        raise ValueError(f"Unsupported format: {format}")

    console.print(f"[green]Output saved to:[/] {file_path}")
    return str(file_path)
