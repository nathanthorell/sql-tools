from datetime import datetime
from typing import Any, Optional, Tuple

import pandas as pd
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from data_compare.data_compare_types import ComparisonConfig, ComparisonResult, QueryResult
from utils import Connection, get_connection
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

    for i, comparison in enumerate(config.comparisons):
        name = comparison.name
        color = COLORS[i % len(COLORS)]
        console.print()
        console.rule(f"[bold {color}]{name}[/]")

        left_conn = None
        right_conn = None

        try:
            console.print(f"Left database type:  [{color}]{comparison.left_db_type}[/]")
            console.print(f"Right database type: [{color}]{comparison.right_db_type}[/]")
            console.print()

            left_conn = get_connection(comparison.left_connection, db_type=comparison.left_db_type)
            right_conn = get_connection(
                comparison.right_connection, db_type=comparison.right_db_type
            )

            result = compare_sql(
                left_conn=left_conn,
                right_conn=right_conn,
                left_query=comparison.left_query,
                right_query=comparison.right_query,
            )

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
