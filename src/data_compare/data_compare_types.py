from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence, Union

import pandas as pd
import psycopg2
import pyodbc
from rich.pretty import Pretty
from rich.table import Table

from utils.rich_utils import COLORS, console

ConnectionType = Union[pyodbc.Connection, "psycopg2.extensions.connection"]
CursorType = Union[pyodbc.Cursor, "psycopg2.extensions.cursor"]


@dataclass
class QueryResult:
    """Contains the results and metadata from a query execution"""

    results: pd.DataFrame
    duration: float

    @property
    def row_count(self) -> int:
        return len(self.results)


class ComparisonResult:
    """Represents the result of comparing two query results"""

    def __init__(self, left: QueryResult, right: QueryResult):
        self.left = left
        self.right = right

        # Initialize variables
        self.left_only = pd.DataFrame()
        self.right_only = pd.DataFrame()
        self.common_rows = pd.DataFrame()
        self.is_equal = False
        self.row_count_match = self.left.row_count == self.right.row_count
        self.shape_match = False
        self.columns_match = False

        left_df = self.left.results.reset_index(drop=True)
        right_df = self.right.results.reset_index(drop=True)

        try:
            self.shape_match = left_df.shape == right_df.shape

            # Check if they have the same column names
            self.columns_match = set(left_df.columns) == set(right_df.columns)

            # For exact comparison, we need matched columns
            if self.columns_match:
                # Sort columns to ensure same order for comparison
                left_sorted = left_df[sorted(left_df.columns)]
                right_sorted = right_df[sorted(right_df.columns)]

                # Check if all values match
                self.is_equal = left_sorted.equals(right_sorted)

                # Calculate differences if needed
                if not self.is_equal:
                    # Find rows that are in left but not in right
                    self.left_only = left_sorted.merge(right_sorted, how="left", indicator=True)
                    self.left_only = self.left_only[self.left_only["_merge"] == "left_only"].drop(
                        "_merge", axis=1
                    )

                    # Find rows that are in right but not in left
                    self.right_only = right_sorted.merge(left_sorted, how="left", indicator=True)
                    self.right_only = self.right_only[
                        self.right_only["_merge"] == "left_only"
                    ].drop("_merge", axis=1)

                    # Find common rows
                    self.common_rows = left_sorted.merge(right_sorted, how="inner")

        except Exception as e:
            console.print(f"[dim]Error during DataFrame comparison: {e}[/]")

    def __str__(self) -> str:
        status = "EQUAL" if self.is_equal else "NOT EQUAL"
        return (
            f"Comparison Result: {status}\n"
            f"Left:  {self.left.row_count} rows, {self.left.duration:.2f}s\n"
            f"Right: {self.right.row_count} rows, {self.right.duration:.2f}s"
        )

    def rich_display(self) -> None:
        """Display the comparison result using Rich formatting"""
        status_color = "green" if self.is_equal else "red"
        row_color = "green" if self.row_count_match else "yellow"

        # Performance comparison
        if self.left.duration > 0 and self.right.duration > 0:
            perf_ratio = self.right.duration / self.left.duration
            perf_text = f"{perf_ratio:.2f}x" + (" faster" if perf_ratio < 1 else " slower")
            perf_color = "green" if perf_ratio < 1 else "yellow" if perf_ratio < 2 else "red"
        else:
            perf_text = "N/A"
            perf_color = "white"

        console.rule(
            f"[bold]Comparison Result: [{status_color}]"
            + f"{self.is_equal and 'EQUAL' or 'NOT EQUAL'}[/]"
        )

        if self.row_count_match:
            console.print(f"[bold]Rows:[/] Both queries returned {self.left.row_count} rows")
        else:
            console.print(
                f"[bold]Rows:[/] [bold {row_color}]{self.left.row_count}[/] vs "
                + f"[bold {row_color}]{self.right.row_count}[/]"
            )

        console.print(f"[bold]Performance:[/] [{perf_color}]{perf_text}[/]")

        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2, 0, 0))
        table.add_column("Query", style="dim")
        table.add_column("Rows", justify="right")
        table.add_column("Duration", justify="right")

        table.add_row("Left", str(self.left.row_count), f"{self.left.duration:.2f}s")
        table.add_row("Right", str(self.right.row_count), f"{self.right.duration:.2f}s")

        console.print(table)

        if not self.is_equal:
            if not self.columns_match:
                console.print("[bold red]Column mismatch:[/]")
                console.print(f"Left columns: {sorted(self.left.results.columns.tolist())}")
                console.print(f"Right columns: {sorted(self.right.results.columns.tolist())}")
            else:
                # Show difference counts
                console.print("[bold]Row Differences:[/]")

                diff_table = Table(show_header=True)
                diff_table.add_column("Category", style="bold")
                diff_table.add_column("Count", justify="right")
                diff_table.add_column("Percentage", justify="right")

                # Rows only in left
                left_only_count = len(self.left_only)
                if self.left.row_count > 0:
                    left_only_pct = f"{left_only_count / self.left.row_count * 100:.1f}% of left"
                else:
                    left_only_pct = "N/A"

                # Rows only in right
                right_only_count = len(self.right_only)
                if self.right.row_count > 0:
                    right_only_pct = (
                        f"{right_only_count / self.right.row_count * 100:.1f}% of right"
                    )
                else:
                    right_only_pct = "N/A"

                # Common rows
                common_count = len(self.common_rows)
                if self.left.row_count > 0:
                    common_pct = f"{common_count / self.left.row_count * 100:.1f}% of left"
                else:
                    common_pct = "N/A"

                diff_table.add_row("In left only", str(left_only_count), left_only_pct)
                diff_table.add_row("In right only", str(right_only_count), right_only_pct)
                diff_table.add_row("Common rows", str(common_count), common_pct)

                console.print(diff_table)

                # Show sample differences if they exist
                max_samples = 5

                if not self.left_only.empty:
                    left_sample_count = min(max_samples, len(self.left_only))
                    console.print(
                        f"\n[bold]Sample rows in left but not in right "
                        f"({left_sample_count} of {len(self.left_only)}):[/]"
                    )
                    console.print(Pretty(self.left_only.head(max_samples)))

                if not self.right_only.empty:
                    right_sample_count = min(max_samples, len(self.right_only))
                    console.print(
                        f"\n[bold]Sample rows in right but not in left "
                        f"({right_sample_count} of {len(self.right_only)}):[/]"
                    )
                    console.print(Pretty(self.right_only.head(max_samples)))

        console.print()


def display_sample_rows(description: str, rows: Sequence[Any], sample_size: int = 5) -> None:
    """Display sample rows in a simple table format"""
    from rich.table import Table

    console.print(
        f"\n[bold]Sample rows in {description} ([cyan]{sample_size}[/] of {len(rows)}):[/]"
    )

    # Get sample rows
    samples = list(rows)[:sample_size]

    # Create a simple table
    table = Table(box=None, padding=(0, 1))

    # Check if we have tuple/list data (multi-column) or single values
    if samples and isinstance(samples[0], (tuple, list)):
        # For multi-column data, just add a generic "column" header for each field
        for i in range(len(samples[0])):
            table.add_column(f"Col {i + 1}")

        # Add each row to the table
        for row in samples:
            table.add_row(*[str(cell) for cell in row])
    else:
        # For single values, use a simple one-column table
        table.add_column("Value")
        for value in samples:
            table.add_row(str(value))

    console.print(table)


@dataclass
class ComparisonItem:
    """Type definition for a single comparison configuration"""

    name: str
    left_connection: str
    right_connection: str
    left_query: str
    right_query: str
    left_db_type: str = "mssql"
    right_db_type: str = "mssql"


class ComparisonConfig:
    """Configuration for SQL comparisons"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sql_dir = config.get("sql_dir", "./sql")
        self.comparisons = self._process_comparisons()

    def _process_comparisons(self) -> List[ComparisonItem]:
        """Process comparison items from config, loading SQL files where needed"""
        comparisons = []

        # Get the comparison list from the config
        items = self.config.get("compare_list", [])
        if not items:
            raise ValueError("No comparisons defined in config")

        for item in items:
            # Process queries - either use inline query or load from file
            left_query = item.get("left_query")
            right_query = item.get("right_query")

            # Load from file if needed
            if not left_query and "left_query_file" in item:
                left_query = self._load_sql_file(item["left_query_file"])
            if not right_query and "right_query_file" in item:
                right_query = self._load_sql_file(item["right_query_file"])

            # Validate
            if not left_query:
                raise ValueError(f"Comparison '{item['name']}' is missing a left query")
            if not right_query:
                raise ValueError(f"Comparison '{item['name']}' is missing a right query")

            # Create ComparisonItem instance
            comparison = ComparisonItem(
                name=item["name"],
                left_connection=item["left_connection"],
                right_connection=item["right_connection"],
                left_query=left_query,
                right_query=right_query,
                left_db_type=item.get("left_db_type", "mssql"),
                right_db_type=item.get("right_db_type", "mssql"),
            )
            comparisons.append(comparison)

        return comparisons

    def _load_sql_file(self, filename: str) -> str:
        """Load SQL query from a file"""
        sql_path = Path(self.sql_dir) / filename

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        with open(sql_path) as f:
            return f.read()

    def rich_display(self) -> None:
        """Display the configuration using Rich formatting"""
        console.rule("[bold]Comparison Configuration")

        for i, comp in enumerate(self.comparisons):
            color = COLORS[i % len(COLORS)]
            console.print(f"[bold {color}]{comp.name}[/]")
            console.print(f"  Left:  [{color}]{comp.left_db_type}[/] - {comp.left_connection}")
            console.print(f"  Right: [{color}]{comp.right_db_type}[/] - {comp.right_connection}")

            # Show query previews if desired
            if self.config.get("show_query_previews", False):
                left_preview = comp.left_query.strip().split("\n")[0][:50] + "..."
                right_preview = comp.right_query.strip().split("\n")[0][:50] + "..."
                console.print(f"  Left query: {left_preview}")
                console.print(f"  Right query: {right_preview}")

        console.print()
