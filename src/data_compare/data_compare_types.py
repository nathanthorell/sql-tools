from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd
import psycopg2
import pyodbc
from rich.pretty import Pretty
from rich.table import Table

from utils import Connection, get_connection, modify_connection_for_database
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
            self.columns_match = self._check_column_match(left_df, right_df)

            if self.columns_match:
                self._compare_dataframes(left_df, right_df)

        except Exception as e:
            console.print(f"[dim]Error during DataFrame comparison: {e}[/]")

    def __str__(self) -> str:
        status = "EQUAL" if self.is_equal else "NOT EQUAL"
        return (
            f"Comparison Result: {status}\n"
            f"Left:  {self.left.row_count} rows, {self.left.duration:.2f}s\n"
            f"Right: {self.right.row_count} rows, {self.right.duration:.2f}s"
        )

    def _check_column_match(self, left_df: pd.DataFrame, right_df: pd.DataFrame) -> bool:
        """Check if column names match between dataframes (case-insensitive)"""
        left_cols_lower = {col.lower() for col in left_df.columns}
        right_cols_lower = {col.lower() for col in right_df.columns}
        return left_cols_lower == right_cols_lower

    def _compare_columns(
        self, left_df: pd.DataFrame, right_df: pd.DataFrame
    ) -> Dict[str, List[str]]:
        """Compare columns between dataframes and categorize them"""
        left_cols_lower = {col.lower(): col for col in left_df.columns}
        right_cols_lower = {col.lower(): col for col in right_df.columns}

        left_only_lower = set(left_cols_lower.keys()) - set(right_cols_lower.keys())
        right_only_lower = set(right_cols_lower.keys()) - set(left_cols_lower.keys())
        matching_lower = set(left_cols_lower.keys()) & set(right_cols_lower.keys())

        # Return original case column names
        return {
            "left_only": sorted([left_cols_lower[col] for col in left_only_lower]),
            "right_only": sorted([right_cols_lower[col] for col in right_only_lower]),
            "matching": sorted([left_cols_lower[col] for col in matching_lower]),
        }

    def _normalize_column_names(
        self, left_df: pd.DataFrame, right_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Create a copy of right_df with column names matching left_df's case"""
        col_mapping = {}
        for left_col in left_df.columns:
            for right_col in right_df.columns:
                if left_col.lower() == right_col.lower():
                    col_mapping[right_col] = left_col
                    break

        # Return a copy with renamed columns to match left case
        if col_mapping:
            return right_df.rename(columns=col_mapping)
        return right_df.copy()

    def _normalize_data_types(
        self, left_df: pd.DataFrame, right_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Normalize data types between dataframes for consistent comparison"""
        left_normalized = left_df.copy()
        right_normalized = right_df.copy()

        for col in left_df.columns:
            # String type normalization
            if left_df[col].dtype == object or right_df[col].dtype == object:
                left_normalized[col] = left_normalized[col].astype(str).str.strip()
                right_normalized[col] = right_normalized[col].astype(str).str.strip()

            # Numeric type normalization
            elif pd.api.types.is_numeric_dtype(left_df[col]) and pd.api.types.is_numeric_dtype(
                right_df[col]
            ):
                left_normalized[col] = pd.to_numeric(left_normalized[col], errors="coerce")
                right_normalized[col] = pd.to_numeric(right_normalized[col], errors="coerce")

            # Date type normalization
            elif pd.api.types.is_datetime64_dtype(left_df[col]) or pd.api.types.is_datetime64_dtype(
                right_df[col]
            ):
                try:
                    left_normalized[col] = pd.to_datetime(left_normalized[col], errors="coerce")
                    right_normalized[col] = pd.to_datetime(right_normalized[col], errors="coerce")
                except Exception:
                    # Fallback to string comparison if datetime conversion fails
                    left_normalized[col] = left_normalized[col].astype(str).str.strip()
                    right_normalized[col] = right_normalized[col].astype(str).str.strip()

        return left_normalized, right_normalized

    def _compare_dataframes(self, left_df: pd.DataFrame, right_df: pd.DataFrame) -> None:
        """Compare two dataframes and identify matching/non-matching rows"""
        # Normalize column names to match case
        right_df_normalized = self._normalize_column_names(left_df, right_df)

        # Normalize data types for proper comparison
        left_normalized, right_normalized = self._normalize_data_types(left_df, right_df_normalized)

        # Sort columns for consistent comparison
        left_sorted = left_normalized[sorted(left_normalized.columns)]
        right_sorted = right_normalized[sorted(right_normalized.columns)]

        # Perform the merge to identify differences
        merged = left_sorted.merge(right_sorted, how="outer", indicator=True)

        # Extract the results
        self.left_only = merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)
        self.right_only = merged[merged["_merge"] == "right_only"].drop("_merge", axis=1)
        self.common_rows = merged[merged["_merge"] == "both"].drop("_merge", axis=1)

        # Sets is_equal if we have no differences (both sets match entirely)
        left_only_count = len(self.left_only)
        right_only_count = len(self.right_only)
        both_count = len(self.common_rows)

        self.is_equal = left_only_count == 0 and right_only_count == 0 and both_count > 0

    def calculate_performance_metrics(self) -> dict[str, str]:
        """Calculate performance metrics between left and right queries."""
        metrics = {
            "perf_text": "N/A",
            "perf_color": "white",
        }

        if self.left.duration > 0 and self.right.duration > 0:
            if self.right.duration < self.left.duration:
                # Right is faster
                speedup_factor = self.left.duration / self.right.duration
                metrics["perf_text"] = f"Right query is {speedup_factor:.2f}x faster than left"
                metrics["perf_color"] = "green"
            elif self.right.duration > self.left.duration:
                # Right is slower
                slowdown_factor = self.right.duration / self.left.duration
                metrics["perf_text"] = f"Right query is {slowdown_factor:.2f}x slower than left"
                metrics["perf_color"] = "yellow" if slowdown_factor < 2 else "red"
            else:
                # Equal times
                metrics["perf_text"] = "Both queries performed at the same speed"

        return metrics

    def rich_display(self) -> None:
        """Display the comparison result using Rich formatting"""
        status_color = "green" if self.is_equal else "red"
        row_color = "green" if self.row_count_match else "yellow"

        perf_metrics = self.calculate_performance_metrics()

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

        console.print(
            f"[bold]Performance:[/] [{perf_metrics['perf_color']}]{perf_metrics['perf_text']}[/]"
        )

        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2, 0, 0))
        table.add_column("Query", style="dim")
        table.add_column("Rows", justify="right")
        table.add_column("Duration", justify="right")

        table.add_row("Left", str(self.left.row_count), f"{self.left.duration:.2f}s")
        table.add_row("Right", str(self.right.row_count), f"{self.right.duration:.2f}s")

        console.print(table)

        if not self.is_equal:
            if not self.columns_match:
                column_comparison = self._compare_columns(self.left.results, self.right.results)

                console.print("[bold red]Column mismatch:[/]")

                # Create a table to display column comparison
                column_table = Table(show_header=True)
                column_table.add_column("Category", style="bold")
                column_table.add_column("Count", justify="right")
                column_table.add_column("Columns")

                left_only = column_comparison["left_only"]
                right_only = column_comparison["right_only"]
                matching = column_comparison["matching"]

                column_table.add_row(
                    "[bold red]Left-only[/]",
                    str(len(left_only)),
                    ", ".join(left_only) if left_only else "[dim]None[/]",
                )

                column_table.add_row(
                    "[bold red]Right-only[/]",
                    str(len(right_only)),
                    ", ".join(right_only) if right_only else "[dim]None[/]",
                )

                column_table.add_row(
                    "[bold green]Matching[/]",
                    str(len(matching)),
                    ", ".join(matching) if matching else "[dim]None[/]",
                )

                console.print(column_table)
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


@dataclass
class ComparisonItem:
    """Type definition for a single comparison configuration"""

    name: str
    left_connection: Connection
    right_connection: Connection
    left_query: str
    right_query: str
    table_name: str
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

            # Get base connections
            left_conn = get_connection(
                item["left_connection"], db_type=item.get("left_db_type", "mssql")
            )
            right_conn = get_connection(
                item["right_connection"], db_type=item.get("right_db_type", "mssql")
            )

            # Check for database overrides and modify connections if needed
            if "left_database" in item:
                left_conn = modify_connection_for_database(left_conn, item["left_database"])
            if "right_database" in item:
                right_conn = modify_connection_for_database(right_conn, item["right_database"])

            comparison = ComparisonItem(
                name=item["name"],
                left_connection=left_conn,
                right_connection=right_conn,
                left_query=left_query,
                right_query=right_query,
                left_db_type=item.get("left_db_type", "mssql"),
                right_db_type=item.get("right_db_type", "mssql"),
                table_name=item.get("table_name", "table_name_not_provided"),
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
