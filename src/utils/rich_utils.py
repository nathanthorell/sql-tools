from typing import List

from rich.console import Console
from rich.table import Table

console = Console()


def create_checksum_table(title: str, environments: List[str]) -> Table:
    """Create a Rich table for checksum comparisons.

    Args:
        title: The title of the table
        environments: List of environment names for column headers

    Returns:
        A configured Rich Table object
    """
    table = Table(title=title)
    table.add_column("Object Name", justify="left", max_width=60)
    for env in environments:
        table.add_column(env, justify="left")
    return table


def print_checksum_comparison(
    table: Table, has_differences: bool, schema_name: str, object_type: str
) -> None:
    """Print the checksum comparison table.

    Args:
        table: The Rich table with comparison data
        has_differences: Whether any differences were found
        schema_name: The schema being compared
        object_type: The type of object being compared (e.g., "view", "stored proc")
    """
    console.print()  # Empty line
    if has_differences:
        console.print(table)
    else:
        console.print(
            f"No definition differences found in schema '{schema_name}' for {object_type}s"
        )
