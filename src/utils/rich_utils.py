from typing import Dict, List, Literal, Optional, Tuple

from rich.console import Console
from rich.table import Table

JustifyType = Literal["left", "center", "right"]

console = Console()
console.print()
console.clear(home=True)

# List of colors to use for any color grouping needs
# These colors are chosen to be distinguishable in both light and dark terminals
COLORS = [
    "green",
    "blue",
    "red",
    "yellow",
    "magenta",
    "cyan",
    "bright_green",
    "bright_blue",
    "bright_red",
    "bright_yellow",
    "bright_magenta",
    "bright_cyan",
]


def create_table(
    title: Optional[str] = None,
    columns: Optional[List[str]] = None,
    padding: Tuple[int, int, int, int] = (0, 1, 0, 1),  # top, right, bottom, left
) -> Table:
    """Create a standardized Rich table with consistent formatting.

    Args:
        title: Optional title for the table
        columns: List of column names
        padding: Tuple of padding values (top, right, bottom, left)

    Returns:
        A configured Rich Table object
    """
    table = Table(
        title=title,
        show_header=columns is not None,
        header_style="bold",
        padding=padding,
    )

    # Add columns if provided
    if columns:
        for col in columns:
            table.add_column(col, justify="left")

    return table


def align_columns(table: Table, alignments: Dict[str, JustifyType]) -> None:
    """Set the alignment for specified columns in the table.

    Args:
        table: The Rich Table to modify
        alignments: Dictionary mapping column names to alignment values
                   ("left", "center", "right")
    """
    for column in table.columns:
        column_name = str(column.header)
        if column_name in alignments:
            # This ensures we're only assigning valid justification values
            column.justify = alignments[column_name]
