from dataclasses import dataclass, field
from typing import List, Optional

from rich.table import Table

from utils.rich_utils import COLORS, console


@dataclass
class ChecksumData:
    object_name: str
    checksums: List[str] = field(default_factory=list)
    environments: List[str] = field(default_factory=list)

    def has_differences(self) -> bool:
        valid_checksums = [cs for cs in self.checksums]
        return len(set(valid_checksums)) > 1


@dataclass
class ComparisonResult:
    schema_name: str
    object_type: str
    checksum_rows: List[ChecksumData] = field(default_factory=list)

    @property
    def has_differences(self) -> bool:
        return any(row.has_differences() for row in self.checksum_rows)

    @property
    def all_checksums(self) -> List[List[str]]:
        return [row.checksums for row in self.checksum_rows if row.has_differences()]


def get_checksum_style(checksums: List[str], current_checksum: str) -> str:
    """Determine the style for a checksum based on its relationship to others.

    Args:
        checksums: List of all checksums for this object across environments
        current_checksum: The current checksum value to style

    Returns:
        Rich style string for the checksum
    """
    if current_checksum == "N/A":
        return "dim"

    # Get all unique valid checksums (excluding N/A)
    unique_checksums = sorted(list({cs for cs in checksums if cs != "N/A"}))

    # If only one unique checksum exists (all match), use green
    if len(unique_checksums) == 1:
        return "green"

    # Otherwise, assign colors based on the index in the unique checksums list
    try:
        color_index = unique_checksums.index(current_checksum)
        return COLORS[color_index % len(COLORS)]
    except (ValueError, IndexError):
        # Fallback if something goes wrong
        return "white"


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


def build_comparison_table(result: ComparisonResult) -> Optional[Table]:
    """Build a Rich table for the comparison result.

    Args:
        result: The comparison result data

    Returns:
        A configured Rich Table with populated data
    """
    if not result.checksum_rows:
        return None

    environments = result.checksum_rows[0].environments
    title = (
        f"{result.object_type.title()}s with Different Definitions in Schema '{result.schema_name}'"
    )
    table = create_checksum_table(
        title,
        environments,
    )

    for row in result.checksum_rows:
        if row.has_differences():
            # Apply color styles to checksums
            styled_checksums = []
            for cs in row.checksums:
                style = get_checksum_style(row.checksums, cs)
                styled_checksums.append(f"[{style}]{cs}[/]")

            table.add_row(row.object_name, *styled_checksums)

    return table


def print_comparison_result(result: ComparisonResult) -> None:
    """Print the comparison result.

    Args:
        result: The comparison result data
    """
    console.print()  # Empty line

    if result.has_differences:
        table = build_comparison_table(result)
        if table:
            console.print(table)
    else:
        message = (
            f"[green]No definition differences found in schema "
            f"'{result.schema_name}' for {result.object_type}s[/green]"
        )
        console.print(message)
