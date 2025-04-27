from dataclasses import dataclass
from typing import Dict, List


@dataclass
class ServerDatabases:
    server_name: str
    databases: List[str]

    def __str__(self) -> str:
        return f"{self.server_name}: {len(self.databases)} databases"


@dataclass
class SchemaSize:
    schema_name: str
    total_rows: int
    total_bytes: float
    used_bytes: float
    unused_bytes: float

    @property
    def total_formatted(self) -> str:
        return format_size(self.total_bytes)

    @property
    def used_formatted(self) -> str:
        return format_size(self.used_bytes)

    @property
    def unused_formatted(self) -> str:
        return format_size(self.unused_bytes)


@dataclass
class DatabaseSize:
    total_bytes: float
    used_bytes: float
    unused_bytes: float
    total_rows: int = 0

    @property
    def total_formatted(self) -> str:
        return format_size(self.total_bytes)

    @property
    def used_formatted(self) -> str:
        return format_size(self.used_bytes)

    @property
    def unused_formatted(self) -> str:
        return format_size(self.unused_bytes)


@dataclass
class ServerResults:
    server_name: str
    databases: Dict[str, DatabaseSize]

    @property
    def total_size(self) -> DatabaseSize:
        """Calculate the total size across all databases."""
        total_bytes = sum(db.total_bytes for db in self.databases.values())
        used_bytes = sum(db.used_bytes for db in self.databases.values())
        unused_bytes = sum(db.unused_bytes for db in self.databases.values())
        total_rows = sum(db.total_rows for db in self.databases.values())
        return DatabaseSize(total_bytes, used_bytes, unused_bytes, total_rows)


def format_size(size_bytes: float, decimal_places: int = 2) -> str:
    """
    Format a size in bytes to a human-readable string with appropriate unit.

    Args:
        size_bytes: Size in bytes
        decimal_places: Number of decimal places to include

    Returns:
        Formatted string with appropriate unit (B, KB, MB, GB, TB)
    """
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0

    while size_bytes >= 1024.0 and unit_index < len(units) - 1:
        size_bytes /= 1024.0
        unit_index += 1

    return f"{size_bytes:.{decimal_places}f} {units[unit_index]}"
