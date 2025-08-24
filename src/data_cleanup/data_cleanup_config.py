from typing import Any, Dict, Set

from utils import get_connection, modify_connection_for_database
from utils.rich_utils import console


class CleanupConfig:
    """Configuration for database cleanup operations"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Validate and assign required fields
        connection_var = config.get("conn")
        if not connection_var:
            raise ValueError("Connection variable not defined in config")
        self.connection_var: str = connection_var

        database = config.get("database")
        if not database:
            raise ValueError("Database is not defined in config")
        self.database: str = database

        cleanup_table = config.get("table")
        if not cleanup_table:
            raise ValueError("Table for cleanup is not defined in config")
        self.cleanup_table: str = cleanup_table

        query = config.get("query_of_data_to_remove")
        if not query:
            raise ValueError("Query for data to remove is not defined in config")
        self.query_of_cleanup_pk_values: str = query

        self.batch_size: int = config.get("batch_size", 1000)
        self.batch_threshold: int = config.get("batch_threshold", 1000)
        self.cleanup_mode: str = config.get("cleanup_mode", "summary")
        self.cleanup_schema: str = config.get("schema", "dbo")

        # Parse FK disable table list
        disable_fk_tables = config.get("disable_foreign_keys_for_tables", [])
        if not isinstance(disable_fk_tables, list):
            raise ValueError("disable_foreign_keys_for_tables must be a list of table names")

        # Normalize table names and store as a set for fast lookups
        self.disable_fk_tables: Set[str] = set()
        for table_name in disable_fk_tables:
            if not isinstance(table_name, str):
                raise ValueError(
                    f"Table name must be a string, got {type(table_name)}: {table_name}"
                )

            # Normalize format to schema.table (lowercase for comparison)
            normalized_name = table_name.strip().lower()
            if "." not in normalized_name:
                # If no schema specified, use the cleanup schema
                normalized_name = f"{self.cleanup_schema.lower()}.{normalized_name}"

            self.disable_fk_tables.add(normalized_name)

        # connection setup
        self.connection = get_connection(self.connection_var)
        self.connection = modify_connection_for_database(self.connection, self.database)

    def should_disable_foreign_keys(self, schema_name: str, table_name: str) -> bool:
        """Check if foreign keys should be disabled for the given table"""
        normalized_table = f"{schema_name.lower()}.{table_name.lower()}"
        return normalized_table in self.disable_fk_tables

    def rich_display(self) -> None:
        """Display the configuration using Rich formatting"""
        console.rule("[bold]Cleanup Configuration")
        console.print(f"Connection: {self.connection}")
        console.print(f"Mode: [bold]{self.cleanup_mode}[/]")
        console.print(f"Batch Size: [bold]{self.batch_size}[/]")
        console.print(f"Batch Threshold: [bold]{self.batch_threshold}[/]")
        if self.disable_fk_tables:
            console.print(f"FK Disable Tables: [bold]{len(self.disable_fk_tables)}[/] configured")
            for table in sorted(self.disable_fk_tables):
                console.print(f"  - {table}")
        else:
            console.print("FK Disable Tables: [dim]None configured[/]")
        console.print()
