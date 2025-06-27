from typing import Any, Dict

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

        # connection setup
        self.connection = get_connection(self.connection_var)
        self.connection = modify_connection_for_database(self.connection, self.database)

    def rich_display(self) -> None:
        """Display the configuration using Rich formatting"""
        console.rule("[bold]Cleanup Configuration")
        console.print(f"Connection: {self.connection}")
        console.print(f"Mode: [bold]{self.cleanup_mode}[/]")
        console.print(f"Batch Size: [bold]{self.batch_size}[/]")
        console.print(f"Batch Threshold: [bold]{self.batch_threshold}[/]")
        console.print()
