from pathlib import Path
from typing import Any, Dict, Literal

from utils import get_connection
from utils.rich_utils import console

DiagramFormat = Literal["dbml", "mermaid", "plantuml"]
ColumnMode = Literal["all", "keys_only", "none"]


class DiagramConfig:
    """Configuration for database diagram generation"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Database connection configuration
        connection_var = config.get("connection_env_var", "DB_DIAGRAM_DB")
        self.connection_var: str = connection_var

        # Schema configuration
        self.schema: str = config.get("schema", "dbo")

        # Column display mode
        column_mode_str = config.get("column_mode", "all")
        if column_mode_str not in ["all", "keys_only", "none"]:
            raise ValueError(
                f"Invalid column_mode '{column_mode_str}'. Must be 'all', 'keys_only', or 'none'"
            )
        # Safe cast since we've validated the value above
        self.column_mode = column_mode_str

        # Diagram format
        diagram_format_str = config.get("diagram_format", "dbml").lower()
        if diagram_format_str not in ["dbml", "mermaid", "plantuml"]:
            raise ValueError(
                f"Invalid diagram_format '{diagram_format_str}'. "
                f"Must be 'dbml', 'mermaid', or 'plantuml'"
            )
        # Safe cast since we've validated the value above
        self.diagram_format = diagram_format_str

        # Output configuration
        self.output_file_base: str = config.get("output_file", "database_erd")
        self.output_directory: str = config.get("output_directory", "./output/diagrams")

        # Database override
        self.database: str = config.get("database", "")

        # Setup connection
        self.connection = get_connection(self.connection_var)

        # Apply database override if specified
        if self.database:
            from utils import modify_connection_for_database

            self.connection = modify_connection_for_database(self.connection, self.database)

    @property
    def output_file_path(self) -> Path:
        """Get the complete output file path with appropriate extension"""
        if self.diagram_format == "plantuml":
            extension = ".puml"
        elif self.diagram_format == "mermaid":
            extension = ".mmd"
        else:  # dbml
            extension = ".dbml"

        output_dir = Path(self.output_directory)
        return output_dir / f"{self.output_file_base}{extension}"

    def rich_display(self) -> None:
        """Display the configuration using Rich formatting"""
        console.rule("[bold]Database Diagram Configuration")
        console.print(f"Connection: [green]{self.connection.server}[/]")
        console.print(f"Database: [bold]{self.connection.database}[/]")
        console.print(f"Schema: [bold]{self.schema}[/]")
        console.print(f"Column Mode: [bold]{self.column_mode}[/]")
        console.print(f"Format: [bold]{self.diagram_format}[/]")
        console.print(f"Output: [bold]{self.output_file_path}[/]")
        console.print()
