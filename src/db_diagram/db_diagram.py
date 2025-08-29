import time
from typing import Any

from dotenv import load_dotenv
from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn

from db_diagram.db_diagram_types import DiagramConfig
from db_diagram.db_diagram_utils import (
    generate_dbml_diagram,
    generate_dbml_diagram_from_tables,
    generate_mermaid_diagram,
    generate_mermaid_diagram_from_tables,
    generate_plantuml_diagram,
    generate_plantuml_diagram_from_tables,
)
from utils import DbTable, Hierarchy, MetadataService
from utils.config_utils import get_config
from utils.rich_utils import console


def main() -> None:
    """
    Main entry point for the db_diagram tool.
    """
    try:
        # Load environment variables and configuration
        load_dotenv(override=True)
        db_diagram_config = get_config("db_diagram")
        config = DiagramConfig(db_diagram_config)

        # Display header and configuration
        console.print()
        console.rule("[bold]Database Diagram Generator[/]")
        console.print(
            "[italic]Generates ERD diagrams from database metadata[/]",
            justify="center",
        )
        console.print()

        config.rich_display()

        # Start timer
        start_time = time.time()

        # Generate diagram with progress indicator
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold magenta]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing database schema...", total=1)

            # Generate diagram based on scope
            if config.scope == "hierarchy":
                # Use new hierarchy-based approach with custom DbTable classes
                service = MetadataService(config.connection)
                tables = _get_hierarchical_tables(service, config)
                progress.update(task, completed=0.5, description="Generating diagram...")

                # Generate diagram based on format using DbTable objects
                if config.diagram_format == "plantuml":
                    diagram_code = generate_plantuml_diagram_from_tables(tables, config.column_mode)
                elif config.diagram_format == "mermaid":
                    diagram_code = generate_mermaid_diagram_from_tables(tables, config.column_mode)
                else:  # Default to DBML
                    diagram_code = generate_dbml_diagram_from_tables(tables, config.column_mode)
            else:
                # Use original SQLAlchemy approach for schema mode
                engine = config.connection.get_sqlalchemy_engine()
                progress.update(task, completed=0.5, description="Generating diagram...")

                # Generate diagram based on format using SQLAlchemy
                if config.diagram_format == "plantuml":
                    diagram_code = generate_plantuml_diagram(
                        engine, config.schema, config.column_mode
                    )
                elif config.diagram_format == "mermaid":
                    diagram_code = generate_mermaid_diagram(
                        engine, config.schema, config.column_mode
                    )
                else:  # Default to DBML
                    diagram_code = generate_dbml_diagram(engine, config.schema, config.column_mode)

            progress.update(task, completed=1, description="Schema analysis complete")

        # Create output directory if it doesn't exist
        config.output_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to file
        with open(config.output_file_path, "w") as f:
            f.write(diagram_code)

        # Calculate elapsed time
        elapsed_time = time.time() - start_time

        # Display results
        console.print()
        console.rule("[bold]Generation Complete[/]")
        console.print(f"[green]Diagram saved to:[/] [bold]{config.output_file_path}[/]")
        console.print(f"Generation time: [bold]{elapsed_time:.2f}[/] seconds")
        console.print(f"Diagram size: [bold]{len(diagram_code):,}[/] characters")

        # Format-specific guidance
        if config.diagram_format == "dbml":
            console.print("\n[dim]You can work with the DBML file using:[/]")
            console.print("  • VS Code with DBML extension for syntax highlighting")
            console.print("  • @dbml/cli npm package for format conversion")
            console.print("  • Any text editor (DBML is human-readable)")
        elif config.diagram_format == "mermaid":
            if len(diagram_code) > 50000:
                console.print(
                    "\n[yellow]WARNING:[/] The generated Mermaid diagram exceeds "
                    "the recommended size for mermaid.live."
                )
                console.print("Consider using 'keys_only' column mode for large schemas.")
            else:
                console.print("\n[dim]You can preview the diagram at:[/] https://mermaid.live")
        else:  # PlantUML
            console.print("\n[dim]You can preview the diagram with:[/]")
            console.print("  • PlantUML extension for VSCode")
            console.print("  • PlantText.com")
            console.print("  • PlantUML Server (http://www.plantuml.com/plantuml/)")

        console.print()

    except Exception as e:
        console.print(f"[bold red]Fatal error:[/] {escape(str(e))}")


def _get_hierarchical_tables(service: MetadataService, config: DiagramConfig) -> list[DbTable]:
    """Get tables related to the base table according to hierarchy configuration"""
    base_table = _find_base_table(service, config)
    hierarchy = service.build_hierarchy(base_table)
    related_tables = _collect_related_tables(hierarchy, config.hierarchy_direction)

    if config.hierarchy_max_depth is not None:
        related_tables = _apply_depth_filter(related_tables, hierarchy, config.hierarchy_max_depth)

    # Populate metadata (including foreign keys) for all tables in the hierarchy
    _populate_table_metadata(service, related_tables)

    return list(related_tables)


def _find_base_table(service: MetadataService, config: DiagramConfig) -> DbTable:
    """Find and validate the base table"""
    if not config.base_table:
        raise ValueError("Base table name is required for hierarchy mode")

    # Create a DbTable object and populate it with metadata
    base_table = DbTable(schema_name=config.schema, table_name=config.base_table)

    # Verify the table exists and populate its metadata
    try:
        service.get_table_columns(base_table)
        service.get_primary_key(base_table)
        service.get_foreign_keys(base_table)
        return base_table
    except Exception as e:
        raise ValueError(
            f"Base table '{config.base_table}' not found in schema '{config.schema}'"
        ) from e


def _collect_related_tables(hierarchy: Hierarchy, direction: str) -> set[DbTable]:
    """Collect related tables based on hierarchy direction"""
    related_tables: set[DbTable] = set([hierarchy.root_table])

    if direction in ["down", "both"]:
        _add_relationship_tables(related_tables, hierarchy.relationships)

    if direction in ["up", "both"]:
        _add_relationship_tables(related_tables, hierarchy.relationships)

    return related_tables


def _add_relationship_tables(related_tables: set[DbTable], relationships: list[Any]) -> None:
    """Add tables from relationships to the related tables set"""
    for rel in relationships:
        related_tables.add(rel.parent_table)
        related_tables.add(rel.referenced_table)


def _apply_depth_filter(
    related_tables: set[DbTable], hierarchy: Hierarchy, max_depth: int
) -> set[DbTable]:
    """Filter tables by maximum hierarchy depth"""
    filtered_tables: set[DbTable] = set([hierarchy.root_table])
    for table_key, level in hierarchy.table_levels.items():
        if level <= max_depth:
            for table in related_tables:
                if f"{table.schema_name}.{table.table_name}" == table_key:
                    filtered_tables.add(table)
                    break
    return filtered_tables


def _populate_table_metadata(service: MetadataService, tables: set[DbTable]) -> None:
    """Populate metadata (columns, primary keys, foreign keys) for all tables"""
    for table in tables:
        try:
            service.get_table_columns(table)
            service.get_primary_key(table)
            service.get_foreign_keys(table)
        except Exception:
            # Skip tables that can't be populated (might not exist)
            continue


if __name__ == "__main__":
    main()
