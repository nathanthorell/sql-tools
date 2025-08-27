import time

from dotenv import load_dotenv
from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn

from db_diagram.db_diagram_types import DiagramConfig
from db_diagram.db_diagram_utils import (
    generate_dbml_diagram,
    generate_mermaid_diagram,
    generate_plantuml_diagram,
)
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

        # Create SQLAlchemy engine
        engine = config.connection.get_sqlalchemy_engine()

        # Start timer
        start_time = time.time()

        # Generate diagram with progress indicator
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold magenta]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing database schema...", total=1)

            # Generate diagram based on format
            if config.diagram_format == "plantuml":
                diagram_code = generate_plantuml_diagram(engine, config.schema, config.column_mode)
            elif config.diagram_format == "mermaid":
                diagram_code = generate_mermaid_diagram(engine, config.schema, config.column_mode)
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


if __name__ == "__main__":
    main()
