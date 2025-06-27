import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn

from data_cleanup.data_cleanup_config import CleanupConfig
from data_cleanup.data_cleanup_utils import (
    calculate_operations,
    display_hierarchy_summary,
    execute_cleanup,
    fetch_ids,
    generate_cleanup_script,
    preload_all_foreign_keys,
)
from utils import DbTable, Hierarchy, MetadataService, get_config
from utils.rich_utils import console


def main() -> None:
    try:
        # Load environment variables and configuration
        load_dotenv(override=True)
        data_cleanup_config = get_config("data_cleanup")
        config = CleanupConfig(data_cleanup_config)

        start_time = time.time()
        # Display header
        console.print()
        console.rule("[bold]SQL Data Cleanup[/]")
        console.print(
            "[italic]Analyzes database relationships and generates cleanup script[/]",
            justify="center",
        )
        console.print()
        console.print(f"Connection: [green]{config.connection.server}[/]")
        console.print(f"Database: [bold]{config.database}[/]")
        console.print(f"Mode: [bold]{config.cleanup_mode}[/]")

        if config.batch_threshold > 0:
            console.print("Batch Processing: [bold]Enabled[/]")
            console.print(f"Batch Size: [bold]{config.batch_size}[/] records")
            console.print(f"Batch Threshold: [bold]{config.batch_threshold}[/] records")
        else:
            console.print("Batch Processing: [bold]Disabled[/]")

        console.print()

        # Setup Base
        service = MetadataService(config.connection)
        root_table = DbTable(schema_name=config.cleanup_schema, table_name=config.cleanup_table)
        service.get_table_columns(root_table)
        service.get_primary_key(root_table)

        # Get the hierarchy
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold magenta]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing relationships...", total=1)
            hierarchy: Hierarchy = service.build_hierarchy(root_table)
            progress.update(
                task, completed=1, description=f"Found {len(hierarchy.relationships)} relationships"
            )

        preload_all_foreign_keys(hierarchy, service)

        # Get the IDs for cleanup
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold magenta]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Fetching data...", total=1)
            root_ids = fetch_ids(config)
            progress.update(
                task,
                completed=1,
                description=f"Found {len(root_ids)} records in {root_table.table_name}",
            )

        if not root_ids:
            console.print("[yellow]No data found for cleanup. Exiting.[/]")
            return

        # Get deletion order
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold magenta]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Determining deletion order...", total=1)
            deletion_order = hierarchy.get_deletion_order()
            progress.update(
                task,
                completed=1,
                description=f"Determined deletion order for {len(deletion_order)} tables",
            )

        operations = calculate_operations(service, hierarchy, root_table, root_ids, config)
        display_hierarchy_summary(hierarchy, operations, deletion_order)

        script = generate_cleanup_script(operations, deletion_order, config)
        script_dir = Path("./output/scripts")
        script_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_file = script_dir / f"{config.database}_cleanup_{timestamp}.sql"

        with open(script_file, "w") as f:
            f.write(script)

        console.print(f"\n[green]Cleanup script saved to: {script_file}[/]")

        # Execute if in execute mode
        if config.cleanup_mode == "execute":
            execute_cleanup(config, operations, deletion_order)

        console.print()
        console.rule("[bold]Cleanup Complete[/]")
        end_time = time.time()
        execution_time = end_time - start_time

        console.print(f"Execution time: {execution_time:.4f} seconds")
        console.print()

    except Exception as e:
        console.print(f"[bold red]Fatal error:[/] {escape(str(e))}")


if __name__ == "__main__":
    main()
