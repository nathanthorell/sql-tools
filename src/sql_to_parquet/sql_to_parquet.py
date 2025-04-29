import os
import time
from pathlib import Path
from typing import List

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy.engine import Engine

from sql_to_parquet.sql_to_parquet_types import ExportConfig, ExportResult, SqlObject
from utils import get_config, get_connection
from utils.rich_utils import COLORS, align_columns, console, create_table


def export_to_parquet(
    engine: Engine,
    sql_object: SqlObject,
    output_dir: Path,
    batch_size: int,
    logging_level: str,
) -> ExportResult:
    result = ExportResult(friendly_name=sql_object.name, full_object_name=sql_object.object)

    query = f"SELECT * FROM [{sql_object.schema}].[{sql_object.object_name}]"
    if sql_object.filter:
        query += f" WHERE {sql_object.filter}"

    file_path = output_dir / f"{sql_object.name}.parquet"
    start_time = time.time()
    is_verbose = logging_level in ["verbose", "debug"]

    try:
        # Execute query and process results in chunks
        row_count = 0
        for i, df_chunk in enumerate(pd.read_sql_query(query, engine, chunksize=batch_size)):
            if i == 0:
                # First chunk, create the file
                df_chunk.to_parquet(file_path, engine="pyarrow", index=False)
            else:
                # Append to existing file
                df_chunk.to_parquet(file_path, engine="pyarrow", index=False, append=True)

            row_count += len(df_chunk)

            if logging_level == "debug":
                console.print(f"Processed chunk {i + 1} with {len(df_chunk)} rows")

        result.status = "Success"
        result.rows_processed = row_count
        result.file_path = str(file_path)

        if is_verbose:
            console.print(f"Successfully exported: {file_path}")
            console.print(f"Rows processed: {row_count:,}")

    except Exception as e:
        result.status = "Error"
        result.error_message = str(e)

        if is_verbose:
            console.print(f"[red]Error exporting[/] [blue]{sql_object.object}[/]: {e}")

    finally:
        result.elapsed_time = time.time() - start_time

        if is_verbose:
            console.print(f"Execution time: {result.elapsed_time:.2f} seconds")

    return result


def print_results_summary(results: List[ExportResult], logging_level: str) -> None:
    """Print a summary of export results based on the logging level."""
    if logging_level in ["summary", "verbose", "debug"]:
        table = create_table(
            columns=["Friendly Name", "SQL Object", "Status", "Rows", "Time (s)", "File"]
        )

        align_columns(table, {"Rows": "right", "Time (s)": "right"})

        for result in results:
            status_style = "green" if result.status == "Success" else "red"
            status_text = f"[{status_style}]{result.status}[/]"

            file_name = os.path.basename(result.file_path) if result.file_path else "N/A"
            time_text = f"{result.elapsed_time:.2f}" if result.elapsed_time else "N/A"

            table.add_row(
                result.friendly_name,
                result.full_object_name,
                status_text,
                f"{result.rows_processed:,}",
                time_text,
                file_name,
            )

        console.print()
        console.print(table)

        # Print error details for failed exports
        if any(r.status == "Error" for r in results):
            console.print("\n[bold red]Errors:[/]")
            for result in results:
                if result.status == "Error":
                    name = result.friendly_name
                    obj = result.full_object_name
                    error = result.error_message
                    console.print(f"[bold]{name}[/] {obj}: [red]{error}[/]")


def main() -> None:
    load_dotenv()
    tool_config = get_config("sql_to_parquet")
    export_config = ExportConfig.from_dict(tool_config)

    # Convert data_dir to absolute path if it's relative
    if not os.path.isabs(export_config.data_dir):
        project_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        data_dir = os.path.join(project_root, export_config.data_dir.lstrip("./"))
    else:
        data_dir = export_config.data_dir

    # Ensure data directory exists
    data_dir_path = Path(data_dir)
    data_dir_path.mkdir(parents=True, exist_ok=True)

    connection = get_connection("SQL_TO_PARQUET_DB")
    engine = connection.get_sqlalchemy_engine()

    console.print()
    console.rule("[bold cyan]SQL to Parquet Export Tool[/]")
    console.print("[italic]Exporting SQL data to Parquet files[/]", justify="center")
    console.print()

    console.print(f"Server: [green]{connection.server}[/]")
    console.print(f"Database: [green]{connection.database}[/]")
    console.print(f"Output directory: [blue]{data_dir_path}[/]")
    console.print(f"Logging level: [blue]{export_config.logging_level}[/]")
    console.print()

    try:
        if not export_config.objects:
            console.print("[bold red]No objects defined in the configuration[/]")
            return

        results = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold magenta]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("• Initializing export process...", total=None)
            progress.update(task, description="• Ready to begin export", completed=True)

            console.print(f"Found [bold]{len(export_config.objects)}[/] objects to process")

            # Process each object
            for i, sql_object in enumerate(export_config.objects):
                color = COLORS[i % len(COLORS)]

                task = progress.add_task(
                    f"• Processing [{color}]{sql_object.name}[/]...", total=None
                )

                if export_config.logging_level in ["verbose", "debug"]:
                    console.print(f"\nName: [{color}]{sql_object.name}[/]")
                    console.print(f"SQL Object: [{color}]{sql_object.object}[/]")

                result = export_to_parquet(
                    engine=engine,
                    sql_object=sql_object,
                    output_dir=data_dir_path,
                    batch_size=export_config.batch_size,
                    logging_level=export_config.logging_level,
                )
                results.append(result)

                if export_config.logging_level in ["verbose", "debug"]:
                    progress.start()

                # Update progress display
                status = "[green]✓ Done[/]" if result.status == "Success" else "[red]✗ Failed[/]"
                progress.update(
                    task,
                    description=f"• {sql_object.name}: {status} ({result.rows_processed:,} rows)",
                    completed=True,
                )

        print_results_summary(results, export_config.logging_level)

        if export_config.logging_level not in ["verbose", "debug"]:
            console.print(" " * 100, end="\r")

        # Print overall statistics
        successful = sum(1 for r in results if r.status == "Success")
        failed = len(results) - successful
        total_rows = sum(r.rows_processed for r in results)

        console.print()
        if failed == 0:
            console.print(f"[bold green]✓ Successfully exported all {successful} objects[/]")
            console.print(f"[bold green]Total rows: {total_rows:,}[/]")
        else:
            console.print(f"[bold yellow]⚠ Exported {successful} objects ({failed} failed)[/]")
            console.print(f"[bold yellow]Total rows: {total_rows:,}[/]")

        console.print()
        console.rule("[bold cyan]Export Complete[/]")
        console.print()

    except pyodbc.Error as ex:
        console.print(f"[bold red]Database error:[/] {ex}")

    finally:
        pass


if __name__ == "__main__":
    main()
