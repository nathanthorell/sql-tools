from typing import Dict, List

from dotenv import load_dotenv

from schema_size.schema_size_types import (
    SchemaSize,
    ServerDatabases,
    ServerResults,
    format_size,
)
from schema_size.schema_size_utils import process_server
from utils import (
    get_config,
    get_connection,
)
from utils.rich_utils import align_columns, console, create_table


def print_schema_table(schema_sizes: List[SchemaSize], server_name: str, db_name: str) -> None:
    """Print a table of schema sizes for a specific database."""
    table = create_table(columns=["Schema", "Row Count", "Total Size", "Used Size", "Unused Size"])

    align_columns(
        table,
        {"Row Count": "right", "Total Size": "right", "Used Size": "right", "Unused Size": "right"},
    )

    for schema in schema_sizes:
        table.add_row(
            schema.schema_name,
            f"{schema.total_rows:,}",
            schema.total_formatted,
            schema.used_formatted,
            schema.unused_formatted,
        )

    total_rows = sum(schema.total_rows for schema in schema_sizes)
    total_bytes = sum(schema.total_bytes for schema in schema_sizes)
    used_bytes = sum(schema.used_bytes for schema in schema_sizes)
    unused_bytes = sum(schema.unused_bytes for schema in schema_sizes)

    console.print(f"\nSchema Sizes for [{server_name}].[{db_name}]:\n")
    console.print(table)
    console.print(
        f"Database Total: {format_size(total_bytes)} "
        f"(Used: {format_size(used_bytes)}, "
        f"Unused: {format_size(unused_bytes)}, "
        f"Rows: {total_rows:,})\n"
    )
    console.rule()


def print_server_summary(server_results: Dict[str, ServerResults]) -> None:
    """Print a summary table of all server results."""
    summary_table = create_table(
        columns=["Server", "Database", "Row Count", "Total Size", "Used Size", "Unused Size"]
    )

    align_columns(
        summary_table,
        {"Row Count": "right", "Total Size": "right", "Used Size": "right", "Unused Size": "right"},
    )

    # Create totals table for server summaries
    totals_table = create_table(
        columns=["Server", "Row Count", "Total Size", "Used Size", "Unused Size"]
    )

    align_columns(
        totals_table,
        {"Row Count": "right", "Total Size": "right", "Used Size": "right", "Unused Size": "right"},
    )

    # Fill tables with data
    for server_name, results in server_results.items():
        # Add each database to the summary table
        for db_name, db_size in results.databases.items():
            summary_table.add_row(
                server_name,
                db_name,
                f"{db_size.total_rows:,}",
                db_size.total_formatted,
                db_size.used_formatted,
                db_size.unused_formatted,
            )

        # Add server total to the totals table
        server_total = results.total_size
        totals_table.add_row(
            server_name,
            f"{server_total.total_rows:,}",
            server_total.total_formatted,
            server_total.used_formatted,
            server_total.unused_formatted,
        )

    console.print("\nDatabase Size Summary:")
    console.print(summary_table)
    console.print("\nServer Totals:")
    console.print(totals_table)


def main() -> None:
    """Main entry point for schema size analysis tool."""
    load_dotenv()
    schema_size_config = get_config("schema_size")
    env_variables = schema_size_config["connections"]
    databases_config = schema_size_config["databases"]
    logging_level = schema_size_config.get("logging_level", "verbose")

    console.print()
    console.rule("[bold cyan]SQL Schema Size Analysis[/]")
    console.print("[italic]Analyzing database and schema storage metrics[/]", justify="center")
    console.print()

    server_configs = {}
    for server_name in env_variables:
        if server_name in databases_config:
            server_configs[server_name] = ServerDatabases(
                server_name=server_name, databases=databases_config[server_name]
            )

    if not server_configs:
        console.print("[bold red]WARNING:[/] No valid server configurations found in config file")
    else:
        console.print(f"Found [bold]{len(server_configs)}[/] server configurations")
        for name, config in server_configs.items():
            console.print(f" [green]{name}[/] - {config}")

    connections = {}
    for server_name, env_var_name in env_variables.items():
        connections[server_name] = get_connection(env_var_name)

    server_results = {}
    for server_name, server_config in server_configs.items():
        if server_name in connections:
            connection = connections[server_name]
            results = process_server(server_config, connection, logging_level)
            server_results[server_name] = results
        else:
            console.print(f"[yellow]Warning:[/] No connection defined for server {server_name}")

    print_server_summary(server_results)

    console.print()
    console.rule("[bold cyan]Analysis Complete[/]")
    console.print()


if __name__ == "__main__":
    main()
