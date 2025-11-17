import hashlib
from typing import Dict, Set

from dotenv import load_dotenv
from rich.markup import escape
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.text import Text

from object_compare import (
    ChecksumData,
    ComparisonResult,
    fetch_definitions,
    print_comparison_result,
)
from utils import Connection, get_config, get_connection, modify_connection_for_database
from utils.rich_utils import console


def compare_definitions(
    connections: Dict[str, Connection],
    schema_name: str,
    object_type: str,
    display_name: str,
    db_type: str = "mssql",
) -> None:
    object_checksums = {}
    all_object_names: Set[str] = set()

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"• Processing {display_name}s...", total=len(connections))

        # Fetch objects and calculate checksums for each environment
        for env, connection in connections.items():
            objects = fetch_definitions(connection, schema_name, object_type, db_type)
            all_object_names.update(objects.keys())

            # Calculate checksums
            object_checksums[env] = {
                obj_name: hashlib.md5(" ".join(definition.split()).encode("utf-8")).hexdigest()[
                    -10:
                ]
                for obj_name, definition in objects.items()
            }
            progress.advance(task)

        progress.update(
            task, description=f"  • Found {len(all_object_names)} {display_name}s. [green]Done![/]"
        )
        progress.update(task, completed=len(connections))

    # Setup table for results
    env_names = list(connections.keys())
    result = ComparisonResult(schema_name=schema_name, object_type=display_name)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        console=console,
    ) as progress:
        compare_task = progress.add_task(
            f"• Comparing {display_name}s...", total=len(all_object_names)
        )

        for obj_name in sorted(all_object_names):
            checksums = [object_checksums[env].get(obj_name, "N/A") for env in env_names]

            checksum_data = ChecksumData(
                object_name=obj_name, checksums=checksums, environments=env_names
            )

            # Only add to results if the checksums are different
            if checksum_data.has_differences():
                result.checksum_rows.append(checksum_data)

            progress.advance(compare_task)

        diff_count = len(result.checksum_rows)
        progress.update(
            compare_task, description=f"  • Found {diff_count} differences. [green]Done![/]"
        )
        progress.update(compare_task, completed=len(all_object_names))

    print_comparison_result(result)


def main() -> None:
    load_dotenv()
    object_compare_config = get_config("object_compare")
    schema = object_compare_config["schema"]
    database = object_compare_config.get("database", None)
    db_type = object_compare_config.get("db_type", "mssql")
    environments = object_compare_config.get("environments", {})
    object_types = object_compare_config.get(
        "object_types", ["stored_proc", "view", "function"]
    )  # use these as defaults if nothing is in the config

    connections: Dict[str, Connection] = {}

    for env_name, env_var in environments.items():
        try:
            connections[env_name] = get_connection(env_var, db_type=db_type)
            if database is not None:
                connections[env_name] = modify_connection_for_database(
                    connections[env_name], database_name=database
                )
        except ValueError as e:
            console.print(f"[yellow]Warning:[/] Failed to connect to {env_name}: {e}")

    header_content = "[bold cyan]SQL Object Comparison Tool[/]\n"
    header_content += f"Database Type: [cyan]{db_type}[/] | Schema: [cyan]{schema}[/]\n"

    # Display connection info for each environment with column alignment
    max_env_len = max(len(env) for env in environments.keys())
    max_server_len = max(
        (len(connections[env].server) if env in connections and connections[env].server else 3)
        for env in environments.keys()
    )

    for env_name in environments.keys():
        if env_name in connections:
            conn = connections[env_name]
            server = escape(conn.server) if conn.server else "N/A"
            database = escape(conn.database) if conn.database else "N/A"
            padded_env = env_name.upper().ljust(max_env_len)
            padded_server = server.ljust(max_server_len)
            header_content += (
                f"\n[green]{padded_env}[/]: Server: {padded_server} | Database: {database}"
            )
        else:
            padded_env = env_name.upper().ljust(max_env_len)
            header_content += f"\n[yellow]{padded_env}[/]: Not connected"

    term_width = console.width or 100
    content_lines = header_content.split("\n")
    max_line_length = max(len(Text.from_markup(line).plain) for line in content_lines)
    ideal_width = max_line_length + 8
    panel_width = min(max(ideal_width, 60), term_width - 4)
    console.print(Panel(header_content, width=panel_width, border_style="cyan"))

    if not connections:
        console.print(
            "[bold red]Error:[/] No valid database connections found.",
            "Please check your environment variables and config.",
        )
        return

    display_names = {
        "stored_proc": "stored procedure",
        "view": "view",
        "function": "function",
        "table": "table",
        "trigger": "trigger",
        "sequence": "sequence",
        "index": "index",
        "type": "type",
        "external_table": "external table",
        "foreign_key": "foreign key",
    }

    for obj_type in object_types:
        if obj_type in display_names:
            display_type = display_names[obj_type]
            console.print(f"\n[bold magenta]⚡ Processing {display_type}s[/]")
            compare_definitions(connections, schema, obj_type, display_type, db_type)
            console.print(f"[bold green]✓ {display_type.capitalize()}s comparison complete![/]")
        else:
            console.print(f"[yellow]Warning:[/] Unknown object type '{obj_type}' skipped")

    print("")


if __name__ == "__main__":
    main()
