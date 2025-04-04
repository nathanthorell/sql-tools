import hashlib
import os
from typing import Dict, Set

import toml
from dotenv import load_dotenv
from rich.markup import escape
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.text import Text

from obj_compare.compare_utils import ChecksumData, ComparisonResult, print_comparison_result
from obj_compare.fetch_objects import (
    fetch_definitions,
)
from utils.rich_utils import console
from utils.utils import Connection, get_connection


def compare_definitions(
    connections: Dict[str, Connection],
    schema_name: str,
    object_type: str,
    display_name: str,
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
            conn = connection.connect()
            objects = fetch_definitions(conn, schema_name, object_type)
            all_object_names.update(objects.keys())

            # Calculate checksums
            object_checksums[env] = {
                obj_name: hashlib.md5(" ".join(definition.split()).encode("utf-8")).hexdigest()[
                    -10:
                ]
                for obj_name, definition in objects.items()
            }
            conn.close()
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
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, "config.toml")

    with open(config_path, "r") as f:
        config = toml.load(f)
        obj_compare_config = config["sql_object_compare"]
        schema: str = obj_compare_config["schema"]
        environments = obj_compare_config.get("environments", {})
        object_types = obj_compare_config.get("object_types", ["stored_proc", "view", "function"])

    connections: Dict[str, Connection] = {}
    connection_info = []

    for env_name, env_var in environments.items():
        try:
            connections[env_name] = get_connection(env_var)
            conn_str = escape(str(connections[env_name]))
            connection_info.append(f"[green]{env_name}[/]: {conn_str}")
        except ValueError as e:
            connection_info.append(f"[yellow]{env_name}[/]: Error - {e}")

    header_content = "[bold cyan]MS SQL Object Comparison Tool[/]\n"

    term_width = console.width or 100
    if term_width < 100:
        formatted_connections = []
        for env_name in environments.items():
            if env_name in connections:
                conn = connections[env_name]
                formatted_connections.append(f"[green]{env_name}[/]:")
                formatted_connections.append(f"  Server: [{conn.server}]")
                formatted_connections.append(f"  Database: [{conn.database}]")
            else:
                formatted_connections.append(f"[yellow]{env_name}[/]: Not connected")
        header_content += "\n" + "\n".join(formatted_connections)
    else:
        # For wider terminals, use the original format
        header_content += "\n" + "\n".join(connection_info)

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
    }

    for obj_type in object_types:
        if obj_type in display_names:
            display_type = display_names[obj_type]
            console.print(f"\n[bold magenta]⚡ Processing {display_type}s[/]")
            compare_definitions(connections, schema, obj_type, display_type)
            console.print(f"[bold green]✓ {display_type.capitalize()}s comparison complete![/]")
        else:
            console.print(f"[yellow]Warning:[/] Unknown object type '{obj_type}' skipped")

    print("")


if __name__ == "__main__":
    main()
