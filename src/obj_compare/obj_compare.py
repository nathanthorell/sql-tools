import os
from typing import Dict

import toml
from dotenv import load_dotenv
from rich.panel import Panel

from obj_compare.proc_compare import compare_proc_definitions
from obj_compare.view_compare import compare_view_definitions
from utils.rich_utils import console
from utils.utils import Connection, get_connection


def main() -> None:
    load_dotenv()
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, "config.toml")

    console.print(Panel("MS SQL Object Comparison Tool", border_style="cyan"))

    with open(config_path, "r") as f:
        config = toml.load(f)
        obj_compare_config = config["sql_object_compare"]
        schema: str = obj_compare_config["schema"]
        environments = obj_compare_config.get("environments", {})

    connections: Dict[str, Connection] = {}
    for env_name, env_var in environments.items():
        try:
            connections[env_name] = get_connection(env_var)
            console.print(f"Connected to [green]{env_name}[/]: {connections[env_name]}")
        except ValueError as e:
            console.print(f"Warning: Could not connect to [yellow]{env_name}[/]: {e}")

    if not connections:
        console.print(
            "[bold red]Error:[/] No valid database connections found.",
            "Please check your environment variables and config.",
        )
        return

    compare_proc_definitions(connections, schema)
    compare_view_definitions(connections, schema)


if __name__ == "__main__":
    main()
