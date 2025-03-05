import hashlib
from typing import Dict, Set

import pyodbc
from prettytable import PrettyTable

from utils.utils import Connection


def fetch_views(conn: pyodbc.Connection, schema_name: str) -> Dict[str, str]:
    query = f"""
    SELECT TABLE_NAME, VIEW_DEFINITION
    FROM INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = '{schema_name}';
    """
    cursor = conn.cursor()
    result = {}
    try:
        cursor.execute(query)
        for row in cursor.fetchall():
            result[row[0]] = row[1]
        return result
    except Exception as e:
        print(f"Error fetching views for schema '{schema_name}': {e}")
        return {}
    finally:
        cursor.close()


def compare_views_for_exclusivity(connections: Dict[str, Connection], schema_name: str) -> None:
    view_data = {}

    # Fetch view names from each server
    for env, connection in connections.items():
        conn = connection.connect()
        view_data[env] = fetch_views(conn, schema_name)

    view_names = {env: set(views.keys()) for env, views in view_data.items()}

    # Prepare a table for exclusive views
    table = PrettyTable()
    table.field_names = ["Server", "Exclusive Views"]
    table.align["Server"] = "l"
    table.align["Exclusive Views"] = "l"
    table.max_width["Exclusive Views"] = 60

    for server, names in view_names.items():
        # Get views unique to this server
        other_servers = [view_names[s] for s in view_names if s != server]
        others_union = set.union(*other_servers)
        exclusive = names - others_union

        if exclusive:
            table.add_row([server, "\n".join(sorted(exclusive))])
        else:
            table.add_row([server, "None"])

    print(f"\nExclusive Views in Schema '{schema_name}':\n")
    print(table)


def compare_view_definitions(connections: Dict[str, Connection], schema_name: str) -> None:
    """
    Compare view definitions across all environments using checksums.
    Only compares views that exist in multiple environments.
    """
    view_checksums = {}
    all_view_names: Set[str] = set()

    # Fetch views and calculate checksums for each environment
    for env, connection in connections.items():
        conn = connection.connect()
        views = fetch_views(conn, schema_name)
        all_view_names.update(views.keys())

        # Calculate checksums
        view_checksums[env] = {
            view_name: hashlib.md5(" ".join(definition.split()).encode("utf-8")).hexdigest()
            for view_name, definition in views.items()
        }

    # Setup table for results
    checksum_table = PrettyTable()
    field_names = ["View Name"] + list(connections.keys())
    checksum_table.field_names = field_names
    for field in field_names:
        checksum_table.align[field] = "l"
    checksum_table.max_width["View Name"] = 60

    has_differences = False
    for view_name in sorted(all_view_names):
        # Skip views that only exist in one environment
        env_count = sum(1 for env in view_checksums if view_name in view_checksums[env])
        if env_count <= 1:
            continue

        # Get checksums for all environments
        checksums = [view_checksums[env].get(view_name, "N/A") for env in connections.keys()]

        # Check if there are differences, ignore N/A
        valid_checksums = [cs for cs in checksums if cs != "N/A"]
        if len(set(valid_checksums)) > 1:
            has_differences = True
            checksum_table.add_row([view_name] + checksums)

    if has_differences:
        print(f"\nViews with Different Definitions in Schema '{schema_name}':\n")
        print(checksum_table)
    else:
        msg = f"\nNo definition differences found in schema '{schema_name}'"
        print(msg)
