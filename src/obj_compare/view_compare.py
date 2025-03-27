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
            view_name: hashlib.md5(" ".join(definition.split()).encode("utf-8")).hexdigest()[-10:]
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
        # Get checksums for all environments
        checksums = [view_checksums[env].get(view_name, "N/A") for env in connections.keys()]

        # Check if there are differences, ignore N/A
        valid_checksums = [cs for cs in checksums]
        if len(set(valid_checksums)) > 1:
            has_differences = True
            checksum_table.add_row([view_name] + checksums)

    if has_differences:
        print(f"\nViews with Different Definitions in Schema '{schema_name}':\n")
        print(checksum_table)
    else:
        msg = f"\nNo definition differences found in schema '{schema_name}'"
        print(msg)
