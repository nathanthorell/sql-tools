from typing import Dict, Set

import pyodbc
from prettytable import PrettyTable

from utils.utils import Connection


def fetch_views(conn: pyodbc.Connection, schema_name: str) -> Set[str]:
    """
    Fetches the names of views in the specified schema.
    Replace this logic with your actual query for fetching view names.
    """
    query = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = '{schema_name}';
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return {row.TABLE_NAME for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error fetching views for schema '{schema_name}': {e}")
        return set()
    finally:
        cursor.close()


def compare_views_for_exclusivity(connections: Dict[str, Connection], schema_name: str) -> None:
    view_names = {}

    # Fetch view names from each server
    for env, connection in connections.items():
        conn = connection.connect()
        view_names[env] = fetch_views(conn, schema_name)

    # Prepare a table for exclusive views
    table = PrettyTable()
    table.field_names = ["Server", "Exclusive Views"]
    table.align["Server"] = "l"
    table.align["Exclusive Views"] = "l"
    table.max_width["Exclusive Views"] = 80  # Wrap text to 80 characters

    server_count = len(view_names)

    for idx, (server, names) in enumerate(view_names.items()):
        # Get views unique to this server
        other_servers = [view_names[s] for s in view_names if s != server]
        others_union = set.union(*other_servers)
        exclusive = names - others_union

        if exclusive:
            exclusive_sorted = sorted(exclusive)
            for i, view in enumerate(exclusive_sorted):
                # Add the server name only once
                if i == 0:  # First view for this server
                    table.add_row([server, view])
                else:  # Subsequent views for this server
                    table.add_row(["", view])
        else:
            # Add a "None" row for servers without exclusive views
            table.add_row([server, "None"])

        # Add a separator row after all rows for the current server, except the last server
        if idx < server_count - 1:
            table.add_row(["------", "-" * 80])  # Full-line separator with hyphens

    print(f"\nExclusive Views in Schema '{schema_name}':\n")
    print(table)
