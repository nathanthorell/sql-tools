from typing import Dict, Set

import pyodbc
from prettytable import PrettyTable

from utils.utils import Connection


def fetch_stored_procs(conn: pyodbc.Connection, schema_name: str) -> Set[str]:
    query = f"""
    SELECT ROUTINE_NAME
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '{schema_name}'
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return {row[0] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error fetching stored procedures for schema '{schema_name}': {e}")
        return set()
    finally:
        cursor.close()


def compare_procs_for_exclusivity(connections: Dict[str, Connection], schema_name: str) -> None:
    proc_names = {}

    # Fetch stored procedure names from each server
    for env, connection in connections.items():
        conn = connection.connect()
        proc_names[env] = fetch_stored_procs(conn, schema_name)

    # Prepare a table for exclusive procedures
    table = PrettyTable()
    table.field_names = ["Server", "Exclusive Procedures"]
    table.align["Server"] = "l"
    table.align["Exclusive Procedures"] = "l"
    table.max_width["Exclusive Procedures"] = 50  # Wrap text to 50 characters

    for server, names in proc_names.items():
        # Get procedures unique to this server
        other_servers = [proc_names[s] for s in proc_names if s != server]
        others_union = set.union(*other_servers)
        exclusive = names - others_union

        if exclusive:
            table.add_row([server, "\n".join(sorted(exclusive))])
        else:
            table.add_row([server, "None"])

    print(f"\nExclusive Stored Procedures in Schema '{schema_name}':\n")
    print(table)
