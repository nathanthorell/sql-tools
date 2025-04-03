import hashlib
from typing import Dict, Set

import pyodbc

from obj_compare.compare_utils import ChecksumData, ComparisonResult, print_comparison_result
from utils.rich_utils import console
from utils.utils import Connection


def fetch_stored_procs(conn: pyodbc.Connection, schema_name: str) -> Dict[str, str]:
    query = f"""
    SELECT ROUTINE_NAME, ROUTINE_DEFINITION
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '{schema_name}'
    """
    cursor = conn.cursor()
    result = {}
    try:
        cursor.execute(query)
        for row in cursor.fetchall():
            result[row[0]] = row[1]
        return result
    except Exception as e:
        console.print(f"Error fetching stored procedures for schema '{schema_name}': {e}")
        return {}
    finally:
        cursor.close()


def compare_proc_definitions(connections: Dict[str, Connection], schema_name: str) -> None:
    """
    Compare procedure definitions across all environments using checksums.
    Only compares procedures that exist in multiple environments.
    """
    proc_checksums = {}
    all_proc_names: Set[str] = set()

    # Fetch procedures and calculate checksums for each environment
    for env, connection in connections.items():
        conn = connection.connect()
        procs = fetch_stored_procs(conn, schema_name)
        all_proc_names.update(procs.keys())

        # Calculate checksums
        proc_checksums[env] = {
            proc_name: hashlib.md5(" ".join(definition.split()).encode("utf-8")).hexdigest()[-10:]
            for proc_name, definition in procs.items()
        }

    # Setup table for results
    env_names = list(connections.keys())
    result = ComparisonResult(schema_name=schema_name, object_type="stored proc")

    for proc_name in sorted(all_proc_names):
        checksums = [proc_checksums[env].get(proc_name, "N/A") for env in env_names]

        checksum_data = ChecksumData(
            object_name=proc_name, checksums=checksums, environments=env_names
        )

        if checksum_data.has_differences():
            result.checksum_rows.append(checksum_data)

    print_comparison_result(result)
