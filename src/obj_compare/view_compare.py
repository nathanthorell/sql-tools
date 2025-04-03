import hashlib
from typing import Dict, Set

import pyodbc

from obj_compare.compare_utils import ChecksumData, ComparisonResult, print_comparison_result
from utils.rich_utils import console
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
        console.print(f"Error fetching views for schema '{schema_name}': {e}")
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
    env_names = list(connections.keys())
    result = ComparisonResult(schema_name=schema_name, object_type="view")

    for view_name in sorted(all_view_names):
        checksums = [view_checksums[env].get(view_name, "N/A") for env in env_names]

        checksum_data = ChecksumData(
            object_name=view_name, checksums=checksums, environments=env_names
        )

        if checksum_data.has_differences():
            result.checksum_rows.append(checksum_data)

    print_comparison_result(result)
