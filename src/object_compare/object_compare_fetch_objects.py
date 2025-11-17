from typing import Dict

from utils import Connection
from utils.rich_utils import console

from . import object_compare_fetch_mssql as mssql
from . import object_compare_fetch_pg as pg


def fetch_definitions(
    conn: Connection, schema_name: str, object_type: str, db_type: str = "mssql"
) -> Dict[str, str]:
    """
    Fetch object definitions for a given schema and object type.

    Args:
        conn: Database connection
        schema_name: Schema to query
        object_type: Type of object to fetch
            (stored_proc, view, function, table, trigger, sequence)
        db_type: Database type (mssql, postgres)

    Returns:
        Dictionary of object names to their definitions
    """
    query = get_query_for_object_type(schema_name, object_type, db_type)
    if not query:
        console.print(f"[yellow]Warning:[/] Unknown object type '{object_type}'")
        return {}

    result = {}

    with conn.get_connection() as db_conn:
        cursor = db_conn.cursor()
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                name = row[0]  # First column is always the object name
                definition = row[1]  # Second column is always the definition

                # Skip objects with NULL definitions
                if definition:
                    result[name] = definition

            return result
        except Exception as e:
            console.print(
                f"Error fetching {object_type} definitions for schema '{schema_name}': {e}"
            )
            return {}
        finally:
            cursor.close()


def get_query_for_object_type(schema_name: str, object_type: str, db_type: str = "mssql") -> str:
    """
    Get the appropriate SQL query for the given object type and database.

    Args:
        schema_name: Schema name to use in the query
        object_type: Type of database object
        db_type: Database type (mssql, postgres)

    Returns:
        SQL query string or empty string if object type is unknown
    """
    if db_type == "mssql":
        query_functions = {
            "stored_proc": mssql.get_mssql_stored_proc_query,
            "view": mssql.get_mssql_view_query,
            "function": mssql.get_mssql_function_query,
            "table": mssql.get_mssql_table_query,
            "trigger": mssql.get_mssql_trigger_query,
            "sequence": mssql.get_mssql_sequence_query,
            "index": mssql.get_mssql_index_query,
            "type": mssql.get_mssql_type_query,
            "external_table": mssql.get_mssql_external_table_query,
            "foreign_key": mssql.get_mssql_foreign_key_query,
        }
    elif db_type == "postgres":
        query_functions = {
            "stored_proc": pg.get_pg_stored_proc_query,
            "view": pg.get_pg_view_query,
            "function": pg.get_pg_function_query,
            "table": pg.get_pg_table_query,
            "trigger": pg.get_pg_trigger_query,
            "sequence": pg.get_pg_sequence_query,
            "index": pg.get_pg_index_query,
            "type": pg.get_pg_type_query,
            "foreign_key": pg.get_pg_foreign_key_query,
        }
    else:
        console.print(f"[yellow]Warning:[/] Unknown database type '{db_type}'")
        return ""

    if object_type in query_functions:
        return query_functions[object_type](schema_name)

    return ""
