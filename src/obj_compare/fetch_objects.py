from typing import Dict

import pyodbc

from utils.rich_utils import console


def fetch_definitions(
    conn: pyodbc.Connection, schema_name: str, object_type: str
) -> Dict[str, str]:
    """
    Fetch object definitions for a given schema and object type.

    Args:
        conn: Database connection
        schema_name: Schema to query
        object_type: Type of object to fetch
            (stored_proc, view, function, table, trigger, sequence)

    Returns:
        Dictionary of object names to their definitions
    """
    query = get_query_for_object_type(schema_name, object_type)
    if not query:
        console.print(f"[yellow]Warning:[/] Unknown object type '{object_type}'")
        return {}

    cursor = conn.cursor()
    result = {}

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
        console.print(f"Error fetching {object_type} definitions for schema '{schema_name}': {e}")
        return {}
    finally:
        cursor.close()


def get_query_for_object_type(schema_name: str, object_type: str) -> str:
    """
    Get the appropriate SQL query for the given object type.

    Args:
        schema_name: Schema name to use in the query
        object_type: Type of database object

    Returns:
        SQL query string or empty string if object type is unknown
    """
    match object_type:
        case "stored_proc":
            return f"""
            SELECT
                OBJECT_NAME(o.object_id) AS procedure_name,
                OBJECT_DEFINITION(o.object_id) AS procedure_definition
            FROM sys.objects o
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type = 'P' AND s.name = '{schema_name}'
            """

        case "view":
            return f"""
            SELECT
                OBJECT_NAME(o.object_id) AS view_name,
                OBJECT_DEFINITION(o.object_id) AS view_definition
            FROM sys.objects o
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type = 'V' AND s.name = '{schema_name}'
            """

        case "function":
            return f"""
            SELECT
                OBJECT_NAME(o.object_id) AS function_name,
                OBJECT_DEFINITION(o.object_id) AS function_definition
            FROM sys.objects o
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('FN', 'IF', 'TF') AND s.name = '{schema_name}'
            """

        case "table":
            return f"""
            SELECT
                t.name AS table_name,
                STRING_AGG(
                    CONCAT(
                        c.name, ' ',
                        UPPER(ty.name),
                        CASE
                            WHEN ty.name IN ('varchar', 'nvarchar', 'char', 'nchar')
                            THEN CONCAT('(',
                                CASE WHEN c.max_length = -1
                                    THEN 'MAX'
                                    ELSE CAST(
                                        CASE WHEN ty.name LIKE 'n%'
                                            THEN c.max_length/2
                                            ELSE c.max_length
                                        END AS VARCHAR
                                    )
                                END, ')'
                            )
                            WHEN ty.name IN ('decimal', 'numeric')
                            THEN CONCAT('(', c.precision, ',', c.scale, ')')
                            ELSE ''
                        END,
                        CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END,
                        CASE WHEN c.is_identity = 1 THEN ' IDENTITY' ELSE '' END
                    ),
                    ','
                ) AS definition
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.columns c ON t.object_id = c.object_id
            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE
                s.name = '{schema_name}'
                AND t.is_external = 0  -- Exclude external tables
                AND t.type = 'U'       -- 'U' means user-created table
            GROUP BY t.name
            """

        case "trigger":
            return f"""
            SELECT
                tr.name AS trigger_name,
                OBJECT_DEFINITION(tr.object_id) AS trigger_definition
            FROM sys.triggers tr
            INNER JOIN sys.objects o ON tr.parent_id = o.object_id
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = '{schema_name}'
            """

        case "sequence":
            return f"""
            SELECT
                seq.name AS sequence_name,
                CONCAT(
                    'TYPE=', t.name,
                    ', START=', CAST(seq.start_value AS VARCHAR),
                    ', INCREMENT=', CAST(seq.increment AS VARCHAR),
                    CASE WHEN seq.minimum_value IS NOT NULL
                        THEN CONCAT(', MIN=', CAST(seq.minimum_value AS VARCHAR))
                        ELSE ''
                    END,
                    CASE WHEN seq.maximum_value IS NOT NULL
                        THEN CONCAT(', MAX=', CAST(seq.maximum_value AS VARCHAR))
                        ELSE ''
                    END,
                    CASE WHEN seq.is_cycling = 1
                        THEN ', CYCLE'
                        ELSE ', NO CYCLE'
                    END,
                    CASE WHEN seq.is_cached = 1
                        THEN CONCAT(', CACHE ', CAST(seq.cache_size AS VARCHAR))
                        ELSE ', NO CACHE'
                    END
                ) AS definition
            FROM sys.sequences seq
            INNER JOIN sys.schemas s ON seq.schema_id = s.schema_id
            INNER JOIN sys.types t ON seq.user_type_id = t.user_type_id
            WHERE s.name = '{schema_name}'
            """

        case _:
            return ""


# def fetch_stored_procs(conn: pyodbc.Connection, schema_name: str) -> Dict[str, str]:
#     query = f"""
#     SELECT ROUTINE_NAME, ROUTINE_DEFINITION
#     FROM INFORMATION_SCHEMA.ROUTINES
#     WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '{schema_name}'
#     """
#     cursor = conn.cursor()
#     result = {}
#     try:
#         cursor.execute(query)
#         for row in cursor.fetchall():
#             result[row[0]] = row[1]
#         return result
#     except Exception as e:
#         console.print(f"Error fetching stored procedures for schema '{schema_name}': {e}")
#         return {}
#     finally:
#         cursor.close()


# def fetch_views(conn: pyodbc.Connection, schema_name: str) -> Dict[str, str]:
#     query = f"""
#     SELECT TABLE_NAME, VIEW_DEFINITION
#     FROM INFORMATION_SCHEMA.VIEWS
#     WHERE TABLE_SCHEMA = '{schema_name}';
#     """
#     cursor = conn.cursor()
#     result = {}
#     try:
#         cursor.execute(query)
#         for row in cursor.fetchall():
#             result[row[0]] = row[1]
#         return result
#     except Exception as e:
#         console.print(f"Error fetching views for schema '{schema_name}': {e}")
#         return {}
#     finally:
#         cursor.close()


# def fetch_functions(conn: pyodbc.Connection, schema_name: str) -> Dict[str, str]:
#     query = f"""
#     SELECT ROUTINE_NAME, ROUTINE_DEFINITION
#     FROM INFORMATION_SCHEMA.ROUTINES
#     WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_SCHEMA = '{schema_name}'
#     """
#     cursor = conn.cursor()
#     result = {}
#     try:
#         cursor.execute(query)
#         for row in cursor.fetchall():
#             result[row[0]] = row[1]
#         return result
#     except Exception as e:
#         console.print(f"Error fetching functions for schema '{schema_name}': {e}")
#         return {}
#     finally:
#         cursor.close()
