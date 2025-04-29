from typing import Dict

from utils import Connection
from utils.rich_utils import console


def fetch_definitions(conn: Connection, schema_name: str, object_type: str) -> Dict[str, str]:
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
            AND OBJECT_NAME(o.object_id) NOT LIKE 'sp[_]%diagram%'
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
            AND OBJECT_NAME(o.object_id) != 'fn_diagramobjects'
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
                AND t.is_external = 0
                AND t.type = 'U'
                AND t.is_ms_shipped = 0
                AND t.name NOT IN ('sysdiagrams', 'database_firewall_rules')
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

        case "index":
            return f"""
            SELECT
                i.name AS index_name,
                CONCAT(
                    'ON ', OBJECT_NAME(i.object_id), ' (',
                    STRING_AGG(
                        CONCAT(c.name, CASE WHEN ic.is_descending_key = 1
                        THEN ' DESC' ELSE ' ASC' END),
                        ', '
                    ) WITHIN GROUP (ORDER BY ic.key_ordinal),
                    ')',
                    CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END,
                    CASE WHEN i.type = 1 THEN ' CLUSTERED'
                        WHEN i.type = 2 THEN ' NONCLUSTERED'
                        WHEN i.type = 3 THEN ' XML'
                        WHEN i.type = 4 THEN ' SPATIAL'
                        WHEN i.type = 5 THEN ' CLUSTERED COLUMNSTORE'
                        WHEN i.type = 6 THEN ' NONCLUSTERED COLUMNSTORE'
                        WHEN i.type = 7 THEN ' NONCLUSTERED HASH'
                        ELSE ''
                    END,
                    CASE WHEN i.filter_definition IS NOT NULL THEN
                        CONCAT(' WHERE ', i.filter_definition) ELSE '' END
                ) AS index_definition
            FROM sys.indexes i
            INNER JOIN sys.objects o ON i.object_id = o.object_id
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            INNER JOIN sys.index_columns ic
                ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE s.name = '{schema_name}'
            AND i.name IS NOT NULL  -- Skip unnamed indexes (like PK_...)
            AND o.is_ms_shipped = 0 -- Skip system objects
            AND OBJECT_NAME(i.object_id) != 'sysdiagrams' -- Exclude sysdiagrams table
            AND NOT (i.is_primary_key = 1 AND o.type = 'TF') -- Exclude table-valued functions
            AND i.type > 0 -- Skip heaps
            GROUP BY i.object_id, i.name, i.is_unique, i.type, i.filter_definition
            """

        case _:
            return ""
