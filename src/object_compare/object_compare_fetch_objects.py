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


def _get_stored_proc_query(schema_name: str) -> str:
    return f"""
        SELECT
            OBJECT_NAME(o.object_id) AS procedure_name,
            OBJECT_DEFINITION(o.object_id) AS procedure_definition
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type = 'P' AND s.name = '{schema_name}'
        AND OBJECT_NAME(o.object_id) NOT LIKE 'sp[_]%diagram%'
        """


def _get_view_query(schema_name: str) -> str:
    return f"""
        SELECT
            OBJECT_NAME(o.object_id) AS view_name,
            OBJECT_DEFINITION(o.object_id) AS view_definition
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type = 'V' AND s.name = '{schema_name}'
        """


def _get_function_query(schema_name: str) -> str:
    return f"""
        SELECT
            OBJECT_NAME(o.object_id) AS function_name,
            OBJECT_DEFINITION(o.object_id) AS function_definition
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type IN ('FN', 'IF', 'TF') AND s.name = '{schema_name}'
        AND OBJECT_NAME(o.object_id) != 'fn_diagramobjects'
        """


def _get_table_query(schema_name: str) -> str:
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


def _get_trigger_query(schema_name: str) -> str:
    return f"""
        SELECT
            tr.name AS trigger_name,
            OBJECT_DEFINITION(tr.object_id) AS trigger_definition
        FROM sys.triggers tr
        INNER JOIN sys.objects o ON tr.parent_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = '{schema_name}'
        """


def _get_sequence_query(schema_name: str) -> str:
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


def _get_index_query(schema_name: str) -> str:
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


def _get_type_query(schema_name: str) -> str:
    return f"""
        SELECT
            tt.name AS type_name,
            CASE
                WHEN tt.is_table_type = 1 THEN
                    -- For table types, get column definitions
                    (SELECT
                        'TABLE_TYPE: ' +
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
                                CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END
                            ),
                            ', '
                        )
                    FROM sys.columns c
                    INNER JOIN sys.table_types tab ON c.object_id = tab.type_table_object_id
                    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                    WHERE tab.user_type_id = tt.user_type_id
                    )
                ELSE
                    -- For scalar types
                    CONCAT(
                        'SCALAR_TYPE: ',
                        'BASE_TYPE=', bt.name,
                        CASE
                            WHEN bt.name IN (
                                'varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary'
                            )
                            THEN CONCAT('(',
                                CASE WHEN tt.max_length = -1
                                    THEN 'MAX'
                                    ELSE CAST(
                                        CASE WHEN bt.name LIKE 'n%'
                                            THEN tt.max_length/2
                                            ELSE tt.max_length
                                        END AS VARCHAR
                                    )
                                END, ')'
                            )
                            WHEN bt.name IN ('decimal', 'numeric')
                            THEN CONCAT('(', tt.precision, ',', tt.scale, ')')
                            ELSE ''
                        END,
                        CASE WHEN tt.is_nullable = 1 THEN ', NULLABLE' ELSE ', NOT NULL' END
                    )
            END AS type_definition
        FROM sys.types tt
        INNER JOIN sys.schemas s ON tt.schema_id = s.schema_id
        LEFT JOIN sys.types bt ON tt.system_type_id = bt.user_type_id
        WHERE
            s.name = '{schema_name}'
            AND tt.is_user_defined = 1
        """


def _get_external_table_query(schema_name: str) -> str:
    return f"""
        SELECT
            et.name AS external_table_name,
            CONCAT(
                'DATA_SOURCE=', ds.name,
                ', LOCATION=', et.location,
                CASE WHEN ff.name IS NOT NULL
                    THEN CONCAT(', FILE_FORMAT=', ff.name)
                    ELSE ''
                END,
                CASE WHEN et.reject_type IS NOT NULL
                    THEN CONCAT(', REJECT_TYPE=', et.reject_type)
                    ELSE ''
                END,
                CASE WHEN et.reject_value IS NOT NULL
                    THEN CONCAT(', REJECT_VALUE=', CAST(et.reject_value AS VARCHAR))
                    ELSE ''
                END,
                CASE WHEN et.reject_sample_value IS NOT NULL
                    THEN CONCAT(
                        ', REJECT_SAMPLE_VALUE=',
                        CAST(et.reject_sample_value AS VARCHAR)
                    )
                    ELSE ''
                END,
                ' | COLUMNS=',
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
                        CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END
                    ),
                    ','
                ) WITHIN GROUP (ORDER BY c.column_id)
            ) AS definition
        FROM sys.external_tables et
        INNER JOIN sys.schemas s ON et.schema_id = s.schema_id
        INNER JOIN sys.external_data_sources ds ON et.data_source_id = ds.data_source_id
        LEFT JOIN sys.external_file_formats ff ON et.file_format_id = ff.file_format_id
        INNER JOIN sys.columns c ON et.object_id = c.object_id
        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE s.name = '{schema_name}'
        GROUP BY
            et.name, ds.name, et.location, ff.name, et.reject_type,
            et.reject_value, et.reject_sample_value
        """


def _get_foreign_key_query(schema_name: str) -> str:
    return f"""
        SELECT
            fk.name AS foreign_key_name,
            CONCAT(
                'TABLE=', OBJECT_NAME(fk.parent_object_id),
                ', FROM_COLUMNS=', (
                    SELECT STRING_AGG(COL_NAME(fkc.parent_object_id, fkc.parent_column_id), ',')
                    FROM sys.foreign_key_columns fkc
                    WHERE fkc.constraint_object_id = fk.object_id
                ),
                ', TO_COLUMNS=', (
                    SELECT STRING_AGG(
                        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id), ','
                    )
                    FROM sys.foreign_key_columns fkc
                    WHERE fkc.constraint_object_id = fk.object_id
                ),
                ', DELETE_ACTION=', fk.delete_referential_action_desc,
                ', UPDATE_ACTION=', fk.update_referential_action_desc,
                ', IS_DISABLED=', CAST(fk.is_disabled AS VARCHAR),
                ', IS_NOT_TRUSTED=', CAST(fk.is_not_trusted AS VARCHAR),
                ', REF_TABLE=', OBJECT_NAME(fk.referenced_object_id),
                ', REF_SCHEMA=', SCHEMA_NAME(OBJECTPROPERTY(fk.referenced_object_id, 'SchemaId'))
            ) AS definition
        FROM sys.foreign_keys fk
        INNER JOIN sys.objects o ON fk.parent_object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = '{schema_name}'
        """


def get_query_for_object_type(schema_name: str, object_type: str) -> str:
    """
    Get the appropriate SQL query for the given object type.

    Args:
        schema_name: Schema name to use in the query
        object_type: Type of database object

    Returns:
        SQL query string or empty string if object type is unknown
    """
    query_functions = {
        "stored_proc": _get_stored_proc_query,
        "view": _get_view_query,
        "function": _get_function_query,
        "table": _get_table_query,
        "trigger": _get_trigger_query,
        "sequence": _get_sequence_query,
        "index": _get_index_query,
        "type": _get_type_query,
        "external_table": _get_external_table_query,
        "foreign_key": _get_foreign_key_query,
    }

    if object_type in query_functions:
        return query_functions[object_type](schema_name)

    return ""
