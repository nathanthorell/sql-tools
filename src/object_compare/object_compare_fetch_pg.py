def get_pg_stored_proc_query(schema_name: str) -> str:
    """Get PostgreSQL stored procedure definitions"""
    return f"""
        SELECT
            p.proname AS procedure_name,
            pg_get_functiondef(p.oid) AS procedure_definition
        FROM pg_proc p
        INNER JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = '{schema_name}'
        AND p.prokind = 'p'
        ORDER BY p.proname
        """


def get_pg_view_query(schema_name: str) -> str:
    """Get PostgreSQL view definitions"""
    return f"""
        SELECT
            viewname AS view_name,
            definition AS view_definition
        FROM pg_views
        WHERE schemaname = '{schema_name}'
        ORDER BY viewname
        """


def get_pg_function_query(schema_name: str) -> str:
    """Get PostgreSQL function definitions"""
    return f"""
        SELECT
            p.proname AS function_name,
            pg_get_functiondef(p.oid) AS function_definition
        FROM pg_proc p
        INNER JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = '{schema_name}'
        AND p.prokind = 'f'
        ORDER BY p.proname
        """


def get_pg_table_query(schema_name: str) -> str:
    """Get PostgreSQL table definitions (column structure)"""
    return f"""
        SELECT
            t.tablename AS table_name,
            STRING_AGG(
                CONCAT(
                    c.column_name, ' ',
                    UPPER(
                        CASE
                            WHEN c.data_type = 'character varying' THEN
                                CONCAT('VARCHAR(', c.character_maximum_length, ')')
                            WHEN c.data_type = 'character' THEN
                                CONCAT('CHAR(', c.character_maximum_length, ')')
                            WHEN c.data_type = 'numeric' THEN
                                CONCAT('NUMERIC(', c.numeric_precision, ',', c.numeric_scale, ')')
                            WHEN c.data_type = 'timestamp without time zone' THEN 'TIMESTAMP'
                            WHEN c.data_type = 'timestamp with time zone' THEN 'TIMESTAMPTZ'
                            WHEN c.data_type = 'time without time zone' THEN 'TIME'
                            ELSE UPPER(c.data_type)
                        END
                    ),
                    CASE WHEN c.is_nullable = 'YES' THEN ' NULL' ELSE ' NOT NULL' END,
                    CASE WHEN c.column_default LIKE 'nextval(%' THEN ' IDENTITY' ELSE '' END
                ),
                ','
                ORDER BY c.ordinal_position
            ) AS definition
        FROM pg_tables t
        INNER JOIN information_schema.columns c
            ON c.table_schema = t.schemaname
            AND c.table_name = t.tablename
        WHERE t.schemaname = '{schema_name}'
        GROUP BY t.tablename
        ORDER BY t.tablename
        """


def get_pg_trigger_query(schema_name: str) -> str:
    """Get PostgreSQL trigger definitions"""
    return f"""
        SELECT
            t.tgname AS trigger_name,
            pg_get_triggerdef(t.oid) AS trigger_definition
        FROM pg_trigger t
        INNER JOIN pg_class c ON t.tgrelid = c.oid
        INNER JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = '{schema_name}'
        AND NOT t.tgisinternal
        ORDER BY t.tgname
        """


def get_pg_sequence_query(schema_name: str) -> str:
    """Get PostgreSQL sequence definitions"""
    return f"""
        SELECT
            s.sequencename AS sequence_name,
            CONCAT(
                'TYPE=', t.typname,
                ', START=', CAST(s.start_value AS TEXT),
                ', INCREMENT=', CAST(s.increment_by AS TEXT),
                CASE WHEN s.min_value IS NOT NULL
                    THEN CONCAT(', MIN=', CAST(s.min_value AS TEXT))
                    ELSE ''
                END,
                CASE WHEN s.max_value IS NOT NULL
                    THEN CONCAT(', MAX=', CAST(s.max_value AS TEXT))
                    ELSE ''
                END,
                CASE WHEN s.cycle
                    THEN ', CYCLE'
                    ELSE ', NO CYCLE'
                END,
                CASE WHEN s.cache_size > 1
                    THEN CONCAT(', CACHE ', CAST(s.cache_size AS TEXT))
                    ELSE ', NO CACHE'
                END
            ) AS definition
        FROM pg_sequences s
        INNER JOIN pg_class c ON c.relname = s.sequencename
            AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname)
        INNER JOIN pg_sequence seq ON seq.seqrelid = c.oid
        INNER JOIN pg_type t ON seq.seqtypid = t.oid
        WHERE s.schemaname = '{schema_name}'
        ORDER BY s.sequencename
        """


def get_pg_index_query(schema_name: str) -> str:
    """Get PostgreSQL index definitions"""
    return f"""
        SELECT
            i.indexname AS index_name,
            CONCAT(
                'ON ', i.tablename, ' (',
                STRING_AGG(
                    a.attname,
                    ', '
                    ORDER BY array_position(ix.indkey, a.attnum)
                ),
                ')',
                CASE WHEN ix.indisunique THEN ' UNIQUE' ELSE '' END,
                ' ', UPPER(am.amname),
                CASE WHEN ix.indpred IS NOT NULL THEN
                    CONCAT(' WHERE ', pg_get_expr(ix.indpred, ix.indrelid)) ELSE '' END
            ) AS index_definition
        FROM pg_indexes i
        INNER JOIN pg_class c ON c.relname = i.indexname AND c.relnamespace = (
            SELECT oid FROM pg_namespace WHERE nspname = i.schemaname
        )
        INNER JOIN pg_index ix ON ix.indexrelid = c.oid
        INNER JOIN pg_class t ON t.oid = ix.indrelid
        INNER JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        INNER JOIN pg_am am ON am.oid = c.relam
        WHERE i.schemaname = '{schema_name}'
        AND NOT ix.indisprimary
        GROUP BY i.indexname, i.tablename, ix.indisunique, am.amname, ix.indpred, ix.indrelid
        ORDER BY i.indexname
        """


def get_pg_type_query(schema_name: str) -> str:
    """Get PostgreSQL user-defined type definitions"""
    return f"""
        WITH composite_types AS (
            SELECT
                t.typname,
                CONCAT(
                    'COMPOSITE_TYPE: ',
                    STRING_AGG(
                        a.attname || ' ' ||
                        UPPER(format_type(a.atttypid, a.atttypmod)) ||
                        CASE WHEN NOT a.attnotnull THEN ' NULL' ELSE ' NOT NULL' END,
                        ', '
                        ORDER BY a.attnum
                    )
                ) AS definition
            FROM pg_type t
            INNER JOIN pg_attribute a ON a.attrelid = t.typrelid
            INNER JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = '{schema_name}'
            AND t.typtype = 'c'
            AND a.attnum > 0
            AND NOT a.attisdropped
            GROUP BY t.typname
        ),
        domain_types AS (
            SELECT
                t.typname,
                CONCAT(
                    'DOMAIN_TYPE: BASE_TYPE=',
                    UPPER(format_type(t.typbasetype, t.typtypmod)),
                    CASE WHEN NOT t.typnotnull THEN ', NULLABLE' ELSE ', NOT NULL' END,
                    CASE WHEN t.typdefault IS NOT NULL
                        THEN ', DEFAULT=' || t.typdefault
                        ELSE ''
                    END
                ) AS definition
            FROM pg_type t
            INNER JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = '{schema_name}'
            AND t.typtype = 'd'
        ),
        enum_types AS (
            SELECT
                t.typname,
                CONCAT(
                    'ENUM_TYPE: ',
                    STRING_AGG(e.enumlabel, ', ' ORDER BY e.enumsortorder)
                ) AS definition
            FROM pg_type t
            INNER JOIN pg_namespace n ON t.typnamespace = n.oid
            INNER JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname = '{schema_name}'
            AND t.typtype = 'e'
            GROUP BY t.typname
        )
        SELECT typname AS type_name, definition AS type_definition
        FROM (
            SELECT typname, definition FROM composite_types
            UNION ALL
            SELECT typname, definition FROM domain_types
            UNION ALL
            SELECT typname, definition FROM enum_types
        ) all_types
        ORDER BY typname
        """


def get_pg_foreign_key_query(schema_name: str) -> str:
    """Get PostgreSQL foreign key definitions"""
    return f"""
        SELECT
            tc.constraint_name AS foreign_key_name,
            CONCAT(
                'TABLE=', tc.table_name,
                ', FROM_COLUMNS=', (
                    SELECT STRING_AGG(kcu.column_name, ',' ORDER BY kcu.ordinal_position)
                    FROM information_schema.key_column_usage kcu
                    WHERE kcu.constraint_name = tc.constraint_name
                    AND kcu.constraint_schema = tc.constraint_schema
                ),
                ', TO_COLUMNS=', (
                    SELECT STRING_AGG(ccu.column_name, ',' ORDER BY kcu.ordinal_position)
                    FROM information_schema.constraint_column_usage ccu
                    INNER JOIN information_schema.key_column_usage kcu
                        ON kcu.constraint_name = ccu.constraint_name
                        AND kcu.constraint_schema = ccu.constraint_schema
                    WHERE ccu.constraint_name = tc.constraint_name
                    AND ccu.constraint_schema = tc.constraint_schema
                ),
                ', DELETE_ACTION=', rc.delete_rule,
                ', UPDATE_ACTION=', rc.update_rule,
                ', REF_TABLE=', ccu.table_name,
                ', REF_SCHEMA=', ccu.table_schema
            ) AS definition
        FROM information_schema.table_constraints tc
        INNER JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.constraint_schema = rc.constraint_schema
        INNER JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
            AND rc.unique_constraint_schema = ccu.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.constraint_schema = '{schema_name}'
        GROUP BY tc.constraint_name, tc.constraint_schema, tc.table_name,
                 rc.delete_rule, rc.update_rule, ccu.table_name, ccu.table_schema
        ORDER BY tc.constraint_name
        """
