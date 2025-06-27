from typing import Any, Dict, List, Optional

from utils.connection_utils import Connection
from utils.db_util_types import (
    DbColumn,
    DbTable,
    ForeignKey,
    Hierarchy,
    PrimaryKey,
    Relationship,
    UniqueKey,
)
from utils.rich_utils import console


class MetadataService:
    """Service for retrieving database metadata"""

    def __init__(self, connection: Connection):
        self.connection = connection

    def get_table_columns(self, table: DbTable) -> List[DbColumn]:
        """Get all columns for a table with their data types"""
        query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE + CASE
                WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL
                THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                ELSE '' END AS DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{table.schema_name}'
            AND TABLE_NAME = '{table.table_name}'
        ORDER BY ORDINAL_POSITION
        """
        columns = []
        with self.connection.get_connection() as db_conn:
            cursor = db_conn.cursor()
            try:
                cursor.execute(query)
                rows = cursor.fetchall()

                for row in rows:
                    column_name = row[0]
                    data_type = row[1]
                    columns.append(DbColumn(column_name, data_type))
            except Exception as e:
                console.print(f"Error getting columns for '{table.full_table_name()}': {e}")
            finally:
                cursor.close()

        # Update table object and return columns
        table.all_columns = columns
        return columns

    def get_primary_key(self, table: DbTable) -> Optional[PrimaryKey]:
        """Get the primary key for a table"""
        query = f"""
        SELECT
            kc.name AS constraint_name,
            c.name AS column_name,
            c.column_id AS column_id,
            TYPE_NAME(c.system_type_id) AS data_type,
            c.is_identity AS is_identity
        FROM sys.key_constraints kc
        INNER JOIN sys.index_columns ic
            ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
        INNER JOIN sys.columns c
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE kc.type = 'PK'
            AND OBJECT_SCHEMA_NAME(kc.parent_object_id) = '{table.schema_name}'
            AND OBJECT_NAME(kc.parent_object_id) = '{table.table_name}'
        ORDER BY ic.key_ordinal
        """

        pk = None
        with self.connection.get_connection() as db_conn:
            cursor = db_conn.cursor()
            try:
                cursor.execute(query)
                rows = cursor.fetchall()

                if rows:
                    # Get constraint name from first row
                    constraint_name = rows[0][0]
                    pk = PrimaryKey(constraint_name)

                    # Process all columns in the key
                    for row in rows:
                        column_name = row[1]
                        data_type = row[3]

                        # Find column in existing columns or create new
                        col = next(
                            (c for c in table.all_columns if c.column_name == column_name), None
                        )
                        if col is None:
                            col = DbColumn(column_name, data_type)

                        pk.columns.append(col)

            except Exception as e:
                console.print(f"Error getting primary key for '{table.full_table_name()}': {e}")
            finally:
                cursor.close()

        # Update table object and return key
        if pk:
            table.primary_key = pk
        return pk

    def get_foreign_keys(self, table: DbTable) -> Dict[str, ForeignKey]:
        """Get foreign keys for a table using system catalog views"""
        query = f"""
        SELECT
            FK.name AS foreign_key_name,
            OBJECT_SCHEMA_NAME(FKC.parent_object_id) AS parent_schema,
            OBJECT_NAME(FKC.parent_object_id) AS parent_table,
            C.name AS parent_column,
            OBJECT_SCHEMA_NAME(FKC.referenced_object_id) AS referenced_schema,
            OBJECT_NAME(FKC.referenced_object_id) AS referenced_table,
            CR.name AS referenced_column,
            FKC.constraint_column_id AS column_ordinal,
            TYPE_NAME(C.system_type_id) AS parent_data_type,
            TYPE_NAME(CR.system_type_id) AS referenced_data_type
        FROM sys.foreign_keys AS FK
        JOIN sys.foreign_key_columns AS FKC ON FK.object_id = FKC.constraint_object_id
        JOIN sys.columns AS C ON FKC.parent_column_id = C.column_id
            AND FKC.parent_object_id = C.object_id
        JOIN sys.columns AS CR ON FKC.referenced_column_id = CR.column_id
            AND FKC.referenced_object_id = CR.object_id
        WHERE OBJECT_SCHEMA_NAME(FK.parent_object_id) = '{table.schema_name}'
            AND OBJECT_NAME(FK.parent_object_id) = '{table.table_name}'
        ORDER BY foreign_key_name, column_ordinal
        """

        foreign_keys = {}
        fk_data = {}

        with self.connection.get_connection() as db_conn:
            cursor = db_conn.cursor()
            try:
                cursor.execute(query)
                rows = cursor.fetchall()

                for row in rows:
                    fk_name = row[0]
                    parent_schema = row[1]
                    parent_table = row[2]
                    parent_column = row[3]
                    referenced_schema = row[4]
                    referenced_table = row[5]
                    referenced_column = row[6]
                    parent_data_type = row[8]
                    referenced_data_type = row[9]

                    # Initialize data structure for this FK if it doesn't exist
                    if fk_name not in fk_data:
                        fk_data[fk_name] = {
                            "parent_schema": parent_schema,
                            "parent_table": parent_table,
                            "referenced_schema": referenced_schema,
                            "referenced_table": referenced_table,
                            "parent_columns": [],
                            "referenced_columns": [],
                        }

                        # Find or create parent column
                        parent_col = next(
                            (c for c in table.all_columns if c.column_name == parent_column), None
                        )
                        if parent_col is None:
                            parent_col = DbColumn(parent_column, parent_data_type)

                        # Create referenced column
                        referenced_col = DbColumn(referenced_column, referenced_data_type)

                        # Add columns to the data
                        fk_data[fk_name]["parent_columns"].append(parent_col)
                        fk_data[fk_name]["referenced_columns"].append(referenced_col)

                    # Create ForeignKey objects from collected data
                    for fk_name, data in fk_data.items():
                        foreign_keys[fk_name] = ForeignKey(
                            name=fk_name,
                            parent_schema=data["parent_schema"],
                            parent_table=data["parent_table"],
                            parent_columns=data["parent_columns"],
                            referenced_schema=data["referenced_schema"],
                            referenced_table=data["referenced_table"],
                            referenced_columns=data["referenced_columns"],
                        )

            except Exception as e:
                print(f"Error getting foreign keys for '{table.full_table_name()}': {e}")
            finally:
                cursor.close()

        # Update the table object with the foreign keys
        table.foreign_keys.update(foreign_keys)
        return foreign_keys

    def get_unique_keys(self, table: DbTable) -> Dict[str, UniqueKey]:
        """Get unique keys for a table"""
        query = f"""
        SELECT DISTINCT
            i.name AS constraint_name,
            c.name AS column_name,
            ic.key_ordinal AS key_ordinal,
            TYPE_NAME(c.system_type_id) AS data_type
        FROM sys.indexes AS i
        JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns AS c
            ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.is_unique = 1
            AND i.is_primary_key = 0  -- Exclude primary keys
            AND OBJECT_SCHEMA_NAME(i.object_id) = '{table.schema_name}'
            AND OBJECT_NAME(i.object_id) = '{table.table_name}'
            AND ic.is_included_column = 0 -- Exclude included columns
        ORDER BY constraint_name, key_ordinal
        """

        unique_keys = {}
        with self.connection.get_connection() as db_conn:
            cursor = db_conn.cursor()
            try:
                cursor.execute(query)
                rows = cursor.fetchall()

                current_key_name = None
                current_key = None

                for row in rows:
                    constraint_name = row[0]
                    column_name = row[1]
                    data_type = row[3]

                    # If we've moved to a new constraint, create a new key
                    if constraint_name != current_key_name:
                        current_key_name = constraint_name
                        current_key = UniqueKey(constraint_name)
                        unique_keys[constraint_name] = current_key

                    # Find column in existing columns or create new
                    col = next((c for c in table.all_columns if c.column_name == column_name), None)
                    if col is None:
                        col = DbColumn(column_name, data_type)

                    # Only add to the columns if current_key exists
                    if current_key is not None:
                        current_key.columns.append(col)

            except Exception as e:
                console.print(f"Error getting unique keys for '{table.full_table_name()}': {e}")
            finally:
                cursor.close()

        # Update the table object with keys
        table.unique_keys.update(unique_keys)
        return unique_keys

    def get_column_data_type(self, table: DbTable, column_name: str) -> str:
        """Returns the data type of a specific column in a table"""
        query = f"""
        SELECT DATA_TYPE + CASE
            WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL
            THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
            ELSE '' END AS DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{table.schema_name}'
            AND TABLE_NAME = '{table.table_name}'
            AND COLUMN_NAME = '{column_name}'
        """

        data_type = ""
        with self.connection.get_connection() as db_conn:
            cursor = db_conn.cursor()
            try:
                cursor.execute(query)
                result = cursor.fetchone()

                if result is not None:
                    data_type = result[0]

            except Exception as e:
                console.print(
                    f"Error getting data type for '{table.full_table_name()}.{column_name}': {e}"
                )
            finally:
                cursor.close()
        return data_type

    def _get_hierarchy_query(self, root_table: DbTable) -> str:
        """Get the SQL query for building hierarchy"""
        return f"""
        WITH hierarchy AS (
            -- Anchor part: Start with the initial table
            SELECT
                FK.name AS foreign_key_name,
                SS_P.name AS parent_schema,
                SO_P.name AS parent_table,
                SC_P.name AS parent_column,
                SS_R.name AS referenced_schema,
                SO_R.name AS referenced_table,
                SC_R.name AS referenced_column,
                1 AS hierarchy_level,
                CAST(
                    SS_P.name + '.' + SO_P.name + ' -> ' + SS_R.name + '.' + SO_R.name
                    AS varchar(900)
                ) AS hierarchy_path
            FROM sys.foreign_key_columns FKC
            INNER JOIN sys.foreign_keys FK ON FK.object_id = FKC.constraint_object_id
            INNER JOIN sys.objects SO_P ON SO_P.object_id = FKC.referenced_object_id
            INNER JOIN sys.schemas SS_P ON SS_P.schema_id = SO_P.schema_id
            INNER JOIN sys.columns SC_P
                ON SC_P.object_id = FKC.referenced_object_id
                AND SC_P.column_id = FKC.referenced_column_id
            INNER JOIN sys.objects SO_R ON SO_R.object_id = FKC.parent_object_id
            INNER JOIN sys.schemas SS_R ON SS_R.schema_id = SO_R.schema_id
            INNER JOIN sys.columns SC_R
                ON SC_R.object_id = FKC.parent_object_id AND SC_R.column_id = FKC.parent_column_id
            WHERE SS_P.name = '{root_table.schema_name}' AND SO_P.name = '{root_table.table_name}'
            UNION ALL
            -- Recursive part: Find tables that reference the previously found tables
            SELECT
                FK.name AS foreign_key_name,
                SS_P.name AS parent_schema,
                SO_P.name AS parent_table,
                SC_P.name AS parent_column,
                SS_R.name AS referenced_schema,
                SO_R.name AS referenced_table,
                SC_R.name AS referenced_column,
                h.hierarchy_level + 1 AS hierarchy_level,
                CAST(
                    h.hierarchy_path + ' -> ' + SS_R.name + '.' + SO_R.name AS VARCHAR(900)
                ) AS hierarchy_path
            FROM sys.foreign_key_columns FKC
            INNER JOIN sys.foreign_keys FK ON FK.object_id = FKC.constraint_object_id
            INNER JOIN sys.objects SO_P ON SO_P.object_id = FKC.referenced_object_id
            INNER JOIN sys.schemas SS_P ON SS_P.schema_id = SO_P.schema_id
            INNER JOIN sys.columns SC_P
                ON SC_P.object_id = FKC.referenced_object_id
                AND SC_P.column_id = FKC.referenced_column_id
            INNER JOIN sys.objects SO_R ON SO_R.object_id = FKC.parent_object_id
            INNER JOIN sys.schemas SS_R ON SS_R.schema_id = SO_R.schema_id
            INNER JOIN sys.columns SC_R
                ON SC_R.object_id = FKC.parent_object_id AND SC_R.column_id = FKC.parent_column_id
            INNER JOIN hierarchy h
                ON h.referenced_schema = SS_P.name AND h.referenced_table = SO_P.name
            WHERE h.hierarchy_path NOT LIKE '%' + SS_R.name + '.' + SO_R.name + '%'
        )

        SELECT
            foreign_key_name,
            parent_schema,
            parent_table,
            parent_column,
            referenced_schema,
            referenced_table,
            referenced_column,
            hierarchy_level,
            hierarchy_path
        FROM hierarchy
        ORDER BY hierarchy_level, foreign_key_name, parent_column;
        """

    def _process_hierarchy_rows(self, rows: list[Any]) -> Dict[str, Dict[str, Any]]:
        """Process hierarchy query results and group by foreign key name"""
        fk_groups = {}

        for row in rows:
            fk_name = row[0]
            parent_schema, parent_table, parent_column = row[1], row[2], row[3]
            child_schema, child_table, child_column = row[4], row[5], row[6]
            hierarchy_level, hierarchy_path = row[7], row[8]

            # Group columns by foreign key name
            if fk_name not in fk_groups:
                fk_groups[fk_name] = {
                    "parent_schema": parent_schema,
                    "parent_table": parent_table,
                    "parent_columns": [],
                    "child_schema": child_schema,
                    "child_table": child_table,
                    "child_columns": [],
                    "hierarchy_level": hierarchy_level,
                    "hierarchy_path": hierarchy_path,
                }

            # Add columns to the group
            fk_groups[fk_name]["parent_columns"].append(parent_column)
            fk_groups[fk_name]["child_columns"].append(child_column)

        return fk_groups

    def _get_or_create_table(
        self,
        schema: str,
        table: str,
        tables_cache: dict[str, DbTable],
        table_levels: dict[str, int],
        hierarchy_paths: dict[str, str],
        level: int,
        path: str,
    ) -> DbTable:
        """Get existing table from cache or create new one"""
        table_key = f"{schema}.{table}"
        if table_key not in tables_cache:
            table_obj = DbTable(schema, table)
            self.get_table_columns(table_obj)
            self.get_primary_key(table_obj)
            tables_cache[table_key] = table_obj
            table_levels[table_key] = level
            hierarchy_paths[table_key] = path
            return table_obj
        else:
            return tables_cache[table_key]

    def _get_or_create_column(self, table: DbTable, column_name: str) -> DbColumn:
        """Get existing column or create new one with data type lookup"""
        col = next((c for c in table.all_columns if c.column_name == column_name), None)
        if col is None:
            data_type = self.get_column_data_type(table, column_name)
            col = DbColumn(column_name, data_type)
        return col

    def _create_relationships_from_groups(
        self,
        fk_groups: dict[str, dict[str, Any]],
        tables_cache: dict[str, DbTable],
        table_levels: dict[str, int],
        hierarchy_paths: dict[str, str],
    ) -> list[Relationship]:
        """Create Relationship objects from foreign key groups"""
        relationships = []

        for fk_name, fk_data in fk_groups.items():
            parent_schema = fk_data["parent_schema"]
            parent_table = fk_data["parent_table"]
            child_schema = fk_data["child_schema"]
            child_table = fk_data["child_table"]
            hierarchy_level = fk_data["hierarchy_level"]
            hierarchy_path = fk_data["hierarchy_path"]

            # Get or create tables
            parent_obj = self._get_or_create_table(
                parent_schema,
                parent_table,
                tables_cache,
                table_levels,
                hierarchy_paths,
                hierarchy_level - 1,
                hierarchy_path.split(" -> ")[0],
            )
            child_obj = self._get_or_create_table(
                child_schema,
                child_table,
                tables_cache,
                table_levels,
                hierarchy_paths,
                hierarchy_level,
                hierarchy_path,
            )

            # Create column objects
            parent_cols = []
            seen_parent_cols = set()
            for col_name in fk_data["parent_columns"]:
                if col_name not in seen_parent_cols:
                    col_obj = self._get_or_create_column(parent_obj, col_name)
                    parent_cols.append(col_obj)
                    seen_parent_cols.add(col_name)

            child_cols = []
            seen_child_cols = set()
            for col_name in fk_data["child_columns"]:
                if col_name not in seen_child_cols:
                    col_obj = self._get_or_create_column(child_obj, col_name)
                    child_cols.append(col_obj)
                    seen_child_cols.add(col_name)

            # Create relationship
            rel = Relationship(
                name=fk_name,
                parent_table=child_obj,
                parent_columns=child_cols,
                referenced_table=parent_obj,
                referenced_columns=parent_cols,
            )
            relationships.append(rel)

        return relationships

    def build_hierarchy(self, root_table: DbTable) -> Hierarchy:
        """Build a hierarchy of related tables starting from a root table"""
        query = self._get_hierarchy_query(root_table)

        tables_cache = {}
        table_levels = {}
        hierarchy_paths = {}

        # Initialize with root table
        root_key = f"{root_table.schema_name}.{root_table.table_name}"
        tables_cache[root_key] = root_table
        table_levels[root_key] = 0
        hierarchy_paths[root_key] = root_table.full_table_name()

        try:
            with self.connection.get_connection() as db_conn:
                cursor = db_conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()

            # Process rows and group by FK
            fk_groups = self._process_hierarchy_rows(rows)

            # Create relationships from groups
            relationships = self._create_relationships_from_groups(
                fk_groups, tables_cache, table_levels, hierarchy_paths
            )

        except Exception as e:
            console.print(f"Error building hierarchy for '{root_table.full_table_name()}': {e}")
            relationships = []

        return Hierarchy(
            root_table=root_table,
            relationships=relationships,
            table_levels=table_levels,
            hierarchy_paths=hierarchy_paths,
        )
