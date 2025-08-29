from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from pydbml import Database as DBMLDatabase
from pydbml.classes import Column as DBMLColumn
from pydbml.classes import Reference as DBMLReference
from pydbml.classes import Table as DBMLTable
from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import ForeignKeyConstraint, Table

from utils import DbColumn, DbTable


def get_clean_table_name(table_name: str) -> str:
    # Replace schema separator with underscore
    if "." in table_name:
        table_name = table_name.replace(".", "_")

    # Replace other special characters
    return table_name.replace(" ", "_").replace("-", "_")


def get_column_type_string(column: Dict[str, Any]) -> str:
    type_str = str(column["type"])
    type_str = type_str.lower()

    # Remove collation information
    if "collate" in type_str:
        type_str = type_str.split("collate")[0].strip()

    # Extract base data type without length/precision
    if "(" in type_str:
        type_str = type_str.split("(")[0].strip()

    # Remove any extra whitespace or quotes
    type_str = type_str.replace('"', "").strip()

    return type_str


def get_table_columns(engine: Engine, table: Table, table_schema: str) -> List[Dict[str, Any]]:
    inspector = inspect(engine)
    pk_constraint = inspector.get_pk_constraint(table.name, schema=table_schema)
    pk_columns = pk_constraint.get("constrained_columns", [])

    columns = []
    for column in table.columns:
        col_info = {
            "name": column.name,
            "type": column.type,
            "nullable": column.nullable,
            "pk": column.name in pk_columns,
            "fk": len(list(column.foreign_keys)) > 0,
        }
        columns.append(col_info)

    return columns


def format_table_definition_mermaid(
    clean_table_name: str, columns: List[Dict[str, Any]], column_mode: str = "all"
) -> List[str]:
    lines = [f"    {clean_table_name} {{"]

    for column in columns:
        if column_mode == "keys_only" and not (column["pk"] or column["fk"]):
            continue

        col_name = column["name"]
        col_type = get_column_type_string(column)

        modifiers = []
        if column["pk"]:
            modifiers.append("PK")
        if column["fk"]:
            modifiers.append("FK")

        modifier_str = f" {', '.join(modifiers)}" if modifiers else ""
        lines.append(f"        {col_type} {col_name}{modifier_str}")

    lines.append("    }")
    return lines


def format_table_definition_plantuml(
    clean_table_name: str, table: Table, column_mode: str = "all"
) -> List[str]:
    if column_mode == "none":
        return [f"entity {clean_table_name} {{}}"]

    puml_lines = [f"entity {clean_table_name} {{"]

    # Iterate through columns with SQLAlchemy-native filtering
    for column in table.columns:
        # Filter columns based on column_mode
        if column_mode == "keys_only" and not (column.primary_key or column.foreign_keys):
            continue

        col_name = column.name
        col_type = get_column_type_string({"type": column.type})

        # Determine column representation
        if column.primary_key:
            puml_lines.append(f"  * {col_name} : {col_type}")
        elif column.foreign_keys:
            puml_lines.append(f"  # {col_name} : {col_type}")
        else:
            puml_lines.append(f"  {col_name} : {col_type}")

    puml_lines.append("}")
    return puml_lines


def get_reflected_metadata(engine: Engine, schema: Optional[str] = None) -> MetaData:
    """Get reflected database metadata for the specified schema"""
    metadata = MetaData()
    if schema:
        metadata.reflect(bind=engine, schema=schema)
    else:
        metadata.reflect(bind=engine)
    return metadata


def _get_temporal_history_tables(engine: Engine, schema: Optional[str] = None) -> Set[str]:
    """Query SQL Server sys tables to get list of temporal history tables"""
    history_tables: Set[str] = set()

    try:
        # Query to find temporal history tables
        sql = """
        SELECT
            SCHEMA_NAME(t.schema_id) as schema_name,
            t.name as table_name
        FROM sys.tables t
        WHERE t.temporal_type = 1  -- HISTORY_TABLE
        """

        # Add schema filter if specified
        if schema:
            sql += " AND SCHEMA_NAME(t.schema_id) = :schema"

        with engine.connect() as conn:
            if schema:
                result = conn.execute(text(sql), {"schema": schema})
            else:
                result = conn.execute(text(sql))

            for row in result:
                # Create fully qualified table name to match metadata keys
                if row.schema_name:
                    full_name = f"{row.schema_name}.{row.table_name}"
                else:
                    full_name = row.table_name
                history_tables.add(full_name)

    except Exception:
        # If we can't query sys tables (non-SQL Server, permissions, etc.),
        # just return empty set and include all tables
        pass

    return history_tables


def get_filtered_tables(
    metadata: MetaData, engine: Engine, schema: Optional[str] = None
) -> Iterator[Tuple[str, Table, str]]:
    """
    Generator that yields (table_name, table, clean_table_name) for tables
    matching the schema filter, excluding temporal history tables
    """
    # Get list of temporal history tables to exclude
    history_tables = _get_temporal_history_tables(engine, schema)

    for table_name, table in metadata.tables.items():
        # Get schema name if it exists
        table_schema = table.schema if table.schema else ""

        # Skip if we're filtering by schema and this table doesn't match
        if schema and table_schema != schema:
            continue

        # Skip temporal history tables
        if table_name in history_tables:
            continue

        # Generate clean table name
        clean_table_name = get_clean_table_name(table_name)

        yield table_name, table, clean_table_name


def process_relationships(
    metadata: MetaData, schema: Optional[str] = None, diagram_format: str = "mermaid"
) -> List[str]:
    relationship_lines = []
    relationship_set: Set[Tuple[str, str]] = set()

    for table_name, table in metadata.tables.items():
        # Skip if we're filtering by schema and this table doesn't match
        if schema and table.schema != schema:
            continue

        foreign_keys = list(table.foreign_key_constraints)
        for fk in foreign_keys:
            # Get parent and referenced table names
            parent_table = get_clean_table_name(table_name)
            ref_table = get_clean_table_name(fk.referred_table.fullname)

            # Create a relationship identifier to avoid duplicates
            rel_id = (ref_table, parent_table)
            if rel_id in relationship_set:
                continue

            # Add to tracked relationships
            relationship_set.add(rel_id)

            # Get the column names for a more descriptive relationship label
            fk_cols = ", ".join([col.name for col in fk.columns])

            # Format relationship based on diagram type
            if diagram_format == "mermaid":
                relationship_lines.append(f'    {ref_table} ||--o{{ {parent_table} : "{fk_cols}"')
            elif diagram_format == "plantuml":
                relationship_lines.append(f"{ref_table} ||--o{{ {parent_table}")

    return relationship_lines


def generate_mermaid_diagram(
    engine: Engine, schema: Optional[str] = None, column_mode: str = "all"
) -> str:
    # Get reflected metadata
    metadata = get_reflected_metadata(engine, schema)

    mermaid_lines = ["erDiagram"]

    # Process tables
    for _table_name, table, clean_table_name in get_filtered_tables(metadata, engine, schema):
        # For "none" column mode, just add empty tables
        if column_mode == "none":
            mermaid_lines.append(f"    {clean_table_name} {{")
            mermaid_lines.append("    }")
            continue

        # For "all" or "keys_only", get columns and format table
        table_schema = table.schema if table.schema else ""
        columns = get_table_columns(engine, table, table_schema)
        mermaid_lines.extend(
            format_table_definition_mermaid(clean_table_name, columns, column_mode)
        )

    # Process relationships
    relationship_lines = process_relationships(metadata, schema, diagram_format="mermaid")
    mermaid_lines.extend(relationship_lines)

    return "\n".join(mermaid_lines)


def generate_plantuml_diagram(
    engine: Engine, schema: Optional[str] = None, column_mode: str = "all"
) -> str:
    # Get reflected metadata
    metadata = get_reflected_metadata(engine, schema)

    # Initialize PlantUML diagram
    puml_lines = ["@startuml", "hide circle", "skinparam linetype ortho"]

    # Process tables
    for _table_name, table, clean_table_name in get_filtered_tables(metadata, engine, schema):
        # Format and add table definition
        puml_lines.extend(format_table_definition_plantuml(clean_table_name, table, column_mode))

    # Process relationships
    relationship_lines = process_relationships(metadata, schema, diagram_format="plantuml")
    puml_lines.extend(relationship_lines)

    # Close diagram
    puml_lines.append("@enduml")

    return "\n".join(puml_lines)


def _add_dbml_columns_to_table(
    engine: Engine, table: Table, table_schema: str, dbml_table: DBMLTable, column_mode: str
) -> None:
    """Add columns to a DBML table based on the column mode"""
    if column_mode == "none":
        return

    inspector = inspect(engine)
    pk_constraint = inspector.get_pk_constraint(table.name, schema=table_schema)
    pk_columns = pk_constraint.get("constrained_columns", [])

    for column in table.columns:
        # Skip non-key columns if in keys_only mode
        if column_mode == "keys_only" and not (
            column.name in pk_columns or len(list(column.foreign_keys)) > 0
        ):
            continue

        # Determine column type and settings
        col_type = get_column_type_string({"type": column.type})

        # Create DBML column
        dbml_column = DBMLColumn(name=column.name, type=col_type, not_null=not column.nullable)

        # Mark as primary key if applicable
        if column.name in pk_columns:
            dbml_column.pk = True

        # Add column to table
        dbml_table.add_column(dbml_column)


def _get_dbml_reference_columns(
    fk: ForeignKeyConstraint, from_table: DBMLTable, to_table: DBMLTable
) -> Tuple[List[DBMLColumn], List[DBMLColumn]]:
    """Get the DBML column objects for a foreign key relationship"""
    from_cols = []
    for fk_col in fk.columns:
        dbml_col = next((c for c in from_table.columns if c.name == fk_col.name), None)
        if dbml_col:
            from_cols.append(dbml_col)

    to_cols = []
    # Get the referenced column names from the foreign key
    for fk_col in fk.columns:
        # The referenced column is the column that this FK column points to
        for fk_element in fk_col.foreign_keys:
            referenced_col_name = fk_element.column.name
            dbml_col = next((c for c in to_table.columns if c.name == referenced_col_name), None)
            if dbml_col:
                to_cols.append(dbml_col)
            break  # Only process the first foreign key per column

    return from_cols, to_cols


def _add_dbml_relationships(
    dbml_db: DBMLDatabase, metadata: MetaData, engine: Engine, schema: Optional[str]
) -> None:
    """Add foreign key relationships to the DBML database"""
    for table_name, table, _clean_table_name in get_filtered_tables(metadata, engine, schema):
        foreign_keys = list(table.foreign_key_constraints)
        for fk in foreign_keys:
            try:
                # Get the DBML tables using clean names
                clean_from_table_name = get_clean_table_name(table_name)
                clean_to_table_name = get_clean_table_name(fk.referred_table.fullname)
                from_table = next(
                    (t for t in dbml_db.tables if t.name == clean_from_table_name), None
                )
                to_table = next((t for t in dbml_db.tables if t.name == clean_to_table_name), None)

                if from_table and to_table:
                    # Get DBML column objects for the relationship
                    from_cols, to_cols = _get_dbml_reference_columns(fk, from_table, to_table)

                    # Create reference if we found the columns
                    if from_cols and to_cols:
                        ref = DBMLReference(
                            type=">",  # many-to-one relationship (FK -> PK)
                            col1=from_cols,
                            col2=to_cols,
                        )

                        dbml_db.add_reference(ref)
            except Exception:
                # Skip problematic references
                continue


def generate_dbml_diagram(
    engine: Engine, schema: Optional[str] = None, column_mode: str = "all"
) -> Any:
    """Generate DBML diagram from database metadata"""
    # Get reflected metadata
    metadata = get_reflected_metadata(engine, schema)

    # Create DBML database object
    dbml_db = DBMLDatabase()

    # Process tables
    for _table_name, table, clean_table_name in get_filtered_tables(metadata, engine, schema):
        # Create DBML table
        dbml_table = DBMLTable(name=clean_table_name)

        # Add columns based on column_mode
        table_schema = table.schema if table.schema else ""
        _add_dbml_columns_to_table(engine, table, table_schema, dbml_table, column_mode)

        # Add table to database
        dbml_db.add_table(dbml_table)

    # Add relationships
    _add_dbml_relationships(dbml_db, metadata, engine, schema)

    return dbml_db.dbml


# ===============================================
# New functions that work with DbTable objects
# ===============================================


def get_column_type_string_from_db_column(column: DbColumn) -> str:
    """Extract clean data type from DbColumn"""
    type_str = column.data_type.lower()

    # Remove collation information
    if "collate" in type_str:
        type_str = type_str.split("collate")[0].strip()

    # Extract base data type without length/precision
    if "(" in type_str:
        type_str = type_str.split("(")[0].strip()

    # Remove any extra whitespace or quotes
    type_str = type_str.replace('"', "").strip()

    return type_str


def _get_clean_table_name_from_db_table(db_table: DbTable) -> str:
    """Get clean table name for diagram from DbTable"""
    table_name = f"{db_table.schema_name}_{db_table.table_name}"
    return get_clean_table_name(table_name)


def generate_dbml_diagram_from_tables(tables: list[DbTable], column_mode: str = "all") -> Any:
    """Generate DBML diagram from DbTable objects"""
    dbml_db = DBMLDatabase()

    # Process tables
    for db_table in tables:
        clean_name = _get_clean_table_name_from_db_table(db_table)
        dbml_table = DBMLTable(name=clean_name)

        # Add columns based on column_mode
        _add_dbml_columns_from_db_table(db_table, dbml_table, column_mode)

        # Add table to database
        dbml_db.add_table(dbml_table)

    # Add relationships
    _add_dbml_relationships_from_db_tables(dbml_db, tables)

    return dbml_db.dbml


def _add_dbml_columns_from_db_table(
    db_table: DbTable, dbml_table: DBMLTable, column_mode: str
) -> None:
    """Add columns from DbTable to DBML table"""
    if column_mode == "none":
        return

    pk_columns = set()
    if db_table.primary_key:
        pk_columns = {col.column_name for col in db_table.primary_key.columns}

    fk_columns: set[str] = set()
    for fk in db_table.foreign_keys.values():
        fk_columns.update(col.column_name for col in fk.parent_columns)

    for column in db_table.all_columns:
        # Filter based on column_mode
        is_pk = column.column_name in pk_columns
        is_fk = column.column_name in fk_columns

        if column_mode == "keys_only" and not (is_pk or is_fk):
            continue

        # Create DBML column
        col_type = get_column_type_string_from_db_column(column)
        dbml_column = DBMLColumn(name=column.column_name, type=col_type, pk=is_pk)

        dbml_table.add_column(dbml_column)


def _add_dbml_relationships_from_db_tables(dbml_db: DBMLDatabase, tables: list[DbTable]) -> None:
    """Add relationships from DbTable objects to DBML database"""
    table_name_map = _build_table_name_map(tables)

    # Process foreign keys
    for db_table in tables:
        parent_clean_name = _get_clean_table_name_from_db_table(db_table)

        for fk in db_table.foreign_keys.values():
            ref_key = f"{fk.referenced_schema}.{fk.referenced_table}"
            if ref_key in table_name_map:
                ref_clean_name = table_name_map[ref_key]
                _add_foreign_key_references(dbml_db, fk, parent_clean_name, ref_clean_name)


def _build_table_name_map(tables: list[DbTable]) -> dict[str, str]:
    """Build a mapping from full table names to clean names"""
    table_name_map = {}
    for db_table in tables:
        clean_name = _get_clean_table_name_from_db_table(db_table)
        table_name_map[f"{db_table.schema_name}.{db_table.table_name}"] = clean_name
    return table_name_map


def _add_foreign_key_references(
    dbml_db: DBMLDatabase, fk: Any, parent_clean_name: str, ref_clean_name: str
) -> None:
    """Add DBML references for a foreign key relationship"""
    # Create relationship for each column pair
    for parent_col, ref_col in zip(fk.parent_columns, fk.referenced_columns, strict=True):
        parent_dbml_col, ref_dbml_col = _find_dbml_columns(
            dbml_db, parent_clean_name, ref_clean_name, parent_col, ref_col
        )

        if parent_dbml_col and ref_dbml_col:
            ref = DBMLReference(
                type=">",
                col1=parent_dbml_col,
                col2=ref_dbml_col,
            )
            dbml_db.add_reference(ref)


def _find_dbml_columns(
    dbml_db: DBMLDatabase,
    parent_clean_name: str,
    ref_clean_name: str,
    parent_col: Any,
    ref_col: Any,
) -> tuple[DBMLColumn | None, DBMLColumn | None]:
    """Find the DBML column objects for a relationship"""
    parent_dbml_table = None
    ref_dbml_table = None

    for table in dbml_db.tables:
        if table.name == parent_clean_name:
            parent_dbml_table = table
        elif table.name == ref_clean_name:
            ref_dbml_table = table

    parent_dbml_col = None
    ref_dbml_col = None

    if parent_dbml_table:
        for col in parent_dbml_table.columns:
            if col.name == parent_col.column_name:
                parent_dbml_col = col
                break

    if ref_dbml_table:
        for col in ref_dbml_table.columns:
            if col.name == ref_col.column_name:
                ref_dbml_col = col
                break

    return parent_dbml_col, ref_dbml_col


def generate_mermaid_diagram_from_tables(tables: list[DbTable], column_mode: str = "all") -> str:
    """Generate Mermaid diagram from DbTable objects"""
    mermaid_lines = ["erDiagram"]

    # Process tables
    for db_table in tables:
        clean_name = _get_clean_table_name_from_db_table(db_table)
        lines = _format_table_definition_mermaid_from_db_table(db_table, clean_name, column_mode)
        mermaid_lines.extend(lines)

    # Add relationships
    relationship_lines = _format_mermaid_relationships_from_db_tables(tables)
    mermaid_lines.extend(relationship_lines)

    return "\n".join(mermaid_lines)


def _format_table_definition_mermaid_from_db_table(
    db_table: DbTable, clean_table_name: str, column_mode: str = "all"
) -> list[str]:
    """Format table definition for Mermaid from DbTable"""
    lines = [f"    {clean_table_name} {{"]

    if column_mode == "none":
        lines.append("    }")
        return lines

    pk_columns = set()
    if db_table.primary_key:
        pk_columns = {col.column_name for col in db_table.primary_key.columns}

    fk_columns: set[str] = set()
    for fk in db_table.foreign_keys.values():
        fk_columns.update(col.column_name for col in fk.parent_columns)

    for column in db_table.all_columns:
        is_pk = column.column_name in pk_columns
        is_fk = column.column_name in fk_columns

        if column_mode == "keys_only" and not (is_pk or is_fk):
            continue

        col_name = column.column_name
        col_type = get_column_type_string_from_db_column(column)

        modifiers = []
        if is_pk:
            modifiers.append("PK")
        if is_fk:
            modifiers.append("FK")

        modifier_str = f" {', '.join(modifiers)}" if modifiers else ""
        lines.append(f"        {col_type} {col_name}{modifier_str}")

    lines.append("    }")
    return lines


def _format_mermaid_relationships_from_db_tables(tables: list[DbTable]) -> list[str]:
    """Format relationships for Mermaid from DbTable objects"""
    lines = []
    table_name_map = {}

    for db_table in tables:
        clean_name = _get_clean_table_name_from_db_table(db_table)
        table_name_map[f"{db_table.schema_name}.{db_table.table_name}"] = clean_name

    # Process foreign keys
    for db_table in tables:
        parent_clean_name = _get_clean_table_name_from_db_table(db_table)

        for fk in db_table.foreign_keys.values():
            ref_key = f"{fk.referenced_schema}.{fk.referenced_table}"
            if ref_key in table_name_map:
                ref_clean_name = table_name_map[ref_key]
                lines.append(f"    {parent_clean_name} ||--o{{ {ref_clean_name} : references")

    return lines


def generate_plantuml_diagram_from_tables(tables: list[DbTable], column_mode: str = "all") -> str:
    """Generate PlantUML diagram from DbTable objects"""
    puml_lines = ["@startuml", "hide circle", "skinparam linetype ortho"]

    # Process tables
    for db_table in tables:
        clean_name = _get_clean_table_name_from_db_table(db_table)
        lines = _format_table_definition_plantuml_from_db_table(db_table, clean_name, column_mode)
        puml_lines.extend(lines)

    # Add relationships
    relationship_lines = _format_plantuml_relationships_from_db_tables(tables)
    puml_lines.extend(relationship_lines)

    puml_lines.append("@enduml")
    return "\n".join(puml_lines)


def _format_table_definition_plantuml_from_db_table(
    db_table: DbTable, clean_table_name: str, column_mode: str = "all"
) -> list[str]:
    """Format table definition for PlantUML from DbTable"""
    if column_mode == "none":
        return [f"entity {clean_table_name} {{}}"]

    puml_lines = [f"entity {clean_table_name} {{"]

    pk_columns = set()
    if db_table.primary_key:
        pk_columns = {col.column_name for col in db_table.primary_key.columns}

    fk_columns: set[str] = set()
    for fk in db_table.foreign_keys.values():
        fk_columns.update(col.column_name for col in fk.parent_columns)

    for column in db_table.all_columns:
        is_pk = column.column_name in pk_columns
        is_fk = column.column_name in fk_columns

        if column_mode == "keys_only" and not (is_pk or is_fk):
            continue

        col_name = column.column_name
        col_type = get_column_type_string_from_db_column(column)

        # Determine column representation
        if is_pk:
            prefix = "+ "
            suffix = " <<PK>>"
        elif is_fk:
            prefix = "  "
            suffix = " <<FK>>"
        else:
            prefix = "  "
            suffix = ""

        puml_lines.append(f"{prefix}{col_name} : {col_type}{suffix}")

    puml_lines.append("}")
    return puml_lines


def _format_plantuml_relationships_from_db_tables(tables: list[DbTable]) -> list[str]:
    """Format relationships for PlantUML from DbTable objects"""
    lines = []
    table_name_map = {}

    for db_table in tables:
        clean_name = _get_clean_table_name_from_db_table(db_table)
        table_name_map[f"{db_table.schema_name}.{db_table.table_name}"] = clean_name

    # Process foreign keys
    for db_table in tables:
        parent_clean_name = _get_clean_table_name_from_db_table(db_table)

        for fk in db_table.foreign_keys.values():
            ref_key = f"{fk.referenced_schema}.{fk.referenced_table}"
            if ref_key in table_name_map:
                ref_clean_name = table_name_map[ref_key]
                lines.append(f"{parent_clean_name} ||--o{{ {ref_clean_name}")

    return lines
