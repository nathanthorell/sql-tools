from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from pydbml import Database as DBMLDatabase
from pydbml.classes import Column as DBMLColumn
from pydbml.classes import Reference as DBMLReference
from pydbml.classes import Table as DBMLTable
from sqlalchemy import MetaData, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Table


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


def get_filtered_tables(
    metadata: MetaData, schema: Optional[str] = None
) -> Iterator[Tuple[str, Table, str]]:
    """
    Generator that yields (table_name, table, clean_table_name) for tables
    matching the schema filter
    """
    for table_name, table in metadata.tables.items():
        # Get schema name if it exists
        table_schema = table.schema if table.schema else ""

        # Skip if we're filtering by schema and this table doesn't match
        if schema and table_schema != schema:
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
    for _table_name, table, clean_table_name in get_filtered_tables(metadata, schema):
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
    for _table_name, table, clean_table_name in get_filtered_tables(metadata, schema):
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


def _add_dbml_relationships(
    dbml_db: DBMLDatabase, metadata: MetaData, schema: Optional[str]
) -> None:
    """Add foreign key relationships to the DBML database"""
    for table_name, table, _clean_table_name in get_filtered_tables(metadata, schema):
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
                    # Get DBML column objects (not just names)
                    from_cols = []
                    for fk_col in fk.columns:
                        dbml_col = next(
                            (c for c in from_table.columns if c.name == fk_col.name), None
                        )
                        if dbml_col:
                            from_cols.append(dbml_col)

                    to_cols = []
                    for fk_element in fk.elements:
                        dbml_col = next(
                            (c for c in to_table.columns if c.name == fk_element.name), None
                        )
                        if dbml_col:
                            to_cols.append(dbml_col)

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
) -> str:
    """Generate DBML diagram from database metadata"""
    # Get reflected metadata
    metadata = get_reflected_metadata(engine, schema)

    # Create DBML database object
    dbml_db = DBMLDatabase()

    # Process tables
    for _table_name, table, clean_table_name in get_filtered_tables(metadata, schema):
        # Create DBML table
        dbml_table = DBMLTable(name=clean_table_name)

        # Add columns based on column_mode
        table_schema = table.schema if table.schema else ""
        _add_dbml_columns_to_table(engine, table, table_schema, dbml_table, column_mode)

        # Add table to database
        dbml_db.add_table(dbml_table)

    # Add relationships
    _add_dbml_relationships(dbml_db, metadata, schema)

    return str(dbml_db.dbml)
