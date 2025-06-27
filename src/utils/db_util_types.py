from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class DbColumn:
    column_name: str
    data_type: str


@dataclass
class PrimaryKey:
    name: str
    columns: List[DbColumn] = field(default_factory=list)


@dataclass
class UniqueKey:
    name: str
    columns: List[DbColumn] = field(default_factory=list)


@dataclass
class ForeignKey:
    name: str
    parent_schema: str
    parent_table: str
    parent_columns: List[DbColumn]
    referenced_schema: str
    referenced_table: str
    referenced_columns: List[DbColumn]

    def __post_init__(self) -> None:
        if len(self.parent_columns) != len(self.referenced_columns):
            raise ValueError("Number of parent columns must match number of referenced columns")


@dataclass
class DbTable:
    schema_name: str
    table_name: str
    primary_key: Optional[PrimaryKey] = None
    unique_keys: Dict[str, UniqueKey] = field(default_factory=dict)
    foreign_keys: Dict[str, ForeignKey] = field(default_factory=dict)
    all_columns: List[DbColumn] = field(default_factory=list)
    where_conditions: str | None = None

    def __hash__(self) -> int:
        """Make DbTable hashable based on schema and table name"""
        return hash((self.schema_name, self.table_name))

    def __eq__(self, other: object) -> bool:
        """Define equality based on schema and table name"""
        if not isinstance(other, DbTable):
            return False
        return (self.schema_name, self.table_name) == (other.schema_name, other.table_name)

    def full_table_name(self) -> str:
        """Returns the fully qualified table name."""
        return f"[{self.schema_name}].[{self.table_name}]"

    def select_sql(self, keys_only: bool = True) -> str:
        """Generates a simple SELECT statement for the table."""
        if keys_only and self.primary_key and self.primary_key.columns:
            columns = ", ".join(column.column_name for column in self.primary_key.columns)
        elif self.all_columns:
            columns = ", ".join(column.column_name for column in self.all_columns)
        else:
            columns = "*"

        sql = f"SELECT {columns} FROM {self.full_table_name()}"
        if self.where_conditions:
            sql += f" WHERE {self.where_conditions}"
        return sql


@dataclass
class Relationship:
    name: str
    parent_table: DbTable
    parent_columns: List[DbColumn]
    referenced_table: DbTable
    referenced_columns: List[DbColumn]

    def __post_init__(self) -> None:
        if len(self.parent_columns) != len(self.referenced_columns):
            raise ValueError("Number of parent columns must match number of referenced columns")


@dataclass
class Hierarchy:
    root_table: DbTable
    relationships: list[Relationship]
    table_levels: Dict[str, int] = field(default_factory=dict)  # "schema.table", level
    hierarchy_paths: Dict[str, str] = field(default_factory=dict)  # "schema.table", "path"

    def get_child_tables(self, parent: DbTable) -> list[DbTable]:
        return [rel.referenced_table for rel in self.relationships if rel.parent_table == parent]

    def get_parent_tables(self, child: DbTable) -> list[DbTable]:
        return [rel.parent_table for rel in self.relationships if rel.referenced_table == child]

    def generate_join_clause(self, rel: Relationship) -> str:
        join_conditions = []
        for i in range(len(rel.parent_columns)):
            parent_col = rel.parent_columns[i].column_name
            ref_col = rel.referenced_columns[i].column_name
            join_conditions.append(
                f"{rel.parent_table.full_table_name()}.{parent_col} = "
                f"{rel.referenced_table.full_table_name()}.{ref_col}"
            )

        return " AND ".join(join_conditions)

    def get_deletion_order(self) -> list[DbTable]:
        """
        Return tables in deletion order based on hierarchy levels.
        Tables with higher levels (child tables) come before tables with lower levels (parents).
        """
        all_tables = []
        table_key_to_table = {}

        # Add root table
        root_key = f"{self.root_table.schema_name}.{self.root_table.table_name}"
        all_tables.append(root_key)
        table_key_to_table[root_key] = self.root_table

        # Add all tables from relationships
        for rel in self.relationships:
            parent_key = f"{rel.parent_table.schema_name}.{rel.parent_table.table_name}"
            ref_key = f"{rel.referenced_table.schema_name}.{rel.referenced_table.table_name}"

            if parent_key not in table_key_to_table:
                all_tables.append(parent_key)
                table_key_to_table[parent_key] = rel.parent_table

            if ref_key not in table_key_to_table:
                all_tables.append(ref_key)
                table_key_to_table[ref_key] = rel.referenced_table

        # Sort tables by hierarchy level (descending)
        # Tables with higher levels (children) should be deleted before lower levels (parents)
        tables_with_levels = []
        for table_key in all_tables:
            level = self.table_levels.get(table_key, 0)
            tables_with_levels.append((table_key, level))

        # Sort by level in descending order (higher levels first)
        tables_with_levels.sort(key=lambda x: x[1], reverse=True)

        # Return tables in proper order
        return [table_key_to_table[table_key] for table_key, _ in tables_with_levels]

    def rebuild_table_levels(self) -> None:
        """Rebuild table levels using all relationships"""
        # Reset levels
        self.table_levels.clear()

        # Root table is level 0
        root_key = f"{self.root_table.schema_name}.{self.root_table.table_name}"
        self.table_levels[root_key] = 0

        # Keep assigning levels until no changes
        changed = True
        max_iterations = 10  # Prevent infinite loops
        iteration = 0

        while changed and iteration < max_iterations:
            changed = False
            iteration += 1

            for rel in self.relationships:
                parent_key = f"{rel.parent_table.schema_name}.{rel.parent_table.table_name}"
                referenced_key = (
                    f"{rel.referenced_table.schema_name}.{rel.referenced_table.table_name}"
                )

                # If referenced table has a level, parent should be at least level + 1
                if referenced_key in self.table_levels:
                    min_parent_level = self.table_levels[referenced_key] + 1

                    if (
                        parent_key not in self.table_levels
                        or self.table_levels[parent_key] < min_parent_level
                    ):
                        self.table_levels[parent_key] = min_parent_level
                        changed = True
