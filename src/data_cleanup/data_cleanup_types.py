from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Set

from utils import DbColumn, DbTable, Hierarchy, Relationship


class ProcessingStatus(Enum):
    """Status of a table in the cascade processing"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"


@dataclass
class CascadeTask:
    """Represents a table that needs to be processed for cascading deletes"""

    table: DbTable
    table_key: str
    ids: Set[Any]
    status: ProcessingStatus = ProcessingStatus.PENDING
    level: int = 0  # Distance from root table

    def __post_init__(self) -> None:
        if not self.table_key:
            self.table_key = f"{self.table.schema_name}.{self.table.table_name}"


@dataclass
class ProcessingQueue:
    """Queue for managing cascade processing tasks"""

    tasks: List[CascadeTask] = field(default_factory=list)
    completed_tables: Set[str] = field(default_factory=set)

    def add_task(self, table: DbTable, ids: Set[Any], level: int = 0) -> CascadeTask:
        """Add a new task to the queue"""
        table_key = f"{table.schema_name}.{table.table_name}"

        # Check if we already have a task for this table
        existing_task = self.get_task(table_key)
        if existing_task:
            # Merge IDs and update level if deeper
            old_count = len(existing_task.ids)
            existing_task.ids.update(ids)
            new_count = len(existing_task.ids)
            existing_task.level = max(existing_task.level, level)

            # Reset status if it was completed but we're adding more IDs
            if existing_task.status == ProcessingStatus.COMPLETED and new_count > old_count:
                existing_task.status = ProcessingStatus.PENDING

            return existing_task
        else:
            # Create new task
            task = CascadeTask(table=table, table_key=table_key, ids=ids, level=level)
            self.tasks.append(task)
            return task

    def get_next_task(self) -> CascadeTask | None:
        """Get the next pending task to process"""
        pending_tasks = [t for t in self.tasks if t.status == ProcessingStatus.PENDING]
        if not pending_tasks:
            return None

        # Process tasks by level (breadth-first) to avoid deep recursion issues
        pending_tasks.sort(key=lambda t: t.level)
        return pending_tasks[0]

    def get_task(self, table_key: str) -> CascadeTask | None:
        """Get a task by table key"""
        return next((t for t in self.tasks if t.table_key == table_key), None)

    def mark_completed(self, table_key: str) -> None:
        """Mark a task as completed"""
        task = self.get_task(table_key)
        if task:
            task.status = ProcessingStatus.COMPLETED
            self.completed_tables.add(table_key)

    def mark_processing(self, table_key: str) -> None:
        """Mark a task as currently being processed"""
        task = self.get_task(table_key)
        if task:
            task.status = ProcessingStatus.PROCESSING

    def has_pending_tasks(self) -> bool:
        """Check if there are any pending tasks"""
        return any(t.status == ProcessingStatus.PENDING for t in self.tasks)

    def get_all_operations(self) -> Dict[str, "CleanupOperation"]:
        """Convert all tasks to cleanup operations"""

        operations = {}
        for task in self.tasks:
            if task.ids:  # Only include tasks with actual IDs
                operations[task.table_key] = CleanupOperation(table=task.table, ids=task.ids)

        return operations

    @property
    def summary(self) -> str:
        """Get a summary of the queue status"""
        pending = sum(1 for t in self.tasks if t.status == ProcessingStatus.PENDING)
        processing = sum(1 for t in self.tasks if t.status == ProcessingStatus.PROCESSING)
        completed = len(self.completed_tables)
        total_records = sum(len(t.ids) for t in self.tasks)

        return (
            f"Tasks: {pending} pending,"
            f"{processing} processing, {completed} completed. "
            f"Total records: {total_records:,}"
        )


@dataclass
class RelationshipMap:
    """Organizes relationships for efficient cascade processing"""

    relationships_by_parent: Dict[str, List[Relationship]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize the relationship map if not provided"""
        pass

    @classmethod
    def from_hierarchy(cls, hierarchy: "Hierarchy") -> "RelationshipMap":
        """Create a relationship map from a hierarchy"""
        relationships_by_parent: Dict[str, List[Relationship]] = {}

        for rel in hierarchy.relationships:
            parent_key = f"{rel.referenced_table.schema_name}.{rel.referenced_table.table_name}"
            if parent_key not in relationships_by_parent:
                relationships_by_parent[parent_key] = []
            relationships_by_parent[parent_key].append(rel)

        return cls(relationships_by_parent=relationships_by_parent)

    def get_child_relationships(self, parent_table_key: str) -> List[Relationship]:
        """Get all relationships where the given table is the parent (referenced table)"""
        return self.relationships_by_parent.get(parent_table_key, [])

    def has_children(self, parent_table_key: str) -> bool:
        """Check if a table has any child relationships"""
        return parent_table_key in self.relationships_by_parent


@dataclass
class CascadeStats:
    """Statistics about the cascade processing"""

    tables_processed: int = 0
    relationships_processed: int = 0
    total_records_found: int = 0
    max_level_reached: int = 0
    processing_time_seconds: float = 0.0

    def update_from_queue(self, queue: ProcessingQueue) -> None:
        """Update stats from a processing queue"""
        self.tables_processed = len(
            [t for t in queue.tasks if t.status == ProcessingStatus.COMPLETED]
        )
        self.total_records_found = sum(len(t.ids) for t in queue.tasks)
        if queue.tasks:
            self.max_level_reached = max(t.level for t in queue.tasks)


def format_id_list_for_sql(ids: Set[Any]) -> str:
    """Format a set of IDs for use in SQL IN clause"""
    formatted_ids = []
    for id_val in ids:
        if isinstance(id_val, str):
            formatted_ids.append(f"'{id_val}'")
        elif id_val is None:
            formatted_ids.append("NULL")
        else:
            formatted_ids.append(str(id_val))

    return ", ".join(formatted_ids)


class CleanupOperation:
    """Represents a cleanup operation for a table - always deletes by primary key"""

    def __init__(self, table: DbTable, ids: Set[Any]) -> None:
        self.table = table
        # For single-column PKs, ids will be a set of single values
        # For multi-column PKs, ids will be a set of tuples
        self.ids = ids

    def generate_delete_sql(self) -> str:
        """Generate DELETE SQL statement for this operation"""
        if not self.ids or not self.table.primary_key or not self.table.primary_key.columns:
            return ""

        pk_columns = self.table.primary_key.columns

        if len(pk_columns) == 1:
            # Single column primary key
            pk_column = pk_columns[0].column_name
            id_list = format_id_list_for_sql(self.ids)
            return f"""
            DELETE FROM [{self.table.schema_name}].[{self.table.table_name}]
            WHERE [{pk_column}] IN ({id_list})
            """
        else:
            # Multi-column primary key
            where_clause = self._build_multi_column_pk_where_clause(pk_columns, self.ids)
            return f"""
            DELETE FROM [{self.table.schema_name}].[{self.table.table_name}]
            WHERE {where_clause}
            """

    def generate_batched_delete_sql(self, batch_size: int) -> List[str]:
        """Generate multiple DELETE statements for batch processing"""
        if (
            not self.ids
            or not self.table.primary_key
            or not self.table.primary_key.columns
            or batch_size <= 0
        ):
            return []

        pk_columns = self.table.primary_key.columns
        id_list = list(self.ids)
        delete_statements = []

        for i in range(0, len(id_list), batch_size):
            batch = id_list[i : i + batch_size]

            if len(pk_columns) == 1:
                # Single column PK
                pk_column = pk_columns[0].column_name
                batch_id_list = format_id_list_for_sql(set(batch))
                delete_sql = f"""
                DELETE FROM [{self.table.schema_name}].[{self.table.table_name}]
                WHERE [{pk_column}] IN ({batch_id_list})
                """
            else:
                # Multi-column PK
                where_clause = self._build_multi_column_pk_where_clause(pk_columns, set(batch))
                delete_sql = f"""
                DELETE FROM [{self.table.schema_name}].[{self.table.table_name}]
                WHERE {where_clause}
                """

            delete_statements.append(delete_sql)

        return delete_statements

    def _build_multi_column_pk_where_clause(
        self, pk_columns: List[DbColumn], pk_values: Set[Any]
    ) -> str:
        """Build WHERE clause for multi-column primary key deletion"""
        where_clauses = []

        for pk_tuple in pk_values:
            # Handle case where pk_tuple might be a single value
            # shouldn't happen for multi-col PK but safety check
            if not isinstance(pk_tuple, (tuple, list)):
                pk_tuple = (pk_tuple,)

            conditions = []
            for i, pk_col in enumerate(pk_columns):
                if i >= len(pk_tuple):
                    break

                val = pk_tuple[i]
                col_name = pk_col.column_name

                if val is None:
                    conditions.append(f"[{col_name}] IS NULL")
                elif isinstance(val, str):
                    val_escaped = val.replace("'", "''")
                    conditions.append(f"[{col_name}] = '{val_escaped}'")
                else:
                    conditions.append(f"[{col_name}] = {val}")

            if conditions:
                where_clauses.append(f"({' AND '.join(conditions)})")

        return " OR ".join(where_clauses) if where_clauses else "1=0"

    def should_use_batching(self, threshold: int) -> bool:
        """Determine if this operation should use batching based on record count"""
        return threshold > 0 and len(self.ids) >= threshold
