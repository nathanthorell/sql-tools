import time
from datetime import datetime
from typing import Any, Dict, List, Set

from rich.markup import escape
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm

from data_cleanup.data_cleanup_config import CleanupConfig
from data_cleanup.data_cleanup_types import (
    CascadeStats,
    CleanupOperation,
    ProcessingQueue,
    RelationshipMap,
    format_id_list_for_sql,
)
from utils import DbColumn, DbTable, Hierarchy, MetadataService, Relationship
from utils.rich_utils import console, create_table


def fetch_ids(config: CleanupConfig) -> List[Any]:
    """Execute a query to get the target IDs for deletion"""
    with config.connection.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(config.query_of_cleanup_pk_values)
        return [row[0] for row in cursor.fetchall()]


def calculate_operations(
    service: MetadataService,
    hierarchy: Hierarchy,
    root_table: DbTable,
    root_ids: List[Any],
    config: CleanupConfig,
) -> Dict[str, CleanupOperation]:
    """Calculate all operations needed for cleanup using recursive cascade"""
    if not root_table.primary_key or not root_table.primary_key.columns:
        raise ValueError(f"Root table {root_table.full_table_name()} must have a primary key")

    # Initialize cascade processing structures
    queue = ProcessingQueue()
    relationship_map = RelationshipMap.from_hierarchy(hierarchy)
    stats = CascadeStats()

    start_time = time.time()

    queue.add_task(root_table, set(root_ids), level=0)

    # Process cascade using breadth-first approach
    iteration = 0
    while queue.has_pending_tasks():
        iteration += 1
        task = queue.get_next_task()

        if not task or not task.ids:
            continue

        # Mark as currently processing
        queue.mark_processing(task.table_key)

        # Find all child relationships for this table
        child_relationships = relationship_map.get_child_relationships(task.table_key)

        if len(child_relationships) > 0:  # Only show if there are relationships to process
            console.print(
                f"Processing {task.table_key}: {len(task.ids):,} records,"
                f" {len(child_relationships)} relationships"
            )

        if not child_relationships:
            console.print(f"  No child relationships found for {task.table_key}")
            queue.mark_completed(task.table_key)
            continue

        # Process each child relationship
        for relationship in child_relationships:
            fk_table = relationship.parent_table
            referenced_table = relationship.referenced_table

            referenced_values = get_referenced_column_values(
                service, referenced_table, task.ids, relationship.referenced_columns, config
            )

            if not referenced_values:
                continue

            # Find child primary key values
            child_pk_values = find_child_primary_keys(
                service, fk_table, relationship.parent_columns, referenced_values, config
            )

            if child_pk_values:
                queue.add_task(fk_table, child_pk_values, level=task.level + 1)

                console.print(f"  → {fk_table.table_name}: {len(child_pk_values):,} records")
                stats.relationships_processed += 1

        # Mark current task as completed
        queue.mark_completed(task.table_key)
        stats.tables_processed += 1

        # Safety check to prevent infinite loops
        if iteration > 1000:
            console.print(
                "[yellow]Warning: Reached maximum iterations (1000). Stopping cascade.[/]"
            )
            break

    # Update final stats
    stats.processing_time_seconds = time.time() - start_time
    stats.update_from_queue(queue)

    # Display final statistics
    console.print("\n[bold]Cascade Processing Complete[/]")
    console.print(f"  Tables processed: {stats.tables_processed}")
    console.print(f"  Relationships processed: {stats.relationships_processed}")
    console.print(f"  Total records found: {stats.total_records_found:,}")
    console.print(f"  Maximum cascade level: {stats.max_level_reached}")
    console.print(f"  Processing time: {stats.processing_time_seconds:.2f} seconds")

    # Convert queue to operations and return
    operations = queue.get_all_operations()
    console.print(f"  Final operations for {len(operations)} tables")

    return operations


def get_referenced_column_values(
    service: MetadataService,
    parent_table: DbTable,
    parent_ids: Set[Any],
    referenced_columns: List[DbColumn],
    config: CleanupConfig,
) -> List[tuple[Any, ...]]:
    """Get the values of referenced columns for the given parent PKs"""
    if not parent_table.primary_key or not parent_table.primary_key.columns:
        console.print(
            f"[yellow]No primary key found for {escape(parent_table.full_table_name())}[/]"
        )
        return []

    pk_columns = parent_table.primary_key.columns
    ref_columns_sql = ", ".join([f"[{col.column_name}]" for col in referenced_columns])

    # Check if we should use batching
    if config.batch_threshold > 0 and len(parent_ids) >= config.batch_threshold:
        console.print(f"      Using batched processing for {len(parent_ids)} IDs")
        return _process_referenced_values_in_batches(
            service, parent_table, parent_ids, ref_columns_sql, pk_columns, config.batch_size
        )
    else:
        # Single query for smaller datasets
        where_clause = _build_pk_where_clause(pk_columns, parent_ids)
        query = f"""
        SELECT DISTINCT {ref_columns_sql}
        FROM [{parent_table.schema_name}].[{parent_table.table_name}]
        WHERE {where_clause}
        """
        results = _execute_referenced_values_query(service, query)
        return results


def _build_pk_where_clause(pk_columns: List[DbColumn], pk_values: Set[Any]) -> str:
    """Build WHERE clause for primary key matching"""
    if len(pk_columns) == 1:
        # Single column PK - use IN clause
        pk_column = pk_columns[0].column_name
        id_list = format_id_list_for_sql(pk_values)
        return f"[{pk_column}] IN ({id_list})"
    else:
        # Multi-column PK - build OR conditions
        where_clauses = []
        for pk_tuple in pk_values:
            # Handle case where pk_tuple might be a single value
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


def find_child_primary_keys(
    service: MetadataService,
    child_table: DbTable,
    foreign_key_columns: List[DbColumn],
    referenced_values: List[tuple[Any, ...]],
    config: CleanupConfig,
) -> Set[Any]:
    """Find primary key values of child records that match the referenced values via foreign key"""
    if not child_table.primary_key or not child_table.primary_key.columns:
        console.print(
            f"[yellow]No primary key found for {escape(child_table.full_table_name())}[/]"
        )
        return set()

    # Check if we should use batching
    if config.batch_threshold > 0 and len(referenced_values) >= config.batch_threshold:
        console.print(f"      Using batched child processing for {len(referenced_values)} values")
        return _process_child_records_in_batches(
            service, child_table, foreign_key_columns, referenced_values, config.batch_size
        )
    else:
        # Single query for smaller datasets
        return _find_child_primary_keys_single_query(
            service, child_table, foreign_key_columns, referenced_values
        )


def _find_child_primary_keys_single_query(
    service: MetadataService,
    child_table: DbTable,
    foreign_key_columns: List[DbColumn],
    referenced_values: List[tuple[Any, ...]],
) -> Set[Any]:
    """Find child primary key values using a single query"""
    if not child_table.primary_key or not child_table.primary_key.columns:
        return set()

    pk_columns = child_table.primary_key.columns

    # Build SELECT clause for primary key columns
    if len(pk_columns) == 1:
        # Single column PK
        pk_select = f"[{pk_columns[0].column_name}]"
    else:
        # Multi-column PK
        pk_select = ", ".join([f"[{col.column_name}]" for col in pk_columns])

    where_clause = build_fk_where_clause(foreign_key_columns, referenced_values)

    if where_clause == "1=0":  # No valid conditions
        console.print("No valid WHERE conditions generated")
        return set()

    query = f"""
    SELECT DISTINCT {pk_select}
    FROM [{child_table.schema_name}].[{child_table.table_name}]
    WHERE {where_clause}
    """

    child_pk_values = set()
    with service.connection.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()

            if len(pk_columns) == 1:
                # Single column PK - store as individual values
                child_pk_values = {row[0] for row in rows}
            else:
                # Multi-column PK - store as tuples
                child_pk_values = {tuple(row) for row in rows}

        except Exception as e:
            console.print(f"[yellow]Error finding child records: {e}[/]")
            console.print(f"[dim]Query: {query}[/]")

    return child_pk_values


def build_fk_where_clause(
    foreign_key_columns: List[DbColumn], referenced_values: List[tuple[Any, ...]]
) -> str:
    """Build WHERE clause for foreign key matching"""
    if len(foreign_key_columns) == 1:
        # Single column FK - use IN clause
        fk_column = foreign_key_columns[0].column_name
        single_values = {val[0] for val in referenced_values}
        id_list = format_id_list_for_sql(single_values)
        return f"[{fk_column}] IN ({id_list})"

    # Multi-column FK - build OR conditions
    where_clauses = []
    for ref_tuple in referenced_values:
        conditions = []
        for i, fk_col in enumerate(foreign_key_columns):
            if i >= len(ref_tuple):
                break

            val = ref_tuple[i]
            col_name = fk_col.column_name

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


def _process_referenced_values_in_batches(
    service: MetadataService,
    parent_table: DbTable,
    parent_ids: Set[Any],
    ref_columns_sql: str,
    pk_columns: List[DbColumn],
    batch_size: int,
) -> List[tuple[Any, ...]]:
    """Process parent IDs in batches to avoid large WHERE clauses"""
    all_results = []
    id_list = list(parent_ids)
    total_batches = (len(id_list) + batch_size - 1) // batch_size

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Processing referenced value batches...", total=total_batches)

        for i in range(0, len(id_list), batch_size):
            batch = id_list[i : i + batch_size]
            where_clause = _build_pk_where_clause(pk_columns, set(batch))

            query = f"""
            SELECT DISTINCT {ref_columns_sql}
            FROM [{parent_table.schema_name}].[{parent_table.table_name}]
            WHERE {where_clause}
            """

            batch_results = _execute_referenced_values_query(service, query)
            all_results.extend(batch_results)

            batch_num = i // batch_size + 1
            progress.update(
                task,
                description=f"Batch {batch_num}/{total_batches}: Found {len(batch_results)} values",
                advance=1,
            )

    # Remove duplicates while preserving order
    seen = set()
    unique_results = []
    for item in all_results:
        if item not in seen:
            seen.add(item)
            unique_results.append(item)

    return unique_results


def _process_child_records_in_batches(
    service: MetadataService,
    child_table: DbTable,
    foreign_key_columns: List[DbColumn],
    referenced_values: List[tuple[Any, ...]],
    batch_size: int,
) -> Set[Any]:
    """Process referenced values in batches to find child primary keys"""
    all_child_pk_values = set()
    total_batches = (len(referenced_values) + batch_size - 1) // batch_size

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Processing child record batches...", total=total_batches)

        for i in range(0, len(referenced_values), batch_size):
            batch = referenced_values[i : i + batch_size]
            batch_child_values = _find_child_primary_keys_single_query(
                service, child_table, foreign_key_columns, batch
            )
            all_child_pk_values.update(batch_child_values)

            batch_num = i // batch_size + 1
            progress.update(
                task,
                description=(
                    f"Batch {batch_num}/{total_batches}: Found {len(batch_child_values)} child PKs"
                ),
                advance=1,
            )

    return all_child_pk_values


def _execute_referenced_values_query(service: MetadataService, query: str) -> List[tuple[Any, ...]]:
    """Execute a query to get referenced column values"""
    result_values = []
    with service.connection.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            result_values = [tuple(row) for row in cursor.fetchall()]
        except Exception as e:
            console.print(f"[yellow]Error getting referenced values: {e}[/]")
    return result_values


def preload_all_foreign_keys(hierarchy: Hierarchy, service: MetadataService) -> None:
    """Preload foreign keys for all tables and discover additional relationships"""
    # Get all tables in the hierarchy
    all_tables = {hierarchy.root_table}
    for rel in hierarchy.relationships:
        all_tables.add(rel.parent_table)
        all_tables.add(rel.referenced_table)

    # Load foreign keys for all tables
    tables_loaded = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Loading foreign keys...", total=len(all_tables))

        tables_needing_fks = [table for table in all_tables if not table.foreign_keys]
        for table in tables_needing_fks:
            if not table.foreign_keys:
                service.get_foreign_keys(table)
                tables_loaded += 1

                progress.update(task, advance=1)
                # progress.update(
                #     task, description=f"Loaded FKs for {escape(table.full_table_name())}"
                # )
            progress.advance(task)

        progress.update(task, description=f"✓ Loaded foreign keys for {tables_loaded} tables")

    # Discover additional relationships not captured in initial hierarchy
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold magenta]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Discovering additional relationships...", total=len(all_tables))

        additional_relationships = []
        tables_in_hierarchy = {f"{t.schema_name}.{t.table_name}" for t in all_tables}

        for table in all_tables:
            for fk_name, fk in table.foreign_keys.items():
                referenced_table_key = f"{fk.referenced_schema}.{fk.referenced_table}"

                # Check if this FK references a table in our hierarchy
                if referenced_table_key in tables_in_hierarchy:
                    referenced_table_obj = next(
                        (
                            t
                            for t in all_tables
                            if t.schema_name == fk.referenced_schema
                            and t.table_name == fk.referenced_table
                        ),
                        None,
                    )

                    if referenced_table_obj and not _relationship_exists(
                        hierarchy, table, referenced_table_obj, fk_name
                    ):
                        new_rel = Relationship(
                            name=fk_name,
                            parent_table=table,
                            parent_columns=fk.parent_columns,
                            referenced_table=referenced_table_obj,
                            referenced_columns=fk.referenced_columns,
                        )
                        additional_relationships.append(new_rel)
                        console.print(f"  Found additional FK: {fk_name}")
            progress.advance(task)
        progress.update(task, description="✓ Relationship discovery complete")
        # progress.update(
        #     task, description="Relationship discovery complete", completed=len(all_tables)
        # )

    if additional_relationships:
        hierarchy.relationships.extend(additional_relationships)
        console.print(f"[green]✓ Found {len(additional_relationships)} additional relationships[/]")

    hierarchy.rebuild_table_levels()

    total_fks = sum(len(table.foreign_keys) for table in all_tables)
    console.print(f"[bold]Total foreign keys loaded: {total_fks}[/]")


def _relationship_exists(
    hierarchy: Hierarchy, parent_table: DbTable, referenced_table: DbTable, fk_name: str
) -> bool:
    """Check if a relationship already exists in the hierarchy"""
    return any(
        (
            rel.parent_table.schema_name == parent_table.schema_name
            and rel.parent_table.table_name == parent_table.table_name
            and rel.referenced_table.schema_name == referenced_table.schema_name
            and rel.referenced_table.table_name == referenced_table.table_name
            and rel.name == fk_name
        )
        for rel in hierarchy.relationships
    )


def generate_cleanup_script(
    operations: Dict[str, CleanupOperation],
    deletion_order: List[DbTable],
    config: CleanupConfig,
) -> str:
    """Generate a SQL script for cleanup operations"""
    script_lines = []
    script_lines.append("-- Data Cleanup Script")
    script_lines.append(f"-- Connection: {config.connection.server}")
    script_lines.append(f"-- Database: {config.database}")
    script_lines.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if config.batch_threshold > 0:
        script_lines.append("-- Batch Processing: Enabled")
        script_lines.append(f"-- Batch Size: {config.batch_size} records")
        script_lines.append(f"-- Batch Threshold: {config.batch_threshold} records")
    else:
        script_lines.append("-- Batch Processing: Disabled")

    script_lines.append("\nBEGIN TRANSACTION;\n")

    total_records = 0
    batched_tables = 0

    # Process tables in deletion order
    for table in deletion_order:
        table_key = f"{table.schema_name}.{table.table_name}"

        if table_key in operations and operations[table_key].ids:
            operation = operations[table_key]
            record_count = len(operation.ids)
            total_records += record_count

            script_lines.append(f"-- Table: {table_key}")
            script_lines.append(f"-- Records to delete: {record_count}")

            # Determine if batching should be used
            use_batching = operation.should_use_batching(config.batch_threshold)

            if use_batching:
                batched_tables += 1
                batch_count = (record_count + config.batch_size - 1) // config.batch_size
                script_lines.append(
                    f"-- Using {batch_count} batches of max {config.batch_size} records each"
                )

                delete_statements = operation.generate_batched_delete_sql(config.batch_size)
                for i, stmt in enumerate(delete_statements):
                    start_idx = i * config.batch_size + 1
                    end_idx = min((i + 1) * config.batch_size, record_count)
                    script_lines.append(
                        f"-- Batch {i + 1}/{batch_count}: records {start_idx}-{end_idx}"
                    )
                    script_lines.append(stmt + ";")
            else:
                delete_sql = operation.generate_delete_sql()
                if delete_sql:
                    script_lines.append(delete_sql + ";")

            script_lines.append("")

    script_lines.append(
        f"-- Script Summary: {total_records} records across {len(operations)} tables"
    )
    if batched_tables > 0:
        script_lines.append(f"-- {batched_tables} tables processed with batching")

    script_lines.append("\n-- COMMIT TRANSACTION;")
    script_lines.append("-- ROLLBACK TRANSACTION;")

    return "\n".join(script_lines)


def display_hierarchy_summary(
    hierarchy: Hierarchy, operations: Dict[str, CleanupOperation], deletion_order: List[DbTable]
) -> None:
    """Display summary information about the cleanup operations"""
    console.print(f"\n[bold]Found {len(hierarchy.relationships)} foreign key relationships[/]")

    # Display tables with records
    active_tables = [(key, len(op.ids)) for key, op in operations.items() if op.ids]

    if active_tables:
        console.print("\n[bold]Tables with records to delete:[/]")
        records_table = create_table(columns=["Table", "Records"])

        for key, count in sorted(active_tables, key=lambda x: x[1], reverse=True):
            records_table.add_row(key, f"[bold red]{count}[/]")

        console.print(records_table)
    else:
        console.print("\n[yellow]No tables have records to delete[/]")

    console.print("\n[bold]Deletion Order (tables with records only):[/]")
    order_table = create_table(columns=["Order", "Table", "Records"])

    tables_with_records = []
    for i, table in enumerate(deletion_order):
        table_key = f"{table.schema_name}.{table.table_name}"
        if table_key in operations and operations[table_key].ids:
            count = len(operations[table_key].ids)
            tables_with_records.append((i + 1, table_key, count))

    for original_order, table_key, count in tables_with_records:
        order_table.add_row(str(original_order), table_key, f"[bold red]{count}[/]")

    console.print(order_table)

    # Show summary statistics
    active_ops = [op for op in operations.values() if op.ids]
    total_tables = len(active_ops)
    total_records = sum(len(op.ids) for op in active_ops)

    console.print(f"\n[bold]Total tables affected: {total_tables}[/]")
    console.print(f"[bold]Total records to delete: {total_records}[/]")


def execute_cleanup(
    config: CleanupConfig, operations: Dict[str, CleanupOperation], deletion_order: List[DbTable]
) -> None:
    """Execute the cleanup operations against the database"""
    if not Confirm.ask("\nAre you sure you want to execute the cleanup operations?"):
        console.print("[yellow]Execution cancelled[/]")
        return

    console.print("[bold]Executing cleanup operations...[/]")

    with config.connection.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")

            for table in deletion_order:
                table_key = f"{table.schema_name}.{table.table_name}"

                if table_key in operations and operations[table_key].ids:
                    operation = operations[table_key]
                    delete_sql = operation.generate_delete_sql()

                    if delete_sql:
                        console.print(f"Deleting from {table_key}...")
                        cursor.execute(delete_sql)
                        console.print(f"[green]Deleted {cursor.rowcount} rows[/]")

            if Confirm.ask("\nCommit the transaction?"):
                cursor.execute("COMMIT TRANSACTION")
                console.print("[bold green]Transaction committed[/]")
            else:
                cursor.execute("ROLLBACK TRANSACTION")
                console.print("[bold yellow]Transaction rolled back[/]")

        except Exception as e:
            cursor.execute("ROLLBACK TRANSACTION")
            console.print(f"[bold red]Error during execution. Transaction rolled back: {e}[/]")
