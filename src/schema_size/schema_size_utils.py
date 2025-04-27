from typing import Dict, List, Optional, Tuple

from rich.table import Table

from schema_size.schema_size_types import (
    DatabaseSize,
    SchemaSize,
    ServerDatabases,
    ServerResults,
    format_size,
)
from utils import (
    Connection,
    modify_connection_for_database,
)
from utils.rich_utils import align_columns, console, create_table


def fetch_schema_sizes(conn: Connection) -> List[SchemaSize]:
    query = """
    SELECT
        s.Name AS SchemaName,
        SUM(p.rows) AS TotalRows,
        SUM(a.total_pages) * 8 * 1024 AS TotalSizeBytes,
        SUM(a.used_pages) * 8 * 1024 AS UsedSizeBytes,
        (SUM(a.total_pages) - SUM(a.used_pages)) * 8 * 1024 AS UnusedSizeBytes
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    GROUP BY s.Name
    ORDER BY TotalSizeBytes DESC;
    """

    with conn.get_connection() as db_conn:
        cursor = db_conn.cursor()
        try:
            cursor.execute(query)
            return [
                SchemaSize(
                    schema_name=row[0],
                    total_rows=row[1],
                    total_bytes=row[2],
                    used_bytes=row[3],
                    unused_bytes=row[4],
                )
                for row in cursor.fetchall()
            ]
        except Exception as e:
            print(f"Error fetching schema sizes: {e}")
            return []
        finally:
            cursor.close()


def process_database(
    server_name: str, db_name: str, connection: Connection, logging_level: str
) -> Optional[DatabaseSize]:
    try:
        db_connection = modify_connection_for_database(connection, db_name)
        schema_sizes = fetch_schema_sizes(db_connection)

        total_rows = sum(schema.total_rows for schema in schema_sizes)
        total_bytes = sum(schema.total_bytes for schema in schema_sizes)
        used_bytes = sum(schema.used_bytes for schema in schema_sizes)
        unused_bytes = sum(schema.unused_bytes for schema in schema_sizes)

        db_size = DatabaseSize(total_bytes, used_bytes, unused_bytes, total_rows)

        if logging_level == "verbose":
            db_table = create_table(
                columns=["Schema", "Row Count", "Total Size", "Used Size", "Unused Size"]
            )

            align_columns(
                db_table,
                {
                    "Row Count": "right",
                    "Total Size": "right",
                    "Used Size": "right",
                    "Unused Size": "right",
                },
            )

            for schema in schema_sizes:
                db_table.add_row(
                    schema.schema_name,
                    f"{schema.total_rows:,}",
                    schema.total_formatted,
                    schema.used_formatted,
                    schema.unused_formatted,
                )

            console.print(f"\nSchema Sizes for [{server_name}].[{db_name}]:\n")
            console.print(db_table)
            console.print(
                f"Database Total: {format_size(total_bytes)} "
                f"(Used: {format_size(used_bytes)}, "
                f"Unused: {format_size(unused_bytes)}, "
                f"Rows: {total_rows:,})\n"
            )
            console.rule()

        return db_size

    except Exception as e:
        print(f"Error processing database '{db_name}' on server '{server_name}': {e}")
        return DatabaseSize(0.0, 0.0, 0.0)


def process_server(
    server_config: ServerDatabases, connection: Connection, logging_level: str
) -> ServerResults:
    """Process all databases on a server to create a ServerResults object."""
    server_name = server_config.server_name
    database_names = server_config.databases

    print(f"Processing server: {server_name} ({len(database_names)} databases)")

    # Process each database
    databases = {}
    for db_name in database_names:
        db_size = process_database(server_name, db_name, connection, logging_level)
        if db_size:
            databases[db_name] = db_size

    return ServerResults(server_name, databases)


def create_server_summary_table(
    database_results: Dict[str, Dict[str, DatabaseSize]],
) -> Tuple[Table, Dict[str, DatabaseSize]]:
    """
    Create a summary table of all database sizes.

    Args:
        database_results: Nested dictionary mapping server names to database names to size metrics

    Returns:
        Tuple of (summary_table, server_totals)
    """
    server_table = create_table(
        columns=["Server", "Database", "Row Count", "Total Size", "Used Space", "Unused Space"]
    )

    align_columns(
        server_table,
        {
            "Row Count": "right",
            "Total Size": "right",
            "Used Space": "right",
            "Unused Space": "right",
        },
    )

    server_totals = {}
    for server_name, db_results in database_results.items():
        # Calculate server totals using aggregate of all database sizes
        total_bytes = sum(db.total_bytes for db in db_results.values())
        used_bytes = sum(db.used_bytes for db in db_results.values())
        unused_bytes = sum(db.unused_bytes for db in db_results.values())
        total_rows = sum(db.total_rows for db in db_results.values())

        # Create a DatabaseSize object for the server total
        server_totals[server_name] = DatabaseSize(
            total_bytes=total_bytes,
            used_bytes=used_bytes,
            unused_bytes=unused_bytes,
            total_rows=total_rows,
        )

        # Add database rows
        for db_name, db_size in db_results.items():
            server_table.add_row(
                server_name,
                db_name,
                f"{db_size.total_rows:,}",
                db_size.total_formatted,
                db_size.used_formatted,
                db_size.unused_formatted,
            )

    return server_table, server_totals
