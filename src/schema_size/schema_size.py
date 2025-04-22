from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pyodbc
from dotenv import load_dotenv
from prettytable import PrettyTable

from utils.utils import Connection, get_config, get_connection, modify_connection_for_database


@dataclass
class ServerDatabases:
    server_name: str
    databases: List[str]

    def __str__(self) -> str:
        return f"{self.server_name}: {len(self.databases)} databases"


@dataclass
class SchemaSize:
    schema_name: str
    total_mb: float
    data_mb: float
    index_mb: float

    def __post_init__(self) -> None:
        """Round the values to 2 decimal places."""
        self.total_mb = round(self.total_mb, 2)
        self.data_mb = round(self.data_mb, 2)
        self.index_mb = round(self.index_mb, 2)

    def add_to_table(self, table: PrettyTable) -> None:
        """Add this schema size data as a row to the given PrettyTable."""
        table.add_row(
            [
                self.schema_name,
                f"{self.total_mb:.2f}",
                f"{self.data_mb:.2f}",
                f"{self.index_mb:.2f}",
            ]
        )


@dataclass
class DatabaseSize:
    total_mb: float
    data_mb: float
    index_mb: float

    def __post_init__(self) -> None:
        """Round the values to 2 decimal places."""
        self.total_mb = round(self.total_mb, 2)
        self.data_mb = round(self.data_mb, 2)
        self.index_mb = round(self.index_mb, 2)

    def add_to_table(self, table: PrettyTable, server_name: str, db_name: str) -> None:
        """Add this database size data as a row to the given PrettyTable."""
        table.add_row(
            [
                server_name,
                db_name,
                f"{self.total_mb:.2f}",
                f"{self.data_mb:.2f}",
                f"{self.index_mb:.2f}",
            ]
        )


@dataclass
class ServerResults:
    server_name: str
    databases: Dict[str, DatabaseSize]

    @property
    def total_size(self) -> DatabaseSize:
        """Calculate the total size across all databases."""
        total_mb = sum(db.total_mb for db in self.databases.values())
        data_mb = sum(db.data_mb for db in self.databases.values())
        index_mb = sum(db.index_mb for db in self.databases.values())
        return DatabaseSize(total_mb, data_mb, index_mb)


def fetch_schema_sizes(conn: pyodbc.Connection) -> List[SchemaSize]:
    query = """
    SELECT
        s.name AS SchemaName,
        SUM(a.total_pages * 8) / 1024 AS TotalSizeMB,
        SUM(CASE WHEN i.type <= 1 THEN a.used_pages * 8 ELSE 0 END) / 1024 AS DataSizeMB,
        SUM(CASE WHEN i.type > 1 THEN a.used_pages * 8 ELSE 0 END) / 1024 AS IndexSizeMB
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    GROUP BY s.name
    ORDER BY TotalSizeMB DESC;
    """

    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return [
            SchemaSize(
                schema_name=row.SchemaName,
                total_mb=row.TotalSizeMB,
                data_mb=row.DataSizeMB,
                index_mb=row.IndexSizeMB,
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
        conn = db_connection.connect()

        schema_sizes = fetch_schema_sizes(conn)

        db_total_mb = sum(schema.total_mb for schema in schema_sizes)
        db_data_mb = sum(schema.data_mb for schema in schema_sizes)
        db_index_mb = sum(schema.index_mb for schema in schema_sizes)

        db_size = DatabaseSize(db_total_mb, db_data_mb, db_index_mb)

        if logging_level == "verbose":
            db_table = PrettyTable()
            db_table.field_names = [
                "Schema",
                "Total Size (MB)",
                "Data Size (MB)",
                "Index Size (MB)",
            ]
            for field in db_table.field_names:
                db_table.align[field] = "l"

            for schema in schema_sizes:
                schema.add_to_table(db_table)

            print(f"\nSchema Sizes for [{server_name}].[{db_name}]:\n")
            print(db_table)
            print(
                f"Database Total: {db_total_mb:.2f} MB "
                f"(Data: {db_data_mb:.2f} MB, "
                f"Index: {db_index_mb:.2f} MB)\n"
            )
            print("-" * 80)

        conn.close()

        return db_size

    except Exception as e:
        print(f"Error processing database '{db_name}' on server '{server_name}': {e}")
        return DatabaseSize(0.0, 0.0, 0.0)


def print_schema_table(schema_sizes: List[SchemaSize], server_name: str, db_name: str) -> None:
    """Print a table of schema sizes for a specific database."""
    table = PrettyTable()
    table.field_names = [
        "Schema",
        "Total Size (MB)",
        "Data Size (MB)",
        "Index Size (MB)",
    ]
    for field in table.field_names:
        table.align[field] = "l"

    for schema in schema_sizes:
        schema.add_to_table(table)

    db_total_mb = sum(schema.total_mb for schema in schema_sizes)
    db_data_mb = sum(schema.data_mb for schema in schema_sizes)
    db_index_mb = sum(schema.index_mb for schema in schema_sizes)

    print(f"\nSchema Sizes for [{server_name}].[{db_name}]:\n")
    print(table)
    print(
        f"Database Total: {db_total_mb:.2f} MB "
        f"(Data: {db_data_mb:.2f} MB, "
        f"Index: {db_index_mb:.2f} MB)\n"
    )
    print("-" * 80)


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
    database_results: Dict[str, Dict[str, Tuple[float, float, float]]],
) -> Tuple[PrettyTable, Dict[str, Tuple[float, float, float]]]:
    """
    Create a summary table of all database sizes.

    Args:
        database_results: Nested dictionary mapping server names to database names to size metrics

    Returns:
        Tuple of (summary_table, server_totals)
    """
    server_table = PrettyTable()
    server_table.field_names = [
        "Server",
        "Database",
        "Total Size (MB)",
        "Data Size (MB)",
        "Index Size (MB)",
    ]
    for field in server_table.field_names:
        server_table.align[field] = "l"

    server_totals = {}
    for server_name, db_results in database_results.items():
        server_total_mb = sum(total for total, _, _ in db_results.values())
        server_data_mb = sum(data for _, data, _ in db_results.values())
        server_index_mb = sum(index for _, _, index in db_results.values())

        # Store server totals
        server_totals[server_name] = (server_total_mb, server_data_mb, server_index_mb)

        # Add database rows
        for db_name, (total, data, index) in db_results.items():
            server_table.add_row(
                [server_name, db_name, f"{total:.2f}", f"{data:.2f}", f"{index:.2f}"]
            )

    return server_table, server_totals


def print_server_summary(server_results: Dict[str, ServerResults]) -> None:
    """Print a summary table of all server results."""
    summary_table = PrettyTable()
    summary_table.field_names = [
        "Server",
        "Database",
        "Total Size (MB)",
        "Data Size (MB)",
        "Index Size (MB)",
    ]
    for field in summary_table.field_names:
        summary_table.align[field] = "l"

    # Create totals table for server summaries
    totals_table = PrettyTable()
    totals_table.field_names = ["Server", "Total Size (MB)", "Data Size (MB)", "Index Size (MB)"]
    for field in totals_table.field_names:
        totals_table.align[field] = "l"

    # Fill tables with data
    for server_name, results in server_results.items():
        # Add each database to the summary table
        for db_name, db_size in results.databases.items():
            db_size.add_to_table(table=summary_table, server_name=server_name, db_name=db_name)

        # Add server total to the totals table
        server_total = results.total_size
        totals_table.add_row(
            [
                server_name,
                f"{server_total.total_mb:.2f}",
                f"{server_total.data_mb:.2f}",
                f"{server_total.index_mb:.2f}",
            ]
        )


def main() -> None:
    """Main entry point for schema size analysis tool."""
    print("Running Schema Size...")
    load_dotenv()
    schema_size_config = get_config("schema_size")
    env_variables = schema_size_config["connections"]
    databases_config = schema_size_config["databases"]
    logging_level = schema_size_config.get("logging_level", "verbose")

    server_configs = {}
    for server_name in env_variables:
        if server_name in databases_config:
            server_configs[server_name] = ServerDatabases(
                server_name=server_name, databases=databases_config[server_name]
            )

    # TODO: Actually handle logging_level for better printing possibly using verbose
    # to print all databases and summary to print server level summary

    if not server_configs:
        print("WARNING: No valid server configurations found in config file")
    else:
        print(f"Found {len(server_configs)} server configurations")
        for name, config in server_configs.items():
            print(f" {name} - {config}")

    connections = {}
    for server_name, env_var_name in env_variables.items():
        connections[server_name] = get_connection(env_var_name)

    server_results = {}
    for server_name, server_config in server_configs.items():
        if server_name in connections:
            connection = connections[server_name]
            results = process_server(server_config, connection, logging_level)
            server_results[server_name] = results
        else:
            print(f"Warning: No connection defined for server {server_name}")

    print_server_summary(server_results)


if __name__ == "__main__":
    main()
