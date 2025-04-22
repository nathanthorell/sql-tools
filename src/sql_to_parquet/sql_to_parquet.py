import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from prettytable import PrettyTable
from sqlalchemy.engine import Engine

from utils.utils import get_config, get_connection


@dataclass
class SqlObject:
    name: str  # Friendly name for the output file
    object: str  # SQL object name (schema.object or just object)
    filter: str = ""  # Optional WHERE clause

    @property
    def schema(self) -> str:
        """Get the schema part of the object name."""
        parts = self.object.split(".", 1)
        return parts[0] if len(parts) == 2 else "dbo"

    @property
    def object_name(self) -> str:
        """Get the object name part without schema."""
        parts = self.object.split(".", 1)
        return parts[1] if len(parts) == 2 else parts[0]


@dataclass
class ExportResult:
    friendly_name: str
    full_object_name: str
    status: str = "Success"
    elapsed_time: Optional[float] = None
    rows_processed: int = 0
    file_path: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class ExportConfig:
    data_dir: str = "./data/"
    batch_size: int = 10000
    logging_level: str = "summary"
    objects: List[SqlObject] = field(default_factory=list)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ExportConfig":
        """Create an ExportConfig instance from a TOML config dictionary."""
        config = cls(
            data_dir=config_dict.get("data_dir", "./data/"),
            batch_size=config_dict.get("batch_size", 10000),
            logging_level=config_dict.get("logging_level", "summary"),
        )

        # Parse the objects list
        objects_list = config_dict.get("objects", [])
        for obj_dict in objects_list:
            config.objects.append(
                SqlObject(
                    name=obj_dict["name"],
                    object=obj_dict["object"],
                    filter=obj_dict.get("filter", ""),
                )
            )

        return config


def export_to_parquet(
    engine: Engine,
    sql_object: SqlObject,
    output_dir: Path,
    batch_size: int,
    logging_level: str,
) -> ExportResult:
    result = ExportResult(friendly_name=sql_object.name, full_object_name=sql_object.object)

    start_time = time.time()
    file_path = output_dir / f"{sql_object.name}.parquet"

    try:
        # Build the SQL query with filter if provided
        query = f"SELECT * FROM [{sql_object.schema}].[{sql_object.object_name}]"
        if sql_object.filter:
            query += f" WHERE {sql_object.filter}"

        if logging_level in ["verbose", "debug"]:
            print(f"Executing query: {query}")

        for i, df_chunk in enumerate(pd.read_sql_query(query, engine, chunksize=batch_size)):
            if i == 0:
                # First chunk, create the file
                df_chunk.to_parquet(file_path, engine="pyarrow", index=False)
            else:
                # Append to existing file
                df_chunk.to_parquet(file_path, engine="pyarrow", index=False, append=True)

            result.rows_processed += len(df_chunk)

            if logging_level == "debug":
                print(f"Processed chunk {i + 1} with {len(df_chunk)} rows")

        end_time = time.time()
        result.elapsed_time = end_time - start_time
        result.file_path = str(file_path)

        if logging_level in ["verbose", "debug"]:
            print(f"Successfully exported [{sql_object.object}] to {file_path}")
            print(f"Rows processed: {result.rows_processed}")
            print(f"Execution time: {result.elapsed_time:.2f} seconds")

    except Exception as e:
        result.status = "Error"
        result.error_message = str(e)

        if logging_level in ["verbose", "debug"]:
            print(f"Error exporting [{sql_object.object}]: {e}")

    return result


def print_results_summary(results: List[ExportResult], logging_level: str) -> None:
    """Print a summary of export results based on the logging level."""
    if logging_level in ["summary", "verbose", "debug"]:
        table = PrettyTable()
        table.field_names = ["Friendly Name", "SQL Object", "Status", "Rows", "Time (s)", "File"]

        for field in table.field_names:
            table.align[field] = "l"

        table.max_width["Friendly Name"] = 30
        table.max_width["SQL Object"] = 40
        table.max_width["File"] = 30

        for result in results:
            table.add_row(
                [
                    result.friendly_name,
                    result.full_object_name,
                    result.status,
                    result.rows_processed,
                    f"{result.elapsed_time:.2f}" if result.elapsed_time else "N/A",
                    os.path.basename(result.file_path) if result.file_path else "N/A",
                ]
            )

        print("\nExport Summary:")
        print(table)

        # Print error details for failed exports
        if any(r.status == "Error" for r in results):
            print("\nErrors:")
            for result in results:
                if result.status == "Error":
                    name = result.friendly_name
                    obj = result.full_object_name
                    error = result.error_message
                    print(f"[{name}] {obj}: {error}")


def main() -> None:
    load_dotenv()
    tool_config = get_config("sql_to_parquet")
    export_config = ExportConfig.from_dict(tool_config)

    # Convert data_dir to absolute path if it's relative
    if not os.path.isabs(export_config.data_dir):
        project_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        data_dir = os.path.join(project_root, export_config.data_dir.lstrip("./"))
    else:
        data_dir = export_config.data_dir

    # Ensure data directory exists
    data_dir_path = Path(data_dir)
    data_dir_path.mkdir(parents=True, exist_ok=True)

    connection = get_connection("SQL_TO_PARQUET_DB")
    engine = connection.get_sqlalchemy_engine()

    print(f"Executing script on server: [{connection.server}] in database: [{connection.database}]")
    print(f"Output directory: {data_dir_path}")
    print(f"Using logging_level: {export_config.logging_level}\n")

    try:
        if not export_config.objects:
            print("No objects defined in the configuration")
            return

        print(f"Found {len(export_config.objects)} objects to process")

        # Process each object
        results: List[ExportResult] = []
        for sql_object in export_config.objects:
            if export_config.logging_level in ["verbose", "debug"]:
                print(f"\nProcessing object: [{sql_object.object}] as [{sql_object.name}]")

            result = export_to_parquet(
                engine=engine,
                sql_object=sql_object,
                output_dir=data_dir_path,
                batch_size=export_config.batch_size,
                logging_level=export_config.logging_level,
            )

            results.append(result)

        print_results_summary(results, export_config.logging_level)

        # Print overall statistics
        successful = sum(1 for r in results if r.status == "Success")
        failed = len(results) - successful
        total_rows = sum(r.rows_processed for r in results)

        print(
            f"\nExported {successful} objects ({failed} failed) with a total of {total_rows} rows"
        )

    except pyodbc.Error as ex:
        print(f"Database error: {ex}")

    finally:
        pass


if __name__ == "__main__":
    main()
