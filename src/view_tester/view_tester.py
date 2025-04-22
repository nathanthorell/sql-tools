import re
import time
from typing import Any, Dict, List

from dotenv import load_dotenv

from utils import Connection, get_config, get_connection


def fetch_views(conn: Connection, schema: str) -> List[str]:
    query = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = '{schema}'
    ORDER BY TABLE_NAME;
    """
    with conn.get_connection() as db_conn:
        cursor = db_conn.cursor()
        try:
            cursor = db_conn.cursor()
            cursor.execute(query)
            views = cursor.fetchall()

            return [view[0] for view in views]
        except Exception as e:
            print(f"Error fetching schema sizes: {e}")
            return []


def execute_view(
    conn: Connection, schema: str, view_name: str, logging_level: str
) -> Dict[str, Any]:
    result = {
        "view_name": view_name,
        "status": "Success",
        "elapsed_time": None,
        "error_message": None,
    }

    with conn.get_connection() as db_conn:
        cursor = db_conn.cursor()
        try:
            start_time = time.time()

            query = f"SELECT TOP 1 * FROM [{schema}].[{view_name}]"
            cursor.execute(query)
            cursor.fetchone()

            end_time = time.time()
            result["elapsed_time"] = f"{end_time - start_time}"

            if logging_level == "verbose":
                print(f"Successfully queried view [{view_name}]")
                print(f"Execution time: {result['elapsed_time']:.2f} seconds")

        except Exception as e:
            result["status"] = "Error"
            error_str = str(e)

            # Look for typical SQL Server error pattern
            if "SQLServer" in error_str or "SQL Server" in error_str:
                # Try to extract the most meaningful part of the error message
                if "Invalid column name" in error_str:
                    # Extract the column name from the error
                    column_match = re.search(r"Invalid column name '([^']+)'", error_str)
                    if column_match:
                        result["error_message"] = f"Invalid column name '{column_match.group(1)}'"
                    else:
                        result["error_message"] = "Invalid column name in view"
                else:
                    # General extraction for other SQL Server errors
                    # Find the most relevant part of the message
                    parts = error_str.split("]")
                    if len(parts) > 2:  # We have parts like [SQLServer][Driver][SQL Server]Message
                        # The relevant message is usually after the last bracket
                        msg_part = parts[-1].split("(")[0].strip()
                        result["error_message"] = msg_part
                    else:
                        result["error_message"] = error_str
            else:
                # For other types of errors
                result["error_message"] = error_str

            # Only print the full error in verbose mode
            if logging_level == "verbose":
                print(f"Error executing view [{view_name}]: {error_str}")

    return result


def print_results_summary(results: List[Dict[str, Any]], logging_level: str) -> None:
    if logging_level == "summary":
        print("\nExecution Summary:")
        print(f"{'View Name':<50} {'Status':<10} {'Execution Time':<15}")
        print("-" * 76)
        for result in results:
            view_name = result["view_name"]
            status = result["status"]
            if result["elapsed_time"]:
                elapsed_time = f"{float(result['elapsed_time']):.2f}s"
            else:
                elapsed_time = "N/A"
            print(f"{view_name:<50} {status:<10} {elapsed_time:<15}")

            # Add the error message line after a view that has errored
            if status == "Error" and result.get("error_message"):
                print(f"    Error: {result['error_message']}")

    elif logging_level == "errors_only":
        print("\nErrors Only:")
        error_count = 0
        for result in results:
            if result["status"] == "Error":
                error_count += 1
                view_name = result["view_name"]
                elapsed_time = f"{result['elapsed_time']:.2f}s" if result["elapsed_time"] else "N/A"
                print(f"{view_name:<50} {'Error':<10} {elapsed_time:<15}")
                if result.get("error_message"):
                    print(f"    Error: {result['error_message']}")

        if error_count == 0:
            print("No errors found.")
        else:
            print(f"\nFound {error_count} errors out of {len(results)} views.")


def main() -> None:
    load_dotenv()
    view_config = get_config("view_tester")
    schema = view_config["schema"]
    logging_level = view_config["logging_level"]

    connection = get_connection("VIEW_TEST_DB")

    print(f"Executing script on server: [{connection.server}] in database: [{connection.database}]")
    print(f"Using logging_level: {logging_level}\n")

    try:
        views = fetch_views(connection, schema)

        if not views:
            print(f"No views found in schema '{schema}'")
            return

        results: List[Dict[str, Any]] = []
        for view_name in views:
            if logging_level == "verbose":
                print(f"Querying view: [{view_name}]\n")

            result = execute_view(connection, schema, view_name, logging_level)
            results.append(result)

            if logging_level == "verbose":
                print("")

        print_results_summary(results, logging_level)

    except Exception as ex:
        print(f"Database error: {ex}")


if __name__ == "__main__":
    main()
