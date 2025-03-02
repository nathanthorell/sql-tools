import os

import pyodbc
import toml
from dotenv import load_dotenv

from usp_tester.utils import execute_procedure
from utils.utils import get_connection


def main() -> None:
    load_dotenv()
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, "config.toml")

    with open(config_path, "r") as f:
        config = toml.load(f)
        usp_config = config["usp_tester"]
        defaults = usp_config["defaults"]
        schema = usp_config["schema"]
        logging_level = usp_config["logging_level"]

    connection = get_connection("USP_TEST_DB")
    conn = connection.connect()

    print(f"Executing script on server: [{connection.server}] in database: [{connection.database}]")
    print(f"Using logging_level: {logging_level}\n")

    try:
        # Fetch stored procedures in the given schema
        query = f"""
        SELECT SPECIFIC_NAME
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        AND ROUTINE_SCHEMA = '{schema}'
        ORDER BY SPECIFIC_NAME;
        """
        cursor = conn.cursor()
        cursor.execute(query)
        stored_procedures = cursor.fetchall()

        results = []
        for proc in stored_procedures:
            proc_name = proc[0]
            print(f"Executing stored procedure: [{proc_name}]")

            result = execute_procedure(conn, schema, proc_name, defaults, logging_level)
            results.append(result)

            if logging_level == "verbose":
                print("")

        if logging_level == "summary":
            print("Execution Summary:")
            print(f"{'Procedure Name':<50} {'Status':<10} {'Execution Time':<15}")
            print("-" * 76)
            for result in results:
                proc_name = result["proc_name"]
                status = result["status"]
                elapsed_time = f"{result['elapsed_time']:.2f}" if result["elapsed_time"] else "N/A"
                print(f"{proc_name:<50} {status:<10} {elapsed_time:<15}")

    except pyodbc.Error as ex:
        print(f"Database error: {ex}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
