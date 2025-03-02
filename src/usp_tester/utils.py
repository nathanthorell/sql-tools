import time
from datetime import date, datetime
from typing import Any, Dict, Union

import pyodbc


def get_default_for_date_type(
    param_name: str, date_defaults: Dict[str, Union[date, datetime]]
) -> Union[date, datetime]:
    """
    Returns the appropriate default for date or datetime parameters based on the parameter name.
    :param param_name: The name of the parameter (e.g., 'start_date', 'end_date').
    :param date_defaults: A dictionary containing default values for date and datetime types.
    :return: The default value for the parameter.
    """
    param_name_lower = param_name.lower()

    if "start" in param_name_lower:
        if "datetime" in param_name_lower:
            return date_defaults["start_datetime"]
        return date_defaults["start_date"]
    elif "end" in param_name_lower:
        if "datetime" in param_name_lower:
            return date_defaults["end_datetime"]
        return date_defaults["end_date"]
    else:
        return date_defaults["start_date"]


def execute_procedure(
    conn: pyodbc.Connection,
    schema: str,
    proc_name: str,
    defaults: Dict[str, Any],
    logging_level: str,
) -> Dict[str, str]:
    """
    Execute a single stored procedure with the given parameters.

    :param conn: The database connection object.
    :param schema: The schema name for the stored procedure.
    :param proc_name: The name of the stored procedure.
    :param defaults: A dictionary containing default values for parameter types.
    :param logging_level: Expects a string value ("verbose", "errors_only", or "summary").
    """
    cursor = conn.cursor()

    try:
        # Fetch parameters for the stored procedure
        param_query = f"""
        SELECT PARAMETER_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.PARAMETERS
        WHERE SPECIFIC_SCHEMA = '{schema}' AND SPECIFIC_NAME = '{proc_name}'
        """
        cursor.execute(param_query)
        parameters = cursor.fetchall()

        # Prepare default mapping for non-date types
        default_map = {
            "int": defaults["integer"],
            "bit": defaults["bit"],
            "decimal": defaults["decimal"],
            "varchar": defaults["varchar"],
            "nvarchar": defaults["varchar"],
        }

        proc_args = []
        for param in parameters:
            param_name, param_type = param

            # Check for date or datetime types based on name and type
            if param_type in ["date", "datetime", "smalldatetime"]:
                proc_args.append(get_default_for_date_type(param_name, defaults))
            else:
                # Use the default mapping for other types
                proc_args.append(
                    default_map.get(param_type, None)
                )  # Fallback to None if type isn't mapped

        # Build the EXEC query with f-strings for the procedure name and placeholders
        placeholder_str = ", ".join(
            [f"'{arg}'" if arg is not None else "NULL" for arg in proc_args]
        )
        exec_query = f"EXEC [{schema}].[{proc_name}] {placeholder_str}"

        start_time = time.time()

        if logging_level == "verbose":
            print(f"Running: {exec_query}")

        cursor.execute(exec_query)
        conn.commit()

        end_time = time.time()
        elapsed_time = end_time - start_time

        if logging_level == "verbose":
            print(f"Executed with arguments: {proc_args} in {elapsed_time:.2f} seconds")

        return {
            "proc_name": proc_name,
            "status": "success",
            "elapsed_time": str(elapsed_time),
        }

    except pyodbc.Error as ex:
        end_time = time.time()

        if logging_level in ["verbose", "errors_only"]:
            print(f"Error executing {proc_name}: {ex}")

        return {
            "proc_name": proc_name,
            "status": "fail",
            "elapsed_time": "",
            "error_message": str(ex),
        }

    finally:
        cursor.close()
