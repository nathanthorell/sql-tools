import os
from typing import Dict

import toml
from dotenv import load_dotenv

from obj_compare.proc_compare import compare_proc_definitions, compare_procs_for_exclusivity
from obj_compare.view_compare import compare_view_definitions, compare_views_for_exclusivity
from utils.utils import Connection, get_connection


def main() -> None:
    load_dotenv()
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, "config.toml")

    connections: Dict[str, Connection] = {
        "DEV": get_connection("OBJ_COMPARE_DEV_DB"),
        "QA": get_connection("OBJ_COMPARE_QA_DB"),
        "UAT": get_connection("OBJ_COMPARE_UAT_DB"),
        "PROD": get_connection("OBJ_COMPARE_PROD_DB"),
    }

    with open(config_path, "r") as f:
        config = toml.load(f)
        obj_compare_config = config["sql_object_compare"]
        schema: str = obj_compare_config["schema"]

    compare_procs_for_exclusivity(connections, schema)
    compare_views_for_exclusivity(connections, schema)
    compare_proc_definitions(connections, schema)
    compare_view_definitions(connections, schema)


if __name__ == "__main__":
    main()
