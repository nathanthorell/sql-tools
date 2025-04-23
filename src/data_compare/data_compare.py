import sys
from typing import Any, Dict

from dotenv import load_dotenv

from data_compare.comparison import run_comparisons
from data_compare.data_compare_types import ComparisonConfig
from utils.config_utils import get_config
from utils.rich_utils import console


def process_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Process the raw TOML config by loading SQL files when needed"""
    processed_config = config.copy()

    # Process each comparison item
    compare_list = config.get("compare_list", [])
    processed_list = []

    for item in compare_list:
        processed_item = item.copy()

        # Process left side
        if "left_query" not in item and "left_query_file" in item:
            query_file = item["left_query_file"]
            processed_item["left_query"] = load_sql_file(query_file)

        # Process right side
        if "right_query" not in item and "right_query_file" in item:
            query_file = item["right_query_file"]
            processed_item["right_query"] = load_sql_file(query_file)

        # Validate the item has queries for both sides
        if "left_query" not in processed_item:
            raise ValueError(f"Comparison '{item.get('name', 'unnamed')}' is missing a left query")
        if "right_query" not in processed_item:
            raise ValueError(f"Comparison '{item.get('name', 'unnamed')}' is missing a right query")

        processed_list.append(processed_item)

    processed_config["compare_list"] = processed_list
    return processed_config


def load_sql_file(file_path: str) -> str:
    """Load SQL query from a file"""
    from pathlib import Path

    sql_path = Path(file_path)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path) as f:
        return f.read()


def main() -> int:
    try:
        load_dotenv(override=True)

        data_compare_config = get_config("data_compare")
        processed_config = process_config(data_compare_config)

        # Load config and run comparisons
        config = ComparisonConfig(processed_config)
        success = run_comparisons(config)

        return 0 if success else 1

    except Exception as e:
        console.print(f"[bold red]Fatal error:[/] {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
