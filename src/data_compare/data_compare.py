from dotenv import load_dotenv

from data_compare.data_compare_types import ComparisonConfig
from data_compare.data_compare_utils import run_comparisons
from utils.config_utils import get_config
from utils.rich_utils import console


def main() -> None:
    try:
        load_dotenv(override=True)
        data_compare_config = get_config("data_compare")
        config = ComparisonConfig(data_compare_config)
        run_comparisons(config)

    except Exception as e:
        console.print(f"[bold red]Fatal error:[/] {e}")


if __name__ == "__main__":
    main()
