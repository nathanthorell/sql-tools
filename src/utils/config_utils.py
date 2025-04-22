import os
from typing import Any, Dict, Optional

import toml


def get_config(tool_name: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    """Helper function to process the config.toml file"""
    if config_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        config_path = os.path.join(project_root, "config.toml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        config = toml.load(f)

    if tool_name not in config:
        raise KeyError(f"Configuration for '{tool_name}' not found in config file")

    tool_config: Dict[str, Any] = dict(config[tool_name])

    if "sql_tools" in config:
        global_config = config["sql_tools"]

        # Apply global logging_level if not set in tool-specific config section
        if "logging_level" in global_config and "logging_level" not in tool_config:
            tool_config["logging_level"] = global_config["logging_level"]

    return tool_config
