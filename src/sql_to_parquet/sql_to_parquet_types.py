from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


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
