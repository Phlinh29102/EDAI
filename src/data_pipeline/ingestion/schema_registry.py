"""Local schema registry metadata for pipeline development."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from data_generator.core.schema import DataSchema


DEFAULT_REGISTRY_PATH = Path("data/schema_registry")


class LocalSchemaRegistry:
    """Persist pipeline schemas as local JSON files."""

    def __init__(self, registry_path: str | Path = DEFAULT_REGISTRY_PATH) -> None:
        self.registry_path = Path(registry_path)

    def register(self, subject: str, schema: Dict[str, Any]) -> str:
        """Register a schema under a subject and return the written path."""
        self.registry_path.mkdir(parents=True, exist_ok=True)
        path = self.registry_path / f"{subject}.json"
        path.write_text(json.dumps(schema, indent=2, sort_keys=True))
        return str(path)

    def get(self, subject: str) -> Optional[Dict[str, Any]]:
        """Load a registered local schema, if present."""
        path = self.registry_path / f"{subject}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def register_generated_schemas(self) -> Dict[str, str]:
        """Register schemas from the generated data source definitions."""
        schema = DataSchema()
        paths = {
            "streaming_event": self.register(
                "streaming_event",
                schema.get_streaming_schema(),
            )
        }
        for table_name, table_schema in schema.get_offline_schema().items():
            paths[f"offline_{table_name}"] = self.register(
                f"offline_{table_name}",
                table_schema,
            )
        return paths


def main() -> None:
    """Register generated schemas into the local schema registry path."""
    registry = LocalSchemaRegistry()
    for subject, path in registry.register_generated_schemas().items():
        print(f"{subject}: {path}")


if __name__ == "__main__":
    main()
