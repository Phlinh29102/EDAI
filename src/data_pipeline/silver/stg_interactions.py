"""Silver transform for stg_interactions."""
from __future__ import annotations

TARGET_TABLE = "stg_interactions"
SOURCE_TABLE = "raw_interactions"


def run(bronze_path: str, output_path: str) -> str:
    """Build stg_interactions from raw_interactions."""
    raise NotImplementedError("stg_interactions transform is not implemented yet")
