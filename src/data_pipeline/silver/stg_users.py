"""Silver transform for stg_users."""
from __future__ import annotations

TARGET_TABLE = "stg_users"
SOURCE_TABLE = "raw_users"


def run(bronze_path: str, output_path: str) -> str:
    """Build stg_users from raw_users."""
    raise NotImplementedError("stg_users transform is not implemented yet")
