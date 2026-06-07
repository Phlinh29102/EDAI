"""Silver transform for stg_videos."""
from __future__ import annotations

TARGET_TABLE = "stg_videos"
SOURCE_TABLE = "raw_videos"


def run(bronze_path: str, output_path: str) -> str:
    """Build stg_videos from raw_videos."""
    raise NotImplementedError("stg_videos transform is not implemented yet")
