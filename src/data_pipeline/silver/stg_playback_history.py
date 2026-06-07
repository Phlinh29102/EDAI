"""Silver transform for stg_playback_history."""
from __future__ import annotations

TARGET_TABLE = "stg_playback_history"
SOURCE_TABLE = "raw_playback_history"


def run(bronze_path: str, output_path: str) -> str:
    """Build stg_playback_history from raw_playback_history."""
    raise NotImplementedError("stg_playback_history transform is not implemented yet")
