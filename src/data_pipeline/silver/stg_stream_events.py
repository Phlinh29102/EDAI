"""Silver transform for stg_stream_events."""
from __future__ import annotations

TARGET_TABLE = "stg_stream_events"
SOURCE_TABLE = "raw_media_events"


def run(bronze_path: str, output_path: str) -> str:
    """Build stg_stream_events from raw_media_events."""
    raise NotImplementedError("stg_stream_events transform is not implemented yet")
