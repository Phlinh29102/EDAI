"""Silver transform for stg_ad_impressions."""
from __future__ import annotations

TARGET_TABLE = "stg_ad_impressions"
SOURCE_TABLE = "raw_ad_impressions"


def run(bronze_path: str, output_path: str) -> str:
    """Build stg_ad_impressions from raw_ad_impressions."""
    raise NotImplementedError("stg_ad_impressions transform is not implemented yet")
