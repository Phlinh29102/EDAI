"""Spark session construction utilities."""
from __future__ import annotations

from typing import Any, Dict, Optional


def get_spark_session(
    app_name: str = "media-data-pipeline",
    extra_conf: Optional[Dict[str, str]] = None,
) -> Any:
    """Create a SparkSession with optional Delta-friendly configuration."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "pyspark is not installed. Install Spark dependencies before using "
            "Spark-based pipeline modules."
        ) from exc

    builder = SparkSession.builder.appName(app_name)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
