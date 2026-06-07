"""Feature pipeline for feat_stream_60m."""
from __future__ import annotations

from data_generator.features.streaming_calculator import StreamingFeatureCalculator

TARGET_TABLE = "feat_stream_60m"

__all__ = ["StreamingFeatureCalculator", "TARGET_TABLE"]
