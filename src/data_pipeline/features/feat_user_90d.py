"""Feature pipeline for feat_user_90d."""
from __future__ import annotations

from data_generator.features.offline_calculator import OfflineFeatureCalculator

TARGET_TABLE = "feat_user_90d"

__all__ = ["OfflineFeatureCalculator", "TARGET_TABLE"]
