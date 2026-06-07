"""Feature pipeline for feat_user_unified."""
from __future__ import annotations

from data_generator.features.feature_engineer import FeatureEngineer

TARGET_TABLE = "feat_user_unified"

__all__ = ["FeatureEngineer", "TARGET_TABLE"]
