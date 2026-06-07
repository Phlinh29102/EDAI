"""Shared pytest fixtures."""
from datetime import date

import pytest
import yaml
import pandas as pd

from data_generator.core.config import GeneratorConfig
from data_generator.offline.ad_impressions import AdImpressionGenerator
from data_generator.offline.interactions import InteractionGenerator
from data_generator.offline.playback_history import PlaybackHistoryGenerator
from data_generator.offline.users import UsersGenerator
from data_generator.offline.videos import VideoGenerator


@pytest.fixture
def dummy_config(tmp_path) -> GeneratorConfig:
    """Provide a GeneratorConfig initialized with a temporary YAML file."""
    config_path = tmp_path / "test_config.yaml"
    config_data = {
        "n_users": 100,
        "n_videos": 50,
        "days_history": 30,
        "advertiser_ids_pool": 10,
        "cost_nano_range": [500000000, 5000000000],
        "skew_ratio_popularity": 0.80,
        "skew_ratio_genre": 0.75,
        "churn_rate_baseline": 0.145,
        "duplicate_rate_offline": 0.02,
        "schema_change_date": "2026-04-01",
        "base_events_per_min": 100,
        "burst_multiplier": 5,
        "burst_windows": ["20:00-20:30"],
        "late_arrival_rate": 0.15,
        "late_delay_min_max": [1, 48],
        "duplicate_rate_stream": 0.02,
        "random_seed": 42,
    }
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)
    return GeneratorConfig(config_path)


@pytest.fixture
def sample_users_df() -> pd.DataFrame:
    """Provide a small users DataFrame for testing referential integrity."""
    return UsersGenerator(n_users=15, seed=42).generate()


@pytest.fixture
def sample_videos_df() -> pd.DataFrame:
    """Provide a small videos DataFrame for testing referential integrity."""
    return VideoGenerator(n_videos=15, seed=42).generate()


@pytest.fixture
def sample_playback_history_df(
    sample_users_df,
    sample_videos_df,
) -> pd.DataFrame:
    """Provide generated playback history records using existing dimensions."""
    return PlaybackHistoryGenerator(
        days_history=30,
        skew_ratio=0.8,
        duplicate_rate=0.02,
        user_ids=sample_users_df["user_id"].tolist(),
        video_ids=sample_videos_df["video_id"].tolist(),
        n_sessions=50,
        playback_end=date(2026, 4, 8),
        seed=42,
    ).generate()


@pytest.fixture
def sample_interactions_df(
    sample_users_df,
    sample_videos_df,
    sample_playback_history_df,
) -> pd.DataFrame:
    """Provide generated interaction records tied to playback sessions."""
    return InteractionGenerator(
        duplicate_rate=0.02,
        user_ids=sample_users_df["user_id"].tolist(),
        video_ids=sample_videos_df["video_id"].tolist(),
        playback_history_df=sample_playback_history_df,
        n_interactions=50,
        seed=42,
    ).generate()


@pytest.fixture
def sample_ad_impressions_df(
    sample_users_df,
    sample_videos_df,
    sample_playback_history_df,
) -> pd.DataFrame:
    """Provide generated ad impression records tied to playback sessions."""
    return AdImpressionGenerator(
        advertiser_id_pool=10,
        cost_nanos_range=(500_000_000, 5_000_000_000),
        schema_change_date=date(2026, 4, 1),
        user_ids=sample_users_df["user_id"].tolist(),
        video_ids=sample_videos_df["video_id"].tolist(),
        playback_history_df=sample_playback_history_df,
        n_impressions=120,
        seed=42,
    ).generate()
