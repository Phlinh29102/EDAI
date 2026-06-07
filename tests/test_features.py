"""Tests for feature engineering calculators and orchestration."""
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from data_generator.core.config import GeneratorConfig
from data_generator.core.utils import RandomDataUtils
from data_generator.features.feature_engineer import FeatureEngineer
from data_generator.features.offline_calculator import OfflineFeatureCalculator
from data_generator.features.streaming_calculator import StreamingFeatureCalculator
from data_generator.streaming.stream_data_generator import StreamDataGenerator


def test_offline_feature_calculator_computes_90_day_user_features():
    tables = {
        "users": pd.DataFrame({"user_id": ["u1", "u2", "u3"]}),
        "videos": pd.DataFrame(
            {
                "video_id": ["v1", "v2", "v3"],
                "video_genre": ["Action", "Drama", "Action"],
            }
        ),
        "playback_history": pd.DataFrame(
            {
                "history_id": ["h1", "h2", "h3", "old"],
                "user_id": ["u1", "u1", "u2", "u1"],
                "video_id": ["v1", "v2", "v3", "v3"],
                "playback_date": [
                    date(2026, 2, 1),
                    date(2026, 1, 20),
                    date(2026, 1, 15),
                    date(2025, 1, 1),
                ],
                "watch_hours": [1.5, 2.0, 0.5, 99.0],
            }
        ),
        "ad_impressions": pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u2"],
                "playback_date": [
                    date(2026, 2, 1),
                    date(2026, 1, 20),
                    date(2026, 1, 15),
                ],
                "clicked": [True, False, False],
            }
        ),
    }

    features = OfflineFeatureCalculator().compute(tables, date(2026, 2, 1))

    u1 = features.set_index("user_id").loc["u1"]
    u3 = features.set_index("user_id").loc["u3"]
    assert u1["f_user_total_watch_hours_90d"] == pytest.approx(3.5)
    assert u1["f_user_distinct_genres_90d"] == 2
    assert u1["f_user_historical_ad_ctr_90d"] == pytest.approx(0.5)
    assert 0.0 <= u1["f_user_subscription_churn_risk_90d"] <= 1.0
    assert u3["f_user_total_watch_hours_90d"] == 0.0
    assert u3["f_user_subscription_churn_risk_90d"] == 1.0


def test_streaming_feature_calculator_computes_rolling_windows():
    window_end = datetime(2026, 2, 1, 12, 0, 0)
    base = int(window_end.timestamp() * 1000)
    events = [
        _event("u1", "playback_start", base - 10 * 60 * 1000),
        _event("u1", "playback_start", base - 20 * 60 * 1000),
        _event("u1", "ad_impression", base - 40 * 60 * 1000, midpoint=True),
        _event("u1", "ad_impression", base - 50 * 60 * 1000, midpoint=False),
        _event("u1", "skip", base - 5 * 60 * 1000, position=5),
        _event("u2", "playback_start", base - 40 * 60 * 1000),
        _event("u2", "skip", base - 10 * 60 * 1000, position=30),
        _event("u1", "playback_start", base - 70 * 60 * 1000),
    ]

    features = StreamingFeatureCalculator(
        burst_threshold_events_60m=5,
        early_skip_seconds=10,
    ).compute(events, window_end)

    u1 = features.set_index("user_id").loc["u1"]
    u2 = features.set_index("user_id").loc["u2"]
    assert u1["f_stream_videos_started_30m"] == 2
    assert u1["f_stream_ad_completion_ratio_60m"] == pytest.approx(0.5)
    assert u1["f_stream_early_skip_rate_60m"] == pytest.approx(0.5)
    assert u1["f_stream_burst_activity_flag"] == 1
    assert u2["f_stream_videos_started_30m"] == 0
    assert u2["f_stream_early_skip_rate_60m"] == 0.0


def test_feature_engineer_merges_and_saves_unified_features(tmp_path):
    offline_path = tmp_path / "offline"
    stream_path = tmp_path / "streaming"
    feature_path = tmp_path / "features"
    config_path = tmp_path / "features.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "data_dir": {
                    "offline": str(offline_path),
                    "streaming": str(stream_path),
                    "features": str(feature_path),
                },
                "late_arrival_rate": 0.0,
                "late_delay_min_max": [1, 1],
                "duplicate_rate_stream": 0.0,
                "base_events_per_min": 1,
                "burst_multiplier": 1,
                "burst_windows": [],
                "ad_click_rate": 0.0,
                "schema_change_date": "2026-01-29",
                "feature_burst_threshold_events_60m": 2,
            }
        )
    )
    config = GeneratorConfig(config_path)
    _write_offline_tables(offline_path)

    stream_generator = StreamDataGenerator(
        config=config,
        utils=RandomDataUtils(seed=42),
        output_path=str(stream_path),
    )
    stream_generator.generate_and_save(
        start_ts=datetime(2026, 2, 1, 12, 0, 0),
        minutes=1,
        user_contexts=[
            {
                "user_id": "u1",
                "session_id": "s1",
                "device_type": "web",
                "platform": "web",
                "video_id": "v1",
                "genre_id": "Action",
            }
        ],
    )

    engineer = FeatureEngineer(config)
    window_end = datetime(2026, 2, 1, 12, 15, 0)
    features = engineer.merge_features(window_end)
    output_file = engineer.save_features(features, window_end)

    assert Path(output_file).exists()
    saved = pq.read_table(output_file).to_pandas()
    assert set(saved["user_id"]) == {"u1", "u2"}
    assert "feature_ts" in saved.columns
    assert engineer.summary()["last_feature_path"] == output_file


def _event(
    user_id: str,
    event_type: str,
    event_timestamp: int,
    midpoint=None,
    position=None,
):
    return {
        "event_id": f"{event_type}_{event_timestamp}",
        "event_type": event_type,
        "event_timestamp": event_timestamp,
        "created_ts": event_timestamp,
        "user_id": user_id,
        "session_id": f"session_{user_id}",
        "device_type": "web",
        "platform": "web",
        "video_id": "v1",
        "genre_id": "Action",
        "playback_position_seconds": position,
        "ad_campaign_id": None,
        "midpoint": midpoint,
        "third_quartile": None,
    }


def _write_offline_tables(root: Path) -> None:
    tables = {
        "users": pd.DataFrame({"user_id": ["u1", "u2"]}),
        "videos": pd.DataFrame(
            {"video_id": ["v1"], "video_genre": ["Action"]}
        ),
        "playback_history": pd.DataFrame(
            {
                "history_id": ["h1"],
                "user_id": ["u1"],
                "video_id": ["v1"],
                "playback_date": [date(2026, 2, 1)],
                "watch_hours": [1.0],
            }
        ),
        "interactions": pd.DataFrame(),
        "ad_impressions": pd.DataFrame(
            {
                "user_id": ["u1"],
                "playback_date": [date(2026, 2, 1)],
            }
        ),
    }
    for table_name, df in tables.items():
        table_path = root / f"{table_name}.parquet"
        table_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(df, preserve_index=False),
            table_path,
        )
