"""Test OfflineDataGenerator orchestration."""
from pathlib import Path

import yaml

from data_generator.core.config import GeneratorConfig
from data_generator.core.schema import DataSchema
from data_generator.core.utils import RandomDataUtils
from data_generator.offline.base_table_generator import BaseTableGenerator
from data_generator.offline.offline_data_generator import OfflineDataGenerator


def test_offline_data_generator_generates_and_saves_all_tables(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_data = {
        "n_users": 6,
        "n_videos": 5,
        "days_history": 14,
        "n_playback_sessions": 8,
        "n_interactions": 7,
        "n_ad_impressions": 9,
        "advertiser_ids_pool": 3,
        "cost_nanos_range": [500_000_000, 5_000_000_000],
        "skew_ratio_popularity": 0.8,
        "skew_ratio_genre": 0.75,
        "duplicate_rate_offline": 0.0,
        "schema_change_date": "2026-04-01",
        "random_seed": 42,
    }
    config_path.write_text(yaml.dump(config_data))
    output_path = tmp_path / "offline"

    generator = OfflineDataGenerator(
        config=GeneratorConfig(config_path),
        schema=DataSchema(),
        utils=RandomDataUtils(seed=42),
        output_path=str(output_path),
    )

    paths = generator.generate_all()

    assert set(paths) == {
        "users",
        "videos",
        "playback_history",
        "interactions",
        "ad_impressions",
    }
    assert all(Path(path).exists() for path in paths.values())
    assert all(
        isinstance(table_generator, BaseTableGenerator)
        for table_generator in generator.table_generators.values()
    )
    assert generator.table_generators["users"].df is not None
    assert generator.table_generators["playback_history"].df is not None


def test_offline_data_generator_summary_reports_table_state(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "n_users": 2,
                "n_videos": 2,
                "n_playback_sessions": 1,
                "n_interactions": 1,
                "n_ad_impressions": 1,
                "random_seed": 42,
            }
        )
    )
    generator = OfflineDataGenerator(
        config=GeneratorConfig(config_path),
        schema=DataSchema(),
        utils=RandomDataUtils(seed=42),
        output_path=str(tmp_path / "offline"),
    )

    assert generator.summary()["tables"] == {}

    generator.generate_all()
    summary = generator.summary()

    assert summary["generator"] == "OfflineDataGenerator"
    assert summary["output_path"] == str(tmp_path / "offline")
    assert summary["tables"]["users"]["row_count"] == 2
    assert summary["tables"]["videos"]["row_count"] == 2
