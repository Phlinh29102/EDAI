"""Test AdImpressionGenerator."""
from datetime import date

import pytest

from data_generator.offline.ad_impressions import AdImpressionGenerator


def test_ad_impression_generator_outputs_expected_schema_and_ranges(
    sample_users_df,
    sample_videos_df,
):
    generator = AdImpressionGenerator(
        advertiser_id_pool=3,
        cost_nanos_range=(500_000_000, 5_000_000_000),
        schema_change_date=date(2026, 4, 1),
        user_ids=sample_users_df["user_id"].tolist(),
        video_ids=sample_videos_df["video_id"].tolist(),
        n_impressions=40,
        playback_start=date(2026, 4, 1),
        playback_end=date(2026, 4, 18),
        seed=7,
    )

    df = generator.generate()

    assert list(df.columns) == [
        "impression_id",
        "user_id",
        "video_id",
        "advertiser_id",
        "cost_nanos",
        "playback_date",
        "midpoint",
        "third_quartile",
        "clicked",
    ]
    assert len(df) == 40
    assert df["impression_id"].is_unique
    assert set(df["user_id"]).issubset(set(sample_users_df["user_id"]))
    assert set(df["video_id"]).issubset(set(sample_videos_df["video_id"]))
    assert df["advertiser_id"].nunique() <= 3
    assert df["cost_nanos"].between(500_000_000, 5_000_000_000).all()
    assert str(df["midpoint"].dtype) == "boolean"
    assert str(df["third_quartile"].dtype) == "boolean"
    assert str(df["clicked"].dtype) == "bool"
    assert df["clicked"].sum() <= 3  # ~3% of 40 impressions


def test_ad_impression_generator_applies_schema_evolution():
    generator = AdImpressionGenerator(
        advertiser_id_pool=2,
        cost_nanos_range=(1, 10),
        schema_change_date=date(2026, 4, 1),
        n_impressions=100,
        playback_start=date(2026, 3, 4),
        playback_end=date(2026, 4, 18),
        seed=42,
    )

    df = generator.generate()
    before_change = df[df["playback_date"] < date(2026, 4, 1)]
    after_change = df[df["playback_date"] >= date(2026, 4, 1)]

    assert not before_change.empty
    assert not after_change.empty
    assert before_change["midpoint"].isna().all()
    assert before_change["third_quartile"].isna().all()
    assert after_change["midpoint"].notna().all()
    assert after_change["third_quartile"].notna().all()
    assert len(before_change) == 60
    assert len(after_change) == 40


def test_ad_impression_generator_summary_includes_configuration():
    generator = AdImpressionGenerator(
        advertiser_id_pool=75,
        cost_nanos_range=(500_000_000, 5_000_000_000),
        schema_change_date=date(2026, 4, 1),
        n_impressions=10,
        seed=42,
    )

    summary = generator.summary()

    assert summary["generator"] == "AdImpressionGenerator"
    assert summary["advertiser_id_pool"] == 75
    assert summary["cost_nanos_range"] == (500_000_000, 5_000_000_000)
    assert summary["schema_change_date"] == "2026-04-01"
    assert summary["n_impressions"] == 10
    assert summary["seed"] == 42


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"advertiser_id_pool": 0}, "advertiser_id_pool must be positive"),
        ({"cost_nanos_range": (0, 10)}, "cost_nanos_range minimum must be positive"),
        (
            {"cost_nanos_range": (10, 1)},
            "cost_nanos_range minimum must be less than or equal",
        ),
        ({"n_impressions": -1}, "n_impressions must be non-negative"),
        (
            {"playback_start": date(2026, 2, 1), "playback_end": date(2026, 1, 1)},
            "playback_start must be before or equal",
        ),
    ],
)
def test_ad_impression_generator_rejects_invalid_arguments(kwargs, error):
    params = {
        "advertiser_id_pool": 1,
        "cost_nanos_range": (1, 10),
        "schema_change_date": date(2026, 4, 1),
    }
    params.update(kwargs)

    with pytest.raises(ValueError, match=error):
        AdImpressionGenerator(**params)
