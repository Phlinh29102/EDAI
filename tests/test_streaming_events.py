"""Test streaming event generators."""
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from data_generator.core.config import GeneratorConfig
from data_generator.core.schema import DataSchema
from data_generator.core.utils import RandomDataUtils
from data_generator.streaming.ad_click import AdClickEventGenerator
from data_generator.streaming.ad_impression import AdImpressionEventGenerator
from data_generator.streaming.base_event_generator import BaseEventGenerator
from data_generator.streaming.pause import PauseEventGenerator
from data_generator.streaming.playback_start import PlaybackStartEventGenerator
from data_generator.streaming.skip import SkipEventGenerator
from data_generator.streaming.stream_data_generator import StreamDataGenerator
from data_generator.streaming.subscription_cancel import SubscriptionCancelEventGenerator


STREAMING_FIELDS = {
    "event_id",
    "event_type",
    "event_timestamp",
    "created_ts",
    "user_id",
    "session_id",
    "device_type",
    "platform",
    "video_id",
    "genre_id",
    "playback_position_seconds",
    "ad_campaign_id",
    "midpoint",
    "third_quartile",
}


@pytest.fixture
def streaming_config(tmp_path) -> GeneratorConfig:
    config_path = tmp_path / "streaming.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "late_arrival_rate": 0.0,
                "late_delay_min_max": [1, 1],
                "duplicate_rate_stream": 0.0,
                "base_events_per_min": 2,
                "burst_multiplier": 3,
                "burst_windows": ["20:00-20:30"],
                "ad_click_rate": 0.0,
                "schema_change_date": "2026-04-01",
                "random_seed": 42,
            }
        )
    )
    return GeneratorConfig(config_path)


@pytest.fixture
def user_ctx() -> dict:
    return {
        "user_id": "user_00000001",
        "session_id": "session_0000000001",
        "device_type": "web",
        "platform": "web",
        "video_id": "video_00000001",
        "genre_id": "Action",
    }


@pytest.mark.parametrize(
    ("generator_cls", "event_type"),
    [
        (PlaybackStartEventGenerator, "playback_start"),
        (PauseEventGenerator, "pause"),
        (SkipEventGenerator, "skip"),
        (AdImpressionEventGenerator, "ad_impression"),
        (AdClickEventGenerator, "ad_click"),
        (SubscriptionCancelEventGenerator, "subscription_cancel"),
    ],
)
def test_event_generators_emit_unified_schema(
    streaming_config,
    user_ctx,
    generator_cls,
    event_type,
):
    generator = generator_cls(streaming_config, RandomDataUtils(seed=42))

    event = generator.generate_event(user_ctx, datetime(2026, 2, 1, 12, 0, 0))

    assert set(event) == STREAMING_FIELDS
    assert event["event_type"] == event_type
    assert event["user_id"] == user_ctx["user_id"]
    assert event["session_id"] == user_ctx["session_id"]
    assert event["created_ts"] >= event["event_timestamp"]
    assert generator.summary()["event_type"] == event_type


def test_base_event_generator_cannot_be_instantiated(streaming_config):
    with pytest.raises(TypeError):
        BaseEventGenerator(streaming_config, RandomDataUtils(seed=42))


def test_playback_start_pause_and_skip_positions(streaming_config, user_ctx):
    ts = datetime(2026, 2, 1, 12, 0, 0)

    playback_start = PlaybackStartEventGenerator(
        streaming_config,
        RandomDataUtils(seed=42),
    ).generate_event(user_ctx, ts)
    pause = PauseEventGenerator(
        streaming_config,
        RandomDataUtils(seed=42),
    ).generate_event({**user_ctx, "playback_position_seconds": 120}, ts)
    skip = SkipEventGenerator(
        streaming_config,
        RandomDataUtils(seed=42),
    ).generate_event({**user_ctx, "playback_position_seconds": 5}, ts)

    assert playback_start["playback_position_seconds"] == 0
    assert pause["playback_position_seconds"] == 120
    assert skip["playback_position_seconds"] == 5


def test_ad_impression_schema_evolution(streaming_config, user_ctx):
    generator = AdImpressionEventGenerator(streaming_config, RandomDataUtils(seed=42))

    before_change = generator.generate_event(user_ctx, datetime(2026, 3, 31, 12, 0, 0))
    after_change = generator.generate_event(user_ctx, datetime(2026, 4, 1, 12, 0, 0))

    assert before_change["midpoint"] is None
    assert before_change["third_quartile"] is None
    assert after_change["midpoint"] is not None
    assert after_change["third_quartile"] is not None
    assert after_change["ad_campaign_id"].startswith("campaign_")


def test_late_arrivals_delay_created_timestamp(tmp_path, user_ctx):
    config_path = tmp_path / "late.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "late_arrival_rate": 1.0,
                "late_delay_min_max": [1, 1],
                "schema_change_date": "2026-04-01",
            }
        )
    )
    generator = PlaybackStartEventGenerator(
        GeneratorConfig(config_path),
        RandomDataUtils(seed=42),
    )

    event = generator.generate_event(user_ctx, datetime(2026, 2, 1, 12, 0, 0))

    assert event["created_ts"] - event["event_timestamp"] == 60 * 60 * 1000


def test_stream_data_generator_applies_bursts_and_duplicates(tmp_path, user_ctx):
    config_path = tmp_path / "stream.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "late_arrival_rate": 0.0,
                "late_delay_min_max": [1, 1],
                "duplicate_rate_stream": 1.0,
                "base_events_per_min": 2,
                "burst_multiplier": 3,
                "burst_windows": ["20:00-20:30"],
                "ad_click_rate": 0.0,
                "schema_change_date": "2026-04-01",
            }
        )
    )
    generator = StreamDataGenerator(
        config=GeneratorConfig(config_path),
        utils=RandomDataUtils(seed=42),
        output_path=str(tmp_path / "stream"),
    )

    events = generator.generate_events(
        start_ts=datetime(2026, 2, 1, 20, 0, 0),
        minutes=1,
        user_contexts=[user_ctx],
    )

    assert len(events) == 36
    assert len({event["event_id"] for event in events}) == 18


def test_stream_data_generator_writes_hourly_avro(tmp_path, streaming_config, user_ctx):
    generator = StreamDataGenerator(
        config=streaming_config,
        utils=RandomDataUtils(seed=42),
        output_path=str(tmp_path / "stream"),
        schema=DataSchema(),
    )
    events = generator.generate_events(
        start_ts=datetime(2026, 2, 1, 12, 0, 0),
        minutes=1,
        user_contexts=[user_ctx],
    )

    paths = generator.save_avro(events)

    assert set(paths) == {"2026020112"}
    output_file = Path(paths["2026020112"])
    assert output_file.exists()
    assert output_file.name == "events.avro"
    assert output_file.read_bytes().startswith(b"Obj\x01")
    assert generator.summary()["schema"] == "StreamingEvent"
    assert set(generator.summary()["schema_fields"]) == STREAMING_FIELDS
    assert generator.summary()["event_generators"]["playback_start"]["event_type"] == (
        "playback_start"
    )
    assert generator.summary()["event_generators"]["ad_click"]["event_type"] == "ad_click"


def test_stream_data_generator_emits_ad_click_after_ad_impression(tmp_path, user_ctx):
    config_path = tmp_path / "stream.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "late_arrival_rate": 0.0,
                "late_delay_min_max": [1, 1],
                "duplicate_rate_stream": 0.0,
                "base_events_per_min": 1,
                "burst_multiplier": 1,
                "burst_windows": [],
                "ad_click_rate": 1.0,
                "schema_change_date": "2026-04-01",
            }
        )
    )
    generator = StreamDataGenerator(
        config=GeneratorConfig(config_path),
        utils=RandomDataUtils(seed=42),
        output_path=str(tmp_path / "stream"),
        schema=DataSchema(),
    )

    events = generator.generate_events(
        start_ts=datetime(2026, 2, 1, 12, 0, 0),
        minutes=1,
        user_contexts=[user_ctx],
    )

    ad_impression = next(
        event for event in events if event["event_type"] == "ad_impression"
    )
    ad_click = next(event for event in events if event["event_type"] == "ad_click")

    assert ad_click["event_timestamp"] > ad_impression["event_timestamp"]
    assert ad_click["user_id"] == ad_impression["user_id"]
    assert ad_click["session_id"] == ad_impression["session_id"]
    assert ad_click["video_id"] == ad_impression["video_id"]
    assert ad_click["ad_campaign_id"] == ad_impression["ad_campaign_id"]


def test_stream_data_generator_emits_session_consistent_event_chains(
    tmp_path,
    streaming_config,
    user_ctx,
):
    generator = StreamDataGenerator(
        config=streaming_config,
        utils=RandomDataUtils(seed=42),
        output_path=str(tmp_path / "stream"),
        schema=DataSchema(),
    )

    events = generator.generate_events(
        start_ts=datetime(2026, 2, 1, 12, 0, 0),
        minutes=1,
        user_contexts=[user_ctx],
    )
    events_by_session = {}
    for event in events:
        events_by_session.setdefault(event["session_id"], []).append(event)

    for session_events in events_by_session.values():
        playback_starts = [
            event for event in session_events if event["event_type"] == "playback_start"
        ]
        assert playback_starts
        playback_video = playback_starts[0]["video_id"]
        playback_user = playback_starts[0]["user_id"]

        for event in session_events:
            assert event["user_id"] == playback_user
            if event["event_type"] != "subscription_cancel":
                assert event["video_id"] == playback_video


def test_stream_data_generator_rejects_events_that_do_not_match_schema(
    tmp_path,
    streaming_config,
):
    generator = StreamDataGenerator(
        config=streaming_config,
        utils=RandomDataUtils(seed=42),
        output_path=str(tmp_path / "stream"),
        schema=DataSchema(),
    )
    invalid_event = {field: None for field in STREAMING_FIELDS}

    with pytest.raises(ValueError, match="must not be null"):
        generator.save_avro([invalid_event])
