"""Smoke test: run offline and streaming pipeline outputs with config/test.yaml."""
from datetime import datetime
from pathlib import Path
import yaml

import pyarrow.parquet as pq

from data_generator.core.config import GeneratorConfig
from data_generator.core.schema import DataSchema
from data_generator.core.utils import RandomDataUtils
from data_generator.offline.offline_data_generator import OfflineDataGenerator
from data_generator.streaming.stream_data_generator import StreamDataGenerator


def test_pipeline_smoke_with_test_config_without_features(tmp_path):
    with open("config/test.yaml") as f:
        config_data = yaml.safe_load(f)

    config_data["schema_change_date"] = "2026-04-01"
    config_data["playback_start"] = "2026-03-04"
    config_data["playback_end"] = "2026-04-08"
    config_data["start_date"] = "2026-03-04"
    config_data["end_date"] = "2026-04-08"
    test_config_path = tmp_path / "test.yaml"
    test_config_path.write_text(yaml.dump(config_data))
    config = GeneratorConfig(test_config_path)
    schema = DataSchema()
    offline_output_path = tmp_path / "offline"
    offline_generator = OfflineDataGenerator(
        config=config,
        schema=schema,
        utils=RandomDataUtils(seed=config.get("random_seed")),
        output_path=str(offline_output_path),
    )

    paths = offline_generator.generate_all()

    assert set(paths) == {
        "users",
        "videos",
        "playback_history",
        "interactions",
        "ad_impressions",
    }
    assert all(Path(path).exists() for path in paths.values())

    users = pq.read_table(paths["users"]).to_pandas()
    videos = pq.read_table(paths["videos"]).to_pandas()
    playback_history = pq.read_table(paths["playback_history"]).to_pandas()
    interactions = pq.read_table(paths["interactions"]).to_pandas()
    ad_impressions = pq.read_table(paths["ad_impressions"]).to_pandas()

    assert len(users) == config.get("n_users")
    assert len(videos) == config.get("n_videos")
    assert len(playback_history) == config.get("n_playback_sessions")
    assert len(interactions) == config.get("n_interactions")
    assert len(ad_impressions) == config.get("n_ad_impressions")

    valid_users = set(users["user_id"])
    valid_videos = set(videos["video_id"])
    for fact_df in [playback_history, interactions, ad_impressions]:
        assert set(fact_df["user_id"]).issubset(valid_users)
        assert set(fact_df["video_id"]).issubset(valid_videos)

    schema_change_date = config.get("schema_change_date")
    before_change = ad_impressions[
        ad_impressions["playback_date"].astype(str) < schema_change_date
    ]
    after_change = ad_impressions[
        ad_impressions["playback_date"].astype(str) >= schema_change_date
    ]
    assert not before_change.empty
    assert not after_change.empty
    assert before_change["midpoint"].isna().all()
    assert before_change["third_quartile"].isna().all()
    assert after_change["midpoint"].notna().all()
    assert after_change["third_quartile"].notna().all()

    summary = offline_generator.summary()
    assert summary["generator"] == "OfflineDataGenerator"
    assert summary["tables"]["users"]["row_count"] == config.get("n_users")

    user_contexts = _streaming_contexts(users, videos)
    stream_generator = StreamDataGenerator(
        config=config,
        utils=RandomDataUtils(seed=config.get("random_seed")),
        output_path=str(tmp_path / "streaming"),
        schema=schema,
    )

    stream_events = stream_generator.generate_events(
        start_ts=datetime(2026, 4, 5, 12, 0, 0),
        minutes=1,
        user_contexts=user_contexts,
    )
    stream_paths = stream_generator.save_avro(stream_events)

    assert set(stream_paths) == {"2026040512"}
    assert all(Path(path).read_bytes().startswith(b"Obj\x01") for path in stream_paths.values())
    assert len(stream_events) >= config.get("base_events_per_min") * 3

    streaming_fields = {
        field["name"] for field in schema.get_streaming_schema()["fields"]
    }
    assert all(set(event) == streaming_fields for event in stream_events)
    assert all(event["created_ts"] >= event["event_timestamp"] for event in stream_events)
    assert {event["event_type"] for event in stream_events}.issubset(
        {
            "playback_start",
            "pause",
            "skip",
            "ad_impression",
            "ad_click",
            "subscription_cancel",
        }
    )
    assert {event["user_id"] for event in stream_events}.issubset(valid_users)

    stream_video_ids = {
        event["video_id"] for event in stream_events if event["video_id"] is not None
    }
    assert stream_video_ids.issubset(valid_videos)

    stream_summary = stream_generator.summary()
    assert stream_summary["generator"] == "StreamDataGenerator"
    assert stream_summary["schema"] == "StreamingEvent"


def _streaming_contexts(users, videos):
    video_records = videos.to_dict("records")
    contexts = []
    for idx, user in enumerate(users.to_dict("records")):
        video = video_records[idx % len(video_records)]
        contexts.append(
            {
                "user_id": user["user_id"],
                "session_id": f"session_{idx + 1:010d}",
                "device_type": "web",
                "platform": "web",
                "video_id": video["video_id"],
                "genre_id": video["video_genre"],
            }
        )
    return contexts
