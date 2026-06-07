"""Tests for the phase-2 data_pipeline package structure."""
from __future__ import annotations

import importlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from data_pipeline.bronze.ingest_streaming import (
    StreamingBronzeIngestor,
    build_bronze_record,
    write_bronze_batch,
)
from data_pipeline.ingestion.schema_registry import LocalSchemaRegistry


EXPECTED_MODULES = [
    "data_pipeline.ingestion.kafka_producer",
    "data_pipeline.ingestion.schema_registry",
    "data_pipeline.ingestion.topic_admin",
    "data_pipeline.bronze.ingest_offline",
    "data_pipeline.bronze.ingest_streaming",
    "data_pipeline.silver.stg_users",
    "data_pipeline.silver.stg_videos",
    "data_pipeline.silver.stg_playback_history",
    "data_pipeline.silver.stg_interactions",
    "data_pipeline.silver.stg_ad_impressions",
    "data_pipeline.silver.stg_stream_events",
    "data_pipeline.gold.dim_users",
    "data_pipeline.gold.dim_videos",
    "data_pipeline.gold.dim_advertisers",
    "data_pipeline.gold.dim_dates",
    "data_pipeline.gold.fact_playback_watch",
    "data_pipeline.gold.fact_interaction",
    "data_pipeline.gold.fact_ad_impression",
    "data_pipeline.gold.fact_stream_event",
    "data_pipeline.gold.obt_user_media_activity_daily",
    "data_pipeline.gold.obt_video_performance_daily",
    "data_pipeline.features.feat_user_90d",
    "data_pipeline.features.feat_stream_60m",
    "data_pipeline.features.feat_unified",
    "data_pipeline.quality.checks",
    "data_pipeline.quality.report",
    "data_pipeline.lineage.datahub_emitter",
    "data_pipeline.common.spark_session",
    "data_pipeline.common.delta_utils",
    "data_pipeline.common.config",
]


def test_data_pipeline_phase_2_modules_import():
    for module_name in EXPECTED_MODULES:
        assert importlib.import_module(module_name)


def test_streaming_bronze_new_package_entrypoint(tmp_path):
    payload = {
        "event_id": "event_1",
        "event_type": "playback_start",
        "event_timestamp": 1,
        "created_ts": 2,
        "user_id": "user_1",
        "session_id": "session_1",
        "video_id": "video_1",
    }

    class Message:
        def value(self):
            return json.dumps(payload).encode("utf-8")

        def key(self):
            return b"user_1"

        def topic(self):
            return "media_events"

        def partition(self):
            return 0

        def offset(self):
            return 7

    record = build_bronze_record(Message())
    output_file = Path(write_bronze_batch([record], tmp_path / "raw_media_events"))
    written = pq.read_table(output_file).to_pandas()

    assert output_file.exists()
    assert written.loc[0, "source_offset"] == "media_events:0:7"

    ingestor = StreamingBronzeIngestor(output_path=tmp_path / "raw_media_events")
    assert ingestor.summary()["ingestor"] == "StreamingBronzeIngestor"


def test_local_schema_registry_registers_generated_schemas(tmp_path):
    registry = LocalSchemaRegistry(tmp_path / "schema_registry")
    paths = registry.register_generated_schemas()

    assert "streaming_event" in paths
    assert "offline_users" in paths
    assert registry.get("streaming_event")["name"] == "StreamingEvent"
