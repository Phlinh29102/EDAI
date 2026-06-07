"""Tests for Bronze ingestion utilities."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from data_generator.bronze.offline_ingest import OfflineBronzeIngestor
from kafka.consumer import build_bronze_record, write_bronze_batch


def test_offline_bronze_ingestor_preserves_rows_and_adds_metadata(tmp_path):
    source_path = tmp_path / "offline"
    bronze_path = tmp_path / "bronze"
    source_path.mkdir()

    source_df = pd.DataFrame(
        {
            "user_id": ["user_1", "user_2"],
            "country": ["US", "GB"],
        }
    )
    source_file = source_path / "users.parquet"
    source_df.to_parquet(source_file, index=False)

    ingestor = OfflineBronzeIngestor(
        source_path=source_path,
        output_path=bronze_path,
        tables=["users"],
    )
    paths = ingestor.ingest_all(batch_id="batch_test")

    assert set(paths) == {"users"}
    output_path = Path(paths["users"])
    assert output_path.exists()
    assert output_path.name == "batch-batch_test.parquet"
    assert output_path.parent.parent.name == "raw_users"

    bronze_df = pq.read_table(output_path).to_pandas()
    assert list(bronze_df["user_id"]) == ["user_1", "user_2"]
    assert list(bronze_df["country"]) == ["US", "GB"]
    assert set(
        [
            "ingest_ts",
            "source_file",
            "source_table",
            "batch_id",
            "source_row_number",
        ]
    ).issubset(bronze_df.columns)
    assert bronze_df["source_file"].eq(str(source_file)).all()
    assert bronze_df["source_table"].eq("users").all()
    assert bronze_df["batch_id"].eq("batch_test").all()
    assert list(bronze_df["source_row_number"]) == [0, 1]

    summary = ingestor.summary()
    assert summary["row_counts"]["users"] == 2


def test_kafka_consumer_bronze_record_and_writer(tmp_path):
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
            return 1

        def offset(self):
            return 42

    record = build_bronze_record(Message())
    assert record["raw_payload"] == json.dumps(payload)
    assert record["kafka_topic"] == "media_events"
    assert record["kafka_partition"] == 1
    assert record["kafka_offset"] == 42
    assert record["source_offset"] == "media_events:1:42"
    assert record["kafka_key"] == "user_1"
    assert record["parse_error"] is None
    assert record["event_id"] == "event_1"

    output_file = Path(write_bronze_batch([record], tmp_path / "raw_media_events"))
    assert output_file.exists()
    assert output_file.parent.name.startswith("ingest_hour=")

    written = pq.read_table(output_file).to_pandas()
    assert len(written) == 1
    assert written.loc[0, "raw_payload"] == json.dumps(payload)
    assert written.loc[0, "source_offset"] == "media_events:1:42"
