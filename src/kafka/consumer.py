"""Consume Kafka media events into the Bronze raw_media_events dataset."""
from __future__ import annotations

import argparse
import json
import signal
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "media_events"
DEFAULT_GROUP_ID = "media_events_bronze_ingest"
DEFAULT_OUTPUT_PATH = Path("data/bronze/raw_media_events")
DEFAULT_BATCH_SIZE = 1000
DEFAULT_POLL_TIMEOUT = 1.0


_STOP_REQUESTED = False


def consume_to_bronze(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    group_id: str = DEFAULT_GROUP_ID,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_messages: Optional[int] = None,
    poll_timeout: float = DEFAULT_POLL_TIMEOUT,
    auto_offset_reset: str = "earliest",
) -> int:
    """
    Consume Kafka JSON events and write append-only Bronze Parquet batches.

    Bronze records preserve the raw Kafka payload plus ingest metadata required
    by the schema design: ingest timestamp, Kafka topic, partition, offset, and
    source offset. Selected event fields are also copied for operational checks;
    Silver should still parse and validate the raw payload into the unified
    event schema.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_messages is not None and max_messages < 0:
        raise ValueError("max_messages must be non-negative")

    consumer_cls = _load_kafka_consumer()
    consumer = consumer_cls(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    output_root = Path(output_path)
    batch: List[Dict[str, Any]] = []
    consumed = 0

    _install_signal_handlers()
    try:
        while not _should_stop(consumed, max_messages):
            message = consumer.poll(poll_timeout)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                raise RuntimeError(f"Kafka consume failed: {error}")

            batch.append(build_bronze_record(message))
            consumed += 1

            if len(batch) >= batch_size:
                write_bronze_batch(batch, output_root)
                consumer.commit(asynchronous=False)
                batch.clear()

        if batch:
            write_bronze_batch(batch, output_root)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    return consumed


def build_bronze_record(message: Any) -> Dict[str, Any]:
    """Build one Bronze raw_media_events record from a Kafka message."""
    ingest_ts = datetime.now(timezone.utc)
    raw_payload = _decode_bytes(message.value())
    parsed_payload, parse_error = _parse_json_payload(raw_payload)
    key = _decode_bytes(message.key())
    topic = message.topic()
    partition = int(message.partition())
    offset = int(message.offset())

    record = {
        "raw_payload": raw_payload,
        "ingest_ts": ingest_ts,
        "source_file": None,
        "kafka_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "source_offset": f"{topic}:{partition}:{offset}",
        "kafka_key": key,
        "parse_error": parse_error,
        "event_id": _payload_value(parsed_payload, "event_id"),
        "event_type": _payload_value(parsed_payload, "event_type"),
        "event_timestamp": _payload_value(parsed_payload, "event_timestamp"),
        "created_ts": _payload_value(parsed_payload, "created_ts"),
        "user_id": _payload_value(parsed_payload, "user_id"),
        "session_id": _payload_value(parsed_payload, "session_id"),
        "video_id": _payload_value(parsed_payload, "video_id"),
    }
    return record


def write_bronze_batch(
    records: Iterable[Dict[str, Any]],
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
) -> str:
    """Write a Bronze batch as Parquet partitioned by ingest hour."""
    batch = list(records)
    if not batch:
        raise ValueError("records must not be empty")

    output_root = Path(output_path)
    ingest_hour = _batch_ingest_hour(batch)
    partition_dir = output_root / f"ingest_hour={ingest_hour}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"part-{uuid.uuid4().hex}.parquet"

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'pyarrow'. Install project dependencies before "
            "running the Kafka Bronze consumer."
        ) from exc

    table = pa.Table.from_pylist(batch, schema=_bronze_schema(pa))
    pq.write_table(table, file_path)
    return str(file_path)


def main() -> None:
    """Parse CLI args and consume Kafka media_events into Bronze storage."""
    parser = argparse.ArgumentParser(
        description="Consume Kafka media_events JSON messages into Bronze Parquet.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic to consume.",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help="Kafka consumer group id.",
    )
    parser.add_argument(
        "--output-path",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Bronze output path for raw_media_events Parquet files.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of messages per Parquet batch.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Optional maximum messages to consume before exiting.",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=DEFAULT_POLL_TIMEOUT,
        help="Kafka poll timeout in seconds.",
    )
    parser.add_argument(
        "--auto-offset-reset",
        choices=["earliest", "latest", "error"],
        default="earliest",
        help="Offset reset policy for a new consumer group.",
    )
    args = parser.parse_args()

    count = consume_to_bronze(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        output_path=args.output_path,
        batch_size=args.batch_size,
        max_messages=args.max_messages,
        poll_timeout=args.poll_timeout,
        auto_offset_reset=args.auto_offset_reset,
    )
    print(f"Consumed {count} events from {args.topic} into {args.output_path}")


def _load_kafka_consumer():
    """Lazy-import and return confluent_kafka.Consumer."""
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'confluent-kafka'. Install project dependencies "
            "or add it with: uv add confluent-kafka"
        ) from exc

    return Consumer


def _install_signal_handlers() -> None:
    """Install graceful shutdown handlers for long-running consumption."""

    def _request_stop(signum, frame) -> None:
        del signum, frame
        global _STOP_REQUESTED
        _STOP_REQUESTED = True

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)


def _should_stop(consumed: int, max_messages: Optional[int]) -> bool:
    if _STOP_REQUESTED:
        return True
    return max_messages is not None and consumed >= max_messages


def _decode_bytes(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    return value.decode("utf-8")


def _parse_json_payload(
    raw_payload: Optional[str],
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    if raw_payload is None:
        return None, "missing payload"
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        return None, str(exc)
    if not isinstance(payload, dict):
        return None, "payload is not a JSON object"
    return payload, None


def _payload_value(
    payload: Optional[Dict[str, Any]],
    field: str,
) -> Any:
    if payload is None:
        return None
    return payload.get(field)


def _batch_ingest_hour(records: List[Dict[str, Any]]) -> str:
    ingest_ts = records[0]["ingest_ts"]
    if not isinstance(ingest_ts, datetime):
        raise TypeError("ingest_ts must be a datetime")
    return ingest_ts.strftime("%Y%m%d%H")


def _bronze_schema(pa):
    """Return the raw_media_events Bronze schema."""
    return pa.schema(
        [
            pa.field("raw_payload", pa.string(), nullable=True),
            pa.field("ingest_ts", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("source_file", pa.string(), nullable=True),
            pa.field("kafka_topic", pa.string(), nullable=False),
            pa.field("kafka_partition", pa.int64(), nullable=False),
            pa.field("kafka_offset", pa.int64(), nullable=False),
            pa.field("source_offset", pa.string(), nullable=False),
            pa.field("kafka_key", pa.string(), nullable=True),
            pa.field("parse_error", pa.string(), nullable=True),
            pa.field("event_id", pa.string(), nullable=True),
            pa.field("event_type", pa.string(), nullable=True),
            pa.field("event_timestamp", pa.int64(), nullable=True),
            pa.field("created_ts", pa.int64(), nullable=True),
            pa.field("user_id", pa.string(), nullable=True),
            pa.field("session_id", pa.string(), nullable=True),
            pa.field("video_id", pa.string(), nullable=True),
        ]
    )


if __name__ == "__main__":
    main()
