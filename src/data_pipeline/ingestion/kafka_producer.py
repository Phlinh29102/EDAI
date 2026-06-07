"""Publish generated streaming Avro events to Kafka as JSON messages."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple


DEFAULT_STREAM_PATH = Path("data/streaming")
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "media_events"


def iter_stream_events(stream_path: str | Path) -> Iterator[Dict[str, Any]]:
    """Yield events from all generated Avro files below stream_path."""
    root = Path(stream_path)
    if not root.exists():
        raise FileNotFoundError(f"Streaming path does not exist: {root}")

    avro_files = sorted(root.rglob("*.avro")) if root.is_dir() else [root]
    if not avro_files:
        raise FileNotFoundError(f"No .avro files found under: {root}")

    for avro_file in avro_files:
        yield from _read_avro_file(avro_file)


def publish_events(
    events: Iterable[Dict[str, Any]],
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
) -> int:
    """Publish events to Kafka with key=user_id and JSON value."""
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'confluent-kafka'. Install project dependencies "
            "or add it with: uv add confluent-kafka"
        ) from exc

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    published = 0

    def _delivery_callback(error, message) -> None:
        del message
        if error is not None:
            raise RuntimeError(f"Kafka delivery failed: {error}")

    for event in events:
        user_id = event.get("user_id")
        if user_id is None:
            raise ValueError(f"Event missing user_id: {event}")

        producer.produce(
            topic=topic,
            key=str(user_id).encode("utf-8"),
            value=json.dumps(event, separators=(",", ":"), sort_keys=True).encode(
                "utf-8"
            ),
            callback=_delivery_callback,
        )
        producer.poll(0)
        published += 1

    producer.flush()
    return published


def publish_stream_path(
    stream_path: str | Path = DEFAULT_STREAM_PATH,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    limit: int | None = None,
) -> int:
    """Read generated stream Avro files and publish events to Kafka."""
    events = iter_stream_events(stream_path)
    if limit is not None:
        events = _limit_events(events, limit)
    return publish_events(events, bootstrap_servers=bootstrap_servers, topic=topic)


def main() -> None:
    """Parse CLI args and publish stream events to Kafka."""
    parser = argparse.ArgumentParser(
        description="Publish generated media stream events to Kafka as JSON.",
    )
    parser.add_argument(
        "--stream-path",
        default=str(DEFAULT_STREAM_PATH),
        help="Path to data/streaming directory or a single .avro file.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic to publish to.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of events to publish.",
    )
    args = parser.parse_args()

    count = publish_stream_path(
        stream_path=args.stream_path,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        limit=args.limit,
    )
    print(f"Published {count} events to {args.topic}")


def _limit_events(
    events: Iterable[Dict[str, Any]],
    limit: int,
) -> Iterator[Dict[str, Any]]:
    """Yield at most *limit* events from the iterable."""
    if limit < 0:
        raise ValueError("limit must be non-negative")

    for index, event in enumerate(events):
        if index >= limit:
            break
        yield event


def _read_avro_file(path: Path) -> List[Dict[str, Any]]:
    """Read all records from a single Avro object container file."""
    data = path.read_bytes()
    offset = 0
    if data[:4] != b"Obj\x01":
        raise ValueError(f"Not an Avro object container file: {path}")
    offset = 4

    metadata, offset = _decode_map(data, offset)
    schema = json.loads(metadata["avro.schema"].decode("utf-8"))
    sync_marker = data[offset : offset + 16]
    offset += 16

    events: List[Dict[str, Any]] = []
    while offset < len(data):
        block_count, offset = _decode_long(data, offset)
        block_size, offset = _decode_long(data, offset)
        block_end = offset + block_size
        block = data[offset:block_end]
        block_offset = 0
        for _ in range(block_count):
            event, block_offset = _decode_record(block, block_offset, schema)
            events.append(event)
        offset = block_end
        if data[offset : offset + 16] != sync_marker:
            raise ValueError(f"Invalid Avro sync marker in {path}")
        offset += 16

    return events


def _decode_map(data: bytes, offset: int) -> Tuple[Dict[str, bytes], int]:
    """Decode an Avro map with bytes values starting at offset."""
    values: Dict[str, bytes] = {}
    while True:
        block_count, offset = _decode_long(data, offset)
        if block_count == 0:
            return values, offset
        if block_count < 0:
            block_count = -block_count
            _, offset = _decode_long(data, offset)
        for _ in range(block_count):
            key, offset = _decode_string(data, offset)
            value, offset = _decode_bytes(data, offset)
            values[key] = value


def _decode_record(
    data: bytes,
    offset: int,
    schema: Dict[str, Any],
) -> Tuple[Dict[str, Any], int]:
    """Decode a single Avro record according to schema."""
    record: Dict[str, Any] = {}
    for field in schema["fields"]:
        record[field["name"]], offset = _decode_value(data, offset, field["type"])
    return record, offset


def _decode_value(data: bytes, offset: int, schema_type: Any) -> Tuple[Any, int]:
    """Decode a single Avro value matching schema_type."""
    if isinstance(schema_type, list):
        union_index, offset = _decode_long(data, offset)
        selected_type = schema_type[union_index]
        if selected_type == "null":
            return None, offset
        return _decode_value(data, offset, selected_type)

    if isinstance(schema_type, dict):
        return _decode_value(data, offset, schema_type["type"])
    if schema_type == "string":
        return _decode_string(data, offset)
    if schema_type in {"int", "long"}:
        return _decode_long(data, offset)
    if schema_type == "boolean":
        return bool(data[offset]), offset + 1
    raise TypeError(f"Unsupported Avro schema type: {schema_type}")


def _decode_string(data: bytes, offset: int) -> Tuple[str, int]:
    """Decode an Avro utf-8 string starting at offset."""
    value, offset = _decode_bytes(data, offset)
    return value.decode("utf-8"), offset


def _decode_bytes(data: bytes, offset: int) -> Tuple[bytes, int]:
    """Decode an Avro bytes value starting at offset."""
    size, offset = _decode_long(data, offset)
    end = offset + size
    return data[offset:end], end


def _decode_long(data: bytes, offset: int) -> Tuple[int, int]:
    """Decode an Avro zig-zag varint long starting at offset."""
    shift = 0
    result = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    value = (result >> 1) ^ -(result & 1)
    return value, offset


if __name__ == "__main__":
    main()
