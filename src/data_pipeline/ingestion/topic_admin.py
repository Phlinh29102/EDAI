"""Kafka topic administration helpers for the media pipeline."""
from __future__ import annotations

import argparse
from typing import Dict, Iterable, List


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "media_events"
DEFAULT_PARTITIONS = 3
DEFAULT_REPLICATION_FACTOR = 1


def ensure_topic(
    topic: str = DEFAULT_TOPIC,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    num_partitions: int = DEFAULT_PARTITIONS,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    timeout: float = 30.0,
) -> bool:
    """Ensure a Kafka topic exists."""
    if num_partitions <= 0:
        raise ValueError("num_partitions must be positive")
    if replication_factor <= 0:
        raise ValueError("replication_factor must be positive")

    admin_client, new_topic_cls, kafka_exception_cls = _load_kafka_admin()
    admin = admin_client({"bootstrap.servers": bootstrap_servers})

    existing_topics = set(list_topics(bootstrap_servers, timeout=timeout))
    if topic in existing_topics:
        return False

    topic_spec = new_topic_cls(
        topic,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    futures = admin.create_topics([topic_spec], request_timeout=timeout)

    try:
        futures[topic].result(timeout=timeout)
    except kafka_exception_cls as exc:
        error = exc.args[0]
        if _is_topic_already_exists(error):
            return False
        raise

    return True


def ensure_topics(
    topics: Iterable[str],
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    num_partitions: int = DEFAULT_PARTITIONS,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    timeout: float = 30.0,
) -> Dict[str, bool]:
    """Ensure multiple topics exist, returning creation status by topic."""
    return {
        topic: ensure_topic(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            timeout=timeout,
        )
        for topic in topics
    }


def list_topics(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    timeout: float = 30.0,
) -> List[str]:
    """Return Kafka topic names visible to the admin client."""
    admin_client, _, _ = _load_kafka_admin()
    admin = admin_client({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=timeout)
    return sorted(metadata.topics)


def delete_topic(
    topic: str = DEFAULT_TOPIC,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    timeout: float = 30.0,
) -> bool:
    """Delete a Kafka topic if it exists."""
    admin_client, _, kafka_exception_cls = _load_kafka_admin()
    if topic not in set(list_topics(bootstrap_servers, timeout=timeout)):
        return False

    admin = admin_client({"bootstrap.servers": bootstrap_servers})
    futures = admin.delete_topics([topic], request_timeout=timeout)
    try:
        futures[topic].result(timeout=timeout)
    except kafka_exception_cls as exc:
        error = exc.args[0]
        if _is_unknown_topic(error):
            return False
        raise

    return True


def main() -> None:
    """Parse CLI args and administer Kafka topics."""
    parser = argparse.ArgumentParser(
        description="Create/list/delete Kafka topics for the media pipeline.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=DEFAULT_PARTITIONS,
        help="Number of partitions when creating a topic.",
    )
    parser.add_argument(
        "--replication-factor",
        type=int,
        default=DEFAULT_REPLICATION_FACTOR,
        help="Replication factor when creating a topic.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Kafka admin request timeout in seconds.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List topics instead of creating one.",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the topic instead of creating one.",
    )
    args = parser.parse_args()

    if args.list:
        for topic in list_topics(args.bootstrap_servers, timeout=args.timeout):
            print(topic)
        return

    if args.delete:
        deleted = delete_topic(
            topic=args.topic,
            bootstrap_servers=args.bootstrap_servers,
            timeout=args.timeout,
        )
        status = "deleted" if deleted else "not found"
        print(f"Topic {args.topic}: {status}")
        return

    created = ensure_topic(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        num_partitions=args.partitions,
        replication_factor=args.replication_factor,
        timeout=args.timeout,
    )
    status = "created" if created else "already exists"
    print(f"Topic {args.topic}: {status}")


def _load_kafka_admin():
    """Lazy-import and return confluent_kafka admin classes."""
    try:
        from confluent_kafka import KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'confluent-kafka'. Install project dependencies "
            "or add it with: uv add confluent-kafka"
        ) from exc

    return AdminClient, NewTopic, KafkaException


def _is_topic_already_exists(error) -> bool:
    """Return True if the error indicates the topic already exists."""
    code = error.code() if hasattr(error, "code") else None
    text = str(error)
    return (
        code == 36
        or "TOPIC_ALREADY_EXISTS" in text
        or "already exists" in text.lower()
    )


def _is_unknown_topic(error) -> bool:
    """Return True if the error indicates the topic does not exist."""
    code = error.code() if hasattr(error, "code") else None
    text = str(error)
    return (
        code in {3, 4}
        or "UNKNOWN_TOPIC" in text
        or "UNKNOWN_TOPIC_OR_PART" in text
        or "does not exist" in text.lower()
        or "not found" in text.lower()
    )


if __name__ == "__main__":
    main()
