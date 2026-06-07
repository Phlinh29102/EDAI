"""StreamDataGenerator - orchestrate streaming events and Avro output."""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
import json
import secrets
from pathlib import Path
from typing import Any, Dict, List, Optional

from data_generator.core.config import GeneratorConfig
from data_generator.core.schema import DataSchema
from data_generator.core.utils import RandomDataUtils
from data_generator.streaming.ad_click import AdClickEventGenerator
from data_generator.streaming.ad_impression import AdImpressionEventGenerator
from data_generator.streaming.pause import PauseEventGenerator
from data_generator.streaming.playback_start import PlaybackStartEventGenerator
from data_generator.streaming.skip import SkipEventGenerator
from data_generator.streaming.subscription_cancel import SubscriptionCancelEventGenerator


class StreamDataGenerator:
    """Generate unified streaming events and save them as hourly Avro partitions."""

    CHAIN_FOLLOWUP_WEIGHTS = {
        "pause": 0.65,
        "skip": 0.35,
    }

    def __init__(
        self,
        config: GeneratorConfig,
        utils: RandomDataUtils,
        output_path: str,
        schema: Optional[DataSchema] = None,
    ) -> None:
        self.config = config
        self.utils = utils
        self.output_path = output_path
        self.schema = schema or DataSchema()
        self.streaming_schema = self.schema.get_streaming_schema()
        self.streaming_fields = [
            field["name"] for field in self.streaming_schema["fields"]
        ]
        self.duplicate_rate = config.get("duplicate_rate_stream", 0.0)
        self.base_events_per_min = config.get("base_events_per_min", 100)
        self.burst_multiplier = config.get("burst_multiplier", 1)
        self.burst_windows = config.get("burst_windows", [])
        self.ad_click_rate = config.get("ad_click_rate", 0.03)
        if not 0.0 <= self.ad_click_rate <= 1.0:
            raise ValueError("ad_click_rate must be between 0.0 and 1.0")
        self.event_generators = {
            "playback_start": PlaybackStartEventGenerator(config, utils),
            "pause": PauseEventGenerator(config, utils),
            "skip": SkipEventGenerator(config, utils),
            "ad_impression": AdImpressionEventGenerator(config, utils),
            "ad_click": AdClickEventGenerator(config, utils),
            "subscription_cancel": SubscriptionCancelEventGenerator(config, utils),
        }

    def generate_events(
        self,
        start_ts: datetime,
        minutes: int,
        user_contexts: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        if minutes < 0:
            raise ValueError("minutes must be non-negative")

        contexts = user_contexts or [self._default_user_context()]
        events: List[Dict[str, Any]] = []

        for minute_offset in range(minutes):
            ts = start_ts + timedelta(minutes=minute_offset)
            event_count = self._events_for_minute(ts)
            for _ in range(event_count):
                context = dict(contexts[int(self.utils.rng.integers(0, len(contexts)))])
                events.extend(self._generate_event_chain(context, ts))

        return events

    def save_avro(self, events: List[Dict[str, Any]]) -> Dict[str, str]:
        partitions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for event in events:
            self._validate_event(event)
            hour = datetime.fromtimestamp(event["event_timestamp"] / 1000).strftime(
                "%Y%m%d%H"
            )
            partitions[hour].append(event)

        output_paths: Dict[str, str] = {}
        for hour, hour_events in partitions.items():
            partition_dir = Path(self.output_path) / f"hour={hour}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            output_file = partition_dir / "events.avro"
            self._write_avro_file(output_file, hour_events)
            output_paths[hour] = str(output_file)

        return output_paths

    def generate_and_save(
        self,
        start_ts: datetime,
        minutes: int,
        user_contexts: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, str]:
        return self.save_avro(self.generate_events(start_ts, minutes, user_contexts))

    def summary(self) -> Dict[str, Any]:
        return {
            "generator": self.__class__.__name__,
            "output_path": self.output_path,
            "base_events_per_min": self.base_events_per_min,
            "burst_multiplier": self.burst_multiplier,
            "burst_windows": self.burst_windows,
            "ad_click_rate": self.ad_click_rate,
            "duplicate_rate": self.duplicate_rate,
            "schema": self.streaming_schema["name"],
            "schema_fields": self.streaming_fields,
            "event_generators": {
                event_type: generator.summary()
                for event_type, generator in self.event_generators.items()
            },
        }

    def _events_for_minute(self, ts: datetime) -> int:
        if self._is_burst_minute(ts):
            return int(self.base_events_per_min * self.burst_multiplier)
        return int(self.base_events_per_min)

    def _is_burst_minute(self, ts: datetime) -> bool:
        current = ts.strftime("%H:%M")
        for window in self.burst_windows:
            start, end = window.split("-")
            if start <= current < end:
                return True
        return False

    def _generate_event_chain(
        self,
        context: Dict[str, Any],
        ts: datetime,
    ) -> List[Dict[str, Any]]:
        chain: List[Dict[str, Any]] = []

        playback_start = self.event_generators["playback_start"].generate_event(
            context,
            ts,
        )
        self._append_event_with_optional_duplicate(chain, playback_start)

        ad_ts = ts + timedelta(seconds=int(self.utils.rng.integers(5, 91)))
        ad_impression = self.event_generators["ad_impression"].generate_event(
            context,
            ad_ts,
        )
        self._append_event_with_optional_duplicate(chain, ad_impression)

        if self.utils.bernoulli(self.ad_click_rate):
            click_context = {
                **context,
                "ad_campaign_id": ad_impression["ad_campaign_id"],
            }
            click_ts = ad_ts + timedelta(seconds=int(self.utils.rng.integers(1, 11)))
            ad_click = self.event_generators["ad_click"].generate_event(
                click_context,
                click_ts,
            )
            self._append_event_with_optional_duplicate(chain, ad_click)

        playback_position = int(self.utils.rng.integers(10, 601))
        followup_context = {
            **context,
            "playback_position_seconds": playback_position,
        }
        followup_type = self._sample_weighted_event_type(self.CHAIN_FOLLOWUP_WEIGHTS)
        followup_ts = ts + timedelta(seconds=playback_position)
        followup = self.event_generators[followup_type].generate_event(
            followup_context,
            followup_ts,
        )
        self._append_event_with_optional_duplicate(chain, followup)

        if self.utils.bernoulli(self.config.get("churn_rate_baseline", 0.0)):
            cancel_ts = followup_ts + timedelta(seconds=1)
            cancel = self.event_generators["subscription_cancel"].generate_event(
                context,
                cancel_ts,
            )
            self._append_event_with_optional_duplicate(chain, cancel)

        return chain

    def _append_event_with_optional_duplicate(
        self,
        events: List[Dict[str, Any]],
        event: Dict[str, Any],
    ) -> None:
        self._validate_event(event)
        events.append(event)
        if self.utils.bernoulli(self.duplicate_rate):
            duplicate = self._duplicate_event(event)
            self._validate_event(duplicate)
            events.append(duplicate)

    def _duplicate_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        duplicate = deepcopy(event)
        duplicate["created_ts"] = int(duplicate["created_ts"]) + 1
        return duplicate

    def _sample_weighted_event_type(self, weights_by_event_type: Dict[str, float]) -> str:
        event_types = list(weights_by_event_type)
        weights = list(weights_by_event_type.values())
        total_weight = sum(weights)
        probabilities = [weight / total_weight for weight in weights]
        return str(self.utils.rng.choice(event_types, p=probabilities))

    def _validate_event(self, event: Dict[str, Any]) -> None:
        event_fields = set(event)
        schema_fields = set(self.streaming_fields)
        missing_fields = schema_fields - event_fields
        extra_fields = event_fields - schema_fields
        if missing_fields:
            raise ValueError(f"Missing streaming fields: {sorted(missing_fields)}")
        if extra_fields:
            raise ValueError(f"Unexpected streaming fields: {sorted(extra_fields)}")

        for field in self.streaming_schema["fields"]:
            name = field["name"]
            expected_type = field["type"]
            value = event[name]
            if value is None:
                if not self._allows_null(expected_type):
                    raise ValueError(f"Field {name} must not be null")
                continue
            if not self._matches_type(value, expected_type):
                raise TypeError(
                    f"Field {name} has invalid type: "
                    f"expected {expected_type}, got {type(value).__name__}"
                )

    def _allows_null(self, expected_type: Any) -> bool:
        return isinstance(expected_type, list) and "null" in expected_type

    def _matches_type(self, value: Any, expected_type: Any) -> bool:
        if isinstance(expected_type, list):
            return any(
                schema_type != "null" and self._matches_type(value, schema_type)
                for schema_type in expected_type
            )
        if isinstance(expected_type, dict):
            return self._matches_type(value, expected_type["type"])
        if expected_type == "string":
            return isinstance(value, str)
        if expected_type in {"int", "long"}:
            return isinstance(value, int) and not isinstance(value, bool)
        if expected_type == "boolean":
            return isinstance(value, bool)
        return True

    def _write_avro_file(self, output_file: Path, events: List[Dict[str, Any]]) -> None:
        sync_marker = secrets.token_bytes(16)
        block = b"".join(self._encode_record(event) for event in events)
        schema_json = json.dumps(self.streaming_schema, separators=(",", ":")).encode(
            "utf-8"
        )

        with output_file.open("wb") as f:
            f.write(b"Obj\x01")
            self._write_map(
                f,
                {
                    "avro.schema": schema_json,
                    "avro.codec": b"null",
                },
            )
            f.write(sync_marker)
            f.write(self._encode_long(len(events)))
            f.write(self._encode_long(len(block)))
            f.write(block)
            f.write(sync_marker)

    def _write_map(self, file_obj, values: Dict[str, bytes]) -> None:
        file_obj.write(self._encode_long(len(values)))
        for key, value in values.items():
            file_obj.write(self._encode_string(key))
            file_obj.write(self._encode_bytes(value))
        file_obj.write(self._encode_long(0))

    def _encode_record(self, event: Dict[str, Any]) -> bytes:
        encoded = bytearray()
        for field in self.streaming_schema["fields"]:
            encoded.extend(self._encode_value(event[field["name"]], field["type"]))
        return bytes(encoded)

    def _encode_value(self, value: Any, schema_type: Any) -> bytes:
        if isinstance(schema_type, list):
            if value is None:
                null_index = schema_type.index("null")
                return self._encode_long(null_index)
            for index, option in enumerate(schema_type):
                if option != "null" and self._matches_type(value, option):
                    return self._encode_long(index) + self._encode_value(value, option)
            raise TypeError(f"Value {value!r} does not match union {schema_type}")

        if isinstance(schema_type, dict):
            return self._encode_value(value, schema_type["type"])
        if schema_type == "string":
            return self._encode_string(value)
        if schema_type in {"int", "long"}:
            return self._encode_long(value)
        if schema_type == "boolean":
            return b"\x01" if value else b"\x00"
        raise TypeError(f"Unsupported Avro schema type: {schema_type}")

    def _encode_string(self, value: str) -> bytes:
        return self._encode_bytes(value.encode("utf-8"))

    def _encode_bytes(self, value: bytes) -> bytes:
        return self._encode_long(len(value)) + value

    def _encode_long(self, value: int) -> bytes:
        unsigned_value = (value << 1) ^ (value >> 63)
        encoded = bytearray()
        while unsigned_value & ~0x7F:
            encoded.append((unsigned_value & 0x7F) | 0x80)
            unsigned_value >>= 7
        encoded.append(unsigned_value)
        return bytes(encoded)

    def _default_user_context(self) -> Dict[str, Any]:
        user_id = int(self.utils.rng.integers(1, 10**8))
        session_id = int(self.utils.rng.integers(1, 10**10))
        video_id = int(self.utils.rng.integers(1, 10**8))
        genre_id = int(self.utils.rng.integers(1, 1_001))
        device_type = str(self.utils.rng.choice(["smart_tv", "web", "mobile_app"]))
        return {
            "user_id": f"user_{user_id:08d}",
            "session_id": f"session_{session_id:010d}",
            "device_type": device_type,
            "platform": device_type,
            "video_id": f"video_{video_id:08d}",
            "genre_id": f"genre_{genre_id:04d}",
        }
