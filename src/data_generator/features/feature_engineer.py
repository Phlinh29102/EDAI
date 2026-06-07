"""FeatureEngineer - load sources, compute, merge, and save feature tables."""
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_generator.core.config import GeneratorConfig
from data_generator.core.schema import DataSchema
from data_generator.features.offline_calculator import OfflineFeatureCalculator
from data_generator.features.streaming_calculator import StreamingFeatureCalculator


class FeatureEngineer:
    """Build unified user feature tables from offline Parquet and streaming Avro."""

    OFFLINE_TABLES = [
        "users",
        "videos",
        "playback_history",
        "interactions",
        "ad_impressions",
    ]

    def __init__(self, config: GeneratorConfig) -> None:
        data_dir = config.get("data_dir", {})
        self.config = config
        self.offline_source_path = data_dir.get("offline", "data/offline")
        self.stream_source_path = data_dir.get("streaming", "data/streaming")
        self.output_path = data_dir.get("features", "data/features")
        self.schema = DataSchema()
        self.offline_calculator = OfflineFeatureCalculator()
        self.streaming_calculator = StreamingFeatureCalculator(
            burst_threshold_events_60m=config.get(
                "feature_burst_threshold_events_60m",
                100,
            ),
            early_skip_seconds=config.get("feature_early_skip_seconds", 10),
        )
        self._offline_cache: Dict[str, pd.DataFrame] | None = None
        self._stream_cache: List[Dict[str, Any]] | None = None
        self._last_feature_path: str | None = None

    def load_offline(self) -> Dict[str, pd.DataFrame]:
        tables: Dict[str, pd.DataFrame] = {}
        root = Path(self.offline_source_path)
        for table_name in self.OFFLINE_TABLES:
            table_path = root / f"{table_name}.parquet"
            if table_path.exists():
                tables[table_name] = pq.read_table(table_path).to_pandas()
            else:
                tables[table_name] = pd.DataFrame()

        self._offline_cache = tables
        return tables

    def load_stream(self) -> Iterator[Dict[str, Any]]:
        root = Path(self.stream_source_path)
        if not root.exists():
            return iter(())

        def _iter_events() -> Iterator[Dict[str, Any]]:
            for avro_file in sorted(root.rglob("*.avro")):
                yield from self._read_avro_file(avro_file)

        return _iter_events()

    def compute_offline_features(self) -> pd.DataFrame:
        tables = self._offline_cache or self.load_offline()
        as_of_date = self._offline_as_of_date(tables)
        return self.offline_calculator.compute(tables, as_of_date)

    def compute_stream_features(self, window_end: datetime) -> pd.DataFrame:
        if self._stream_cache is None:
            self._stream_cache = list(self.load_stream())
        return self.streaming_calculator.compute(self._stream_cache, window_end)

    def merge_features(self, window_end: datetime) -> pd.DataFrame:
        offline_features = self.compute_offline_features()
        stream_features = self.compute_stream_features(window_end)
        merged = offline_features.merge(stream_features, on="user_id", how="outer")

        fill_values = {
            "f_user_total_watch_hours_90d": 0.0,
            "f_user_distinct_genres_90d": 0,
            "f_user_historical_ad_ctr_90d": 0.0,
            "f_user_subscription_churn_risk_90d": 1.0,
            "f_stream_videos_started_30m": 0,
            "f_stream_ad_completion_ratio_60m": 0.0,
            "f_stream_early_skip_rate_60m": 0.0,
            "f_stream_burst_activity_flag": 0,
        }
        merged = merged.fillna(fill_values)
        merged["feature_ts"] = pd.Timestamp(window_end).floor("15min")
        return merged.sort_values("user_id").reset_index(drop=True)

    def save_features(self, df: pd.DataFrame, ts: datetime) -> str:
        output_dir = Path(self.output_path) / f"refresh_ts={ts.strftime('%Y%m%d%H%M')}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "features.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), output_file)
        self._last_feature_path = str(output_file)
        return str(output_file)

    def summary(self) -> Dict[str, Any]:
        return {
            "engineer": self.__class__.__name__,
            "offline_source_path": self.offline_source_path,
            "stream_source_path": self.stream_source_path,
            "output_path": self.output_path,
            "last_feature_path": self._last_feature_path,
            "offline": self.offline_calculator.summary(),
            "streaming": self.streaming_calculator.summary(),
        }

    def _offline_as_of_date(self, tables: Dict[str, pd.DataFrame]) -> date:
        playback = tables.get("playback_history", pd.DataFrame())
        if not playback.empty and "playback_date" in playback.columns:
            return pd.to_datetime(playback["playback_date"]).dt.date.max()

        value = self.config.get(
            "feature_as_of_date",
            self.config.get("schema_change_date"),
        )
        if value is None:
            return datetime.utcnow().date()
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return datetime.fromisoformat(value).date()

    def _read_avro_file(self, path: Path) -> List[Dict[str, Any]]:
        data = path.read_bytes()
        offset = 0
        if data[:4] != b"Obj\x01":
            raise ValueError(f"Not an Avro object container file: {path}")
        offset = 4

        metadata, offset = self._decode_map(data, offset)
        schema = json.loads(metadata["avro.schema"].decode("utf-8"))
        sync_marker = data[offset : offset + 16]
        offset += 16

        events: List[Dict[str, Any]] = []
        while offset < len(data):
            block_count, offset = self._decode_long(data, offset)
            block_size, offset = self._decode_long(data, offset)
            block_end = offset + block_size
            block = data[offset:block_end]
            block_offset = 0
            for _ in range(block_count):
                event, block_offset = self._decode_record(block, block_offset, schema)
                events.append(event)
            offset = block_end
            if data[offset : offset + 16] != sync_marker:
                raise ValueError(f"Invalid Avro sync marker in {path}")
            offset += 16

        return events

    def _decode_map(self, data: bytes, offset: int) -> Tuple[Dict[str, bytes], int]:
        values: Dict[str, bytes] = {}
        while True:
            block_count, offset = self._decode_long(data, offset)
            if block_count == 0:
                return values, offset
            if block_count < 0:
                block_count = -block_count
                _, offset = self._decode_long(data, offset)
            for _ in range(block_count):
                key, offset = self._decode_string(data, offset)
                value, offset = self._decode_bytes(data, offset)
                values[key] = value

    def _decode_record(
        self,
        data: bytes,
        offset: int,
        schema: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], int]:
        record: Dict[str, Any] = {}
        for field in schema["fields"]:
            record[field["name"]], offset = self._decode_value(
                data,
                offset,
                field["type"],
            )
        return record, offset

    def _decode_value(
        self,
        data: bytes,
        offset: int,
        schema_type: Any,
    ) -> Tuple[Any, int]:
        if isinstance(schema_type, list):
            union_index, offset = self._decode_long(data, offset)
            selected_type = schema_type[union_index]
            if selected_type == "null":
                return None, offset
            return self._decode_value(data, offset, selected_type)

        if isinstance(schema_type, dict):
            return self._decode_value(data, offset, schema_type["type"])
        if schema_type == "string":
            return self._decode_string(data, offset)
        if schema_type in {"int", "long"}:
            return self._decode_long(data, offset)
        if schema_type == "boolean":
            return bool(data[offset]), offset + 1
        raise TypeError(f"Unsupported Avro schema type: {schema_type}")

    def _decode_string(self, data: bytes, offset: int) -> Tuple[str, int]:
        value, offset = self._decode_bytes(data, offset)
        return value.decode("utf-8"), offset

    def _decode_bytes(self, data: bytes, offset: int) -> Tuple[bytes, int]:
        size, offset = self._decode_long(data, offset)
        end = offset + size
        return data[offset:end], end

    def _decode_long(self, data: bytes, offset: int) -> Tuple[int, int]:
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
