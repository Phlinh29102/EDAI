"""QualityReport - profile data and generate quality reports."""
from collections import Counter
from datetime import datetime, timezone
from json import loads
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from coursework.core.config import GeneratorConfig


class QualityReport:
    """Profile generated data and produce quality metrics reports."""

    def __init__(
        self,
        output_path: str,
        config: Optional[GeneratorConfig] = None,
    ) -> None:
        self.output_path = output_path
        self.config = config
        self.last_report: str = ""
        self._last_metrics: Dict[str, Any] = {}

    def profile_offline_tables(self, table_name: str, path: str) -> Dict[str, Any]:
        path = Path(path)
        if path.is_dir():
            files = list(path.rglob("*.parquet"))
            if not files:
                return {"table": table_name, "error": "No parquet files found"}
            df = pq.read_table(path).to_pandas()
        elif path.is_file() and path.suffix == ".parquet":
            df = pq.read_table(path).to_pandas()
        else:
            return {"table": table_name, "error": f"Invalid path: {path}"}

        if df.empty:
            return {"table": table_name, "row_count": 0}

        metrics: Dict[str, Any] = {
            "table": table_name,
            "row_count": len(df),
            "column_count": len(df.columns),
        }

        if table_name == "users":
            metrics["distinct_users"] = int(df["user_id"].nunique())
            if "user_age" in df.columns:
                metrics["age"] = {
                    "min": int(df["user_age"].min()),
                    "max": int(df["user_age"].max()),
                    "mean": float(round(df["user_age"].mean(), 1)),
                }
            if "country" in df.columns:
                metrics["countries"] = {
                    "distinct": int(df["country"].nunique()),
                    "top": df["country"].value_counts().head(3).to_dict(),
                }
            if "user_subscription" in df.columns:
                metrics["subscription_distribution"] = (
                    df["user_subscription"].value_counts(normalize=True).mul(100).round(1).to_dict()
                )

        elif table_name == "videos":
            metrics["distinct_videos"] = int(df["video_id"].nunique())
            if "video_genre" in df.columns:
                genre_dist = df["video_genre"].value_counts(normalize=True).mul(100).round(1)
                metrics["genre_distribution"] = genre_dist.to_dict()
                top_genre_pct = float(genre_dist.iloc[0]) if not genre_dist.empty else 0.0
                metrics["genre_skew"] = {
                    "top_genre": str(genre_dist.index[0]) if not genre_dist.empty else None,
                    "top_genre_pct": top_genre_pct,
                    "bottom_3_pct": float(genre_dist.tail(3).sum()) if len(genre_dist) >= 3 else 0.0,
                }
            if "video_duration" in df.columns:
                metrics["duration"] = {
                    "min": int(df["video_duration"].min()),
                    "max": int(df["video_duration"].max()),
                    "mean": float(round(df["video_duration"].mean(), 1)),
                }

        elif table_name == "playback_history":
            metrics["distinct_users"] = int(df["user_id"].nunique())
            metrics["distinct_videos"] = int(df["video_id"].nunique())
            if "watch_hours" in df.columns:
                metrics["watch_hours"] = {
                    "total": float(round(df["watch_hours"].sum(), 1)),
                    "mean": float(round(df["watch_hours"].mean(), 2)),
                    "median": float(round(df["watch_hours"].median(), 2)),
                }
            if "video_id" in df.columns:
                video_counts = df["video_id"].value_counts()
                total_views = len(df)
                top_20_pct = video_counts.head(int(len(video_counts) * 0.2)).sum()
                metrics["popularity_skew"] = {
                    "top_20_pct_videos_share": float(round(top_20_pct / total_views * 100, 1))
                    if total_views > 0 else 0.0,
                    "total_distinct_videos": int(video_counts.nunique()),
                }
            if "history_id" in df.columns:
                before_dedup = int(df.duplicated(subset=["history_id"]).sum())
                metrics["duplicate_rate"] = {
                    "before_dedup_pct": float(round(before_dedup / len(df) * 100, 2)),
                    "after_dedup_pct": 0.0,
                    "dedup_key": "history_id",
                }

        elif table_name == "interactions":
            metrics["distinct_users"] = int(df["user_id"].nunique())
            if "interaction_type" in df.columns:
                metrics["interaction_distribution"] = (
                    df["interaction_type"].value_counts(normalize=True).mul(100).round(1).to_dict()
                )
            if "interaction_id" in df.columns:
                before_dedup = int(df.duplicated(subset=["interaction_id"]).sum())
                metrics["duplicate_rate"] = {
                    "before_dedup_pct": float(round(before_dedup / len(df) * 100, 2)),
                    "after_dedup_pct": 0.0,
                    "dedup_key": "interaction_id",
                }

        elif table_name == "ad_impressions":
            metrics["distinct_users"] = int(df["user_id"].nunique())
            metrics["distinct_videos"] = int(df["video_id"].nunique())
            metrics["distinct_advertisers"] = int(df["advertiser_id"].nunique())
            if "cost_nanos" in df.columns:
                metrics["cost_nanos"] = {
                    "min": int(df["cost_nanos"].min()),
                    "max": int(df["cost_nanos"].max()),
                    "mean": float(round(df["cost_nanos"].mean(), 1)),
                    "median": float(round(df["cost_nanos"].median(), 1)),
                }
            if "clicked" in df.columns:
                click_count = int(df["clicked"].sum())
                metrics["ctr"] = float(round(click_count / len(df) * 100, 2))
            if "playback_date" in df.columns and self.config is not None:
                schema_change = self.config.get("schema_change_date", "2026-04-01")
                if isinstance(schema_change, str):
                    schema_change_date = datetime.fromisoformat(schema_change).date()
                else:
                    schema_change_date = schema_change
                df["_date"] = pd.to_datetime(df["playback_date"]).dt.date
                before = df[df["_date"] < schema_change_date]
                after = df[df["_date"] >= schema_change_date]
                if not before.empty:
                    se = {
                        "legacy_row_count": len(before),
                        "current_row_count": len(after),
                        "legacy_ratio": float(round(len(before) / len(df) * 100, 1)),
                        "current_ratio": float(round(len(after) / len(df) * 100, 1)),
                    }
                    if "midpoint" in df.columns:
                        se["legacy_midpoint_null_pct"] = float(
                            round(before["midpoint"].isna().mean() * 100, 1)
                        )
                        se["current_midpoint_null_pct"] = float(
                            round(after["midpoint"].isna().mean() * 100, 1)
                        )
                    if "third_quartile" in df.columns:
                        se["legacy_third_quartile_null_pct"] = float(
                            round(before["third_quartile"].isna().mean() * 100, 1)
                        )
                        se["current_third_quartile_null_pct"] = float(
                            round(after["third_quartile"].isna().mean() * 100, 1)
                        )
                    metrics["schema_evolution"] = se

        self._last_metrics[f"offline_{table_name}"] = metrics
        return metrics

    def profile_stream(self, events_path: str) -> Dict[str, Any]:
        root = Path(events_path)
        if not root.exists():
            return {"error": f"Streaming path not found: {events_path}"}

        events: List[Dict[str, Any]] = []
        for avro_file in sorted(root.rglob("*.avro")):
            events.extend(self._read_avro(avro_file))

        if not events:
            return {"event_count": 0}

        df = pd.DataFrame(events)
        metrics: Dict[str, Any] = {
            "event_count": len(df),
            "time_range": {},
        }

        if "event_timestamp" in df.columns:
            ts_min = df["event_timestamp"].min()
            ts_max = df["event_timestamp"].max()
            metrics["time_range"] = {
                "start_ms": int(ts_min),
                "start_iso": datetime.fromtimestamp(ts_min / 1000).isoformat(),
                "end_ms": int(ts_max),
                "end_iso": datetime.fromtimestamp(ts_max / 1000).isoformat(),
                "span_hours": float(round((ts_max - ts_min) / 3_600_000, 2)),
            }

        if "event_type" in df.columns:
            type_dist = df["event_type"].value_counts()
            metrics["event_type_distribution"] = type_dist.to_dict()
            if len(df) > 0 and "event_timestamp" in df.columns:
                minute_bins = df["event_timestamp"] // 60_000
                events_per_min = minute_bins.value_counts()
                metrics["burst_analysis"] = {
                    "baseline_events_per_min": int(events_per_min.median()),
                    "peak_events_per_min": int(events_per_min.max()),
                    "burst_multiplier": float(
                        round(events_per_min.max() / events_per_min.median(), 1)
                    ) if events_per_min.median() > 0 else 1.0,
                }

        if "user_id" in df.columns:
            metrics["distinct_users"] = int(df["user_id"].nunique())
        if "session_id" in df.columns:
            metrics["distinct_sessions"] = int(df["session_id"].nunique())
        if "video_id" in df.columns:
            metrics["distinct_videos"] = int(df["video_id"].dropna().nunique())

        if "created_ts" in df.columns and "event_timestamp" in df.columns:
            late = df[df["created_ts"] > df["event_timestamp"]]
            metrics["late_arrival"] = {
                "rate_pct": float(round(len(late) / len(df) * 100, 2)),
                "count": len(late),
            }
            if not late.empty:
                delays_hours = (late["created_ts"] - late["event_timestamp"]) / 3_600_000
                metrics["late_arrival"]["delay_hours"] = {
                    "min": float(round(delays_hours.min(), 2)),
                    "max": float(round(delays_hours.max(), 2)),
                    "mean": float(round(delays_hours.mean(), 2)),
                }

        if "event_id" in df.columns:
            before_dedup = int(df.duplicated(subset=["event_id"]).sum())
            metrics["duplicate_rate"] = {
                "before_dedup_pct": float(round(before_dedup / len(df) * 100, 2)),
                "after_dedup_pct": 0.0,
                "dedup_key": "event_id",
            }

        if "playback_position_seconds" in df.columns:
            non_null = df["playback_position_seconds"].dropna()
            if not non_null.empty:
                metrics["playback_position"] = {
                    "min": float(non_null.min()),
                    "max": float(non_null.max()),
                    "mean": float(round(non_null.mean(), 1)),
                }

        if "ad_campaign_id" in df.columns:
            non_null = df["ad_campaign_id"].dropna()
            metrics["distinct_campaigns"] = int(non_null.nunique())

        self._last_metrics["streaming"] = metrics
        return metrics

    def profile_features(self, features_path: str) -> Dict[str, Any]:
        path = Path(features_path)
        if not path.exists():
            return {"error": f"Features path not found: {features_path}"}

        if path.is_dir():
            parquet_files = list(path.rglob("*.parquet"))
            if not parquet_files:
                return {"error": "No feature parquet files found"}
            df = pq.read_table(path).to_pandas()
        else:
            df = pq.read_table(path).to_pandas()

        if df.empty:
            return {"row_count": 0}

        metrics: Dict[str, Any] = {
            "row_count": len(df),
            "column_count": len(df.columns),
        }

        feature_cols = [col for col in df.columns if col.startswith("f_")]
        metrics["feature_column_count"] = len(feature_cols)

        for col in feature_cols:
            col_metrics: Dict[str, Any] = {}
            null_count = int(df[col].isna().sum())
            col_metrics["null_count"] = null_count
            col_metrics["null_pct"] = float(round(null_count / len(df) * 100, 2))

            if pd.api.types.is_numeric_dtype(df[col]):
                col_metrics["min"] = float(df[col].min()) if not df[col].isna().all() else None
                col_metrics["max"] = float(df[col].max()) if not df[col].isna().all() else None
                col_metrics["mean"] = float(round(df[col].mean(), 2)) if not df[col].isna().all() else None
                col_metrics["nonzero_count"] = int((df[col] != 0).sum())
                col_metrics["nonzero_pct"] = float(
                    round((df[col] != 0).sum() / len(df) * 100, 2)
                )

            if "feature_" not in col:
                metrics[col] = col_metrics

        if "feature_ts" in df.columns:
            metrics["feature_timestamp"] = {
                "min": str(df["feature_ts"].min()),
                "max": str(df["feature_ts"].max()),
            }

        self._last_metrics["features"] = metrics
        return metrics

    def generate_report(self, metrics: Dict[str, Any]) -> str:
        lines: List[str] = []
        lines.append("=" * 60)
        lines.append("QUALITY REPORT")
        lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
        lines.append("=" * 60)

        for section_key, section_data in metrics.items():
            if not isinstance(section_data, dict):
                continue

            lines.append("")
            lines.append(f"--- {section_key.upper()} ---")

            if "error" in section_data:
                lines.append(f"  ERROR: {section_data['error']}")
                continue

            if "row_count" in section_data:
                lines.append(f"  Rows: {section_data['row_count']:,}")

            if "event_count" in section_data:
                lines.append(f"  Events: {section_data['event_count']:,}")

            if "distinct_users" in section_data:
                lines.append(f"  Distinct users: {section_data['distinct_users']:,}")
            if "distinct_videos" in section_data:
                lines.append(f"  Distinct videos: {section_data['distinct_videos']:,}")

            if isinstance(section_data.get("duplicate_rate"), dict):
                dr = section_data["duplicate_rate"]
                lines.append(f"  Duplicate rate (key: {dr['dedup_key']}):")
                lines.append(f"    Before dedup: {dr['before_dedup_pct']}%")
                lines.append(f"    After dedup:  {dr['after_dedup_pct']}%")
            elif "duplicate_rate" in section_data:
                lines.append(f"  Duplicate rate: {section_data['duplicate_rate']}%")

            if "ctr" in section_data:
                lines.append(f"  CTR: {section_data['ctr']}%")

            if "schema_evolution" in section_data:
                se = section_data["schema_evolution"]
                lines.append(f"  Schema evolution:")
                lines.append(f"    Legacy partitions: {se['legacy_row_count']:,} ({se['legacy_ratio']}%)")
                lines.append(f"    Current partitions: {se['current_row_count']:,} ({se['current_ratio']}%)")
                if "legacy_midpoint_null_pct" in se:
                    lines.append(f"    Midpoint NULL (legacy): {se['legacy_midpoint_null_pct']}%")
                    lines.append(f"    Midpoint NULL (current): {se['current_midpoint_null_pct']}%")
                if "legacy_third_quartile_null_pct" in se:
                    lines.append(f"    Third-quartile NULL (legacy): {se['legacy_third_quartile_null_pct']}%")
                    lines.append(f"    Third-quartile NULL (current): {se['current_third_quartile_null_pct']}%")

            if "popularity_skew" in section_data:
                ps = section_data["popularity_skew"]
                lines.append(f"  Popularity skew:")
                lines.append(f"    Top 20% videos account for {ps['top_20_pct_videos_share']}% of views")

            if "late_arrival" in section_data:
                la = section_data["late_arrival"]
                lines.append(f"  Late arrivals: {la['rate_pct']}% ({la['count']:,} events)")
                if "delay_hours" in la:
                    dh = la["delay_hours"]
                    lines.append(f"    Delay hours -> min: {dh['min']}, max: {dh['max']}, mean: {dh['mean']}")

            if "burst_analysis" in section_data:
                ba = section_data["burst_analysis"]
                lines.append(f"  Burst analysis:")
                lines.append(f"    Baseline events/min: {ba['baseline_events_per_min']}")
                lines.append(f"    Peak events/min: {ba['peak_events_per_min']}")
                lines.append(f"    Burst multiplier: {ba['burst_multiplier']}x")

            if "event_type_distribution" in section_data:
                lines.append(f"  Event type distribution:")
                for etype, count in sorted(
                    section_data["event_type_distribution"].items(),
                    key=lambda x: -x[1],
                ):
                    pct = count / max(section_data.get("event_count", 1), 1) * 100
                    lines.append(f"    {etype}: {count:,} ({pct:.1f}%)")

            if "genre_skew" in section_data:
                gs = section_data["genre_skew"]
                lines.append(f"  Genre skew:")
                lines.append(f"    Top: {gs['top_genre']} ({gs['top_genre_pct']}%)")
                lines.append(f"    Bottom 3 genres combined: {gs['bottom_3_pct']}%")

            if "subscription_distribution" in section_data:
                lines.append(f"  Subscription distribution:")
                for tier, pct in section_data["subscription_distribution"].items():
                    lines.append(f"    {tier}: {pct}%")

            if "interaction_distribution" in section_data:
                lines.append(f"  Interaction type distribution:")
                for itype, pct in section_data["interaction_distribution"].items():
                    lines.append(f"    {itype}: {pct}%")

        lines.append("")
        lines.append("=" * 60)

        report = "\n".join(lines)
        self.last_report = report

        output_dir = Path(self.output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / "quality_report.txt"
        report_path.write_text(report)

        return report

    def summary(self) -> Dict[str, Any]:
        return {
            "report": self.__class__.__name__,
            "output_path": self.output_path,
            "last_report_path": str(Path(self.output_path) / "quality_report.txt"),
            "report_length": len(self.last_report),
            "sections_profiled": list(self._last_metrics.keys()),
        }

    def _read_avro(self, path: Path) -> List[Dict[str, Any]]:
        data = path.read_bytes()
        offset = 0
        if data[:4] != b"Obj\x01":
            raise ValueError(f"Not an Avro object container file: {path}")
        offset = 4

        metadata, offset = self._decode_map(data, offset)
        schema = loads(metadata["avro.schema"].decode("utf-8"))
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
                raise ValueError(f"Invalid sync marker in {path}")
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
            record[field["name"]], offset = self._decode_value(data, offset, field["type"])
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
