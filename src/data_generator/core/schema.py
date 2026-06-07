"""DataSchema - define Parquet and Avro schemas."""
from typing import Any, Dict
import pyarrow as pa

class DataSchema:
    def __init__(self):
        self.offline_schema = {
            "users": {
                "user_id": {"type": "string", "nullable": False},
                "user_age": {"type": "int32", "nullable": False},
                "country": {"type": "string", "nullable": False},
                "user_subscription": {"type": "string", "nullable": False},
                "signup_ts": {"type": "timestamp_ms", "nullable": False},
            },
            "videos": {
                "video_id": {"type": "string", "nullable": False},
                "video_title": {"type": "string", "nullable": False},
                "video_genre": {"type": "string", "nullable": False},
                "video_duration": {"type": "int32", "nullable": False},
                "upload_date": {"type": "date32", "nullable": False},
            },
            "playback_history": {
                "session_id": {"type": "string", "nullable": False},
                "history_id": {"type": "string", "nullable": False},
                "user_id": {"type": "string", "nullable": False},
                "video_id": {"type": "string", "nullable": False},
                "playback_date": {"type": "date32", "nullable": False},
                "watch_hours": {"type": "float64", "nullable": False},
            },
            "interactions": {
                "interaction_id": {"type": "string", "nullable": False},
                "user_id": {"type": "string", "nullable": False},
                "video_id": {"type": "string", "nullable": False},
                "interaction_type": {"type": "string", "nullable": False},
                "likes": {"type": "bool", "nullable": True},
            },
            "ad_impressions": {
                "impression_id": {"type": "string", "nullable": False},
                "user_id": {"type": "string", "nullable": False},
                "video_id": {"type": "string", "nullable": False},
                "advertiser_id": {"type": "string", "nullable": False},
                "cost_nanos": {"type": "int64", "nullable": False},
                "midpoint": {"type": "bool", "nullable": True},
                "third_quartile": {"type": "bool", "nullable": True},
                "clicked": {"type": "bool", "nullable": False},
            },
        }

        self.streaming_schema = {
            "type": "record",
            "name": "StreamingEvent",
            "namespace": "data_generator.events",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {
                    "name": "event_timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {
                    "name": "created_ts",
                    "type": {"type": "long", "logicalType": "timestamp-millis"},
                },
                {"name": "user_id", "type": "string"},
                {"name": "session_id", "type": "string"},
                {"name": "device_type", "type": "string"},
                {"name": "platform", "type": "string"},
                {"name": "video_id", "type": ["null", "string"], "default": None},
                {"name": "genre_id", "type": ["null", "string"], "default": None},
                {
                    "name": "playback_position_seconds",
                    "type": ["null", "int"],
                    "default": None,
                },
                {"name": "ad_campaign_id", "type": ["null", "string"], "default": None},
                {"name": "midpoint", "type": ["null", "boolean"], "default": None},
                {"name": "third_quartile", "type": ["null", "boolean"], "default": None},
            ],
        }
    def get_offline_schema(self, table: str | None = None) -> dict:
        """
        Get the offline schema for a specific table or all offline tables.
        Args:
            table (str | None): The name of the table (e.g., 'users', 'videos').
                                 If None, return all offline schemas.
        Returns:
            dict: The schema definition for the specified table or all offline schemas.
        """
        if table is None:
            return self.offline_schema

        if table not in self.offline_schema:
            raise KeyError(f"Unknown offline table: {table}")

        return self.offline_schema[table]
    def get_streaming_schema(self) -> Dict[str, Any]:
        """
        Get the streaming schema.
        Returns:
            Dict[str, Any]: The Avro schema definition for streaming events.
        """
        return self.streaming_schema
    
    def to_pyarrow_schema(self, table: str) -> pa.Schema:
        type_mapping = {
            "string": pa.string(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "float64": pa.float64(),
            "bool": pa.bool_(),
            "date32": pa.date32(),
            "timestamp_ms": pa.timestamp("ms"),
        }
        if table not in self.offline_schema:
            raise KeyError(f"Unknown offline table: {table}")

        fields = []
        for name, spec in self.offline_schema[table].items():
            fields.append(
                pa.field(
                    name,
                    type_mapping[spec["type"]],
                    nullable=spec["nullable"],
                )
            )
        return pa.schema(fields)
