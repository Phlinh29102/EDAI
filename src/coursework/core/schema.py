"""DataSchema - định nghĩa Parquet schema và Avro schema."""
import pyarrow as pa

class DataSchema:
    def __init__(self):
        self.offline_schema = {
            "users": pa.schema([
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("user_age", pa.int32(), nullable=False),
                pa.field("country", pa.string(), nullable=False),
                pa.field("user_subscription", pa.string(), nullable=False),
                pa.field("signup_ts", pa.timestamp('ms'), nullable=False),
            ]),
            "videos": pa.schema([
                pa.field("video_id", pa.string(), nullable=False),
                pa.field("video_title", pa.string(), nullable=False),
                pa.field("video_genre", pa.string(), nullable=False),
                pa.field("video_duration", pa.int32(), nullable=False),
                pa.field("upload_date", pa.date32(), nullable=False),
            ]),
            "playback_history": pa.schema([
                pa.field("session_id", pa.string(), nullable=False),
                pa.field("history_id", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("video_id", pa.string(), nullable=False),
                pa.field("playback_date", pa.date32(), nullable=False),
                pa.field("watch_hours", pa.float64(), nullable=False),
            ]),
            "interactions": pa.schema([
                pa.field("interaction_id", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("video_id", pa.string(), nullable=False),
                pa.field("interaction_type", pa.string(), nullable=False),
                pa.field("likes", pa.bool_(), nullable=True),
            ]),
            "ad_impressions": pa.schema([
                pa.field("impression_id", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("video_id", pa.string(), nullable=False),
                pa.field("advertiser_id", pa.string(), nullable=False),
                pa.field("cost_nanos", pa.int64(), nullable=False),
                pa.field("midpoint", pa.bool_(), nullable=True),
                pa.field("thirdQuartile", pa.bool_(), nullable=True),
            ]),
        }

        self.streaming_schema = {
            "type": "record",
            "name": "StreamingEvent",
            "namespace": "coursework.events",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "event_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "created_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "user_id", "type": "string"},
                {"name": "session_id", "type": "string"},
                {"name": "device_type", "type": "string"},
                {"name": "platform", "type": "string"},
                {"name": "video_id", "type": ["null", "string"], "default": "null"},
                {"name": "genre_id", "type": ["null", "string"], "default": "null"},
                {"name": "playback_position_seconds", "type": ["null", "int"], "default": "null"},
                {"name": "ad_campaign_id", "type": ["null", "string"], "default": "null"},
                {"name": "midpoint", "type": ["null", "boolean"], "default": "null"},
                {"name": "third_quartile", "type": ["null", "boolean"], "default": "null"},
            ]
        }
    def get_offline_schema(self) -> pa.Schema:
        return self.offline_schema
    def get_streaming_schema(self) -> dict:
        return self.streaming_schema
    def summary(self) -> str:
        return f"Offline schema:\n{self.offline_schema}\nStreaming schema:\n{self.streaming_schema}"