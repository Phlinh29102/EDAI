"""StreamingFeatureCalculator - compute rolling user features from events."""
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable

import pandas as pd


class StreamingFeatureCalculator:
    """Compute rolling streaming features from unified event dictionaries."""

    FEATURE_COLUMNS = [
        "f_stream_videos_started_30m",
        "f_stream_ad_completion_ratio_60m",
        "f_stream_early_skip_rate_60m",
        "f_stream_burst_activity_flag",
    ]

    def __init__(
        self,
        burst_threshold_events_60m: int = 100,
        early_skip_seconds: int = 10,
    ) -> None:
        if burst_threshold_events_60m <= 0:
            raise ValueError("burst_threshold_events_60m must be positive")
        if early_skip_seconds < 0:
            raise ValueError("early_skip_seconds must be non-negative")

        self.burst_threshold_events_60m = burst_threshold_events_60m
        self.early_skip_seconds = early_skip_seconds
        self.last_summary: Dict[str, Any] = {}

    def compute(
        self,
        events: Iterable[Dict[str, Any]],
        window_end: datetime,
    ) -> pd.DataFrame:
        event_df = pd.DataFrame(list(events))
        if event_df.empty:
            return self._empty_result(window_end)

        event_df = event_df.copy()
        window_end_ms = int(window_end.timestamp() * 1000)
        window_60m_start_ms = int(
            (window_end - timedelta(minutes=60)).timestamp() * 1000
        )
        window_30m_start_ms = int(
            (window_end - timedelta(minutes=30)).timestamp() * 1000
        )
        events_60m = event_df[
            (event_df["event_timestamp"] > window_60m_start_ms)
            & (event_df["event_timestamp"] <= window_end_ms)
        ].copy()
        events_30m = event_df[
            (event_df["event_timestamp"] > window_30m_start_ms)
            & (event_df["event_timestamp"] <= window_end_ms)
        ].copy()

        users = pd.DataFrame(
            {"user_id": sorted(events_60m["user_id"].dropna().unique())}
        )
        if users.empty:
            return self._empty_result(window_end)

        features = users
        for frame in [
            self._videos_started(events_30m),
            self._ad_completion_ratio(events_60m),
            self._early_skip_rate(events_60m),
            self._burst_flag(events_60m),
        ]:
            features = features.merge(frame, on="user_id", how="left")

        features = features.fillna(
            {
                "f_stream_videos_started_30m": 0,
                "f_stream_ad_completion_ratio_60m": 0.0,
                "f_stream_early_skip_rate_60m": 0.0,
                "f_stream_burst_activity_flag": 0,
            }
        )
        features["f_stream_videos_started_30m"] = features[
            "f_stream_videos_started_30m"
        ].astype("int64")
        features["f_stream_burst_activity_flag"] = features[
            "f_stream_burst_activity_flag"
        ].astype("int64")

        self.last_summary = {
            "calculator": self.__class__.__name__,
            "window_end": window_end.isoformat(),
            "row_count": len(features),
            "event_count_60m": len(events_60m),
            "burst_threshold_events_60m": self.burst_threshold_events_60m,
        }
        return features[["user_id"] + self.FEATURE_COLUMNS]

    def summary(self) -> Dict[str, Any]:
        return self.last_summary or {
            "calculator": self.__class__.__name__,
            "row_count": 0,
            "burst_threshold_events_60m": self.burst_threshold_events_60m,
            "early_skip_seconds": self.early_skip_seconds,
        }

    def _empty_result(self, window_end: datetime) -> pd.DataFrame:
        self.last_summary = {
            "calculator": self.__class__.__name__,
            "window_end": window_end.isoformat(),
            "row_count": 0,
            "event_count_60m": 0,
            "burst_threshold_events_60m": self.burst_threshold_events_60m,
        }
        return pd.DataFrame(columns=["user_id"] + self.FEATURE_COLUMNS)

    def _videos_started(self, events: pd.DataFrame) -> pd.DataFrame:
        starts = events[events["event_type"] == "playback_start"]
        return (
            starts.groupby("user_id")
            .size()
            .rename("f_stream_videos_started_30m")
            .reset_index()
        )

    def _ad_completion_ratio(self, events: pd.DataFrame) -> pd.DataFrame:
        impressions = events[events["event_type"] == "ad_impression"].copy()
        if impressions.empty:
            return pd.DataFrame(
                columns=["user_id", "f_stream_ad_completion_ratio_60m"]
            )

        impressions["_completed"] = (
            impressions["midpoint"].fillna(False).astype(bool)
            | impressions["third_quartile"].fillna(False).astype(bool)
        ).astype("int64")
        grouped = impressions.groupby("user_id")["_completed"].agg(["sum", "count"])
        grouped["f_stream_ad_completion_ratio_60m"] = (
            grouped["sum"] / grouped["count"]
        )
        return grouped[["f_stream_ad_completion_ratio_60m"]].reset_index()

    def _early_skip_rate(self, events: pd.DataFrame) -> pd.DataFrame:
        users = pd.DataFrame({"user_id": events["user_id"].dropna().unique()})
        starts = (
            events[events["event_type"] == "playback_start"]
            .groupby("user_id")
            .size()
            .rename("_starts")
            .reset_index()
        )
        skips = events[
            (events["event_type"] == "skip")
            & (
                events["playback_position_seconds"].fillna(
                    self.early_skip_seconds + 1
                )
                <= self.early_skip_seconds
            )
        ]
        early_skips = (
            skips.groupby("user_id").size().rename("_early_skips").reset_index()
        )
        rate = users.merge(starts, on="user_id", how="left").merge(
            early_skips,
            on="user_id",
            how="left",
        )
        rate[["_starts", "_early_skips"]] = rate[
            ["_starts", "_early_skips"]
        ].fillna(0)
        rate["f_stream_early_skip_rate_60m"] = rate.apply(
            lambda row: 0.0
            if row["_starts"] == 0
            else min(row["_early_skips"] / row["_starts"], 1.0),
            axis=1,
        )
        return rate[["user_id", "f_stream_early_skip_rate_60m"]]

    def _burst_flag(self, events: pd.DataFrame) -> pd.DataFrame:
        event_counts = events.groupby("user_id").size().rename("_event_count")
        result = event_counts.reset_index()
        result["f_stream_burst_activity_flag"] = (
            result["_event_count"] >= self.burst_threshold_events_60m
        ).astype("int64")
        return result[["user_id", "f_stream_burst_activity_flag"]]
