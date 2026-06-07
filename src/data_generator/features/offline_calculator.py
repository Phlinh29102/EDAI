"""OfflineFeatureCalculator - compute 90-day stable user features."""
from datetime import date, timedelta
from typing import Any, Dict

import numpy as np
import pandas as pd


class OfflineFeatureCalculator:
    """Compute user-level offline features from generated Parquet tables."""

    FEATURE_COLUMNS = [
        "f_user_total_watch_hours_90d",
        "f_user_distinct_genres_90d",
        "f_user_historical_ad_ctr_90d",
        "f_user_subscription_churn_risk_90d",
    ]

    def __init__(self, window_days: int = 90) -> None:
        if window_days <= 0:
            raise ValueError("window_days must be positive")
        self.window_days = window_days
        self.last_summary: Dict[str, Any] = {}

    def compute(self, df: Dict[str, pd.DataFrame], as_of_date: date) -> pd.DataFrame:
        """
        Compute offline features keyed by user_id.

        Args:
            df (Dict[str, pd.DataFrame]): Offline tables keyed by table name.
            as_of_date (date): Inclusive end date for the stable feature window.

        Returns:
            pd.DataFrame: One row per user with 90-day offline features.
        """
        users = df.get("users", pd.DataFrame(columns=["user_id"]))
        playback = df.get("playback_history", pd.DataFrame())
        videos = df.get("videos", pd.DataFrame())
        ad_impressions = df.get("ad_impressions", pd.DataFrame())

        feature_df = pd.DataFrame({"user_id": users["user_id"].drop_duplicates()})
        playback_90d = self._filter_playback_window(playback, as_of_date)

        total_watch = self._total_watch_hours(playback_90d)
        distinct_genres = self._distinct_genres(playback_90d, videos)
        ctr = self._historical_ad_ctr(ad_impressions, as_of_date)
        churn_risk = self._churn_risk(feature_df, playback_90d, as_of_date)

        for features in [total_watch, distinct_genres, ctr, churn_risk]:
            feature_df = feature_df.merge(features, on="user_id", how="left")

        fill_values = {
            "f_user_total_watch_hours_90d": 0.0,
            "f_user_distinct_genres_90d": 0,
            "f_user_historical_ad_ctr_90d": 0.0,
            "f_user_subscription_churn_risk_90d": 1.0,
        }
        feature_df = feature_df.fillna(fill_values)
        feature_df["f_user_distinct_genres_90d"] = feature_df[
            "f_user_distinct_genres_90d"
        ].astype("int64")

        self.last_summary = {
            "calculator": self.__class__.__name__,
            "window_days": self.window_days,
            "as_of_date": as_of_date.isoformat(),
            "row_count": len(feature_df),
        }
        return feature_df[["user_id"] + self.FEATURE_COLUMNS]

    def summary(self) -> Dict[str, Any]:
        return self.last_summary or {
            "calculator": self.__class__.__name__,
            "window_days": self.window_days,
            "row_count": 0,
        }

    def _filter_playback_window(
        self,
        playback: pd.DataFrame,
        as_of_date: date,
    ) -> pd.DataFrame:
        if playback.empty:
            return playback.copy()

        start_date = as_of_date - timedelta(days=self.window_days - 1)
        playback = playback.copy()
        playback["playback_date"] = pd.to_datetime(playback["playback_date"]).dt.date
        return playback[
            (playback["playback_date"] >= start_date)
            & (playback["playback_date"] <= as_of_date)
        ].copy()

    def _total_watch_hours(self, playback: pd.DataFrame) -> pd.DataFrame:
        if playback.empty:
            return pd.DataFrame(
                columns=["user_id", "f_user_total_watch_hours_90d"]
            )

        return (
            playback.groupby("user_id", as_index=False)["watch_hours"]
            .sum()
            .rename(columns={"watch_hours": "f_user_total_watch_hours_90d"})
        )

    def _distinct_genres(
        self,
        playback: pd.DataFrame,
        videos: pd.DataFrame,
    ) -> pd.DataFrame:
        if playback.empty or videos.empty:
            return pd.DataFrame(columns=["user_id", "f_user_distinct_genres_90d"])

        joined = playback[["user_id", "video_id"]].merge(
            videos[["video_id", "video_genre"]],
            on="video_id",
            how="left",
        )
        return (
            joined.groupby("user_id", as_index=False)["video_genre"]
            .nunique(dropna=True)
            .rename(columns={"video_genre": "f_user_distinct_genres_90d"})
        )

    def _historical_ad_ctr(
        self,
        ad_impressions: pd.DataFrame,
        as_of_date: date,
    ) -> pd.DataFrame:
        if ad_impressions.empty:
            return pd.DataFrame(columns=["user_id", "f_user_historical_ad_ctr_90d"])

        impressions = ad_impressions.copy()
        if "playback_date" in impressions.columns:
            start_date = as_of_date - timedelta(days=self.window_days - 1)
            impressions["playback_date"] = pd.to_datetime(
                impressions["playback_date"]
            ).dt.date
            impressions = impressions[
                (impressions["playback_date"] >= start_date)
                & (impressions["playback_date"] <= as_of_date)
            ]

        if impressions.empty:
            return pd.DataFrame(columns=["user_id", "f_user_historical_ad_ctr_90d"])

        if "clicked" in impressions.columns:
            clicks = impressions["clicked"].fillna(False).astype(bool)
        else:
            clicks = pd.Series(False, index=impressions.index)

        ctr_input = impressions.assign(_clicked=clicks.astype(np.int64))
        grouped = ctr_input.groupby("user_id")["_clicked"].agg(["sum", "count"])
        grouped["f_user_historical_ad_ctr_90d"] = grouped["sum"] / grouped["count"]
        return grouped[["f_user_historical_ad_ctr_90d"]].reset_index()

    def _churn_risk(
        self,
        users: pd.DataFrame,
        playback: pd.DataFrame,
        as_of_date: date,
    ) -> pd.DataFrame:
        if users.empty:
            return pd.DataFrame(
                columns=["user_id", "f_user_subscription_churn_risk_90d"]
            )

        if playback.empty:
            return users.assign(f_user_subscription_churn_risk_90d=1.0)[
                ["user_id", "f_user_subscription_churn_risk_90d"]
            ]

        activity = playback.groupby("user_id").agg(
            last_playback_date=("playback_date", "max"),
            session_count=("history_id", "nunique"),
            watch_hours=("watch_hours", "sum"),
        )
        activity["days_since_last"] = activity["last_playback_date"].apply(
            lambda value: (as_of_date - value).days
        )
        recency_component = (activity["days_since_last"] / self.window_days).clip(0, 1)
        frequency_component = 1.0 - (activity["session_count"] / 20.0).clip(0, 1)
        engagement_component = 1.0 - (activity["watch_hours"] / 20.0).clip(0, 1)
        activity["f_user_subscription_churn_risk_90d"] = (
            0.50 * recency_component
            + 0.30 * frequency_component
            + 0.20 * engagement_component
        ).clip(0, 1)

        return users[["user_id"]].merge(
            activity[["f_user_subscription_churn_risk_90d"]].reset_index(),
            on="user_id",
            how="left",
        ).fillna({"f_user_subscription_churn_risk_90d": 1.0})
