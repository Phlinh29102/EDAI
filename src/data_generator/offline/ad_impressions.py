"""AdImpressionGenerator - schema evolution for VAST tracking fields."""
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


class AdImpressionGenerator:
    """Generate the offline ad_impressions fact table."""

    def __init__(
        self,
        advertiser_id_pool: int,
        cost_nanos_range: Tuple[int, int],
        schema_change_date: date,
        user_ids: Optional[List[str]] = None,
        video_ids: Optional[List[str]] = None,
        playback_history_df: Optional[pd.DataFrame] = None,
        n_impressions: Optional[int] = None,
        ad_click_rate: float = 0.03,
        legacy_partition_ratio: float = 0.60,
        playback_start: date = date(2025, 4, 1),
        playback_end: date = date(2026, 6, 7),
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize the AdImpressionGenerator.

        Args:
            advertiser_id_pool (int): Number of unique advertiser IDs to generate.
            cost_nanos_range (Tuple[int, int]): Minimum and maximum cost in nanos.
            schema_change_date (date): Date when midpoint/third_quartile became available.
            user_ids (Optional[List[str]]): Existing user IDs for referential integrity.
            video_ids (Optional[List[str]]): Existing video IDs for referential integrity.
            playback_history_df (Optional[pd.DataFrame]): Playback sessions to sample
                ad impression user/video/date rows from.
            n_impressions (Optional[int]): Total ad impressions to generate.
            ad_click_rate (float): Probability that an impression results in a click.
            legacy_partition_ratio (float): Target share of impressions before the
                schema change date.
            playback_start (date): Earliest playback date.
            playback_end (date): Latest playback date.
            seed (Optional[int]): Random seed for reproducible data generation.
        """
        if advertiser_id_pool <= 0:
            raise ValueError("advertiser_id_pool must be positive")
        if cost_nanos_range[0] <= 0:
            raise ValueError("cost_nanos_range minimum must be positive")
        if cost_nanos_range[0] > cost_nanos_range[1]:
            raise ValueError(
                "cost_nanos_range minimum must be less than or equal to maximum"
            )
        if n_impressions is not None and n_impressions < 0:
            raise ValueError("n_impressions must be non-negative")
        if not 0.0 <= ad_click_rate <= 1.0:
            raise ValueError("ad_click_rate must be between 0.0 and 1.0")
        if not 0.0 <= legacy_partition_ratio <= 1.0:
            raise ValueError("legacy_partition_ratio must be between 0.0 and 1.0")
        if playback_start > playback_end:
            raise ValueError("playback_start must be before or equal to playback_end")

        self.advertiser_id_pool = advertiser_id_pool
        self.cost_nanos_range = cost_nanos_range
        self.schema_change_date = schema_change_date
        self.user_ids = user_ids
        self.video_ids = video_ids
        self.playback_history_df = playback_history_df
        self.n_impressions = n_impressions
        self.ad_click_rate = ad_click_rate
        self.legacy_partition_ratio = legacy_partition_ratio
        self.playback_start = playback_start
        self.playback_end = playback_end
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        if user_ids is not None and not user_ids:
            raise ValueError("user_ids must not be empty")
        if video_ids is not None and not video_ids:
            raise ValueError("video_ids must not be empty")
        if playback_history_df is not None:
            missing_columns = {"user_id", "video_id", "playback_date"} - set(
                playback_history_df.columns
            )
            if missing_columns:
                raise ValueError(
                    f"playback_history_df missing columns: {sorted(missing_columns)}"
                )

    def generate(self) -> pd.DataFrame:
        """
        Generate the ad impressions DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing ad impression records with columns:
                          impression_id, user_id, video_id, advertiser_id,
                          cost_nanos, playback_date, midpoint, and third_quartile.
        """
        playback_rows = self._sample_playback_rows()
        user_ids = self.user_ids or self._default_user_ids()
        video_ids = self.video_ids or self._default_video_ids()
        n_impressions = (
            self.n_impressions
            if self.n_impressions is not None
            else self._default_impression_count(user_ids)
        )

        if n_impressions == 0:
            return pd.DataFrame(
                {
                    "impression_id": pd.Series(dtype="string"),
                    "user_id": pd.Series(dtype="string"),
                    "video_id": pd.Series(dtype="string"),
                    "advertiser_id": pd.Series(dtype="string"),
                    "cost_nanos": pd.Series(dtype="int64"),
                    "playback_date": pd.Series(dtype="object"),
                    "midpoint": pd.Series(dtype="boolean"),
                    "third_quartile": pd.Series(dtype="boolean"),
                    "clicked": pd.Series(dtype="boolean"),
                }
            )

        if playback_rows is not None:
            sampled_users = playback_rows["user_id"].to_numpy()
            sampled_videos = playback_rows["video_id"].to_numpy()
            playback_dates = playback_rows["playback_date"].reset_index(drop=True)
        else:
            sampled_users = self.rng.choice(user_ids, size=n_impressions)
            sampled_videos = self.rng.choice(video_ids, size=n_impressions)
            playback_dates = self._generate_playback_dates(n_impressions)
        midpoint, third_quartile = self._generate_tracking_fields(playback_dates)
        clicked = self.rng.random(size=n_impressions) < self.ad_click_rate

        return pd.DataFrame(
            {
                "impression_id": [
                    f"impression_{idx:010d}" for idx in range(1, n_impressions + 1)
                ],
                "user_id": sampled_users,
                "video_id": sampled_videos,
                "advertiser_id": self.rng.choice(
                    self._advertiser_ids(),
                    size=n_impressions,
                ),
                "cost_nanos": self.rng.integers(
                    self.cost_nanos_range[0],
                    self.cost_nanos_range[1] + 1,
                    size=n_impressions,
                    dtype=np.int64,
                ),
                "playback_date": playback_dates,
                "midpoint": midpoint,
                "third_quartile": third_quartile,
                "clicked": clicked,
            }
        )

    def summary(self) -> Dict[str, object]:
        """
        Provide a summary of the generator's configuration.

        Returns:
            Dict[str, object]: Configuration settings used for generation.
        """
        return {
            "generator": self.__class__.__name__,
            "advertiser_id_pool": self.advertiser_id_pool,
            "cost_nanos_range": self.cost_nanos_range,
            "schema_change_date": self.schema_change_date.isoformat(),
            "n_users": len(self.user_ids) if self.user_ids is not None else None,
            "n_videos": len(self.video_ids) if self.video_ids is not None else None,
            "uses_playback_history": self.playback_history_df is not None,
            "n_impressions": self.n_impressions,
            "ad_click_rate": self.ad_click_rate,
            "legacy_partition_ratio": self.legacy_partition_ratio,
            "playback_start": self.playback_start.isoformat(),
            "playback_end": self.playback_end.isoformat(),
            "seed": self.seed,
        }

    def _advertiser_ids(self) -> List[str]:
        return [
            f"advertiser_{idx:05d}"
            for idx in range(1, self.advertiser_id_pool + 1)
        ]

    def _generate_playback_dates(self, size: int) -> pd.Series:
        legacy_count = int(round(size * self.legacy_partition_ratio))
        current_count = size - legacy_count
        legacy_dates = self._sample_dates_before_schema_change(legacy_count)
        current_dates = self._sample_dates_on_or_after_schema_change(current_count)
        dates = legacy_dates + current_dates
        if not dates:
            return pd.Series(dtype="object")

        shuffled_positions = self.rng.permutation(len(dates))
        return pd.Series([dates[int(position)] for position in shuffled_positions])

    def _sample_playback_rows(self) -> Optional[pd.DataFrame]:
        if self.playback_history_df is None:
            return None

        n_impressions = (
            self.n_impressions
            if self.n_impressions is not None
            else len(self.playback_history_df)
        )
        if n_impressions == 0:
            return pd.DataFrame({"user_id": [], "video_id": [], "playback_date": []})

        playback_rows = self.playback_history_df[
            ["user_id", "video_id", "playback_date"]
        ].reset_index(drop=True)
        legacy_rows = playback_rows[
            playback_rows["playback_date"] < self.schema_change_date
        ]
        current_rows = playback_rows[
            playback_rows["playback_date"] >= self.schema_change_date
        ]

        legacy_count = int(round(n_impressions * self.legacy_partition_ratio))
        current_count = n_impressions - legacy_count
        sampled_parts = []

        if not legacy_rows.empty:
            sampled_parts.append(self._sample_rows(legacy_rows, legacy_count))
        elif legacy_count:
            current_count += legacy_count

        if not current_rows.empty:
            sampled_parts.append(self._sample_rows(current_rows, current_count))
        elif current_count:
            sampled_parts.append(self._sample_rows(legacy_rows, current_count))

        sampled = pd.concat(sampled_parts, ignore_index=True)
        shuffled_positions = self.rng.permutation(len(sampled))
        return sampled.iloc[shuffled_positions].reset_index(drop=True)

    def _sample_rows(self, df: pd.DataFrame, size: int) -> pd.DataFrame:
        if size == 0:
            return df.iloc[[]].copy()

        positions = self.rng.choice(
            len(df),
            size=size,
            replace=size > len(df),
        )
        return df.iloc[positions].reset_index(drop=True)

    def _sample_dates_before_schema_change(self, size: int) -> List[date]:
        if size == 0:
            return []

        end_date = min(self.playback_end, self.schema_change_date - pd.Timedelta(days=1))
        if self.playback_start > end_date:
            return self._sample_dates_on_or_after_schema_change(size)

        return self._sample_dates(self.playback_start, end_date, size)

    def _sample_dates_on_or_after_schema_change(self, size: int) -> List[date]:
        if size == 0:
            return []

        start_date = max(self.playback_start, self.schema_change_date)
        if start_date > self.playback_end:
            return self._sample_dates_before_schema_change(size)

        return self._sample_dates(start_date, self.playback_end, size)

    def _sample_dates(self, start_date: date, end_date: date, size: int) -> List[date]:
        start_day = pd.Timestamp(start_date).toordinal()
        end_day = pd.Timestamp(end_date).toordinal()
        days = self.rng.integers(start_day, end_day + 1, size=size)
        return [date.fromordinal(int(day)) for day in days]

    def _generate_tracking_fields(
        self,
        playback_dates: pd.Series,
    ) -> Tuple[pd.Series, pd.Series]:
        midpoint: List[Optional[bool]] = []
        third_quartile: List[Optional[bool]] = []

        for playback_date in playback_dates:
            if playback_date < self.schema_change_date:
                midpoint.append(None)
                third_quartile.append(None)
                continue

            reached_midpoint = bool(self.rng.random() < 0.72)
            midpoint.append(reached_midpoint)
            third_quartile.append(bool(reached_midpoint and self.rng.random() < 0.68))

        return (
            pd.Series(midpoint, dtype="boolean"),
            pd.Series(third_quartile, dtype="boolean"),
        )

    def _default_user_ids(self) -> List[str]:
        size = self.n_impressions if self.n_impressions is not None else 1_000
        return [f"user_{idx:08d}" for idx in range(1, size + 1)]

    def _default_video_ids(self) -> List[str]:
        return [f"video_{idx:08d}" for idx in range(1, 1_001)]

    def _default_impression_count(self, user_ids: List[str]) -> int:
        if self.playback_history_df is not None:
            return len(self.playback_history_df)
        return len(user_ids) * 2
