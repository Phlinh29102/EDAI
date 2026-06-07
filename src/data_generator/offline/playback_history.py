"""PlaybackHistoryGenerator - includes session_id."""
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

class PlaybackHistoryGenerator:
    """Generate the offline playback_history fact table."""

    def __init__(
        self,
        days_history: int,
        skew_ratio: float,
        duplicate_rate: float,
        user_ids: Optional[List[str]] = None,
        video_ids: Optional[List[str]] = None,
        n_sessions: Optional[int] = None,
        watches_per_session_range: Tuple[int, int] = (1, 4),
        playback_end: date = date(2026, 1, 31),
        watch_hours_range: Tuple[float, float] = (0.05, 4.0),
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize the PlaybackHistoryGenerator.

        Args:
            days_history (int): Number of historical days to generate playback for.
            skew_ratio (float): Zipf-like skew for video popularity.
            duplicate_rate (float): Proportion of records to duplicate.
            user_ids (Optional[List[str]]): Existing user IDs for referential integrity.
            video_ids (Optional[List[str]]): Existing video IDs for referential integrity.
            n_sessions (Optional[int]): Total playback rows to generate.
            watches_per_session_range (Tuple[int, int]): Min/max video watches sharing a
                session_id.
            playback_end (date): Last possible playback date.
            watch_hours_range (Tuple[float, float]): Minimum and maximum watch hours.
            seed (Optional[int]): Random seed for reproducible data generation.
        """
        if days_history <= 0:
            raise ValueError("days_history must be positive")
        if skew_ratio < 0:
            raise ValueError("skew_ratio must be non-negative")
        if not 0.0 <= duplicate_rate <= 1.0:
            raise ValueError("duplicate_rate must be between 0.0 and 1.0")
        if n_sessions is not None and n_sessions < 0:
            raise ValueError("n_sessions must be non-negative")
        if watches_per_session_range[0] <= 0:
            raise ValueError("watches_per_session_range minimum must be positive")
        if watches_per_session_range[0] > watches_per_session_range[1]:
            raise ValueError(
                "watches_per_session_range minimum must be less than or equal to maximum"
            )
        if watch_hours_range[0] <= 0:
            raise ValueError("watch_hours_range minimum must be positive")
        if watch_hours_range[0] > watch_hours_range[1]:
            raise ValueError(
                "watch_hours_range minimum must be less than or equal to maximum"
            )

        self.days_history = days_history
        self.skew_ratio = skew_ratio
        self.duplicate_rate = duplicate_rate
        self.user_ids = user_ids
        self.video_ids = video_ids
        self.n_sessions = n_sessions
        self.watches_per_session_range = watches_per_session_range
        self.playback_end = playback_end
        self.watch_hours_range = watch_hours_range
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        if user_ids is not None and not user_ids:
            raise ValueError("user_ids must not be empty")
        if video_ids is not None and not video_ids:
            raise ValueError("video_ids must not be empty")

    def generate(self) -> pd.DataFrame:
        """
        Generate the playback history DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing playback records with columns:
                          session_id, history_id, user_id, video_id,
                          playback_date, and watch_hours.
        """
        user_ids = self.user_ids or self._default_user_ids()
        video_ids = self.video_ids or self._default_video_ids()
        n_sessions = self.n_sessions if self.n_sessions is not None else len(user_ids) * 5

        if n_sessions == 0:
            return pd.DataFrame(
                {
                    "session_id": pd.Series(dtype="string"),
                    "history_id": pd.Series(dtype="string"),
                    "user_id": pd.Series(dtype="string"),
                    "video_id": pd.Series(dtype="string"),
                    "playback_date": pd.Series(dtype="object"),
                    "watch_hours": pd.Series(dtype="float64"),
                }
            )

        history_ids = [f"history_{idx:010d}" for idx in range(1, n_sessions + 1)]
        session_ids, sampled_users, playback_dates = self._generate_session_contexts(
            row_count=n_sessions,
            user_ids=user_ids,
        )
        sampled_videos = self.rng.choice(
            video_ids,
            size=n_sessions,
            replace=True,
            p=self._zipf_weights(len(video_ids)),
        )
        watch_hours = self.rng.uniform(
            self.watch_hours_range[0],
            self.watch_hours_range[1],
            size=n_sessions,
        )

        df = pd.DataFrame(
            {
                "session_id": session_ids,
                "history_id": history_ids,
                "user_id": sampled_users,
                "video_id": sampled_videos,
                "playback_date": playback_dates,
                "watch_hours": watch_hours.astype(np.float64),
            }
        )

        return self._inject_duplicates(df, key_cols=["history_id"])

    def summary(self) -> Dict[str, object]:
        """
        Provide a summary of the generator's configuration.

        Returns:
            Dict[str, object]: Configuration settings used for generation.
        """
        return {
            "generator": self.__class__.__name__,
            "days_history": self.days_history,
            "skew_ratio": self.skew_ratio,
            "duplicate_rate": self.duplicate_rate,
            "n_users": len(self.user_ids) if self.user_ids is not None else None,
            "n_videos": len(self.video_ids) if self.video_ids is not None else None,
            "n_sessions": self.n_sessions,
            "watches_per_session_range": self.watches_per_session_range,
            "playback_end": self.playback_end.isoformat(),
            "watch_hours_range": self.watch_hours_range,
            "seed": self.seed,
        }

    def _generate_session_contexts(
        self,
        row_count: int,
        user_ids: List[str],
    ) -> Tuple[List[str], List[str], List[date]]:
        session_ids: List[str] = []
        session_user_ids: List[str] = []
        session_dates: List[date] = []
        session_idx = 1
        playback_dates = self._generate_playback_dates(row_count)

        while len(session_ids) < row_count:
            watches_in_session = int(
                self.rng.integers(
                    self.watches_per_session_range[0],
                    self.watches_per_session_range[1] + 1,
                )
            )
            user_id = str(self.rng.choice(user_ids))
            playback_date = playback_dates.iloc[len(session_ids)]
            session_ids.extend([f"session_{session_idx:010d}"] * watches_in_session)
            session_user_ids.extend([user_id] * watches_in_session)
            session_dates.extend([playback_date] * watches_in_session)
            session_idx += 1

        session_ids = session_ids[:row_count]
        session_user_ids = session_user_ids[:row_count]
        session_dates = session_dates[:row_count]
        shuffled_positions = self.rng.permutation(row_count)
        return (
            [session_ids[int(position)] for position in shuffled_positions],
            [session_user_ids[int(position)] for position in shuffled_positions],
            [session_dates[int(position)] for position in shuffled_positions],
        )


    def _generate_playback_dates(self, size: int) -> pd.Series:
        start_date = self.playback_end - timedelta(days=self.days_history - 1)
        start_day = pd.Timestamp(start_date).toordinal()
        end_day = pd.Timestamp(self.playback_end).toordinal()
        days = self.rng.integers(start_day, end_day + 1, size=size)
        return pd.Series([date.fromordinal(int(day)) for day in days])

    def _zipf_weights(self, n: int) -> np.ndarray:
        ranks = np.arange(1, n + 1, dtype=np.float64)
        weights = 1.0 / np.power(ranks, self.skew_ratio)
        return weights / weights.sum()

    def _inject_duplicates(self, df: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
        if self.duplicate_rate == 0.0:
            return df.copy()

        duplicate_count = int(round(len(df) * self.duplicate_rate))
        if duplicate_count == 0:
            return df.copy()

        duplicate_positions = self.rng.choice(
            len(df),
            size=duplicate_count,
            replace=duplicate_count > len(df),
        )
        duplicates = df.iloc[duplicate_positions].copy()
        combined = pd.concat([df, duplicates], ignore_index=True)
        shuffled_positions = self.rng.permutation(len(combined))
        return combined.iloc[shuffled_positions].reset_index(drop=True)

    def _default_user_ids(self) -> List[str]:
        size = self.n_sessions if self.n_sessions is not None else 1_000
        return [f"user_{idx:08d}" for idx in range(1, size + 1)]

    def _default_video_ids(self) -> List[str]:
        return [f"video_{idx:08d}" for idx in range(1, 1_001)]
