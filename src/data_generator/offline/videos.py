"""VideoGenerator."""
from datetime import date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


class VideoGenerator:
    """Generate the offline videos dimension table."""

    DEFAULT_GENRES = [
        "Action",
        "Drama",
        "Comedy",
        "Documentary",
        "Thriller",
        "Romance",
        "Sci-Fi",
        "Animation",
        "Sports",
        "Kids",
    ]

    def __init__(
        self,
        n_videos: int,
        genre_skew: float = 1.0,
        genres: Optional[List[str]] = None,
        duration_range: Tuple[int, int] = (300, 7200),
        upload_start: date = date(2020, 1, 1),
        upload_end: date = date(2026, 1, 31),
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize the VideoGenerator.

        Args:
            n_videos (int): Total number of unique videos to generate.
            genre_skew (float): Zipf-like skew for assigning video genres.
            genres (Optional[List[str]]): Pool of available video genres.
            duration_range (Tuple[int, int]): Minimum and maximum duration in seconds.
            upload_start (date): The earliest possible upload date.
            upload_end (date): The latest possible upload date.
            seed (Optional[int]): Random seed for reproducible data generation.
        """
        if n_videos < 0:
            raise ValueError("n_videos must be non-negative")
        if genre_skew < 0:
            raise ValueError("genre_skew must be non-negative")
        if duration_range[0] > duration_range[1]:
            raise ValueError("duration_range minimum must be less than or equal to maximum")
        if duration_range[0] <= 0:
            raise ValueError("duration_range minimum must be positive")
        if upload_start > upload_end:
            raise ValueError("upload_start must be before or equal to upload_end")

        self.n_videos = n_videos
        self.genre_skew = genre_skew
        self.genres = genres if genres is not None else self.DEFAULT_GENRES
        self.duration_range = duration_range
        self.upload_start = upload_start
        self.upload_end = upload_end
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        if not self.genres:
            raise ValueError("genres must not be empty")

    def generate(self) -> pd.DataFrame:
        """
        Generate the videos DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing video records with columns:
                          video_id, video_title, video_genre, video_duration,
                          and upload_date.
        """
        video_ids = [f"video_{idx:08d}" for idx in range(1, self.n_videos + 1)]
        video_titles = [f"Video {idx:08d}" for idx in range(1, self.n_videos + 1)]
        video_genres = self.rng.choice(
            self.genres,
            size=self.n_videos,
            p=self._genre_weights(),
        )
        video_durations = self.rng.integers(
            self.duration_range[0],
            self.duration_range[1] + 1,
            size=self.n_videos,
            dtype=np.int32,
        )
        upload_dates = self._generate_upload_dates()

        return pd.DataFrame(
            {
                "video_id": video_ids,
                "video_title": video_titles,
                "video_genre": video_genres,
                "video_duration": video_durations,
                "upload_date": upload_dates,
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
            "n_videos": self.n_videos,
            "genre_skew": self.genre_skew,
            "genres": self.genres,
            "duration_range": self.duration_range,
            "upload_start": self.upload_start.isoformat(),
            "upload_end": self.upload_end.isoformat(),
            "seed": self.seed,
        }

    def _genre_weights(self) -> np.ndarray:
        """
        Calculate descending probability weights for video genres.

        Returns:
            np.ndarray: Normalized probabilities where earlier genres are more common
                        as genre_skew increases.
        """
        ranks = np.arange(1, len(self.genres) + 1, dtype=np.float64)
        weights = 1.0 / np.power(ranks, self.genre_skew)
        return weights / weights.sum()

    def _generate_upload_dates(self) -> pd.Series:
        """
        Generate random upload dates uniformly within the configured date range.

        Returns:
            pd.Series: Pandas Series of Python date values.
        """
        start_day = pd.Timestamp(self.upload_start).toordinal()
        end_day = pd.Timestamp(self.upload_end).toordinal()
        days = self.rng.integers(start_day, end_day + 1, size=self.n_videos)
        return pd.Series([date.fromordinal(int(day)) for day in days])
