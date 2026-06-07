"""InteractionGenerator."""
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


class InteractionGenerator:
    """Generate the offline interactions fact table."""

    DEFAULT_INTERACTION_TYPES = ["like", "dislike", "comment", "share"]

    def __init__(
        self,
        duplicate_rate: float,
        user_ids: Optional[List[str]] = None,
        video_ids: Optional[List[str]] = None,
        playback_history_df: Optional[pd.DataFrame] = None,
        n_interactions: Optional[int] = None,
        interaction_types: Optional[List[str]] = None,
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize the InteractionGenerator.

        Args:
            duplicate_rate (float): Proportion of records to duplicate.
            user_ids (Optional[List[str]]): Existing user IDs for referential integrity.
            video_ids (Optional[List[str]]): Existing video IDs for referential integrity.
            playback_history_df (Optional[pd.DataFrame]): Playback sessions to sample
                user/video pairs from.
            n_interactions (Optional[int]): Total interactions to generate.
            interaction_types (Optional[List[str]]): Pool of interaction event types.
            seed (Optional[int]): Random seed for reproducible data generation.
        """
        if not 0.0 <= duplicate_rate <= 1.0:
            raise ValueError("duplicate_rate must be between 0.0 and 1.0")
        if n_interactions is not None and n_interactions < 0:
            raise ValueError("n_interactions must be non-negative")

        self.duplicate_rate = duplicate_rate
        self.user_ids = user_ids
        self.video_ids = video_ids
        self.playback_history_df = playback_history_df
        self.n_interactions = n_interactions
        self.interaction_types = (
            interaction_types
            if interaction_types is not None
            else self.DEFAULT_INTERACTION_TYPES
        )
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        if user_ids is not None and not user_ids:
            raise ValueError("user_ids must not be empty")
        if video_ids is not None and not video_ids:
            raise ValueError("video_ids must not be empty")
        if not self.interaction_types:
            raise ValueError("interaction_types must not be empty")
        if playback_history_df is not None:
            missing_columns = {"user_id", "video_id"} - set(playback_history_df.columns)
            if missing_columns:
                raise ValueError(
                    f"playback_history_df missing columns: {sorted(missing_columns)}"
                )

    def generate(self) -> pd.DataFrame:
        """
        Generate the interactions DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing interaction records with columns:
                          interaction_id, user_id, video_id, interaction_type,
                          and likes.
        """
        playback_pairs = self._sample_playback_pairs()
        user_ids = self.user_ids or self._default_user_ids()
        video_ids = self.video_ids or self._default_video_ids()
        n_interactions = (
            self.n_interactions
            if self.n_interactions is not None
            else self._default_interaction_count(user_ids)
        )

        if n_interactions == 0:
            return pd.DataFrame(
                {
                    "interaction_id": pd.Series(dtype="string"),
                    "user_id": pd.Series(dtype="string"),
                    "video_id": pd.Series(dtype="string"),
                    "interaction_type": pd.Series(dtype="string"),
                    "likes": pd.Series(dtype="boolean"),
                }
            )

        interaction_ids = [
            f"interaction_{idx:010d}" for idx in range(1, n_interactions + 1)
        ]
        if playback_pairs is not None:
            sampled_users = playback_pairs["user_id"].to_numpy()
            sampled_videos = playback_pairs["video_id"].to_numpy()
        else:
            sampled_users = self.rng.choice(user_ids, size=n_interactions)
            sampled_videos = self.rng.choice(video_ids, size=n_interactions)
        interaction_types = self.rng.choice(
            self.interaction_types,
            size=n_interactions,
            p=self._interaction_type_weights(),
        )
        likes = self._generate_likes(interaction_types)

        df = pd.DataFrame(
            {
                "interaction_id": interaction_ids,
                "user_id": sampled_users,
                "video_id": sampled_videos,
                "interaction_type": interaction_types,
                "likes": likes,
            }
        )

        return self._inject_duplicates(df)

    def summary(self) -> Dict[str, object]:
        """
        Provide a summary of the generator's configuration.

        Returns:
            Dict[str, object]: Configuration settings used for generation.
        """
        return {
            "generator": self.__class__.__name__,
            "duplicate_rate": self.duplicate_rate,
            "n_users": len(self.user_ids) if self.user_ids is not None else None,
            "n_videos": len(self.video_ids) if self.video_ids is not None else None,
            "uses_playback_history": self.playback_history_df is not None,
            "n_interactions": self.n_interactions,
            "interaction_types": self.interaction_types,
            "seed": self.seed,
        }

    def _interaction_type_weights(self) -> np.ndarray:
        base_weights = {
            "like": 0.62,
            "dislike": 0.08,
            "comment": 0.18,
            "share": 0.12,
        }
        weights = np.array(
            [base_weights.get(interaction_type, 1.0) for interaction_type in self.interaction_types],
            dtype=np.float64,
        )
        return weights / weights.sum()

    def _generate_likes(self, interaction_types: np.ndarray) -> pd.Series:
        likes: List[Optional[bool]] = []
        for interaction_type in interaction_types:
            if interaction_type == "like":
                likes.append(True)
            elif interaction_type == "dislike":
                likes.append(False)
            else:
                likes.append(None)
        return pd.Series(likes, dtype="boolean")

    def _sample_playback_pairs(self) -> Optional[pd.DataFrame]:
        if self.playback_history_df is None:
            return None

        n_interactions = (
            self.n_interactions
            if self.n_interactions is not None
            else len(self.playback_history_df)
        )
        if n_interactions == 0:
            return pd.DataFrame({"user_id": [], "video_id": []})

        positions = self.rng.choice(
            len(self.playback_history_df),
            size=n_interactions,
            replace=n_interactions > len(self.playback_history_df),
        )
        return self.playback_history_df.iloc[positions][["user_id", "video_id"]].reset_index(
            drop=True
        )

    def _default_interaction_count(self, user_ids: List[str]) -> int:
        if self.playback_history_df is not None:
            return len(self.playback_history_df)
        return len(user_ids) * 3

    def _inject_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
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
        size = self.n_interactions if self.n_interactions is not None else 1_000
        return [f"user_{idx:08d}" for idx in range(1, size + 1)]

    def _default_video_ids(self) -> List[str]:
        return [f"video_{idx:08d}" for idx in range(1, 1_001)]
