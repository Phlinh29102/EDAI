"""RandomDataUtils - zipf weights, duplicate injection, late timestamp."""
from typing import Optional, Any, List
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

class RandomDataUtils:
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the RandomDataUtils instance with an optional random seed.
        Args:
            seed (Optional[int]): Seed value for random number generator reproducibility.
        """
        self.seed = seed
        self.rng = np.random.default_rng(seed)

    def zipf_weights(self, n: int, skew: float) -> np.ndarray:
        """
        Generate Zipfian distribution weights for 'n' elements.
        Args:
            n (int): The number of elements to generate weights for.
            skew (float): The skewness parameter (a value > 0). Higher values mean more skew.

        Returns:
            np.ndarray: An array of normalized probabilities (weights) that sum to 1.
        """
        if n <= 0:
            raise ValueError("n must be greater than 0")
        if skew < 0:
            raise ValueError("skew must be non-negative")

        ranks = np.arange(1, n + 1, dtype=np.float64)
        weights = 1.0 / np.power(ranks, skew)
        return weights / weights.sum()

    def sample_zipf(self, ids: List[Any], size: int, skew: float) -> List[Any]:
        """
        Sample from a list of IDs based on a Zipfian distribution.
        Args:
            ids (List[Any]): List of items (e.g., video_ids) to sample from.
            size (int): The number of samples to generate.
            skew (float): The Zipf skew parameter for distribution weights.
        Returns:
            List[Any]: A list of length 'size' containing sampled items.
        """
        if size < 0:
            raise ValueError("size must be non-negative")
        if size == 0:
            return []
        if len(ids) == 0 and size > 0:
            raise ValueError("ids must not be empty when size is greater than 0")

        weights = self.zipf_weights(len(ids), skew)
        return self.rng.choice(ids, size=size, replace=True, p=weights).tolist()

    def bernoulli(self, p: float) -> bool:
        """
        Perform a Bernoulli trial (coin flip) with probability 'p'.
        Args:
            p (float): The probability of success (True), between 0.0 and 1.0.
        Returns:
            bool: True if the trial succeeds, False otherwise.
        """
        if not 0.0 <= p <= 1.0:
            raise ValueError("p must be between 0.0 and 1.0")

        return bool(self.rng.random() < p)

    def inject_duplicates(self, df: pd.DataFrame, rate: float, key_cols: List[str]) -> pd.DataFrame:
        """
        Inject artificial duplicates into a DataFrame to simulate data anomalies.
        Args:
            df (pd.DataFrame): The original pandas DataFrame.
            rate (float): The proportion of the dataset to duplicate (e.g., 0.02 for 2%).
            key_cols (List[str]): List of column names that should form the uniqueness key (not necessarily changed, just for context).
        Returns:
            pd.DataFrame: A new DataFrame with the injected duplicate rows appended and shuffled.
        """
        if not 0.0 <= rate <= 1.0:
            raise ValueError("rate must be between 0.0 and 1.0")
        missing_cols = [col for col in key_cols if col not in df.columns]
        if missing_cols:
            raise KeyError(f"Missing key columns: {missing_cols}")

        duplicate_count = int(round(len(df) * rate))
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

    def generate_late_timestamps(self, ts: datetime, rate: float, delay_min: int, delay_max: int) -> datetime:
        """
        Simulate late arriving events by artificially delaying a timestamp for a percentage of events.
        Args:
            ts (datetime): The original true event timestamp.
            rate (float): The probability (0.0 to 1.0) that this timestamp is delayed (late arrival).
            delay_min (int): The minimum delay in hours.
            delay_max (int): The maximum delay in hours.
        Returns:
            datetime: The (potentially delayed) created timestamp.
        """
        if delay_min < 0 or delay_max < 0:
            raise ValueError("delay_min and delay_max must be non-negative")
        if delay_min > delay_max:
            raise ValueError("delay_min must be less than or equal to delay_max")

        if not self.bernoulli(rate):
            return ts

        delay_hours = int(self.rng.integers(delay_min, delay_max + 1))
        return ts + timedelta(hours=delay_hours)

    def summary(self) -> str:
        """
        Provide a string summary of the utility configuration.
        Returns:
            str: Description of the utility class state.
        """
        seed_state = self.seed if self.seed is not None else "None"
        return f"RandomDataUtils(seed={seed_state})"
