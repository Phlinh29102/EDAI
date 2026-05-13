"""RandomDataUtils - zipf weights, duplicate injection, late timestamp."""
from typing import Optional, Any, List
import numpy as np
import pandas as pd
from datetime import datetime

class RandomDataUtils:
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the RandomDataUtils instance with an optional random seed.
        Args:
            seed (Optional[int]): Seed value for random number generator reproducibility.
        """
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
        # TODO: Calculate inverse power-law weights for n ranks using the given skew, then normalize to sum to 1
        pass

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
        # TODO: Generate zipf weights for len(ids), then use self.rng.choice to sample 'size' elements from 'ids'
        pass

    def bernoulli(self, p: float) -> bool:
        """
        Perform a Bernoulli trial (coin flip) with probability 'p'.
        Args:
            p (float): The probability of success (True), between 0.0 and 1.0.
        Returns:
            bool: True if the trial succeeds, False otherwise.
        """
        # TODO: Return True with probability p, False with probability 1-p using self.rng
        pass

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
        # TODO: Randomly sample 'rate' proportion of rows from df, optionally modify their timestamps slightly, and append them back to df
        pass

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
        # TODO: Use bernoulli(rate) to decide if late. If yes, add a random delay between delay_min and delay_max hours to ts.
        pass

    def summary(self) -> str:
        """
        Provide a string summary of the utility configuration.
        Returns:
            str: Description of the utility class state.
        """
        # TODO: Return a string indicating the state, e.g. whether a seed is set
        pass