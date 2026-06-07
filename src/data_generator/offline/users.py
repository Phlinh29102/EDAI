"""UsersGenerator."""
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


class UsersGenerator:
    """Generate the offline users dimension table."""

    DEFAULT_COUNTRIES = [
        "US",
        "GB",
        "CA",
        "AU",
        "DE",
        "FR",
        "IN",
        "JP",
        "BR",
        "VN",
    ]
    DEFAULT_SUBSCRIPTION_TIERS = ["free", "basic", "premium", "family"]

    def __init__(
        self,
        n_users: int,
        subscription_tiers: Optional[List[str]] = None,
        countries: Optional[List[str]] = None,
        age_range: Tuple[int, int] = (13, 80),
        signup_start: datetime = datetime(2024, 6, 1),
        signup_end: datetime = datetime(2026, 6, 7, 23, 59, 59),
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize the UsersGenerator.

        Args:
            n_users (int): Total number of unique users to generate.
            subscription_tiers (Optional[List[str]]): Pool of available subscription plans.
            countries (Optional[List[str]]): Pool of available country codes.
            age_range (Tuple[int, int]): Minimum and maximum age boundary for users.
            signup_start (datetime): The earliest possible signup date.
            signup_end (datetime): The latest possible signup date.
            seed (Optional[int]): Random seed for reproducible data generation.
        """
        if n_users < 0:
            raise ValueError("n_users must be non-negative")
        if age_range[0] > age_range[1]:
            raise ValueError("age_range minimum must be less than or equal to maximum")
        if signup_start > signup_end:
            raise ValueError("signup_start must be before or equal to signup_end")

        self.n_users = n_users
        self.subscription_tiers = (
            subscription_tiers
            if subscription_tiers is not None
            else self.DEFAULT_SUBSCRIPTION_TIERS
        )
        self.countries = countries if countries is not None else self.DEFAULT_COUNTRIES
        self.age_range = age_range
        self.signup_start = signup_start
        self.signup_end = signup_end
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        if not self.subscription_tiers:
            raise ValueError("subscription_tiers must not be empty")
        if not self.countries:
            raise ValueError("countries must not be empty")

    def generate(self) -> pd.DataFrame:
        """
        Generate the users DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing user records with columns:
                          user_id, user_age, country, user_subscription, and signup_ts.
        """
        user_ids = [f"user_{idx:08d}" for idx in range(1, self.n_users + 1)]
        user_ages = self.rng.integers(
            self.age_range[0],
            self.age_range[1] + 1,
            size=self.n_users,
            dtype=np.int32,
        )
        countries = self.rng.choice(self.countries, size=self.n_users)
        subscriptions = self.rng.choice(
            self.subscription_tiers,
            size=self.n_users,
            p=self._subscription_weights(),
        )
        signup_ts = self._generate_signup_timestamps()

        return pd.DataFrame(
            {
                "user_id": user_ids,
                "user_age": user_ages,
                "country": countries,
                "user_subscription": subscriptions,
                "signup_ts": signup_ts,
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
            "n_users": self.n_users,
            "subscription_tiers": self.subscription_tiers,
            "countries": self.countries,
            "age_range": self.age_range,
            "signup_start": self.signup_start.isoformat(),
            "signup_end": self.signup_end.isoformat(),
            "seed": self.seed,
        }

    def _subscription_weights(self) -> np.ndarray:
        """
        Calculate descending probability weights for subscription tiers.

        Returns:
            np.ndarray: Normalized array of probabilities ensuring the first tier 
                        is the most common, following a Zipf-like distribution.
        """
        ranks = np.arange(1, len(self.subscription_tiers) + 1, dtype=np.float64)
        weights = 1.0 / ranks
        return weights / weights.sum()

    def _generate_signup_timestamps(self) -> pd.Series:
        """
        Generate random signup timestamps uniformly distributed within the configured date range.

        Returns:
            pd.Series: Pandas Series of timestamps with day precision ('datetime64[ms]'),
                       all set to midnight to limit partition cardinality.
        """
        start_ns = pd.Timestamp(self.signup_start).value
        end_ns = pd.Timestamp(self.signup_end).value
        timestamps = self.rng.integers(start_ns, end_ns + 1, size=self.n_users)
        return pd.Series(
            pd.to_datetime(timestamps).floor("D").astype("datetime64[ms]")
        )
