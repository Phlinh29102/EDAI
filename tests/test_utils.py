"""Test RandomDataUtils: zipf weights, duplicates, and late timestamps."""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from data_generator.core.utils import RandomDataUtils


def test_zipf_weights_are_normalized_and_skewed():
    """Verify Zipf weights sum to 1.0 and descend in order for skewed distributions."""
    utils = RandomDataUtils(seed=42)

    weights = utils.zipf_weights(n=4, skew=1.0)

    np.testing.assert_allclose(weights.sum(), 1.0)
    np.testing.assert_allclose(weights, np.array([0.48, 0.24, 0.16, 0.12]))
    assert weights[0] > weights[1] > weights[2] > weights[3]


def test_zipf_weights_with_zero_skew_are_uniform():
    """Verify that a zero skew results in uniform probabilities."""
    utils = RandomDataUtils(seed=42)

    weights = utils.zipf_weights(n=3, skew=0.0)

    np.testing.assert_allclose(weights, np.array([1 / 3, 1 / 3, 1 / 3]))


@pytest.mark.parametrize(
    ("n", "skew", "error"),
    [
        (0, 1.0, "n must be greater than 0"),
        (3, -0.1, "skew must be non-negative"),
    ],
)
def test_zipf_weights_reject_invalid_arguments(n, skew, error):
    """Ensure ValueError is raised for invalid 'n' or negative 'skew'."""
    utils = RandomDataUtils(seed=42)

    with pytest.raises(ValueError, match=error):
        utils.zipf_weights(n=n, skew=skew)


def test_sample_zipf_returns_requested_size_from_ids():
    """Verify that the generated sample has the correct size and contains only valid IDs."""
    utils = RandomDataUtils(seed=42)
    ids = ["video_1", "video_2", "video_3"]

    sample = utils.sample_zipf(ids=ids, size=20, skew=1.2)

    assert len(sample) == 20
    assert set(sample).issubset(ids)


def test_sample_zipf_allows_empty_sample_from_empty_ids():
    """Verify that requesting 0 samples from an empty list safely returns an empty list."""
    utils = RandomDataUtils(seed=42)

    assert utils.sample_zipf(ids=[], size=0, skew=1.0) == []


@pytest.mark.parametrize("size", [-1])
def test_sample_zipf_rejects_invalid_size(size):
    """Ensure ValueError is raised if the requested sample size is negative."""
    utils = RandomDataUtils(seed=42)

    with pytest.raises(ValueError, match="size must be non-negative"):
        utils.sample_zipf(ids=["a"], size=size, skew=1.0)


def test_sample_zipf_rejects_empty_ids_when_samples_requested():
    """Ensure ValueError is raised when trying to sample from an empty list of IDs."""
    utils = RandomDataUtils(seed=42)

    with pytest.raises(ValueError, match="ids must not be empty"):
        utils.sample_zipf(ids=[], size=1, skew=1.0)


def test_bernoulli_respects_probability_boundaries():
    """Verify deterministic outcomes for absolute probabilities of 0.0 and 1.0."""
    utils = RandomDataUtils(seed=42)

    assert utils.bernoulli(0.0) is False
    assert utils.bernoulli(1.0) is True


@pytest.mark.parametrize("p", [-0.01, 1.01])
def test_bernoulli_rejects_invalid_probability(p):
    """Ensure ValueError is raised if the probability is outside the [0.0, 1.0] range."""
    utils = RandomDataUtils(seed=42)

    with pytest.raises(ValueError, match="p must be between"):
        utils.bernoulli(p)


def test_inject_duplicates_appends_duplicates_and_preserves_columns():
    """Verify duplicates are added correctly, shuffled, and original columns remain intact."""
    utils = RandomDataUtils(seed=42)
    df = pd.DataFrame(
        {
            "event_id": ["e1", "e2", "e3", "e4"],
            "user_id": ["u1", "u2", "u3", "u4"],
            "value": [10, 20, 30, 40],
        }
    )

    result = utils.inject_duplicates(df, rate=0.5, key_cols=["event_id"])

    assert len(result) == 6
    assert list(result.columns) == list(df.columns)
    assert result.duplicated(subset=["event_id"]).sum() == 2
    pd.testing.assert_frame_equal(df, df.copy())


def test_inject_duplicates_with_zero_rate_returns_copy_without_new_rows():
    """Ensure a 0% duplicate rate returns an identical but distinct copy of the DataFrame."""
    utils = RandomDataUtils(seed=42)
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})

    result = utils.inject_duplicates(df, rate=0.0, key_cols=["id"])

    assert result is not df
    pd.testing.assert_frame_equal(result, df)


@pytest.mark.parametrize("rate", [-0.1, 1.1])
def test_inject_duplicates_rejects_invalid_rate(rate):
    """Ensure ValueError is raised for duplicate rates outside the [0.0, 1.0] range."""
    utils = RandomDataUtils(seed=42)
    df = pd.DataFrame({"id": [1]})

    with pytest.raises(ValueError, match="rate must be between"):
        utils.inject_duplicates(df, rate=rate, key_cols=["id"])


def test_inject_duplicates_rejects_missing_key_columns():
    """Ensure KeyError is raised if the specified uniqueness key column is missing."""
    utils = RandomDataUtils(seed=42)
    df = pd.DataFrame({"id": [1]})

    with pytest.raises(KeyError, match="Missing key columns"):
        utils.inject_duplicates(df, rate=0.1, key_cols=["missing_id"])


def test_generate_late_timestamps_returns_original_when_not_late():
    """Ensure the original timestamp is returned unaltered if the late arrival rate is 0.0."""
    utils = RandomDataUtils(seed=42)
    ts = datetime(2026, 1, 1, 12, 0, 0)

    assert utils.generate_late_timestamps(ts, rate=0.0, delay_min=1, delay_max=48) == ts


def test_generate_late_timestamps_adds_delay_when_late():
    """Check that a correctly calculated delay is added when an event is flagged as late."""
    utils = RandomDataUtils(seed=42)
    ts = datetime(2026, 1, 1, 12, 0, 0)

    delayed = utils.generate_late_timestamps(ts, rate=1.0, delay_min=3, delay_max=3)

    assert delayed == ts + timedelta(hours=3)


@pytest.mark.parametrize(
    ("delay_min", "delay_max", "error"),
    [
        (-1, 2, "must be non-negative"),
        (3, 2, "less than or equal"),
    ],
)
def test_generate_late_timestamps_rejects_invalid_delay_range(
    delay_min,
    delay_max,
    error,
):
    """Ensure ValueError is raised for negative delays or when minimum delay > maximum delay."""
    utils = RandomDataUtils(seed=42)

    with pytest.raises(ValueError, match=error):
        utils.generate_late_timestamps(
            datetime(2026, 1, 1),
            rate=0.5,
            delay_min=delay_min,
            delay_max=delay_max,
        )


def test_summary_includes_seed_state():
    """Verify the utility summary string accurately reflects the initialized seed state."""
    assert RandomDataUtils(seed=42).summary() == "RandomDataUtils(seed=42)"
    assert RandomDataUtils().summary() == "RandomDataUtils(seed=None)"
