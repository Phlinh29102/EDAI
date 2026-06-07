"""Reusable data quality checks for pipeline tables."""
from __future__ import annotations

from typing import Any, Dict, Iterable

import pandas as pd


def check_unique(df: pd.DataFrame, columns: Iterable[str]) -> Dict[str, Any]:
    """Return duplicate-count metrics for a uniqueness constraint."""
    cols = list(columns)
    duplicate_count = int(df.duplicated(subset=cols).sum())
    return {
        "check": "unique",
        "columns": cols,
        "passed": duplicate_count == 0,
        "duplicate_count": duplicate_count,
    }


def check_not_null(df: pd.DataFrame, columns: Iterable[str]) -> Dict[str, Any]:
    """Return null-count metrics for required columns."""
    cols = list(columns)
    null_counts = {column: int(df[column].isna().sum()) for column in cols}
    return {
        "check": "not_null",
        "columns": cols,
        "passed": all(count == 0 for count in null_counts.values()),
        "null_counts": null_counts,
    }


def check_referential_subset(
    fact_df: pd.DataFrame,
    dim_df: pd.DataFrame,
    fact_column: str,
    dim_column: str,
) -> Dict[str, Any]:
    """Validate that fact keys are a subset of dimension keys."""
    missing = set(fact_df[fact_column].dropna()) - set(dim_df[dim_column].dropna())
    return {
        "check": "referential_subset",
        "fact_column": fact_column,
        "dim_column": dim_column,
        "passed": not missing,
        "missing_count": len(missing),
    }
