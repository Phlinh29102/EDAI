"""Shared Silver staging interfaces."""
from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd


class SilverTransform(Protocol):
    """Protocol for Silver table transforms."""

    source_table: str
    target_table: str

    def run(self, bronze_path: str | Path, output_path: str | Path) -> str:
        """Transform Bronze input into a Silver table."""


def require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """Validate required input columns."""
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
