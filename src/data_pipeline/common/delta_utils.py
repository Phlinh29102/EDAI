"""Delta/Parquet storage helper functions."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def ensure_parent(path: str | Path) -> Path:
    """Create the parent directory for a file path and return the Path."""
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def write_parquet(
    df: pd.DataFrame,
    path: str | Path,
    columns: Optional[Iterable[str]] = None,
) -> str:
    """Write a pandas DataFrame as Parquet."""
    resolved = ensure_parent(path)
    if columns is not None:
        df = df[list(columns)]
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, resolved)
    return str(resolved)


def read_parquet(path: str | Path) -> pd.DataFrame:
    """Read a Parquet file or dataset into pandas."""
    return pq.read_table(path).to_pandas()
