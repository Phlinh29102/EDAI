"""Offline Bronze ingestion for generated Parquet source tables."""
from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_SOURCE_PATH = Path("data/offline")
DEFAULT_OUTPUT_PATH = Path("data/bronze")
DEFAULT_TABLES = (
    "users",
    "videos",
    "playback_history",
    "interactions",
    "ad_impressions",
)


class OfflineBronzeIngestor:
    """Ingest generated offline Parquet tables into append-only Bronze datasets."""

    def __init__(
        self,
        source_path: str | Path = DEFAULT_SOURCE_PATH,
        output_path: str | Path = DEFAULT_OUTPUT_PATH,
        tables: Iterable[str] = DEFAULT_TABLES,
    ) -> None:
        self.source_path = Path(source_path)
        self.output_path = Path(output_path)
        self.tables = tuple(tables)
        self._last_paths: Dict[str, str] = {}
        self._last_row_counts: Dict[str, int] = {}

    def ingest_all(self, batch_id: Optional[str] = None) -> Dict[str, str]:
        """Ingest all configured offline tables into Bronze."""
        batch_id = batch_id or self._new_batch_id()
        paths: Dict[str, str] = {}
        for table_name in self.tables:
            paths[table_name] = self.ingest_table(table_name, batch_id=batch_id)

        self._last_paths = paths
        return paths

    def ingest_table(self, table_name: str, batch_id: Optional[str] = None) -> str:
        """Ingest one generated offline table into its raw Bronze table."""
        batch_id = batch_id or self._new_batch_id()
        source_file = self._source_file_for_table(table_name)
        df = self._read_source_table(source_file)
        bronze_df = self._with_bronze_metadata(
            df=df,
            source_table=table_name,
            source_file=source_file,
            batch_id=batch_id,
        )
        output_file = self._write_table(
            df=bronze_df,
            source_table=table_name,
            batch_id=batch_id,
        )
        self._last_paths[table_name] = output_file
        self._last_row_counts[table_name] = len(bronze_df)
        return output_file

    def summary(self) -> Dict[str, Any]:
        """Return the latest Bronze ingestion summary."""
        return {
            "ingestor": self.__class__.__name__,
            "source_path": str(self.source_path),
            "output_path": str(self.output_path),
            "tables": list(self.tables),
            "paths": dict(self._last_paths),
            "row_counts": dict(self._last_row_counts),
        }

    def _source_file_for_table(self, table_name: str) -> Path:
        candidates = [
            self.source_path / f"{table_name}.parquet",
            self.source_path / table_name,
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            f"No source Parquet found for table '{table_name}' under {self.source_path}"
        )

    def _read_source_table(self, source_file: Path) -> pd.DataFrame:
        table = pq.read_table(source_file)
        return table.to_pandas()

    def _with_bronze_metadata(
        self,
        df: pd.DataFrame,
        source_table: str,
        source_file: Path,
        batch_id: str,
    ) -> pd.DataFrame:
        result = df.copy()
        ingest_ts = datetime.now(timezone.utc)
        result["ingest_ts"] = ingest_ts
        result["source_file"] = str(source_file)
        result["source_table"] = source_table
        result["batch_id"] = batch_id
        result["source_row_number"] = range(len(result))
        return result

    def _write_table(
        self,
        df: pd.DataFrame,
        source_table: str,
        batch_id: str,
    ) -> str:
        ingest_date = self._ingest_date(df)
        output_dir = (
            self.output_path
            / f"raw_{source_table}"
            / f"ingest_date={ingest_date}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"batch-{batch_id}.parquet"
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, output_file)
        return str(output_file)

    def _ingest_date(self, df: pd.DataFrame) -> str:
        if df.empty:
            return datetime.now(timezone.utc).strftime("%Y%m%d")
        value = df["ingest_ts"].iloc[0]
        if isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
        if not isinstance(value, datetime):
            raise TypeError("ingest_ts must be a datetime")
        return value.strftime("%Y%m%d")

    def _new_batch_id(self) -> str:
        return uuid.uuid4().hex


def ingest_offline_bronze(
    source_path: str | Path = DEFAULT_SOURCE_PATH,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    tables: Iterable[str] = DEFAULT_TABLES,
    batch_id: Optional[str] = None,
) -> Dict[str, str]:
    """Convenience wrapper for offline Bronze ingestion."""
    return OfflineBronzeIngestor(
        source_path=source_path,
        output_path=output_path,
        tables=tables,
    ).ingest_all(batch_id=batch_id)


def main() -> None:
    """Run offline Bronze ingestion from the command line."""
    parser = argparse.ArgumentParser(
        description="Ingest generated offline Parquet tables into Bronze.",
    )
    parser.add_argument(
        "--source-path",
        default=str(DEFAULT_SOURCE_PATH),
        help="Path containing generated offline Parquet tables.",
    )
    parser.add_argument(
        "--output-path",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Bronze output root.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=list(DEFAULT_TABLES),
        help="Offline table names to ingest.",
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional stable batch id for this ingestion run.",
    )
    args = parser.parse_args()

    ingestor = OfflineBronzeIngestor(
        source_path=args.source_path,
        output_path=args.output_path,
        tables=args.tables,
    )
    paths = ingestor.ingest_all(batch_id=args.batch_id)
    for table_name, path in paths.items():
        print(f"{table_name}: {path}")


if __name__ == "__main__":
    main()
