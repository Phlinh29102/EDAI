"""BaseTableGenerator - abstract base for offline table generators."""
from abc import abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_generator.core.base_generator import BaseGenerator
from data_generator.core.config import GeneratorConfig
from data_generator.core.utils import RandomDataUtils


class BaseTableGenerator(BaseGenerator):
    """Base class for offline table generators."""

    def __init__(
        self,
        config: GeneratorConfig,
        schema: pa.Schema,
        utils: RandomDataUtils,
    ) -> None:
        """
        Initialize the base table generator.

        Args:
            config (GeneratorConfig): Loaded generator configuration.
            schema (pa.Schema): Expected PyArrow schema for required columns.
            utils (RandomDataUtils): Shared random-data utility instance.
        """
        super().__init__(config=config, utils=utils)
        self.schema = schema
        self.df: Optional[pd.DataFrame] = None

    @abstractmethod
    def generate(self) -> pd.DataFrame:
        """
        Generate the table DataFrame.

        Returns:
            pd.DataFrame: Generated table records.
        """
        raise NotImplementedError

    def post_process(self) -> pd.DataFrame:
        """
        Validate generated data against the required schema and reorder columns.

        Returns:
            pd.DataFrame: Validated DataFrame with schema columns first.
        """
        if self.df is None:
            self.df = self.generate()

        if not isinstance(self.df, pd.DataFrame):
            raise TypeError("generate() must return a pandas DataFrame")

        missing_columns = [
            field.name for field in self.schema if field.name not in self.df.columns
        ]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        null_columns = [
            field.name
            for field in self.schema
            if not field.nullable and self.df[field.name].isna().any()
        ]
        if null_columns:
            raise ValueError(f"Non-nullable columns contain nulls: {null_columns}")

        schema_columns = [field.name for field in self.schema]
        extra_columns = [col for col in self.df.columns if col not in schema_columns]
        self.df = self.df[schema_columns + extra_columns].copy()

        self._validate_pyarrow_conversion()
        return self.df

    def save(self, output_path: str | Path) -> str:
        """
        Save the generated table as a single Parquet file.

        Args:
            output_path (str | Path): File path or dataset directory for Parquet output.

        Returns:
            str: Path where the Parquet data was written.
        """
        df = self.post_process()
        output_path = Path(output_path)

        if output_path.suffix != ".parquet":
            output_path = output_path.with_suffix(".parquet")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, output_path)
        return str(output_path)

    def summary(self) -> Dict[str, Any]:
        """
        Provide a summary of generator state.

        Returns:
            Dict[str, Any]: Current generator configuration and data state.
        """
        return {
            "generator": self.__class__.__name__,
            "schema": [field.name for field in self.schema],
            "row_count": 0 if self.df is None else len(self.df),
            "columns": [] if self.df is None else list(self.df.columns),
            "config": self.config.as_dict(),
        }

    def _validate_pyarrow_conversion(self) -> None:
        schema_df = self.df[[field.name for field in self.schema]]
        pa.Table.from_pandas(
            schema_df,
            schema=self.schema,
            preserve_index=False,
        )
