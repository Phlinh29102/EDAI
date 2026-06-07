"""Test BaseTableGenerator."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from data_generator.core.utils import RandomDataUtils
from data_generator.offline.base_table_generator import BaseTableGenerator


class ExampleTableGenerator(BaseTableGenerator):
    def __init__(self, config, schema, utils, generated_df):
        super().__init__(config=config, schema=schema, utils=utils)
        self.generated_df = generated_df

    def generate(self) -> pd.DataFrame:
        return self.generated_df


@pytest.fixture
def example_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("value", pa.int32(), nullable=False),
        ]
    )


def test_post_process_validates_schema_and_preserves_extra_columns(
    dummy_config,
    example_schema,
):
    df = pd.DataFrame(
        {
            "value": pd.Series([1, 2], dtype="int32"),
            "playback_date": pd.to_datetime(["2026-01-01", "2026-01-02"]).date,
            "id": ["a", "b"],
        }
    )
    generator = ExampleTableGenerator(
        config=dummy_config,
        schema=example_schema,
        utils=RandomDataUtils(seed=42),
        generated_df=df,
    )

    result = generator.post_process()

    assert list(result.columns) == ["id", "value", "playback_date"]
    assert len(result) == 2


def test_post_process_rejects_missing_required_columns(dummy_config, example_schema):
    generator = ExampleTableGenerator(
        config=dummy_config,
        schema=example_schema,
        utils=RandomDataUtils(seed=42),
        generated_df=pd.DataFrame({"id": ["a"]}),
    )

    with pytest.raises(ValueError, match="Missing required columns"):
        generator.post_process()


def test_post_process_rejects_nulls_in_non_nullable_columns(
    dummy_config,
    example_schema,
):
    generator = ExampleTableGenerator(
        config=dummy_config,
        schema=example_schema,
        utils=RandomDataUtils(seed=42),
        generated_df=pd.DataFrame({"id": ["a"], "value": [None]}),
    )

    with pytest.raises(ValueError, match="Non-nullable columns contain nulls"):
        generator.post_process()


def test_save_writes_single_file_with_playback_date(dummy_config, example_schema, tmp_path):
    df = pd.DataFrame(
        {
            "id": ["a", "b"],
            "value": pd.Series([1, 2], dtype="int32"),
            "playback_date": pd.to_datetime(["2026-01-01", "2026-01-02"]).date,
        }
    )
    generator = ExampleTableGenerator(
        config=dummy_config,
        schema=example_schema,
        utils=RandomDataUtils(seed=42),
        generated_df=df,
    )

    output_path = generator.save(tmp_path / "example_dataset")

    assert Path(output_path).exists()
    assert Path(output_path).suffix == ".parquet"


def test_save_writes_single_parquet_file_without_partition_column(
    dummy_config,
    example_schema,
    tmp_path,
):
    df = pd.DataFrame(
        {
            "id": ["a", "b"],
            "value": pd.Series([1, 2], dtype="int32"),
        }
    )
    generator = ExampleTableGenerator(
        config=dummy_config,
        schema=example_schema,
        utils=RandomDataUtils(seed=42),
        generated_df=df,
    )

    output_path = generator.save(tmp_path / "example.parquet")

    assert output_path == str(tmp_path / "example.parquet")
    assert Path(output_path).exists()


def test_summary_reports_state(dummy_config, example_schema):
    generator = ExampleTableGenerator(
        config=dummy_config,
        schema=example_schema,
        utils=RandomDataUtils(seed=42),
        generated_df=pd.DataFrame(
            {"id": ["a"], "value": pd.Series([1], dtype="int32")}
        ),
    )

    assert generator.summary()["row_count"] == 0

    generator.post_process()
    summary = generator.summary()

    assert summary["generator"] == "ExampleTableGenerator"
    assert summary["schema"] == ["id", "value"]
    assert summary["row_count"] == 1
    assert summary["columns"] == ["id", "value"]
    assert summary["config"]["random_seed"] == 42
