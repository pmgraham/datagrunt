"""Tests for the ParquetReader public class."""

import pyarrow as pa
import pytest

from datagrunt.parquet_api.parquetreader import ParquetReader


def test_to_dataframe(sample_parquet):
    assert ParquetReader(sample_parquet).to_dataframe().shape == (4, 3)


def test_get_sample(sample_parquet):
    assert ParquetReader(sample_parquet).get_sample().height == 4


def test_to_arrow_and_dicts(sample_parquet):
    reader = ParquetReader(sample_parquet)
    assert isinstance(reader.to_arrow_table(), pa.Table)
    assert reader.to_dicts()[0]["name"] == "John"


def test_normalize_columns(messy_parquet):
    assert ParquetReader(messy_parquet, normalize_columns=True).to_dataframe().columns == ["first_name", "age"]


def test_query_data(sample_parquet):
    reader = ParquetReader(sample_parquet)
    result = reader.query_data(f"SELECT count(*) AS n FROM {reader.db_table}")
    assert result.pl()["n"][0] == 4


def test_empty_returns_empty_objects(empty_parquet):
    reader = ParquetReader(empty_parquet)
    assert reader.to_dataframe().is_empty()
    assert reader.to_dicts() == []
    assert reader.get_sample().is_empty()
    assert reader.to_arrow_table().num_rows == 0
    assert reader.query_data("SELECT 1") == []


def test_rejects_non_parquet(sample_files):
    with pytest.raises(ValueError, match="not a Parquet file"):
        ParquetReader(sample_files["data.csv"])


def test_missing_file(nonexistent_parquet):
    with pytest.raises(FileNotFoundError):
        ParquetReader(nonexistent_parquet)


def test_context_manager(sample_parquet):
    with ParquetReader(sample_parquet) as reader:
        reader.query_data(f"SELECT 1 FROM {reader.db_table}")
