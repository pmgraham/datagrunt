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


def test_rejects_reserved_source_option(sample_parquet):
    with pytest.raises(ValueError, match="source"):
        ParquetReader(sample_parquet, source="bad").to_dataframe()


def test_parquetreader_to_dataframe_kwargs(sample_parquet):
    """Verify that flat kwargs are forwarded to pl.read_parquet."""
    reader = ParquetReader(sample_parquet)
    df = reader.to_dataframe(n_rows=2)
    assert df.height == 2
    assert list(df.columns) == ["name", "age", "city"]


def test_get_sample_n_rows(tmp_path):
    import polars as pl

    path = tmp_path / "wide.parquet"
    pl.DataFrame({"n": list(range(30))}).write_parquet(str(path))
    reader = ParquetReader(str(path))
    assert reader.get_sample().height == 20  # default unchanged
    assert reader.get_sample(n_rows=5).height == 5
    assert reader.get_sample(n_rows=100).height == 30  # capped at file size


def test_get_sample_n_rows_invalid(sample_parquet):
    reader = ParquetReader(sample_parquet)
    for bad in (0, -3, 2.5, "ten", True):
        with pytest.raises(ValueError):
            reader.get_sample(n_rows=bad)


def test_get_sample_n_rows_invalid_on_empty_file(empty_parquet):
    """Invalid n_rows raises even when the file is empty."""
    with pytest.raises(ValueError):
        ParquetReader(empty_parquet).get_sample(n_rows=0)
