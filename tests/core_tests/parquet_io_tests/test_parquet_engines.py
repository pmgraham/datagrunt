"""Tests for the Parquet reader/writer engines."""

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.core.parquet_io.engines import (
    ParquetReaderEngine,
    set_parquet_export_filename,
)


def test_to_dataframe_round_trips(sample_parquet):
    engine = ParquetReaderEngine(sample_parquet)
    df = engine.to_dataframe()
    assert df.shape == (4, 3)
    assert df.columns == ["name", "age", "city"]


def test_get_sample_caps_rows(tmp_path):
    path = tmp_path / "big.parquet"
    pl.DataFrame({"n": list(range(50))}).write_parquet(str(path))
    engine = ParquetReaderEngine(str(path))
    assert engine.get_sample().height == 20


def test_to_arrow_and_dicts(sample_parquet):
    engine = ParquetReaderEngine(sample_parquet)
    assert isinstance(engine.to_arrow_table(), pa.Table)
    dicts = engine.to_dicts()
    assert dicts[0]["name"] == "John"


def test_normalize_columns_constructor(messy_parquet):
    engine = ParquetReaderEngine(messy_parquet, normalize_columns=True)
    assert engine.to_dataframe().columns == ["first_name", "age"]


def test_normalize_columns_per_call_override(messy_parquet):
    engine = ParquetReaderEngine(messy_parquet)
    assert engine.to_dataframe(normalize_columns=True).columns == ["first_name", "age"]
    assert engine.to_dataframe().columns == ["First Name!", "#Age@"]


def test_read_options_forwarded(sample_parquet):
    engine = ParquetReaderEngine(sample_parquet, columns=["name"])
    assert engine.to_dataframe().columns == ["name"]


def test_reserved_read_option_rejected(sample_parquet):
    engine = ParquetReaderEngine(sample_parquet, source="bad")
    with pytest.raises(ValueError, match="source"):
        engine.to_dataframe()


def test_query_data_uses_db_table(sample_parquet):
    engine = ParquetReaderEngine(sample_parquet)
    relation = engine.query_data(f"SELECT count(*) AS n FROM {engine.db_table}")
    assert relation.pl()["n"][0] == 4
    engine.close()


def test_set_parquet_export_filename():
    assert set_parquet_export_filename("output.csv") == "output.csv"
    assert set_parquet_export_filename("output.csv", "custom.csv") == "custom.csv"
    with pytest.raises(ValueError):
        set_parquet_export_filename("output.csv", "   ")
