"""Tests that CSV engines satisfy the shared reader/writer protocol."""

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.core.csv_io.factories import CSVEngineFactory
from datagrunt.core.csv_io.protocol import (
    CSVReaderEngineProtocol,
    CSVWriterEngineProtocol,
    arrow_to_polars,
    resolve_normalize_columns,
)

ALL_ENGINES = ("duckdb", "polars", "pyarrow")

READER_METHODS = (
    "get_sample",
    "to_dataframe",
    "to_arrow_table",
    "to_dicts",
    "query_data",
    "close",
)

WRITER_METHODS = (
    "write_csv",
    "write_excel",
    "write_json",
    "write_json_newline_delimited",
    "write_parquet",
)

READER_ATTRIBUTES = ("filepath", "lenient", "normalize_columns", "queries", "db_table", "delimiter")


class TestNormalizeColumnHelpers:
    def test_resolve_inherits_instance_default(self):
        assert resolve_normalize_columns(True, None) is True
        assert resolve_normalize_columns(False, None) is False

    def test_resolve_per_call_overrides_instance_default(self):
        assert resolve_normalize_columns(False, True) is True
        assert resolve_normalize_columns(True, False) is False

    def test_arrow_to_polars_single_column_returns_dataframe(self):
        table = pa.table({"value": ["42"]})
        df = arrow_to_polars(table)
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (1, 1)


class TestReaderEngineProtocol:
    @pytest.mark.parametrize("engine", ALL_ENGINES)
    def test_factory_reader_is_protocol_instance(self, sample_csv, engine):
        reader = CSVEngineFactory(sample_csv, engine).create_reader()
        assert isinstance(reader, CSVReaderEngineProtocol)

    @pytest.mark.parametrize("engine", ALL_ENGINES)
    def test_reader_exposes_required_attributes(self, sample_csv, engine):
        reader = CSVEngineFactory(sample_csv, engine).create_reader()
        for attr in READER_ATTRIBUTES:
            assert hasattr(reader, attr), f"{engine} missing {attr}"

    @pytest.mark.parametrize("engine", ALL_ENGINES)
    def test_reader_exposes_required_methods(self, sample_csv, engine):
        reader = CSVEngineFactory(sample_csv, engine).create_reader()
        for method in READER_METHODS:
            assert callable(getattr(reader, method)), f"{engine} missing {method}"


class TestWriterEngineProtocol:
    @pytest.mark.parametrize("engine", ALL_ENGINES)
    def test_factory_writer_is_protocol_instance(self, sample_csv, engine):
        writer = CSVEngineFactory(sample_csv, engine).create_writer()
        assert isinstance(writer, CSVWriterEngineProtocol)

    @pytest.mark.parametrize("engine", ALL_ENGINES)
    def test_writer_exposes_required_attributes(self, sample_csv, engine):
        writer = CSVEngineFactory(sample_csv, engine).create_writer()
        for attr in ("filepath", "lenient", "normalize_columns", "queries", "db_table"):
            assert hasattr(writer, attr), f"{engine} missing {attr}"

    @pytest.mark.parametrize("engine", ALL_ENGINES)
    def test_writer_exposes_required_methods(self, sample_csv, engine):
        writer = CSVEngineFactory(sample_csv, engine).create_writer()
        for method in WRITER_METHODS:
            assert callable(getattr(writer, method)), f"{engine} missing {method}"
