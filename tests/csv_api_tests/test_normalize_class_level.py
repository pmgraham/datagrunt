"""Tests for class-level normalize_columns on engines, CSVReader, and CSVWriter."""

import polars as pl
import pytest

from datagrunt import CSVReader, CSVWriter
from datagrunt.core import CSVEngineFactory

ALL_ENGINES = ["polars", "duckdb", "pyarrow"]

MIXED_HEADERS_CSV = "First Name,Last Name,Age Group\nJohn,Doe,30-40\nJane,Smith,20-30"
ORIGINAL_HEADERS = ["First Name", "Last Name", "Age Group"]
NORMALIZED_HEADERS = ["first_name", "last_name", "age_group"]


@pytest.fixture
def mixed_headers_csv(tmp_path):
    """A CSV whose headers change under normalization."""
    csv_file = tmp_path / "mixed_headers.csv"
    csv_file.write_text(MIXED_HEADERS_CSV)
    return csv_file


class TestEngineFactoryPlumbing:
    """The factory forwards normalize_columns into every engine it builds."""

    def test_factory_forwards_flag_to_reader_engines(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            assert reader_engine.normalize_columns is True

    def test_factory_forwards_flag_to_writer_engines(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            writer_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_writer()
            assert writer_engine.normalize_columns is True

    def test_engines_default_to_no_normalization(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine).create_reader()
            writer_engine = CSVEngineFactory(mixed_headers_csv, engine).create_writer()
            assert reader_engine.normalize_columns is False
            assert writer_engine.normalize_columns is False
