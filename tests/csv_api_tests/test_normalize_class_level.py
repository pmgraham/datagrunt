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


class TestReaderEngineInstanceNormalization:
    """Reader engines apply the constructor-level flag when no per-call value is given."""

    def test_to_dataframe_uses_instance_flag(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            assert list(reader_engine.to_dataframe().columns) == NORMALIZED_HEADERS

    def test_get_sample_uses_instance_flag(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            assert list(reader_engine.get_sample().columns) == NORMALIZED_HEADERS

    def test_to_arrow_table_uses_instance_flag(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            assert reader_engine.to_arrow_table().column_names == NORMALIZED_HEADERS

    def test_to_dicts_uses_instance_flag(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            assert list(reader_engine.to_dicts()[0].keys()) == NORMALIZED_HEADERS

    def test_per_call_false_overrides_instance_true(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            df = reader_engine.to_dataframe(normalize_columns=False)
            assert list(df.columns) == ORIGINAL_HEADERS

    def test_per_call_true_overrides_instance_false(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine).create_reader()
            df = reader_engine.to_dataframe(normalize_columns=True)
            assert list(df.columns) == NORMALIZED_HEADERS


def _query_result_to_dataframe(result, engine):
    """DuckDB's query_data returns a relation; polars/pyarrow return a DataFrame."""
    return result.pl() if engine == "duckdb" else result


class TestQueryDataInstanceNormalization:
    """With the instance flag set, SQL is written AND returned in normalized names."""

    def test_query_written_against_normalized_names(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            sql = f"SELECT first_name FROM {reader_engine.db_table} WHERE last_name = 'Doe'"
            df = _query_result_to_dataframe(reader_engine.query_data(sql), engine)
            assert df.columns == ["first_name"]
            assert df["first_name"].to_list() == ["John"]

    def test_select_star_returns_normalized_names(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            sql = f"SELECT * FROM {reader_engine.db_table}"
            df = _query_result_to_dataframe(reader_engine.query_data(sql), engine)
            assert df.columns == NORMALIZED_HEADERS

    def test_default_instance_still_queries_original_names(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine).create_reader()
            sql = f'SELECT "First Name" FROM {reader_engine.db_table}'
            df = _query_result_to_dataframe(reader_engine.query_data(sql), engine)
            assert df.columns == ["First Name"]

    def test_legacy_per_call_true_queries_original_and_renames_result(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine).create_reader()
            sql = f'SELECT "First Name" FROM {reader_engine.db_table}'
            df = _query_result_to_dataframe(reader_engine.query_data(sql, normalize_columns=True), engine)
            assert df.columns == ["first_name"]

    def test_per_call_false_overrides_instance_true_query(self, mixed_headers_csv):
        for engine in ALL_ENGINES:
            reader_engine = CSVEngineFactory(mixed_headers_csv, engine, normalize_columns=True).create_reader()
            sql = f'SELECT "First Name" FROM {reader_engine.db_table}'
            df = _query_result_to_dataframe(reader_engine.query_data(sql, normalize_columns=False), engine)
            assert df.columns == ["First Name"]

    def test_mixed_modes_reimport_table_correctly(self, mixed_headers_csv):
        """Alternating inherit and legacy calls re-imports the table per mode.

        Each fresh query must see the vocabulary its mode implies; stale
        relations from prior calls are documented as invalidated.
        """
        reader_engine = CSVEngineFactory(mixed_headers_csv, "duckdb", normalize_columns=True).create_reader()
        inherit_sql = f"SELECT first_name FROM {reader_engine.db_table}"
        legacy_sql = f'SELECT "First Name" FROM {reader_engine.db_table}'

        assert reader_engine.query_data(inherit_sql).pl().columns == ["first_name"]
        assert reader_engine.query_data(legacy_sql, normalize_columns=False).pl().columns == ["First Name"]
        assert reader_engine.query_data(inherit_sql).pl().columns == ["first_name"]
