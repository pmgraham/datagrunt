from pathlib import Path

from duckdb import DuckDBPyRelation
import polars as pl
import pyarrow as pa
import pytest

from src.datagrunt.core.engines import (
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine,
    EngineFactory,
    EngineProperties
)

class TestEngines:
    def test_engine_properties(self):
        """Test EngineProperties default values."""
        props = EngineProperties("test.csv")
        assert props.dataframe_sample_rows == 20
        assert props.csv_export_filename == 'output.csv'
        assert props.valid_engines == ('duckdb', 'polars')

    def test_duckdb_reader_creation(self, engine_factory):
        """Test creation of DuckDB reader engine."""
        reader = engine_factory.create_reader()
        assert isinstance(reader, CSVReaderDuckDBEngine)

    def test_polars_reader_creation(self, sample_csv):
        """Test creation of Polars reader engine."""
        factory = EngineFactory(sample_csv, 'polars')
        reader = factory.create_reader()
        assert isinstance(reader, CSVReaderPolarsEngine)

    def test_duckdb_writer_creation(self, engine_factory):
        """Test creation of DuckDB writer engine."""
        writer = engine_factory.create_writer()
        assert isinstance(writer, CSVWriterDuckDBEngine)

    def test_polars_writer_creation(self, sample_csv):
        """Test creation of Polars writer engine."""
        factory = EngineFactory(sample_csv, 'polars')
        writer = factory.create_writer()
        assert isinstance(writer, CSVWriterPolarsEngine)

    def test_duckdb_reader_to_dataframe(self, engine_factory):
        """Test DuckDB reader's to_dataframe method."""
        reader = engine_factory.create_reader()
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2  # Based on our sample data

    def test_polars_reader_to_dataframe(self, sample_csv):
        """Test Polars reader's to_dataframe method."""
        factory = EngineFactory(sample_csv, 'polars')
        reader = factory.create_reader()
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2

    def test_duckdb_reader_to_arrow(self, engine_factory):
        """Test DuckDB reader's to_arrow_table method."""
        reader = engine_factory.create_reader()
        table = reader.to_arrow_table()
        assert isinstance(table, pa.Table)

    def test_duckdb_reader_query_data(self, engine_factory):
        """Test DuckDB reader's query_data method."""
        reader = engine_factory.create_reader()
        result = reader.query_data(f"SELECT * FROM {reader.db_table} LIMIT 1")
        assert isinstance(result, DuckDBPyRelation)

    def test_write_operations(self, tmp_path, engine_factory):
        """Test various write operations for both engines."""
        writer = engine_factory.create_writer()

        # Test CSV write
        csv_path = str(tmp_path / "test_output.csv")
        writer.write_csv(csv_path)
        assert Path(csv_path).exists()

        # Test Parquet write
        parquet_path = str(tmp_path / "test_output.parquet")
        writer.write_parquet(parquet_path)
        assert Path(parquet_path).exists()

        # Test JSON write
        json_path = str(tmp_path / "test_output.json")
        writer.write_json(json_path)
        assert Path(json_path).exists()

    def test_normalized_columns(self, sample_csv):
        """Test column normalization functionality."""
        factory = EngineFactory(sample_csv, 'polars')
        reader = factory.create_reader()
        df = reader.to_dataframe(normalize_columns=True)
        assert all(col.islower() for col in df.columns)
        assert all(' ' not in col for col in df.columns)

    def test_polars_reader_to_dicts(self, sample_csv):
        """Test conversion to dictionary list."""
        factory = EngineFactory(sample_csv, 'polars')
        reader = factory.create_reader()
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert all(isinstance(d, dict) for d in dicts)
        assert len(dicts) == 2

    def test_invalid_file_handling(self, tmp_path):
        """Test handling of non-existent files."""
        non_existent_file = str(tmp_path / "doesnotexist.csv")
        with pytest.raises(FileNotFoundError) as exc_info:
            EngineFactory(non_existent_file, 'duckdb')
        assert 'No such file or directory' in str(exc_info.value)
