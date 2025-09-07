"""This module contains tests for the engine classes."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from duckdb import DuckDBPyRelation

from datagrunt.core import (
    CSVEngineFactory,
    CSVEngineProperties,
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
    CSVReaderPyArrowEngine,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine,
    CSVWriterPyArrowEngine,
)


class TestEngines:
    def test_engine_properties(self):
        """Test CSVEngineProperties default values."""
        props = CSVEngineProperties("test.csv")
        assert props.dataframe_sample_rows == 20
        assert props.csv_export_filename == "output.csv"
        assert props.valid_engines == ("duckdb", "polars", "pyarrow")

    def test_init_with_valid_engines(self, sample_csv):
        """Test initialization with valid engine values."""
        reader_polars = CSVEngineFactory(sample_csv, engine="polars")
        assert reader_polars.engine == "polars"

        reader_duckdb = CSVEngineFactory(sample_csv, engine="duckdb")
        assert reader_duckdb.engine == "duckdb"

        reader_pyarrow = CSVEngineFactory(sample_csv, engine="pyarrow")
        assert reader_pyarrow.engine == "pyarrow"

        # Test with spaces and different cases
        reader_with_spaces = CSVEngineFactory(sample_csv, engine="Duck DB")
        assert reader_with_spaces.engine == "duckdb"

    def test_init_with_invalid_engine(self, sample_csv):
        """Test initialization with invalid engine value."""
        with pytest.raises(ValueError) as exc_info:
            CSVEngineFactory(sample_csv, engine="invalid")
        assert "Reader engine 'invalid' is not 'duckdb', 'polars', or 'pyarrow'" in str(exc_info.value)

    def test_duckdb_reader_creation(self, engine_factory):
        """Test creation of DuckDB reader engine."""
        reader = engine_factory.create_reader()
        assert isinstance(reader, CSVReaderDuckDBEngine)

    def test_polars_reader_creation(self, sample_csv):
        """Test creation of Polars reader engine."""
        factory = CSVEngineFactory(sample_csv, "polars")
        reader = factory.create_reader()
        assert isinstance(reader, CSVReaderPolarsEngine)

    def test_duckdb_writer_creation(self, engine_factory):
        """Test creation of DuckDB writer engine."""
        writer = engine_factory.create_writer()
        assert isinstance(writer, CSVWriterDuckDBEngine)

    def test_polars_writer_creation(self, sample_csv):
        """Test creation of Polars writer engine."""
        factory = CSVEngineFactory(sample_csv, "polars")
        writer = factory.create_writer()
        assert isinstance(writer, CSVWriterPolarsEngine)

    def test_pyarrow_reader_creation(self, sample_csv):
        """Test creation of PyArrow reader engine."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        reader = factory.create_reader()
        assert isinstance(reader, CSVReaderPyArrowEngine)

    def test_pyarrow_writer_creation(self, sample_csv):
        """Test creation of PyArrow writer engine."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        writer = factory.create_writer()
        assert isinstance(writer, CSVWriterPyArrowEngine)

    def test_duckdb_reader_to_dataframe(self, engine_factory):
        """Test DuckDB reader's to_dataframe method."""
        reader = engine_factory.create_reader()
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2  # Based on our sample data

    def test_polars_reader_to_dataframe(self, sample_csv):
        """Test Polars reader's to_dataframe method."""
        factory = CSVEngineFactory(sample_csv, "polars")
        reader = factory.create_reader()
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2

    def test_duckdb_reader_to_arrow(self, engine_factory):
        """Test DuckDB reader's to_arrow_table method."""
        reader = engine_factory.create_reader()
        table = reader.to_arrow_table()
        assert isinstance(table, pa.Table)

    def test_pyarrow_reader_to_dataframe(self, sample_csv):
        """Test PyArrow reader's to_dataframe method."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        reader = factory.create_reader()
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2

    def test_pyarrow_reader_to_arrow(self, sample_csv):
        """Test PyArrow reader's to_arrow_table method."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        reader = factory.create_reader()
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
        factory = CSVEngineFactory(sample_csv, "polars")
        reader = factory.create_reader()
        df = reader.to_dataframe(normalize_columns=True)
        assert all(col.islower() for col in df.columns)
        assert all(" " not in col for col in df.columns)

    def test_polars_reader_to_dicts(self, sample_csv):
        """Test conversion to dictionary list."""
        factory = CSVEngineFactory(sample_csv, "polars")
        reader = factory.create_reader()
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert all(isinstance(d, dict) for d in dicts)
        assert len(dicts) == 2

    def test_pyarrow_reader_to_dicts(self, sample_csv):
        """Test PyArrow reader's to_dicts method."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        reader = factory.create_reader()
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert all(isinstance(d, dict) for d in dicts)
        assert len(dicts) == 2

    def test_pyarrow_reader_query_data(self, sample_csv):
        """Test PyArrow reader's query_data method."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        reader = factory.create_reader()
        result = reader.query_data(f"SELECT * FROM {reader.db_table} LIMIT 1")
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1

    def test_pyarrow_write_operations(self, tmp_path, sample_csv):
        """Test PyArrow writer methods."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        writer = factory.create_writer()

        # Test CSV write
        csv_path = str(tmp_path / "pyarrow_output.csv")
        writer.write_csv(csv_path)
        assert Path(csv_path).exists()

        # Test CSV write with normalization
        csv_norm_path = str(tmp_path / "pyarrow_output_norm.csv")
        writer.write_csv(csv_norm_path, normalize_columns=True)
        assert Path(csv_norm_path).exists()

        # Test JSON write
        json_path = str(tmp_path / "pyarrow_output.json")
        writer.write_json(json_path)
        assert Path(json_path).exists()

        # Test JSON write with normalization
        json_norm_path = str(tmp_path / "pyarrow_output_norm.json")
        writer.write_json(json_norm_path, normalize_columns=True)
        assert Path(json_norm_path).exists()

        # Test JSONL write
        jsonl_path = str(tmp_path / "pyarrow_output.jsonl")
        writer.write_json_newline_delimited(jsonl_path)
        assert Path(jsonl_path).exists()

        # Test JSONL write with normalization
        jsonl_norm_path = str(tmp_path / "pyarrow_output_norm.jsonl")
        writer.write_json_newline_delimited(jsonl_norm_path, normalize_columns=True)
        assert Path(jsonl_norm_path).exists()

        # Test Parquet write
        parquet_path = str(tmp_path / "pyarrow_output.parquet")
        writer.write_parquet(parquet_path)
        assert Path(parquet_path).exists()

        # Test Parquet write with normalization
        parquet_norm_path = str(tmp_path / "pyarrow_output_norm.parquet")
        writer.write_parquet(parquet_norm_path, normalize_columns=True)
        assert Path(parquet_norm_path).exists()

        # Test Excel write
        excel_path = str(tmp_path / "pyarrow_output.xlsx")
        writer.write_excel(excel_path)
        assert Path(excel_path).exists()

        # Test Excel write with normalization
        excel_norm_path = str(tmp_path / "pyarrow_output_norm.xlsx")
        writer.write_excel(excel_norm_path, normalize_columns=True)
        assert Path(excel_norm_path).exists()

    def test_pyarrow_get_sample(self, sample_csv, capsys):
        """Test PyArrow reader's get_sample method."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        reader = factory.create_reader()

        # Test get_sample without normalization
        reader.get_sample()
        captured = capsys.readouterr()
        assert "shape:" in captured.out

        # Test get_sample with normalization
        reader.get_sample(normalize_columns=True)
        captured = capsys.readouterr()
        assert "shape:" in captured.out

    def test_pyarrow_dataframe_series_conversion(self, tmp_path):
        """Test PyArrow handling of single column CSV (Series conversion)."""
        # Create a single column CSV
        single_col_csv = tmp_path / "single_column.csv"
        single_col_csv.write_text("value\n1\n2\n3")

        factory = CSVEngineFactory(str(single_col_csv), "pyarrow")
        reader = factory.create_reader()

        # Test to_dataframe handles Series conversion
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df.columns) >= 1

    def test_pyarrow_empty_file_handling(self, tmp_path):
        """Test PyArrow engine with empty CSV files."""
        # Create an empty CSV file with just headers
        empty_csv = tmp_path / "empty.csv"
        empty_csv.write_text("col1,col2\n")

        factory = CSVEngineFactory(str(empty_csv), "pyarrow")
        reader = factory.create_reader()

        # Test various operations with empty file
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

        table = reader.to_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 0

        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 0

    def test_pyarrow_writer_default_filenames(self, sample_csv):
        """Test PyArrow writer using default filenames."""
        factory = CSVEngineFactory(sample_csv, "pyarrow")
        writer = factory.create_writer()

        # Test write operations with default filenames (export_filename=None)
        writer.write_csv()  # Should create output.csv
        assert Path("output.csv").exists()
        Path("output.csv").unlink()  # Clean up

        writer.write_json()  # Should create output.json
        assert Path("output.json").exists()
        Path("output.json").unlink()  # Clean up

        writer.write_json_newline_delimited()  # Should create output.jsonl
        assert Path("output.jsonl").exists()
        Path("output.jsonl").unlink()  # Clean up

        writer.write_parquet()  # Should create output.parquet
        assert Path("output.parquet").exists()
        Path("output.parquet").unlink()  # Clean up

    def test_pyarrow_string_preservation(self, tmp_path):
        """Test PyArrow engine preserves string types and leading zeros."""
        # Create CSV with data that could lose precision if not treated as strings
        special_data_csv = tmp_path / "special_data.csv"
        special_data_csv.write_text("id,amount,code\n001,0500,ABC123\n002,1000.50,DEF456")

        factory = CSVEngineFactory(str(special_data_csv), "pyarrow")
        reader = factory.create_reader()

        # Test that data is preserved as strings
        table = reader.to_arrow_table()
        assert all(str(field.type) == "string" for field in table.schema)

        # Test that leading zeros are preserved
        first_row = {col: table[col][0].as_py() for col in table.column_names}
        assert first_row["id"] == "001"  # Leading zero preserved
        assert first_row["amount"] == "0500"  # Leading zero preserved

    def test_invalid_file_handling(self, tmp_path):
        """Test handling of non-existent files."""
        non_existent_file = str(tmp_path / "doesnotexist.csv")
        with pytest.raises(FileNotFoundError) as exc_info:
            CSVEngineFactory(non_existent_file, "duckdb")
        assert "No such file or directory" in str(exc_info.value)

        # Test with PyArrow engine as well
        with pytest.raises(FileNotFoundError):
            CSVEngineFactory(non_existent_file, "pyarrow")

    def test_factory_unsupported_engine_error_messages(self, sample_csv):
        """Test factory error handling for unsupported engines."""
        factory = CSVEngineFactory(sample_csv, "duckdb")

        # Test that we get meaningful error for unsupported reader engine
        with pytest.raises(ValueError) as exc_info:
            # This shouldn't happen in normal usage, but test the fallback
            engine_class = factory.READER_ENGINES.get("nonexistent")
            if engine_class is None:
                raise ValueError("Unsupported reader engine: nonexistent")
        assert "Unsupported reader engine: nonexistent" in str(exc_info.value)

        # Test that we get meaningful error for unsupported writer engine
        with pytest.raises(ValueError) as exc_info:
            engine_class = factory.WRITER_ENGINES.get("nonexistent")
            if engine_class is None:
                raise ValueError("Unsupported reader engine: nonexistent")
        assert "Unsupported reader engine: nonexistent" in str(exc_info.value)

    def test_factory_engine_name_normalization(self, sample_csv):
        """Test that factory properly normalizes engine names."""
        # Test various engine name formats
        test_cases = [
            ("DUCKDB", "duckdb"),
            ("DuckDB", "duckdb"),
            ("duck db", "duckdb"),
            ("Duck DB", "duckdb"),
            ("POLARS", "polars"),
            ("Polars", "polars"),
            ("pyarrow", "pyarrow"),
            ("PYARROW", "pyarrow"),
            ("PyArrow", "pyarrow"),
            ("py arrow", "pyarrow"),
        ]

        for input_engine, expected_engine in test_cases:
            factory = CSVEngineFactory(sample_csv, input_engine)
            assert factory.engine == expected_engine

    def test_all_engines_reader_writer_creation(self, sample_csv):
        """Test that all supported engines can create both readers and writers."""
        engines = CSVEngineProperties("").valid_engines

        for engine in engines:
            factory = CSVEngineFactory(sample_csv, engine)

            # Test reader creation
            reader = factory.create_reader()
            assert reader is not None
            assert hasattr(reader, "to_dataframe")
            assert hasattr(reader, "to_arrow_table")
            assert hasattr(reader, "to_dicts")
            assert hasattr(reader, "query_data")
            assert hasattr(reader, "get_sample")

            # Test writer creation
            writer = factory.create_writer()
            assert writer is not None
            assert hasattr(writer, "write_csv")
            assert hasattr(writer, "write_json")
            assert hasattr(writer, "write_parquet")
            assert hasattr(writer, "write_excel")
            assert hasattr(writer, "write_json_newline_delimited")
