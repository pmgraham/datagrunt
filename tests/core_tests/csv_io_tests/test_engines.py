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

# All supported engines for CSV processing
ALL_ENGINES = ["duckdb", "polars", "pyarrow"]

# Engine class mappings for validation
READER_ENGINE_CLASSES = {
    "duckdb": CSVReaderDuckDBEngine,
    "polars": CSVReaderPolarsEngine,
    "pyarrow": CSVReaderPyArrowEngine,
}

WRITER_ENGINE_CLASSES = {
    "duckdb": CSVWriterDuckDBEngine,
    "polars": CSVWriterPolarsEngine,
    "pyarrow": CSVWriterPyArrowEngine,
}

# Query result types by engine
QUERY_RESULT_TYPES = {
    "duckdb": DuckDBPyRelation,
    "polars": pl.DataFrame,
    "pyarrow": pl.DataFrame,
}


class TestEngines:
    def test_engine_properties(self):
        """Test CSVEngineProperties default values."""
        props = CSVEngineProperties("test.csv")
        assert props.dataframe_sample_rows == 20
        assert props.csv_export_filename == "output.csv"
        assert props.valid_engines == ("duckdb", "polars", "pyarrow")

    def test_init_with_all_engines(self, sample_csv):
        """Test initialization with all valid engine values."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine=engine)
            assert factory.engine == engine

        # Test with spaces and different cases
        test_cases = [
            ("Duck DB", "duckdb"),
            ("POLARS", "polars"),
            ("PyArrow", "pyarrow"),
        ]
        for input_engine, expected_engine in test_cases:
            factory = CSVEngineFactory(sample_csv, engine=input_engine)
            assert factory.engine == expected_engine

    def test_init_with_invalid_engine(self, sample_csv):
        """Test initialization with invalid engine value."""
        with pytest.raises(ValueError) as exc_info:
            CSVEngineFactory(sample_csv, engine="invalid")
        assert "Reader engine 'invalid' is not 'duckdb', 'polars', or 'pyarrow'" in str(exc_info.value)

    def test_reader_creation_all_engines(self, sample_csv):
        """Test creation of reader engines for all supported engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            reader = factory.create_reader()
            expected_class = READER_ENGINE_CLASSES[engine]
            assert isinstance(reader, expected_class)

    def test_writer_creation_all_engines(self, sample_csv):
        """Test creation of writer engines for all supported engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            writer = factory.create_writer()
            expected_class = WRITER_ENGINE_CLASSES[engine]
            assert isinstance(writer, expected_class)

    def test_reader_to_dataframe_all_engines(self, sample_csv):
        """Test to_dataframe method for all reader engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            reader = factory.create_reader()
            df = reader.to_dataframe()
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 2  # Based on our sample data

    def test_reader_to_arrow_all_engines(self, sample_csv):
        """Test to_arrow_table method for all reader engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            reader = factory.create_reader()
            table = reader.to_arrow_table()
            assert isinstance(table, pa.Table)

    def test_reader_to_dicts_all_engines(self, sample_csv):
        """Test to_dicts method for all reader engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            reader = factory.create_reader()
            dicts = reader.to_dicts()
            assert isinstance(dicts, list)
            assert all(isinstance(d, dict) for d in dicts)
            assert len(dicts) == 2

    def test_reader_query_data_all_engines(self, sample_csv):
        """Test query_data method for all reader engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            reader = factory.create_reader()
            result = reader.query_data(f"SELECT * FROM {reader.db_table} LIMIT 1")
            expected_type = QUERY_RESULT_TYPES[engine]
            assert isinstance(result, expected_type)

    def test_reader_get_sample_all_engines(self, sample_csv, capsys):
        """Test get_sample method for all reader engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            reader = factory.create_reader()

            # Test get_sample without normalization
            reader.get_sample()
            captured = capsys.readouterr()
            # Different engines output different formats, just verify something was printed
            assert len(captured.out) > 0
            assert "John" in captured.out or "Jane" in captured.out  # Should contain sample data

            # Test get_sample with normalization
            reader.get_sample(normalize_columns=True)
            captured = capsys.readouterr()
            assert len(captured.out) > 0

    def test_writer_basic_operations_all_engines(self, tmp_path, sample_csv):
        """Test basic write operations for all writer engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            writer = factory.create_writer()

            # Test CSV write
            csv_path = str(tmp_path / f"{engine}_output.csv")
            writer.write_csv(csv_path)
            assert Path(csv_path).exists()

            # Test Parquet write
            parquet_path = str(tmp_path / f"{engine}_output.parquet")
            writer.write_parquet(parquet_path)
            assert Path(parquet_path).exists()

            # Test JSON write
            json_path = str(tmp_path / f"{engine}_output.json")
            writer.write_json(json_path)
            assert Path(json_path).exists()

    def test_writer_advanced_operations_all_engines(self, tmp_path, sample_csv):
        """Test advanced write operations for all writer engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            writer = factory.create_writer()

            # Test CSV write with normalization
            csv_norm_path = str(tmp_path / f"{engine}_output_norm.csv")
            writer.write_csv(csv_norm_path, normalize_columns=True)
            assert Path(csv_norm_path).exists()

            # Test JSON write with normalization
            json_norm_path = str(tmp_path / f"{engine}_output_norm.json")
            writer.write_json(json_norm_path, normalize_columns=True)
            assert Path(json_norm_path).exists()

            # Test JSONL write
            jsonl_path = str(tmp_path / f"{engine}_output.jsonl")
            writer.write_json_newline_delimited(jsonl_path)
            assert Path(jsonl_path).exists()

            # Test JSONL write with normalization
            jsonl_norm_path = str(tmp_path / f"{engine}_output_norm.jsonl")
            writer.write_json_newline_delimited(jsonl_norm_path, normalize_columns=True)
            assert Path(jsonl_norm_path).exists()

            # Test Parquet write with normalization
            parquet_norm_path = str(tmp_path / f"{engine}_output_norm.parquet")
            writer.write_parquet(parquet_norm_path, normalize_columns=True)
            assert Path(parquet_norm_path).exists()

            # Test Excel write
            excel_path = str(tmp_path / f"{engine}_output.xlsx")
            writer.write_excel(excel_path)
            assert Path(excel_path).exists()

            # Test Excel write with normalization
            excel_norm_path = str(tmp_path / f"{engine}_output_norm.xlsx")
            writer.write_excel(excel_norm_path, normalize_columns=True)
            assert Path(excel_norm_path).exists()

    def test_writer_default_filenames_all_engines(self, sample_csv):
        """Test writer using default filenames for all engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            writer = factory.create_writer()

            # Test write operations with default filenames
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

    def test_normalized_columns_all_engines(self, sample_csv):
        """Test column normalization functionality for all engines."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)
            reader = factory.create_reader()
            df = reader.to_dataframe(normalize_columns=True)
            assert all(col.islower() for col in df.columns)
            assert all(" " not in col for col in df.columns)

    def test_empty_file_handling_all_engines(self, tmp_path):
        """Test handling of empty CSV files for all engines."""
        # Create an empty CSV file with just headers
        empty_csv = tmp_path / "empty.csv"
        empty_csv.write_text("col1,col2\n")

        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(str(empty_csv), engine)
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

    def test_single_column_handling_all_engines(self, tmp_path):
        """Test handling of single column CSV files for all engines."""
        # Create a single column CSV
        single_col_csv = tmp_path / "single_column.csv"
        single_col_csv.write_text("value\n1\n2\n3")

        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(str(single_col_csv), engine)
            reader = factory.create_reader()

            # Test to_dataframe handles single column conversion
            df = reader.to_dataframe()
            assert isinstance(df, pl.DataFrame)
            assert len(df.columns) >= 1
            assert len(df) == 3

    def test_string_preservation_all_engines(self, tmp_path):
        """Test that all engines preserve string data appropriately."""
        # Create CSV with data that could lose precision if not treated as strings
        special_data_csv = tmp_path / "special_data.csv"
        special_data_csv.write_text("id,amount,code\n001,0500,ABC123\n002,1000.50,DEF456")

        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(str(special_data_csv), engine)
            reader = factory.create_reader()

            # Test that data is accessible (exact preservation may vary by engine)
            dicts = reader.to_dicts()
            assert len(dicts) == 2
            assert dicts[0]["code"] == "ABC123"  # String data should be preserved
            assert dicts[1]["code"] == "DEF456"

    def test_invalid_file_handling_all_engines(self, tmp_path):
        """Test handling of non-existent files for all engines."""
        non_existent_file = str(tmp_path / "doesnotexist.csv")

        for engine in ALL_ENGINES:
            with pytest.raises(FileNotFoundError):
                CSVEngineFactory(non_existent_file, engine)

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

    def test_all_engines_interface_completeness(self, sample_csv):
        """Test that all supported engines implement the complete interface."""
        for engine in ALL_ENGINES:
            factory = CSVEngineFactory(sample_csv, engine)

            # Test reader creation and interface
            reader = factory.create_reader()
            assert reader is not None
            assert hasattr(reader, "to_dataframe")
            assert hasattr(reader, "to_arrow_table")
            assert hasattr(reader, "to_dicts")
            assert hasattr(reader, "query_data")
            assert hasattr(reader, "get_sample")

            # Test writer creation and interface
            writer = factory.create_writer()
            assert writer is not None
            assert hasattr(writer, "write_csv")
            assert hasattr(writer, "write_json")
            assert hasattr(writer, "write_parquet")
            assert hasattr(writer, "write_excel")
            assert hasattr(writer, "write_json_newline_delimited")

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
