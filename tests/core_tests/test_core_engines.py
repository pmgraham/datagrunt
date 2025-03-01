import polars as pl
import duckdb
import json
import pyarrow as pa
from src.datagrunt.core.engines import (
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine
)
from unittest.mock import Mock

class TestCSVReaderDuckDBEngine:
    """Test suite for CSVReaderDuckDBEngine"""

    def test_init(self, sample_csv_engines):
        reader = CSVReaderDuckDBEngine(sample_csv_engines)
        assert reader.filepath == sample_csv_engines
        assert hasattr(reader, 'queries')

    def test_db_table(self, sample_csv_engines):
        reader = CSVReaderDuckDBEngine(sample_csv_engines)
        assert isinstance(reader.db_table, str)
        assert reader.db_table.startswith('testengines')

    def test_create_table(self, sample_csv_engines):
        reader = CSVReaderDuckDBEngine(sample_csv_engines)
        result = reader.queries.create_table()
        assert isinstance(result, duckdb.DuckDBPyRelation)

    def test_create_table_normalized(self, sample_csv_engines):
        reader = CSVReaderDuckDBEngine(sample_csv_engines)
        result = reader.queries.create_table(normalize_columns=True)
        assert isinstance(result, duckdb.DuckDBPyRelation)
        # Check if columns are normalized
        columns = result.columns
        assert all(c.islower() for c in columns)

    def test_get_sample(self, sample_csv_engines, capsys):
        """Test get_sample method displays data correctly"""
        # Initialize reader
        reader = CSVReaderDuckDBEngine(sample_csv_engines)

        # Call get_sample
        reader.get_sample()

        # Capture the output
        captured = capsys.readouterr()

        # Verify output contains expected elements
        assert "Name" in captured.out
        assert "Age" in captured.out
        assert "City" in captured.out
        assert "John Doe" in captured.out

        # Test with normalized columns
        reader.get_sample(normalize_columns=True)
        captured = capsys.readouterr()

        # Verify normalized column names
        assert "name" in captured.out.lower()
        assert "age" in captured.out.lower()
        assert "city" in captured.out.lower()

    def test_to_dataframe(self, sample_csv_engines):
        reader = CSVReaderDuckDBEngine(sample_csv_engines)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0

    def test_to_arrow_table(self, sample_csv_engines):
        reader = CSVReaderDuckDBEngine(sample_csv_engines)
        table = reader.to_arrow_table()
        assert isinstance(table, pa.Table)

    def test_to_dicts(self, sample_csv_engines):
        reader = CSVReaderDuckDBEngine(sample_csv_engines)
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert all(isinstance(d, dict) for d in dicts)

class TestCSVReaderPolarsEngine:
    """Test suite for CSVReaderPolarsEngine"""

    def test_create_dataframe(self, sample_csv_engines):
        reader = CSVReaderPolarsEngine(sample_csv_engines)
        df = reader._create_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0

    def test_create_dataframe_sample(self, sample_csv_engines, create_large_csv):
        """Test _create_dataframe_sample method"""
        reader = CSVReaderPolarsEngine(sample_csv_engines)

        # Test default behavior
        df = reader._create_dataframe_sample()
        assert isinstance(df, pl.DataFrame)
        assert len(df) <= reader.DATAFRAME_SAMPLE_ROWS
        assert list(df.columns) == ["Name", "Age", "City"]

        # Test with normalized columns
        df_normalized = reader._create_dataframe_sample(normalize_columns=True)
        assert isinstance(df_normalized, pl.DataFrame)
        assert len(df_normalized) <= reader.DATAFRAME_SAMPLE_ROWS
        assert list(df_normalized.columns) == ["name", "age", "city"]

        # Verify sample size with larger dataset
        large_csv = create_large_csv(reader.DATAFRAME_SAMPLE_ROWS + 10)
        large_reader = CSVReaderPolarsEngine(large_csv)
        large_df = large_reader._create_dataframe_sample()
        assert len(large_df) == reader.DATAFRAME_SAMPLE_ROWS

    def test_create_dataframe_normalized(self, sample_csv_engines):
        reader = CSVReaderPolarsEngine(sample_csv_engines)
        df = reader._create_dataframe(normalize_columns=True)
        assert isinstance(df, pl.DataFrame)
        assert all(c.islower() for c in df.columns)

    def test_get_sample(self, sample_csv_engines, capsys):
        """Test get_sample method displays data correctly"""
        # Initialize reader
        reader = CSVReaderPolarsEngine(sample_csv_engines)

        # Call get_sample
        reader.get_sample()

        # Capture the output
        captured = capsys.readouterr()

        # Verify output contains expected elements
        assert "Name" in captured.out
        assert "Age" in captured.out
        assert "City" in captured.out
        assert "John Doe" in captured.out

        # Test with normalized columns
        reader.get_sample(normalize_columns=True)
        captured = capsys.readouterr()

        # Verify normalized column names
        assert "name" in captured.out.lower()
        assert "age" in captured.out.lower()
        assert "city" in captured.out.lower()

    def test_to_dataframe(self, sample_csv_engines):
        reader = CSVReaderPolarsEngine(sample_csv_engines)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)

    def test_to_arrow_table(self, sample_csv_engines):
        reader = CSVReaderPolarsEngine(sample_csv_engines)
        table = reader.to_arrow_table()
        assert isinstance(table, pa.Table)

    def test_to_dicts(self, sample_csv_engines):
        reader = CSVReaderPolarsEngine(sample_csv_engines)
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert all(isinstance(d, dict) for d in dicts)

class TestCSVWriterDuckDBEngine:
    """Test suite for CSVWriterDuckDBEngine"""

    def test_db_table_property(self, sample_csv_engines):
        # Arrange
        mock_queries = Mock()
        mock_queries.database_table_name = "testengines"

        engine = CSVWriterDuckDBEngine(sample_csv_engines)
        engine.queries = mock_queries  # Set the mocked queries object

        # Act
        result = engine.db_table

        # Assert
        assert result == "testengines"
        assert result == engine.queries.database_table_name

    def test_set_out_filename(self, sample_csv_engines):
        # Arrange
        engine = CSVWriterDuckDBEngine(sample_csv_engines)
        default_filename = "default.csv"
        custom_filename = "custom.csv"

        # Test case 1: When out_filename is provided
        result1 = engine._set_out_filename(default_filename, custom_filename)
        assert result1 == custom_filename

        # Test case 2: When out_filename is None
        result2 = engine._set_out_filename(default_filename)
        assert result2 == default_filename

        # Test case 3: When out_filename is empty string
        result3 = engine._set_out_filename(default_filename, "")
        assert result3 == default_filename

    def test_write_csv(self, sample_csv_engines, tmp_path):
        writer = CSVWriterDuckDBEngine(sample_csv_engines)
        out_file = tmp_path / "output.csv"
        writer.write_csv(str(out_file))
        assert out_file.exists()

    def test_write_excel(self, sample_csv_engines, tmp_path):
        writer = CSVWriterDuckDBEngine(sample_csv_engines)
        out_file = tmp_path / "output.xlsx"
        writer.write_excel(str(out_file))
        assert out_file.exists()

    def test_write_json(self, sample_csv_engines, tmp_path):
        writer = CSVWriterDuckDBEngine(sample_csv_engines)
        out_file = tmp_path / "output.json"
        writer.write_json(str(out_file))
        assert out_file.exists()
        # Verify JSON is valid
        with open(out_file) as f:
            json.load(f)

    def test_write_parquet(self, sample_csv_engines, tmp_path):
        writer = CSVWriterDuckDBEngine(sample_csv_engines)
        out_file = tmp_path / "output.parquet"
        writer.write_parquet(str(out_file))
        assert out_file.exists()

class TestCSVWriterPolarsEngine:
    """Test suite for CSVWriterPolarsEngine"""

    def test_set_out_filename(self, sample_csv_engines):
        # Arrange
        engine = CSVWriterPolarsEngine(sample_csv_engines)
        default_filename = "default.csv"
        custom_filename = "custom.csv"

        # Test case 1: When out_filename is provided
        result1 = engine._set_out_filename(default_filename, custom_filename)
        assert result1 == custom_filename

        # Test case 2: When out_filename is None
        result2 = engine._set_out_filename(default_filename)
        assert result2 == default_filename

        # Test case 3: When out_filename is empty string
        result3 = engine._set_out_filename(default_filename, "")
        assert result3 == default_filename

    def test_write_csv(self, sample_csv_engines, tmp_path):
        writer = CSVWriterPolarsEngine(sample_csv_engines)
        out_file = tmp_path / "output.csv"
        writer.write_csv(str(out_file))
        assert out_file.exists()

    def test_write_excel(self, sample_csv_engines, tmp_path):
        writer = CSVWriterPolarsEngine(sample_csv_engines)
        out_file = tmp_path / "output.xlsx"
        writer.write_excel(str(out_file))
        assert out_file.exists()

    def test_write_json(self, sample_csv_engines, tmp_path):
        writer = CSVWriterPolarsEngine(sample_csv_engines)
        out_file = tmp_path / "output.json"
        writer.write_json(str(out_file))
        assert out_file.exists()
        # Verify JSON is valid
        with open(out_file) as f:
            json.load(f)

    def test_write_parquet(self, sample_csv_engines, tmp_path):
        writer = CSVWriterPolarsEngine(sample_csv_engines)
        out_file = tmp_path / "output.parquet"
        writer.write_parquet(str(out_file))
        assert out_file.exists()

    def test_write_json_newline_delimited(self, sample_csv_engines, tmp_path):
        writer = CSVWriterPolarsEngine(sample_csv_engines)
        out_file = tmp_path / "output.ndjson"
        writer.write_json_newline_delimited(str(out_file))
        assert out_file.exists()
        # Verify each line is valid JSON
        with open(out_file) as f:
            for line in f:
                json.loads(line.strip())
