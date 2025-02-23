import os
import pytest

from src.datagrunt import CSVWriter
from src.datagrunt.core import CSVWriterPolarsEngine

class TestCSVWriter:
    def test_initialization(self, sample_csv):
        """Test basic initialization of CSVWriter."""
        writer = CSVWriter(sample_csv)
        assert isinstance(writer, CSVWriter)
        assert writer.filepath == sample_csv
        assert writer.engine == 'duckdb'

    def test_csv_writer_invalid_engine(self, temp_csv_file):
        """Test that an invalid engine raises a ValueError."""
        with pytest.raises(ValueError) as exc_info:
            CSVWriter(temp_csv_file, engine="invalid")
        assert "Writer engine 'invalid' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params." in str(exc_info.value)

    def test_csv_writer_default_engine(self, temp_csv_file):
        """Test that the default engine is 'polars'."""
        writer = CSVWriter(temp_csv_file)
        assert writer.engine == "duckdb"

    def test_csv_writer_set_writer_engine_polars(self, temp_csv_file):
        """Test that _set_writer_engine returns a Polars engine."""
        writer = CSVWriter(temp_csv_file, engine="polars")
        engine = writer._set_writer_engine()
        assert isinstance(engine, CSVWriterPolarsEngine)

    def test_write_csv(self, sample_csv):
        """Test writing to CSV."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.csv"
        writer.write_csv(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_json(self, sample_csv):
        """Test writing to JSON."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.json"
        writer.write_json(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_parquet(self, sample_csv):
        """Test writing to Parquet."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.parquet"
        writer.write_parquet(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_excel(self, sample_csv):
        """Test writing to Excel."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.xlsx"
        writer.write_excel(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_write_json_newline_delimited(self, sample_csv):
        """Test writing to newline-delimited JSON."""
        writer = CSVWriter(sample_csv)
        output_file = "output_test.jsonl"
        writer.write_json_newline_delimited(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_excel_empty(self, empty_csv):
        """Test writing an empty dataframe to excel"""
        writer = CSVWriter(empty_csv)
        output_file = "empty_output_test.xlsx"
        writer.write_excel(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_excel_blank(self, temp_blank_csv_file):
        """Test writing a blank file dataframe to excel"""
        writer = CSVWriter(temp_blank_csv_file)
        output_file = "blank_output_test.xlsx"
        writer.write_excel(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_parquet_empty(self, empty_csv):
        """Test writing an empty dataframe to parquet"""
        writer = CSVWriter(empty_csv)
        output_file = "empty_output_test.parquet"
        writer.write_parquet(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_parquet_blank(self, temp_blank_csv_file):
        """Test writing a blank file dataframe to parquet"""
        writer = CSVWriter(temp_blank_csv_file)
        output_file = "blank_output_test.parquet"
        writer.write_parquet(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_csv_empty(self, empty_csv):
        """Test writing an empty dataframe to csv"""
        writer = CSVWriter(empty_csv)
        output_file = "empty_output_test.csv"
        writer.write_csv(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_csv_blank(self, temp_blank_csv_file):
        """Test writing a blank file dataframe to csv"""
        writer = CSVWriter(temp_blank_csv_file)
        output_file = "blank_output_test.csv"
        writer.write_csv(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_json_empty(self, empty_csv):
        """Test writing an empty dataframe to json"""
        writer = CSVWriter(empty_csv)
        output_file = "empty_output_test.json"
        writer.write_json(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_json_blank(self, temp_blank_csv_file):
        """Test writing a blank file dataframe to json"""
        writer = CSVWriter(temp_blank_csv_file)
        output_file = "blank_output_test.json"
        writer.write_json(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_json_newline_delimited_empty(self, empty_csv):
        """Test writing an empty dataframe to jsonl"""
        writer = CSVWriter(empty_csv)
        output_file = "empty_output_test.jsonl"
        writer.write_json_newline_delimited(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)

    def test_csv_writer_write_json_newline_delimited_blank(self, temp_blank_csv_file):
        """Test writing a blank file dataframe to jsonl"""
        writer = CSVWriter(temp_blank_csv_file)
        output_file = "blank_output_test.jsonl"
        writer.write_json_newline_delimited(output_file)
        assert os.path.exists(output_file)
        # Cleanup
        os.remove(output_file)
