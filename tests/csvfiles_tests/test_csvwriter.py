import os
import pytest
from src.datagrunt import CSVWriter

class TestCSVWriter:
    def test_initialization(self, sample_csv):
        """Test basic initialization of CSVWriter."""
        writer = CSVWriter(sample_csv)
        assert isinstance(writer, CSVWriter)
        assert writer.filepath == sample_csv
        assert writer.engine == 'duckdb'

    def test_invalid_engine(self, sample_csv):
        """Test initialization with invalid engine."""
        with pytest.raises(ValueError):
            CSVWriter(sample_csv, engine='invalid')

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
