"""Unit tests for CSVWriterPolarsEngine."""

import os
import pytest
from src.datagrunt.core.engines import CSVWriterPolarsEngine

@pytest.fixture
def sample_csv_file(tmp_path):
    """Fixture to create a sample CSV file for testing."""
    filepath = tmp_path / "test.csv"
    with open(filepath, 'w') as f:
        f.write('Name,Age,City\n')
        f.write('Alice,25,New York\n')
        f.write('Bob,30,London\n')
        f.write('Charlie,28,Paris\n')
    return filepath

@pytest.fixture
def csv_writer(sample_csv_file):
    """Fixture to create a CSVWriterPolarsEngine instance."""
    return CSVWriterPolarsEngine(sample_csv_file)

@pytest.fixture
def output_files(tmp_path):
    """Fixture to create output file paths."""
    return {
        'csv': tmp_path / "test_out.csv",
        'excel': tmp_path / "test_out.xlsx",
        'json': tmp_path / "test_out.json",
        'ndjson': tmp_path / "test_out.jsonl",
        'parquet': tmp_path / "test_out.parquet"
    }

def test_set_out_filename(csv_writer):
    """Test the _set_out_filename method of CSVWriterPolarsEngine."""
    # Test with default filename
    default_filename = "default.csv"
    result = csv_writer._set_out_filename(default_filename)
    assert result == default_filename, "Should return default filename when no out_filename is provided"

    # Test with custom filename
    custom_filename = "custom.csv"
    result = csv_writer._set_out_filename(default_filename, custom_filename)
    assert result == custom_filename, "Should return custom filename when provided"

    # Test with None as custom filename
    result = csv_writer._set_out_filename(default_filename, None)
    assert result == default_filename, "Should return default filename when None is provided as out_filename"

def test_write_csv(csv_writer, output_files):
    """Test if write_csv exports the correct CSV file."""
    csv_writer.write_csv(out_filename=output_files['csv'])
    assert os.path.exists(output_files['csv'])

def test_write_excel(csv_writer, output_files):
    """Test if write_excel exports the correct Excel file."""
    csv_writer.write_csv(out_filename=output_files['excel'])
    assert os.path.exists(output_files['excel'])

def test_write_json(csv_writer, output_files):
    """Test if write_json exports the correct JSON file."""
    csv_writer.write_csv(out_filename=output_files['json'])
    assert os.path.exists(output_files['json'])

def test_write_json_newline_delimited(csv_writer, output_files):
    """Test if write_json_newline_delimited exports the correct JSON newline delimited file."""
    csv_writer.write_csv(out_filename=output_files['ndjson'])
    assert os.path.exists(output_files['ndjson'])

def test_write_parquet(csv_writer, output_files):
    """Test if write_parquet exports the correct Parquet file."""
    csv_writer.write_parquet(out_filename=output_files['parquet'])
    assert os.path.exists(output_files['parquet'])
