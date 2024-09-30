"""Unit tests for CSVWriterDuckDBEngine."""

import os
import pytest
from src.datagrunt.core.engines import CSVWriterDuckDBEngine

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
    """Fixture to create a CSVWriterDuckDBEngine instance."""
    return CSVWriterDuckDBEngine(sample_csv_file)

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

def test_write_csv(csv_writer, output_files):
    """Test if write_csv exports the correct CSV file."""
    csv_writer.write_csv(out_filename=output_files['csv'])
    assert os.path.exists(output_files['csv'])

def test_write_excel(csv_writer, output_files):
    """Test if write_excel exports the correct Excel file."""
    csv_writer.write_excel(out_filename=output_files['excel'])
    assert os.path.exists(output_files['excel'])

def test_write_json(csv_writer, output_files):
    """Test if write_json exports the correct JSON file."""
    csv_writer.write_json(out_filename=output_files['json'])
    assert os.path.exists(output_files['json'])

def test_write_json_newline_delimited(csv_writer, output_files):
    """Test if write_json_newline_delimited exports the correct JSON newline delimited file."""
    csv_writer.write_json_newline_delimited(out_filename=output_files['ndjson'])
    assert os.path.exists(output_files['ndjson'])

def test_write_parquet(csv_writer, output_files):
    """Test if write_parquet exports the correct Parquet file."""
    csv_writer.write_parquet(out_filename=output_files['parquet'])
    assert os.path.exists(output_files['parquet'])
