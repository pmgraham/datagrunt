"""Unit tests for CSVWriter."""

import os
import pytest
from src.datagrunt.csvfile import CSVWriter

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
def output_files(tmp_path):
    """Fixture to create output file paths."""
    return {
        'csv': tmp_path / "test_out.csv",
        'excel': tmp_path / "test_out.xlsx",
        'json': tmp_path / "test_out.json",
        'ndjson': tmp_path / "test_out.ndjson",
        'parquet': tmp_path / "test_out.parquet"
    }

def test_invalid_engine(sample_csv_file):
    """Test that an error is raised for an invalid writer engine."""
    with pytest.raises(ValueError) as exc_info:
        CSVWriter(sample_csv_file, engine='invalid')
    assert str(exc_info.value) == """Writer engine 'invalid' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

def test_write_csv_polars(sample_csv_file, output_files):
    """Test that the write_csv method calls the Polars engine."""
    writer = CSVWriter(sample_csv_file)
    writer.write_csv(out_filename=output_files['csv'])
    assert os.path.exists(output_files['csv'])

def test_write_csv_duckdb(sample_csv_file, output_files):
    """Test that the write_csv method calls the DuckDB engine."""
    writer = CSVWriter(sample_csv_file, engine='duckdb')
    writer.write_csv(out_filename=output_files['csv'])
    assert os.path.exists(output_files['csv'])

def test_write_excel_polars(sample_csv_file, output_files):
    """Test that the write_excel method calls the Polars engine."""
    writer = CSVWriter(sample_csv_file)
    writer.write_excel(out_filename=output_files['excel'])
    assert os.path.exists(output_files['excel'])

def test_write_excel_duckdb(sample_csv_file, output_files):
    """Test that the write_excel method calls the DuckDB engine."""
    writer = CSVWriter(sample_csv_file, engine='duckdb')
    writer.write_excel(out_filename=output_files['excel'])
    assert os.path.exists(output_files['excel'])

def test_write_json_polars(sample_csv_file, output_files):
    """Test that the write_json method calls the Polars engine."""
    writer = CSVWriter(sample_csv_file)
    writer.write_json(out_filename=output_files['json'])
    assert os.path.exists(output_files['json'])

def test_write_json_duckdb(sample_csv_file, output_files):
    """Test that the write_json method calls the DuckDB engine."""
    writer = CSVWriter(sample_csv_file, engine='duckdb')
    writer.write_json(out_filename=output_files['json'])
    assert os.path.exists(output_files['json'])

def test_write_json_newline_delimited_polars(sample_csv_file, output_files):
    """Test that the write_json_newline_delimited method calls the Polars engine."""
    writer = CSVWriter(sample_csv_file)
    writer.write_json_newline_delimited(out_filename=output_files['ndjson'])
    assert os.path.exists(output_files['ndjson'])

def test_write_json_newline_delimited_duckdb(sample_csv_file, output_files):
    """Test that the write_json_newline_delimited method calls the DuckDB engine."""
    writer = CSVWriter(sample_csv_file, engine='duckdb')
    writer.write_json_newline_delimited(out_filename=output_files['ndjson'])
    assert os.path.exists(output_files['ndjson'])

def test_write_parquet_polars(sample_csv_file, output_files):
    """Test that the write_parquet method calls the Polars engine."""
    writer = CSVWriter(sample_csv_file)
    writer.write_parquet(out_filename=output_files['parquet'])
    assert os.path.exists(output_files['parquet'])

def test_write_parquet_duckdb(sample_csv_file, output_files):
    """Test that the write_parquet method calls the DuckDB engine."""
    writer = CSVWriter(sample_csv_file, engine='duckdb')
    writer.write_parquet(out_filename=output_files['parquet'])
    assert os.path.exists(output_files['parquet'])
