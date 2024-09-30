"""Unit tests for CSVReader."""

import pytest
from unittest.mock import patch
from src.datagrunt.csvfile import CSVReader

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

def test_invalid_engine(sample_csv_file):
    """Test that an error is raised for an invalid reader engine."""
    with pytest.raises(ValueError) as exc_info:
        CSVReader(sample_csv_file, engine='invalid')
    assert str(exc_info.value) == """Reader engine 'invalid' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

@patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.get_sample')
def test_get_sample_polars(mock_get_sample, sample_csv_file):
    """Test that the get_sample method calls the Polars engine."""
    reader = CSVReader(sample_csv_file)
    reader.get_sample()
    mock_get_sample.assert_called_once()

@patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.get_sample')
def test_get_sample_duckdb(mock_get_sample, sample_csv_file):
    """Test that the get_sample method calls the DuckDB engine."""
    reader = CSVReader(sample_csv_file, engine='duckdb')
    reader.get_sample()
    mock_get_sample.assert_called_once()

@patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.to_dataframe')
def test_to_dataframe_polars(mock_to_dataframe, sample_csv_file):
    """Test that the to_dataframe method calls the Polars engine."""
    reader = CSVReader(sample_csv_file)
    reader.to_dataframe()
    mock_to_dataframe.assert_called_once()

@patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.to_dataframe')
def test_to_dataframe_duckdb(mock_to_dataframe, sample_csv_file):
    """Test that the to_dataframe method calls the DuckDB engine."""
    reader = CSVReader(sample_csv_file, engine='duckdb')
    reader.to_dataframe()
    mock_to_dataframe.assert_called_once()

@patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.to_arrow_table')
def test_to_arrow_table_polars(mock_to_arrow_table, sample_csv_file):
    """Test that the to_arrow_table method calls the Polars engine."""
    reader = CSVReader(sample_csv_file)
    reader.to_arrow_table()
    mock_to_arrow_table.assert_called_once()

@patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.to_arrow_table')
def test_to_arrow_table_duckdb(mock_to_arrow_table, sample_csv_file):
    """Test that the to_arrow_table method calls the DuckDB engine."""
    reader = CSVReader(sample_csv_file, engine='duckdb')
    reader.to_arrow_table()
    mock_to_arrow_table.assert_called_once()

@patch('src.datagrunt.csvfile.CSVReaderPolarsEngine.to_dicts')
def test_to_dicts_polars(mock_to_dicts, sample_csv_file):
    """Test that the to_dicts method calls the Polars engine."""
    reader = CSVReader(sample_csv_file)
    reader.to_dicts()
    mock_to_dicts.assert_called_once()

@patch('src.datagrunt.csvfile.CSVReaderDuckDBEngine.to_dicts')
def test_to_dicts_duckdb(mock_to_dicts, sample_csv_file):
    """Test that the to_dicts method calls the DuckDB engine."""
    reader = CSVReader(sample_csv_file, engine='duckdb')
    reader.to_dicts()
    mock_to_dicts.assert_called_once()

@patch('src.datagrunt.csvfile.duckdb.sql')
def test_query_data(mock_sql, sample_csv_file):
    """Test that the query_data method calls DuckDB SQL with the correct query."""
    reader = CSVReader(sample_csv_file)
    query = "SELECT * FROM {reader.db_table}"
    reader.query_data(query)
    mock_sql.assert_called_with(query)
