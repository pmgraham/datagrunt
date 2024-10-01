"""Unit tests for CSVReaderDuckDBEngine."""

import pytest
import polars as pl
from unittest.mock import patch
import sys
import os

# Get the absolute path of the directory containing your test file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the parent directory of 'datagrunt' to the Python path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, project_root)

from src.datagrunt.core.engines import CSVReaderDuckDBEngine

@pytest.fixture
def sample_csv_file(tmp_path):
    """Fixture to create a sample CSV file for testing."""
    filepath = tmp_path / "test.csv"
    data = {
        'Name': pl.Series(['Alice', 'Bob', 'Charlie'], dtype=pl.Utf8),
        'Age': pl.Series(['25', '30', '28'], dtype=pl.Utf8),
        'City': pl.Series(['New York', 'London', 'Paris'], dtype=pl.Utf8)
    }
    df = pl.DataFrame(data)
    df.write_csv(filepath)
    return filepath, df

@pytest.fixture
def csv_reader(sample_csv_file):
    """Fixture to create a CSVReaderDuckDBEngine instance."""
    filepath, _ = sample_csv_file
    return CSVReaderDuckDBEngine(filepath)

def test_get_sample(csv_reader):
    """Test if get_sample reads the CSV file and returns a sample."""
    with patch('builtins.print') as mocked_print:
        print(csv_reader.get_sample())
        mocked_print.assert_called_once()

def test_to_dataframe(csv_reader, sample_csv_file):
    """Test if to_dataframe reads the CSV file and returns a Polars dataframe."""
    _, original_df = sample_csv_file
    df = csv_reader.to_dataframe()
    assert df.shape == original_df.shape

def test_to_arrow_table(csv_reader, sample_csv_file):
    """Test if to_arrow_table reads the CSV file and returns a PyArrow table."""
    _, original_df = sample_csv_file
    table = csv_reader.to_arrow_table()
    assert table.num_rows == len(original_df)
    assert table.num_columns == len(original_df.columns)

def test_to_dicts(csv_reader, sample_csv_file):
    """Test if to_dicts reads the CSV file and returns a list of dictionaries."""
    filepath, original_df = sample_csv_file
    dicts = csv_reader.to_dicts()
    assert len(dicts) == len(original_df)
    for i, row in enumerate(dicts):
        for key in original_df.columns:
            assert row[key] == original_df[key][i]
