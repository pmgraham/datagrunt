import pytest
# from pathlib import Path
import polars as pl

import sys
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../src/datagrunt')  # Add the parent directory to the search path

@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.csv"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_csv(file_path)
    return str(file_path)

@pytest.fixture
def empty_csv_path(tmp_path):
    """Create an empty CSV file for testing."""
    file_path = tmp_path / "empty.csv"
    file_path.write_text("")
    return str(file_path)

@pytest.fixture
def large_csv_path(tmp_path):
    """Create a large CSV file for testing (>1GB)."""
    file_path = tmp_path / "large.csv"
    # Create a large DataFrame
    df = pl.DataFrame({
        'col1': pl.Series(range(1000000)),
        'col2': pl.Series(['test' * 100 for _ in range(1000000)])
    })
    df.write_csv(file_path)
    return str(file_path)

@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
