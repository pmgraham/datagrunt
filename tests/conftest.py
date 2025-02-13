import sys
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../src/datagrunt')  # Add the parent directory to the search path

import pytest
from pathlib import Path
import pandas as pd
import polars as pl

@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.csv"
    data = {
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    }
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
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
    data = {
        'col1': range(1000000),
        'col2': ['test' * 100] * 1000000
    }
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    return str(file_path)

@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
