import pytest
import polars as pl
import os
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

@pytest.fixture
def sample_default_csv_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "output.csv"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_csv(file_path)
    return str(file_path)

@pytest.fixture
def sample_parquet_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.parquet"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_parquet(file_path)
    return str(file_path)

@pytest.fixture
def sample_default_parquet_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "output.parquet"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_parquet(file_path)
    return str(file_path)

@pytest.fixture
def sample_json_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.json"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_json(file_path)
    return str(file_path)

@pytest.fixture
def sample_default_json_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "output.json"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_json(file_path)
    return str(file_path)

@pytest.fixture
def sample_ndjson_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.jsonl"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_ndjson(file_path)
    return str(file_path)

@pytest.fixture
def sample_default_ndjson_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "output.jsonl"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_ndjson(file_path)
    return str(file_path)

@pytest.fixture
def sample_excel_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "test.xlsx"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_excel(file_path)
    return str(file_path)

@pytest.fixture
def sample_default_excel_path(tmp_path):
    """Create a sample CSV file for testing."""
    file_path = tmp_path / "output.xlsx"
    df = pl.DataFrame({
        'name': ['John', 'Jane'],
        'age': [30, 25],
        'city': ['New York', 'Los Angeles']
    })
    df.write_excel(file_path)
    return str(file_path)

@pytest.fixture(autouse=True, scope="session")
def cleanup_files():
    """Cleanup any files created in the root directory after all tests run."""
    yield  # this lets the tests run first
    # Clean up files after tests
    files_to_clean = [
        "output.csv",
        "output.parquet",
        "output.json",
        "output.jsonl",
        "output.xlsx"
    ]
    for file in files_to_clean:
        try:
            if os.path.exists(file):
                os.remove(file)
        except Exception as e:
            print(f"Warning: Could not remove {file}: {e}")
