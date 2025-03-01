import pytest
import polars as pl
import os
import sys
sys.path.append('../')  # Add the parent directory to the search path
sys.path.append('../src/datagrunt')  # Add the parent directory to the search path

from src.datagrunt.core.csvproperties import CSVFormatter

# Dummy CSV data for testing
CSV_DATA = """col1,col2,col3
1,a,True
2,b,False
3,c,True"""

EMPTY_CSV_DATA = ""

BLANK_CSV_DATA = "   "

CSV_HEADERS = ['name', 'age', 'city']
CSV_ROWS = [
    ['John', '30', 'New York'],
    ['Jane', '25', 'Los Angeles']
]

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
def formatter(sample_csv_path):
    return CSVFormatter(sample_csv_path)  # Path doesn't matter for these tests

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

# Fixture to create temporary test files
@pytest.fixture
def temp_files(tmp_path):
    # Create empty CSV file
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("")

    # Create CSV with only whitespace
    blank_csv = tmp_path / "blank.csv"
    blank_csv.write_text("   \n   \n")

    # Create normal CSV file
    normal_csv = tmp_path / "test.csv"
    normal_csv.write_text("name,age,city\nJohn,30,New York\nJane,25,London")

    # Create tab-separated CSV
    tab_csv = tmp_path / "tab.csv"
    tab_csv.write_text("name\tage\tcity\nJohn\t30\tNew York")

    # Create semicolon-separated CSV
    semicolon_csv = tmp_path / "semicolon.csv"
    semicolon_csv.write_text("name;age;city\nJohn;30;New York")

    # Create large CSV file (>1GB) - simulated with property override
    large_csv = tmp_path / "large.csv"
    large_csv.write_text("header1,header2\ndata1,data2")

    return {
        'empty': empty_csv,
        'blank': blank_csv,
        'normal': normal_csv,
        'tab': tab_csv,
        'semicolon': semicolon_csv,
        'large': large_csv
    }

@pytest.fixture
def sample_csv():
    """Create a sample CSV file for testing."""
    content = "name,age,city\nJohn,30,New York\nJane,25,Los Angeles"
    filepath = "test_sample.csv"
    with open(filepath, "w") as f:
        f.write(content)
    yield filepath
    # Cleanup after tests
    os.remove(filepath)

@pytest.fixture
def empty_csv():
    """Create an empty CSV file for testing."""
    filepath = "test_empty.csv"
    with open(filepath, "w") as f:
        f.write("")
    yield filepath
    # Cleanup after tests
    os.remove(filepath)

@pytest.fixture
def temp_csv_file(tmp_path):
    """Fixture to create a temporary CSV file."""
    file_path = tmp_path / "test.csv"
    with open(file_path, "w") as f:
        f.write(CSV_DATA)
    return str(file_path)


@pytest.fixture
def temp_empty_csv_file(tmp_path):
    """Fixture to create a temporary empty CSV file."""
    file_path = tmp_path / "empty.csv"
    with open(file_path, "w") as f:
        f.write(EMPTY_CSV_DATA)
    return str(file_path)

@pytest.fixture
def temp_blank_csv_file(tmp_path):
    """Fixture to create a temporary blank CSV file."""
    file_path = tmp_path / "blank.csv"
    with open(file_path, "w") as f:
        f.write(BLANK_CSV_DATA)
    return str(file_path)
