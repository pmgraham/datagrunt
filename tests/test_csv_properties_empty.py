import pytest
import tempfile
import os
from src.datagrunt.csvfile import CSVReader

@pytest.fixture
def empty_csv_file():
    """Creates a completely empty CSV file"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        path = f.name
    yield path
    os.unlink(path)

@pytest.fixture
def blank_csv_file():
    """Creates a CSV file with only whitespace and newlines"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        f.write("\n\n   \n")
        path = f.name
    yield path
    os.unlink(path)

def test_empty_csv_reader_polars():
    """Test CSVReader with empty file using Polars engine"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        path = f.name

    try:
        reader = CSVReader(path, engine='polars')

        # Test properties
        assert reader.is_empty is True
        assert reader.is_blank is True

        # Test conversions return empty objects
        assert reader.to_dataframe().is_empty()
        assert len(reader.to_dicts()) == 0
        assert reader.to_arrow_table().num_rows == 0

    finally:
        os.unlink(path)

def test_blank_csv_reader_polars():
    """Test CSVReader with blank file using Polars engine"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        f.write("\n\n   \n")
        path = f.name

    try:
        reader = CSVReader(path, engine='polars')

        # Test properties
        assert reader.is_empty is False
        assert reader.is_blank is True

        # Test conversions return empty objects
        assert reader.to_dataframe().is_empty()
        assert len(reader.to_dicts()) == 0
        assert reader.to_arrow_table().num_rows == 0

    finally:
        os.unlink(path)

def test_empty_csv_reader_duckdb():
    """Test CSVReader with empty file using DuckDB engine"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        path = f.name

    try:
        reader = CSVReader(path, engine='duckdb')

        # Test properties
        assert reader.is_empty is True
        assert reader.is_blank is True

        # Test conversions return empty objects
        assert reader.to_dataframe().is_empty()
        assert len(reader.to_dicts()) == 0
        assert reader.to_arrow_table().num_rows == 0

    finally:
        os.unlink(path)

def test_blank_csv_reader_duckdb():
    """Test CSVReader with blank file using DuckDB engine"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        f.write("\n\n   \n")
        path = f.name

    try:
        reader = CSVReader(path, engine='duckdb')

        # Test properties
        assert reader.is_empty is False
        assert reader.is_blank is True

        # Test conversions return empty objects
        assert reader.to_dataframe().is_empty()
        assert len(reader.to_dicts()) == 0
        assert reader.to_arrow_table().num_rows == 0

    finally:
        os.unlink(path)
