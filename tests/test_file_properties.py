"""Unit tests for FileProperties."""

import pytest
from src.datagrunt.core.fileproperties import FileProperties

@pytest.fixture
def sample_files(tmp_path):
    """Fixture to create sample files for testing."""
    files = {
        'empty_file': tmp_path / 'empty.txt',
        'small_file': tmp_path / 'small.txt',
        'large_file': tmp_path / 'large.txt',
        'csv_file': tmp_path / 'test.csv',
        'excel_file': tmp_path / 'test.xlsx',
        'parquet_file': tmp_path / 'test.parquet',
        'json_file': tmp_path / 'test.json'
    }

    # Create empty file
    files['empty_file'].touch()

    # Create small file
    files['small_file'].write_text('This is a small file.\n')

    # Create large file (simulated)
    files['large_file'].write_bytes(b'\0' * (1024 * 1024 * 1024))  # 1GB of null bytes

    # Create CSV file
    files['csv_file'].write_text('Name,Age,City\nAlice,25,New York\n')

    # Create dummy Excel file
    files['excel_file'].write_text('This is a dummy Excel file.\n')

    # Create dummy Parquet file
    files['parquet_file'].write_text('This is a dummy Parquet file.\n')

    # Create JSON file
    files['json_file'].write_text('{"name": "John", "age": 30, "city": "New York"}\n')

    return files

def test_empty_file_properties(sample_files):
    """Test properties of an empty file."""
    props = FileProperties(sample_files['empty_file'])
    assert props.size_in_bytes == 0
    assert props.is_empty
    assert not props.is_large

def test_small_file_properties(sample_files):
    """Test properties of a small file."""
    props = FileProperties(sample_files['small_file'])
    assert props.size_in_bytes > 0
    assert not props.is_empty
    assert not props.is_large

def test_large_file_properties(sample_files):
    """Test properties of a large file."""
    props = FileProperties(sample_files['large_file'])
    assert props.size_in_gb >= 1.0
    assert not props.is_empty
    assert props.is_large

def test_csv_file_properties(sample_files):
    """Test properties of a CSV file."""
    props = FileProperties(sample_files['csv_file'])
    assert props.is_structured
    assert props.is_standard
    assert not props.is_proprietary
    assert props.is_csv
    assert not props.is_excel
    assert props.is_tabular

def test_excel_file_properties(sample_files):
    """Test properties of an Excel file."""
    props = FileProperties(sample_files['excel_file'])
    assert props.is_structured
    assert not props.is_standard
    assert props.is_proprietary
    assert not props.is_csv
    assert props.is_excel
    assert props.is_tabular

def test_parquet_file_properties(sample_files):
    """Test properties of a Parquet file."""
    props = FileProperties(sample_files['parquet_file'])
    assert props.is_structured
    assert props.is_standard
    assert not props.is_proprietary
    assert not props.is_csv
    assert not props.is_excel
    assert not props.is_tabular
    assert props.is_apache

def test_json_file_properties(sample_files):
    """Test properties of a JSON file."""
    props = FileProperties(sample_files['json_file'])
    assert props.is_semi_structured
    assert props.is_standard
    assert not props.is_structured
    assert not props.is_proprietary
    assert not props.is_csv
    assert not props.is_excel
    assert not props.is_tabular
