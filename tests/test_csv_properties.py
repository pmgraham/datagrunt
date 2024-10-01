"""Unit tests for CSVProperties."""
import pytest
from src.datagrunt.core.fileproperties import CSVProperties

@pytest.fixture
def sample_csv_files(tmp_path):
    """Fixture to create sample CSV files for testing."""
    files = {
        'empty_file': tmp_path / 'empty.csv',
        'comma_file': tmp_path / 'comma.csv',
        'pipe_file': tmp_path / 'pipe.csv',
        'space_file': tmp_path / 'space.csv',
        'tab_file': tmp_path / 'tab.csv',
        'text_file': tmp_path / 'text.txt'
    }
    # Create empty file
    files['empty_file'].touch()
    # Create comma-separated file
    files['comma_file'].write_text('Name,Age,City\nAlice,25,New York\n')
    # Create pipe-separated file
    files['pipe_file'].write_text('Name|Age|City\nBob|30|London\n')
    # Create space-separated file
    files['space_file'].write_text('Name Age City\nCharlie 28 Paris\n')
    # Create tab-separated file
    files['tab_file'].write_text('Name\tAge\tCity\nDavid\t35\tTokyo\n')
    # Create text file (non-CSV)
    files['text_file'].write_text('Name\tAge\tCity\nDavid\t35\tTokyo\n')
    return files

def test_delimiter_inference(sample_csv_files):
    """Test if delimiters are correctly inferred for different CSV files."""
    assert CSVProperties(sample_csv_files['empty_file']).delimiter == ','
    assert CSVProperties(sample_csv_files['comma_file']).delimiter == ','
    assert CSVProperties(sample_csv_files['pipe_file']).delimiter == '|'
    assert CSVProperties(sample_csv_files['space_file']).delimiter == ' '
    assert CSVProperties(sample_csv_files['tab_file']).delimiter == '\t'

def test_row_counting(sample_csv_files):
    """Test if row counts are correctly determined."""
    empty_props = CSVProperties(sample_csv_files['empty_file'])
    assert empty_props.row_count_with_header == 0
    assert empty_props.row_count_without_header == -1
    comma_props = CSVProperties(sample_csv_files['comma_file'])
    assert comma_props.row_count_with_header == 2
    assert comma_props.row_count_without_header == 1

def test_column_extraction(sample_csv_files):
    """Test if column names are correctly extracted."""
    assert CSVProperties(sample_csv_files['comma_file']).columns == ['Name', 'Age', 'City']
    assert CSVProperties(sample_csv_files['pipe_file']).columns == ['Name', 'Age', 'City']

def test_invalid_csv(sample_csv_files):
    """Test if a ValueError is raised for a non-CSV file."""
    with pytest.raises(ValueError):
        CSVProperties(sample_csv_files['text_file'])
