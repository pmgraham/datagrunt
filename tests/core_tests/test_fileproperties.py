import pytest
from datagrunt.core import FileProperties

class TestFileProperties:
    """Test suite for FileProperties"""

    def test_initialization(self, sample_files):
        """Test basic initialization of FileProperties."""
        file_props = FileProperties(sample_files['data.csv'])

        assert isinstance(file_props, FileProperties)
        assert file_props.filename == 'data.csv'
        assert file_props.extension == '.csv'
        assert file_props.extension_string == 'csv'
        assert file_props.size_in_bytes > 0

    def test_file_type_checks(self, sample_files):
        """Test various file type checking properties."""
        csv_file = FileProperties(sample_files['data.csv'])
        xlsx_file = FileProperties(sample_files['test.xlsx'])
        json_file = FileProperties(sample_files['test.json'])

        # Test CSV file
        assert csv_file.is_structured
        assert csv_file.is_standard
        assert csv_file.is_csv
        assert not csv_file.is_excel
        assert not csv_file.is_semi_structured

        # Test Excel file
        assert xlsx_file.is_structured
        assert xlsx_file.is_proprietary
        assert xlsx_file.is_excel
        assert not xlsx_file.is_csv

        # Test JSON file
        assert json_file.is_semi_structured
        assert not json_file.is_structured
        assert json_file.is_standard

    def test_file_size_properties(self, sample_files):
        """Test file size related properties."""
        empty_file = FileProperties(sample_files['empty.csv'])
        data_file = FileProperties(sample_files['data.csv'])

        assert empty_file.is_empty
        assert empty_file.size_in_bytes == 0
        assert not data_file.is_empty
        assert data_file.size_in_bytes > 0

    def test_blank_file_detection(self, sample_files):
        """Test blank file detection."""
        empty_file = FileProperties(sample_files['empty.csv'])
        blank_file = FileProperties(sample_files['blank.csv'])
        data_file = FileProperties(sample_files['data.csv'])

        assert empty_file.is_blank
        assert blank_file.is_blank
        assert not data_file.is_blank

    def test_file_format_properties(self, sample_files):
        """Test file format related properties."""
        csv_file = FileProperties(sample_files['data.csv'])
        parquet_file = FileProperties(sample_files['test.parquet'])

        assert csv_file.is_tabular
        assert parquet_file.is_apache
        assert not csv_file.is_apache
        assert not parquet_file.is_tabular

    def test_error_handling(self):
        """Test error handling for non-existent files."""
        with pytest.raises(FileNotFoundError):
            FileProperties("nonexistent_file.csv")
