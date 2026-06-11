"""This module contains tests for the FileProperties class."""

import pytest

from datagrunt import CSVReader, CSVWriter
from datagrunt.core import FileProperties


class TestFileProperties:
    """Test suite for FileProperties"""

    def test_initialization(self, sample_files):
        """Test basic initialization of FileProperties."""
        file_props = FileProperties(sample_files["data.csv"])

        assert isinstance(file_props, FileProperties)
        assert file_props.filename == "data.csv"
        assert file_props.extension == ".csv"
        assert file_props.extension_string == "csv"
        assert file_props.size_in_bytes > 0

    def test_file_type_checks(self, sample_files):
        """Test various file type checking properties."""
        csv_file = FileProperties(sample_files["data.csv"])
        xlsx_file = FileProperties(sample_files["test.xlsx"])
        json_file = FileProperties(sample_files["test.json"])

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
        empty_file = FileProperties(sample_files["empty.csv"])
        data_file = FileProperties(sample_files["data.csv"])

        assert empty_file.is_empty
        assert empty_file.size_in_bytes == 0
        assert not data_file.is_empty
        assert data_file.size_in_bytes > 0

    def test_blank_file_detection(self, sample_files):
        """Test blank file detection."""
        empty_file = FileProperties(sample_files["empty.csv"])
        blank_file = FileProperties(sample_files["blank.csv"])
        data_file = FileProperties(sample_files["data.csv"])

        assert empty_file.is_blank
        assert blank_file.is_blank
        assert not data_file.is_blank

    def test_file_format_properties(self, sample_files):
        """Test file format related properties."""
        csv_file = FileProperties(sample_files["data.csv"])
        parquet_file = FileProperties(sample_files["test.parquet"])

        assert csv_file.is_tabular
        assert parquet_file.is_apache
        assert not csv_file.is_apache
        assert not parquet_file.is_tabular

    def test_error_handling(self):
        """Test error handling for non-existent files."""
        with pytest.raises(FileNotFoundError):
            FileProperties("nonexistent_file.csv")

    def test_is_pdf(self, tmp_path):
        """Test PDF detection and classification."""
        pdf_path = tmp_path / "doc.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 dummy")

        pdf_file = FileProperties(pdf_path)
        assert pdf_file.is_pdf
        assert pdf_file.extension_string == "pdf"
        assert pdf_file.is_unstructured
        assert not pdf_file.is_csv
        assert not pdf_file.is_structured
        assert not pdf_file.is_semi_structured


class TestPathValidation:
    """Test path validation at construction for FileProperties and CSV APIs."""

    def test_directory_raises_value_error(self, tmp_path):
        """A directory path raises a clear ValueError, not a raw IsADirectoryError."""
        with pytest.raises(ValueError, match="is a directory, not a file"):
            FileProperties(tmp_path)

    def test_missing_path_raises_file_not_found(self, tmp_path):
        """A missing path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            FileProperties(tmp_path / "does_not_exist.csv")

    def test_csv_reader_on_directory_raises_value_error(self, tmp_path):
        """CSVReader on a directory raises a clean ValueError at construction."""
        with pytest.raises(ValueError, match="is a directory, not a file"):
            CSVReader(tmp_path)

    def test_csv_writer_on_directory_raises_value_error(self, tmp_path):
        """CSVWriter on a directory raises a clean ValueError at construction."""
        with pytest.raises(ValueError, match="is a directory, not a file"):
            CSVWriter(tmp_path)

    def test_csv_reader_on_missing_path_raises_file_not_found(self, tmp_path):
        """CSVReader on a missing path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            CSVReader(tmp_path / "missing.csv")

    def test_csv_writer_on_missing_path_raises_file_not_found(self, tmp_path):
        """CSVWriter on a missing path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            CSVWriter(tmp_path / "missing.csv")

    def test_valid_file_still_constructs(self, sample_files):
        """A normal file path still constructs without error."""
        reader = CSVReader(sample_files["data.csv"])
        assert reader.filename == "data.csv"
