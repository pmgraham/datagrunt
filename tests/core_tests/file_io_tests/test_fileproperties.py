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


class TestBlankFileBinaryDetection:
    """is_blank must not misclassify binary/invalid-UTF-8 files as blank (#146).

    The old heuristic decoded any sub-10MB file as text with ``errors="ignore"``,
    which silently dropped undecodable bytes — so a non-empty binary file could
    decode to whitespace and be reported blank. Strict decoding now treats any
    non-text bytes as content.
    """

    def test_invalid_utf8_bytes_not_blank(self, tmp_path):
        """A non-empty file of invalid-UTF-8 bytes must not be blank."""
        garbage = tmp_path / "garbage.csv"
        garbage.write_bytes(b"\xff" * 4096)

        assert not FileProperties(garbage).is_blank

    def test_pdf_bytes_not_blank(self, tmp_path):
        """A sub-10MB PDF with a binary body must not be blank."""
        pdf = tmp_path / "doc.pdf"
        pdf.write_bytes(b"%PDF-1.4\n" + bytes(range(256)) * 16)

        assert not FileProperties(pdf).is_blank

    def test_parquet_bytes_not_blank(self, tmp_path):
        """A sub-10MB Parquet file with a binary body must not be blank."""
        parquet = tmp_path / "data.parquet"
        parquet.write_bytes(b"PAR1" + bytes(range(256)) * 16 + b"PAR1")

        assert not FileProperties(parquet).is_blank

    def test_whitespace_only_text_still_blank(self, tmp_path):
        """A whitespace-only text file must remain blank."""
        blank = tmp_path / "blank.csv"
        blank.write_text("   \n\t\n  \n")

        assert FileProperties(blank).is_blank

    def test_empty_file_still_blank(self, tmp_path):
        """An empty file must remain blank."""
        empty = tmp_path / "empty.csv"
        empty.write_bytes(b"")

        assert FileProperties(empty).is_blank

    def test_normal_text_not_blank(self, tmp_path):
        """A normal text file with content must not be blank."""
        data = tmp_path / "data.csv"
        data.write_text("name,age\nalice,30\n")

        assert not FileProperties(data).is_blank


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
