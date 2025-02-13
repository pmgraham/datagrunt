import pytest
from src.datagrunt.core.fileproperties import FileProperties, CSVProperties

class TestFileProperties:
    def test_file_properties(self, sample_csv_path):
        props = FileProperties(sample_csv_path)
        assert props.is_csv
        assert props.is_structured
        assert not props.is_semi_structured
        assert not props.is_unstructured

    def test_empty_file(self, empty_csv_path):
        props = FileProperties(empty_csv_path)
        assert props.is_empty
        assert props.is_blank

class TestCSVProperties:
    def test_csv_properties(self, sample_csv_path):
        props = CSVProperties(sample_csv_path)
        assert props.delimiter == ','
        assert len(props.columns) == 3
        assert props.row_count_with_header == 3
