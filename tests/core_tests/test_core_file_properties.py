import pytest
from src.datagrunt.core.fileproperties import FileProperties, CSVProperties

class TestFileProperties:
    def test_file_size_properties(self, temp_files):
        fp = FileProperties(temp_files['normal'])
        assert fp.size_in_bytes > 0
        assert fp.size_in_kb > 0
        assert fp.size_in_mb > 0
        assert isinstance(fp.size_in_gb, float)
        assert isinstance(fp.size_in_tb, float)

    def test_file_type_checks(self, temp_files):
        fp = FileProperties(temp_files['normal'])
        assert fp.is_structured == True
        assert fp.is_semi_structured == False
        assert fp.is_unstructured == False
        assert fp.is_standard == True
        assert fp.is_proprietary == False
        assert fp.is_csv == True
        assert fp.is_excel == False
        assert fp.is_apache == False

    def test_empty_and_blank_files(self, temp_files):
        empty_fp = FileProperties(temp_files['empty'])
        blank_fp = FileProperties(temp_files['blank'])

        assert empty_fp.is_empty == True
        assert blank_fp.is_blank == True

    def test_file_size_classification(self, temp_files):
        normal_fp = FileProperties(temp_files['normal'])
        assert normal_fp.is_large == False

class TestCSVProperties:
    def test_csv_initialization(self, temp_files):
        csv = CSVProperties(temp_files['normal'])
        assert isinstance(csv, CSVProperties)
        assert csv.delimiter == ','

    def test_csv_delimiter_inference(self, temp_files):
        tab_csv = CSVProperties(temp_files['tab'])
        semicolon_csv = CSVProperties(temp_files['semicolon'])

        assert tab_csv.delimiter == '\t'
        assert semicolon_csv.delimiter == ';'

    def test_csv_columns(self, temp_files):
        csv = CSVProperties(temp_files['normal'])
        expected_columns = ['name', 'age', 'city']

        assert csv.columns == expected_columns
        assert csv.columns_string == 'name, age, city'
        assert csv.column_count == 3

    def test_csv_row_counts(self, temp_files):
        csv = CSVProperties(temp_files['normal'])

        assert csv.row_count_with_header == 3
        assert csv.row_count_without_header == 2

    def test_invalid_file_extension(self, temp_files):
        # Create a file with invalid extension
        invalid_file = temp_files['normal'].parent / "test.txt"
        invalid_file.write_text("some,data")

        with pytest.raises(ValueError):
            CSVProperties(invalid_file)

    def test_empty_csv_attributes(self, temp_files):
        csv = CSVProperties(temp_files['empty'])

        assert csv.delimiter == ','
        assert csv.columns == []
        assert csv.column_count == 0
        assert csv.row_count_with_header == 0

    @pytest.mark.parametrize("test_file,expected_delimiter", [
        ('normal', ','),
        ('tab', '\t'),
        ('semicolon', ';')
    ])

    def test_different_delimiters(self, temp_files, test_file, expected_delimiter):
        csv = CSVProperties(temp_files[test_file])
        assert csv.delimiter == expected_delimiter
