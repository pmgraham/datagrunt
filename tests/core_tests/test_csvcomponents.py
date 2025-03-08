from datagrunt.core import (
    CSVDelimiter,
    CSVDialect,
    CSVRows,
    CSVColumns,
    CSVColumnNameNormalizer,
    CSVComponents
)

class TestCSVDelimiter:
    """Test suite for CSVDelimiter class."""

    def test_comma_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files['comma.csv'])
        assert delimiter.delimiter == ','

    def test_semicolon_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files['semicolon.csv'])
        assert delimiter.delimiter == ';'

    def test_tab_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files['tab.csv'])
        assert delimiter.delimiter == '\t'

    def test_empty_file_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files['empty.csv'])
        assert delimiter.delimiter == ','  # Should return default delimiter

class TestCSVDialect:
    """Test suite for CSVDialect class."""

    def test_basic_dialect(self, sample_csv_files):
        dialect = CSVDialect(sample_csv_files['comma.csv'])
        assert dialect.quotechar in ['"', "'"]
        assert dialect.newline_delimiter in ['\r\n', '\n']
        assert isinstance(dialect.doublequote, bool)
        assert isinstance(dialect.skipinitialspace, bool)

    def test_quoted_file_dialect(self, sample_csv_files):
        dialect = CSVDialect(sample_csv_files['quoted.csv'])
        assert dialect.quotechar == '"'
        assert dialect.quoting == 'no quoting'

class TestCSVRows:
    """Test suite for CSVRows class."""

    def test_row_counts(self, sample_csv_files):
        rows = CSVRows(sample_csv_files['comma.csv'])
        assert rows.row_count_with_header == 3
        assert rows.row_count_without_header == 2

    def test_first_row(self, sample_csv_files):
        rows = CSVRows(sample_csv_files['comma.csv'])
        assert rows.first_row == 'Name,Age,City'

    def test_empty_file(self, sample_csv_files):
        rows = CSVRows(sample_csv_files['empty.csv'])
        assert rows.first_row == ''
        assert rows.row_count_with_header == 0

class TestCSVColumns:
    """Test suite for CSVColumns class."""

    def test_column_count(self, sample_csv_files):
        columns = CSVColumns(sample_csv_files['comma.csv'])
        assert columns.columns_count == 3

    def test_column_names(self, sample_csv_files):
        columns = CSVColumns(sample_csv_files['comma.csv'])
        assert columns.columns == ['Name', 'Age', 'City']
        assert columns.columns_string == 'Name, Age, City'

    def test_byte_string(self, sample_csv_files):
        columns = CSVColumns(sample_csv_files['comma.csv'])
        assert isinstance(columns.columns_byte_string, bytes)

class TestCSVColumnNameNormalizer:
    """Test suite for CSVColumnNameNormalizer class."""

    def test_basic_normalization(self, sample_csv_files):
        normalizer = CSVColumnNameNormalizer(sample_csv_files['messy_headers.csv'])
        normalized = normalizer.columns_normalized
        assert 'first_name' in normalized
        assert all(c.islower() and ' ' not in c for c in normalized)

    def test_duplicate_handling(self, tmp_path):
        # Create file with duplicate column names
        test_file = tmp_path / "duplicate.csv"
        test_file.write_text("Name,Name,name")
        normalizer = CSVColumnNameNormalizer(str(test_file))
        assert len(set(normalizer.columns_normalized)) == 3  # Should have unique names

    def test_number_prefix_handling(self, tmp_path):
        test_file = tmp_path / "numbers.csv"
        test_file.write_text("1column,2column")
        normalizer = CSVColumnNameNormalizer(str(test_file))
        assert all(col.startswith('_') for col in normalizer.columns_normalized)

class TestCSVComponents:
    """Test suite for CSVComponents class."""

    def test_inheritance(self, sample_csv_files):
        components = CSVComponents(sample_csv_files['comma.csv'])
        assert hasattr(components, 'filepath')
        assert hasattr(components, 'filename')
        assert hasattr(components, 'extension')

    def test_component_integration(self, sample_csv_files):
        components = CSVComponents(sample_csv_files['comma.csv'])
        assert components.delimiter == ','
        assert components.columns_count == 3
        assert components.row_count_with_header == 3
        assert components.row_count_without_header == 2

    def test_normalization_integration(self, sample_csv_files):
        components = CSVComponents(sample_csv_files['messy_headers.csv'])
        assert all(c.islower() and ' ' not in c for c in components.columns_normalized)
        assert isinstance(components.columns_to_normalized_mapping, dict)

    def test_dialect_integration(self, sample_csv_files):
        components = CSVComponents(sample_csv_files['quoted.csv'])
        assert components.quotechar == '"'
        assert isinstance(components.doublequote, bool)
        assert isinstance(components.skipinitialspace, bool)
