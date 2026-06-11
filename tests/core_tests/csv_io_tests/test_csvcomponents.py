from datagrunt.core import (
    CSVColumnNameNormalizer,
    CSVColumns,
    CSVComponents,
    CSVDelimiter,
    CSVDialect,
    CSVRows,
    CSVStringSample,
)


class TestCSVStringSample:
    """Test suite for the CSVStringSample class."""

    def test_csv_string_sample_by_quality(self, tmp_path):
        # Create a temporary CSV file with mixed quality rows
        test_file = tmp_path / "quality_test.csv"
        test_file.write_text("a,b,c\n1,2,3\n1,,\n4,5,6\n,,")

        # Get the quality sample
        sampler = CSVStringSample(str(test_file))
        quality_sample = sampler.csv_string_sample_by_quality

        # The top two rows should be the ones with no nulls
        assert "1,2,3" in quality_sample
        assert "4,5,6" in quality_sample
        assert "1,," not in quality_sample

    def test_csv_string_sample(self, tmp_path):
        # Create a temporary CSV file
        test_file = tmp_path / "sample_test.csv"
        test_file.write_text("a,b,c\n1,2,3\n4,5,6\n7,8,9")

        # Get the sample
        sampler = CSVStringSample(str(test_file))
        sample = sampler.csv_string_sample

        # The sample should contain the header and the first two rows
        assert "a,b,c" in sample
        assert "1,2,3" in sample
        assert "4,5,6" in sample
        assert "7,8,9" not in sample


class TestCSVDelimiter:
    """Test suite for CSVDelimiter class."""

    def test_comma_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files["comma.csv"])
        assert delimiter.delimiter == ","

    def test_tab_delimiter(self, tmp_path):
        # Create a TSV file
        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text("a\tb\tc\n1\t2\t3")

        delimiter = CSVDelimiter(str(tsv_file))
        assert delimiter.delimiter == "\t"

    def test_semicolon_delimiter(self, tmp_path):
        # Create a semicolon-delimited file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a;b;c\n1;2;3")

        delimiter = CSVDelimiter(str(csv_file))
        assert delimiter.delimiter == ";"

    def test_pipe_delimiter(self, tmp_path):
        # Create a pipe-delimited file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a|b|c\n1|2|3")

        delimiter = CSVDelimiter(str(csv_file))
        assert delimiter.delimiter == "|"

    def test_space_delimiter_fallback(self, tmp_path):
        # Create a file with no clear delimiters
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("abc123\ndef456")

        delimiter = CSVDelimiter(str(csv_file))
        assert delimiter.delimiter == " "


class TestCSVColumns:
    """Test suite for CSVColumns class."""

    def test_columns_extraction(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Name,Age,City\nJohn,25,NYC")

        columns = CSVColumns(str(csv_file))
        assert columns.columns == ["Name", "Age", "City"]
        assert columns.columns_count == 3

    def test_columns_string_representation(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("A,B,C\n1,2,3")

        columns = CSVColumns(str(csv_file))
        assert columns.columns_string == "A, B, C"

    def test_empty_file_columns(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")

        columns = CSVColumns(str(csv_file))
        assert columns.columns == []
        assert columns.columns_count == 0


class TestCSVColumnNameNormalizer:
    """Test suite for CSVColumnNameNormalizer class."""

    def test_normalize_special_characters(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("First Name,E-mail Address,Phone#\nJohn,john@test.com,123")

        normalizer = CSVColumnNameNormalizer(str(csv_file))
        expected = ["first_name", "e_mail_address", "phone"]
        assert normalizer.columns_normalized == expected

    def test_normalize_leading_digits(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("1st_column,2nd_column,normal_column\na,b,c")

        normalizer = CSVColumnNameNormalizer(str(csv_file))
        expected = ["_1st_column", "_2nd_column", "normal_column"]
        assert normalizer.columns_normalized == expected

    def test_normalize_duplicate_names(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,name,Name\na,b,c")

        normalizer = CSVColumnNameNormalizer(str(csv_file))
        # The normalizer should create unique names for duplicates
        normalized = normalizer.columns_normalized
        assert len(normalized) == 3
        assert len(set(normalized)) == 3  # All names should be unique
        assert all("name" in col for col in normalized)  # All should contain "name"

    def test_columns_to_normalized_mapping(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("First Name,Last Name\nJohn,Doe")

        normalizer = CSVColumnNameNormalizer(str(csv_file))
        mapping = normalizer.columns_to_normalized_mapping
        assert mapping["First Name"] == "first_name"
        assert mapping["Last Name"] == "last_name"


class TestCSVRows:
    """Test suite for CSVRows class."""

    def test_row_count_with_header(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n4,5,6\n7,8,9")

        rows = CSVRows(str(csv_file))
        assert rows.row_count_with_header == 4

    def test_row_count_without_header(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n4,5,6")

        rows = CSVRows(str(csv_file))
        assert rows.row_count_without_header == 2

    def test_first_row_extraction(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("Name,Age,City\nJohn,25,NYC")

        rows = CSVRows(str(csv_file))
        assert rows.first_row == "Name,Age,City"

    def test_row_count_with_quoted_embedded_newlines(self, tmp_path):
        # A quoted field containing an embedded newline is one CSV record,
        # not two physical lines. Row counts must match what the engines parse.
        csv_file = tmp_path / "embedded_newlines.csv"
        csv_file.write_text('name,notes\nalice,"line1\nline2"\nbob,"hello"\n')

        rows = CSVRows(str(csv_file))
        # header + 2 data records, despite 4 physical lines
        assert rows.row_count_with_header == 3
        assert rows.row_count_without_header == 2

    def test_row_count_excludes_comment_and_blank_records(self, tmp_path):
        csv_file = tmp_path / "comments_blanks.csv"
        csv_file.write_text("# leading comment\n\nname,notes\nalice,1\n\nbob,2\n")

        rows = CSVRows(str(csv_file))
        # header + 2 data records; comment and blank lines excluded
        assert rows.row_count_with_header == 3
        assert rows.row_count_without_header == 2


class TestCSVDialect:
    """Test suite for CSVDialect class."""

    def test_quote_character_detection(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('"Name","Age"\n"John Doe","25"')

        dialect = CSVDialect(str(csv_file))
        assert dialect.quotechar == '"'

    def test_newline_delimiter_detection(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3")

        dialect = CSVDialect(str(csv_file))
        assert dialect.newline_delimiter in ["\r\n", "\n"]

    def test_empty_file_dialect_handling(self, tmp_path):
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")

        dialect = CSVDialect(str(csv_file))
        assert dialect.quotechar == '"'  # Default fallback
        assert dialect.escapechar is None
        assert dialect.doublequote is False  # Default when dialect is None
        assert dialect.skipinitialspace is False  # Default when dialect is None
        assert dialect.quoting == "quote minimal"  # Default when dialect is None

    def test_dialect_properties_with_valid_csv(self, tmp_path):
        """Test all dialect properties with a properly formatted CSV."""
        csv_file = tmp_path / "valid.csv"
        csv_file.write_text('"Name","Age"\n"John",25\n"Jane",30')

        dialect = CSVDialect(str(csv_file))
        # Test all properties are accessible
        assert dialect.quotechar == '"'
        assert isinstance(dialect.doublequote, bool)
        assert isinstance(dialect.skipinitialspace, bool)
        assert dialect.quoting in ["no quoting", "quote all", "quote minimal", "quote non-numeric"]
        assert dialect.newline_delimiter in ["\r\n", "\n", "\r"]


class TestCSVComponents:
    """Test suite for CSVComponents integrated class."""

    def test_integrated_components(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text('"First Name","Age Group"\n"John Doe","25-30"')

        components = CSVComponents(str(csv_file))

        # Test delimiter detection
        assert components.delimiter == ","

        # Test column extraction
        assert "First Name" in components.columns
        assert "Age Group" in components.columns

        # Test normalization
        assert "first_name" in components.columns_normalized
        assert "age_group" in components.columns_normalized

        # Test row counting
        assert components.row_count_with_header == 2
        assert components.row_count_without_header == 1

        # Test string samples
        assert "First Name" in components.csv_string_sample
        assert "John Doe" in components.csv_string_sample

    def test_components_with_complex_data(self, tmp_path):
        csv_file = tmp_path / "complex.csv"
        csv_file.write_text('ID,Amount,Description\n001,1000.50,"Product A"\n002,0500,"Product B"')

        components = CSVComponents(str(csv_file))

        # Test that the sample contains the expected data structure
        sample = components.csv_string_sample
        assert "ID" in sample
        assert "Amount" in sample
        assert "Description" in sample

        # Test column mapping
        mapping = components.columns_to_normalized_mapping
        assert mapping["ID"] == "id"
        assert mapping["Amount"] == "amount"
        assert mapping["Description"] == "description"

    def test_semicolon_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files["semicolon.csv"])
        assert delimiter.delimiter == ";"

    def test_tab_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files["tab.csv"])
        assert delimiter.delimiter == "\t"

    def test_empty_file_delimiter(self, sample_csv_files):
        delimiter = CSVDelimiter(sample_csv_files["empty.csv"])
        assert delimiter.delimiter == ","  # Should return default delimiter
