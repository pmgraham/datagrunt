"""Unit tests for CSVWriter."""

import pytest
import csv
import re
import polars as pl
import tempfile
from src.datagrunt.csvfile import CSVColumnFormatter

# TODO we hvae failures. Make sure all unit test scenarios pass as expected.

class TestCSVColumnFormatter:

    @pytest.fixture
    def csv_file_path(self):
        # Create a temporary CSV file for testing
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            writer = csv.writer(temp_file)
            writer.writerow(["Original Name", "Another Column!", "123 Invalid", "Normal_Column"])
            writer.writerows([
                [1, 4, 7, 10],
                [2, 5, 8, 11],
                [3, 6, 9, 12]
            ])
        return temp_file.name

    @pytest.fixture
    def formatter(self, csv_file_path):
        return CSVColumnFormatter(csv_file_path)

    def test_remove_invalid_chars(self, formatter):
        assert formatter._remove_invalid_chars("Hello World! 123") == "Hello World 123"
        assert formatter._remove_invalid_chars("Test@#$%^&*()") == "Test"
        assert formatter._remove_invalid_chars("No-Change") == "No_Change"

    def test_add_valid_prefix(self, formatter):
        assert formatter._add_valid_prefix("123column") == "_123column"
        assert formatter._add_valid_prefix("column") == "column"
        assert formatter._add_valid_prefix("_underscorePrefix") == "_underscorePrefix"

    def test_replace_spaces_periods_with_underscores(self, formatter):
        assert formatter._replace_spaces_periods_with_underscores("Hello   World.csv") == "Hello_World_csv"
        assert formatter._replace_spaces_periods_with_underscores("No.Spaces.Or.Periods") == "No_Spaces_Or_Periods"
        assert formatter._replace_spaces_periods_with_underscores("Already_Correct") == "Already_Correct"

    def test_remove_consecutive_non_alphanumeric_duplicates(self, formatter):
        assert formatter._remove_consecutive_non_alphanumeric_duplicates("hello__world") == "hello_world"
        assert formatter._remove_consecutive_non_alphanumeric_duplicates("apple!!!orange") == "apple!orange"
        assert formatter._remove_consecutive_non_alphanumeric_duplicates("AABBCC") == "AABBCC"
        assert formatter._remove_consecutive_non_alphanumeric_duplicates("user@@@gmail.com") == "user@gmail.com"

    def test_normalize_column_name(self, formatter):
        assert formatter.normalize_column_name("  Hello, World! 123.csv  ") == "hello_world_123_csv"
        assert formatter.normalize_column_name("UPPER CASE") == "upper_case"
        assert formatter.normalize_column_name("123 Invalid Start") == "_123_invalid_start"
        assert formatter.normalize_column_name("multiple   spaces") == "multiple_spaces"

    def test_normalize_column_names(self, formatter):
        expected = {
            "Original Name": "original_name",
            "Another Column!": "another_column",
            "123 Invalid": "_123_invalid",
            "Normal_Column": "normal_column"
        }
        assert formatter.normalize_column_names() == expected

    def test_normalize_dataframe_column_names(self, formatter, csv_file_path):
        df = pl.read_csv(csv_file_path)
        updated_df = formatter.normalize_dataframe_column_names(df)
        expected_columns = ["original_name", "another_column", "_123_invalid", "normal_column"]
        assert updated_df.columns == expected_columns

    def test_apply_string_functions_in_sequence(self):
        result = CSVColumnFormatter._apply_string_functions_in_sequence(
            "HELLO WORLD",
            str.lower,
            str.strip,
            lambda s: s.replace(" ", "_")
        )
        assert result == "hello_world"

    def test_regex_patterns(self, formatter):
        assert re.match(formatter.REGEX_PATTERNS['invalid_chars'], "@#$%") is not None
        assert re.match(formatter.REGEX_PATTERNS['valid_prefix'], "abc") is not None
        assert re.match(formatter.REGEX_PATTERNS['valid_prefix'], "123") is None
        assert re.match(formatter.REGEX_PATTERNS['spaces_periods'], "  .  ") is not None
        assert re.match(formatter.REGEX_PATTERNS['consecutive_duplicates'], "__") is not None

    def test_edge_cases(self, formatter):
        assert formatter.normalize_column_name("") == "_"
        assert formatter.normalize_column_name("a" * 100) == "a" * 100
        assert formatter.normalize_column_name("!@#$%^&*()") == "_"

    def test_normalize_column_names_with_duplicates(self, csv_file_path):
        # Create a new CSV file with duplicate column names
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            writer = csv.writer(temp_file)
            writer.writerow(["Name", "name", "NAME", "First Name", "first_name"])
            writer.writerow([1, 2, 3, 4, 5])

        formatter = CSVColumnFormatter(temp_file.name)
        normalized = formatter.normalize_column_names()
        assert len(set(normalized.values())) == len(normalized), "Normalized names should be unique"
        assert all(name.islower() for name in normalized.values()), "All names should be lowercase"

    def test_csv_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            CSVColumnFormatter("non_existent_file.csv")

    def test_empty_csv_file(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            pass  # Create an empty file

        formatter = CSVColumnFormatter(temp_file.name)
        assert formatter.normalize_column_names() == {}

    def test_csv_file_with_only_header(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            writer = csv.writer(temp_file)
            writer.writerow(["Column1", "Column2", "Column3"])

        formatter = CSVColumnFormatter(temp_file.name)
        normalized = formatter.normalize_column_names()
        assert normalized == {"Column1": "column1", "Column2": "column2", "Column3": "column3"}
