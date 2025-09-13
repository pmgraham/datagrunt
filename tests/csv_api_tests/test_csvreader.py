"""This module contains tests for the CSVReader class."""

import polars as pl
import pyarrow as pa
from duckdb import DuckDBPyRelation

from datagrunt import CSVReader

# All supported engines for CSVReader
ALL_ENGINES = ["polars", "duckdb", "pyarrow"]

# Engine-specific expected return types for query_data method
ENGINE_QUERY_TYPES = {
    "duckdb": DuckDBPyRelation,
    "polars": pl.DataFrame,
    "pyarrow": pl.DataFrame
}


class TestCSVReader:
    """Test suite for CSVReader class."""

    def test_to_dataframe(self, sample_csv):
        """Test conversion to dataframe with all engines."""
        for engine in ALL_ENGINES:
            reader = CSVReader(sample_csv, engine=engine)
            df = reader.to_dataframe()
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 2
            assert list(df.columns) == ["name", "age", "city"]

    def test_to_arrow_table(self, sample_csv):
        """Test conversion to Arrow table with all engines."""
        for engine in ALL_ENGINES:
            reader = CSVReader(sample_csv, engine=engine)
            table = reader.to_arrow_table()
            assert isinstance(table, pa.Table)

    def test_to_dicts(self, sample_csv):
        """Test conversion to list of dictionaries with all engines."""
        for engine in ALL_ENGINES:
            reader = CSVReader(sample_csv, engine=engine)
            dicts = reader.to_dicts()
            assert isinstance(dicts, list)
            assert len(dicts) == 2
            assert all(isinstance(d, dict) for d in dicts)
            assert dicts[0]["name"] == "John"
            assert dicts[1]["name"] == "Jane"

    def test_query_data(self, sample_csv):
        """Test querying data with all engines."""
        for engine in ALL_ENGINES:
            reader = CSVReader(sample_csv, engine=engine)
            result = reader.query_data(f"""SELECT * FROM {reader.db_table} WHERE age > '25'""")
            expected_type = ENGINE_QUERY_TYPES[engine]
            assert isinstance(result, expected_type)

    def test_empty_file_handling(self, empty_csv):
        """Test handling of empty files."""
        for engine in ALL_ENGINES:
            reader = CSVReader(empty_csv, engine=engine)
            assert isinstance(reader.to_dataframe(), pl.DataFrame)
            assert len(reader.to_dataframe()) == 0
            assert isinstance(reader.to_dicts(), list)
            assert len(reader.to_dicts()) == 0
            assert isinstance(reader.to_arrow_table(), pa.Table)

    def test_blank_file_handling(self, blank_csv):
        """Test handling of blank files (containing only whitespace)."""
        for engine in ALL_ENGINES:
            reader = CSVReader(blank_csv, engine=engine)
            assert isinstance(reader.to_dataframe(), pl.DataFrame)
            assert len(reader.to_dataframe()) == 0
            assert isinstance(reader.to_dicts(), list)
            assert len(reader.to_dicts()) == 0
            assert isinstance(reader.to_arrow_table(), pa.Table)

    def test_normalize_columns(self, tmp_path):
        """Test column name normalization with all engines."""
        # Create CSV with mixed case and spaces in column names
        csv_content = "First Name,Last Name,Age Group\nJohn,Doe,30-40\nJane,Smith,20-30"
        csv_file = tmp_path / "test_normalize.csv"
        csv_file.write_text(csv_content)

        expected_columns = ["first_name", "last_name", "age_group"]

        for engine in ALL_ENGINES:
            reader = CSVReader(str(csv_file), engine=engine)
            df = reader.to_dataframe(normalize_columns=True)
            assert list(df.columns) == expected_columns

    def test_get_sample(self, sample_csv, capsys):
        """Test get_sample method."""
        for engine in ALL_ENGINES:
            reader = CSVReader(sample_csv, engine=engine)
            reader.get_sample()
            captured = capsys.readouterr()
            assert captured.out  # Verify that something was printed
            assert "John" in captured.out
            assert "Jane" in captured.out

    def test_to_dataframe_empty_and_blank_files(self, tmp_path):
        """Test to_dataframe method specifically for empty and blank files."""
        # Test empty file (0 bytes)
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")

        for engine in ALL_ENGINES:
            reader_empty = CSVReader(str(empty_file), engine=engine)
            df_empty = reader_empty.to_dataframe()
            assert isinstance(df_empty, pl.DataFrame)
            assert len(df_empty) == 0
            assert df_empty.shape == (0, 0)

        # Test blank file (only whitespace and newlines)
        blank_file = tmp_path / "blank.csv"
        blank_file.write_text("\n   \n  \n")

        for engine in ALL_ENGINES:
            reader_blank = CSVReader(str(blank_file), engine=engine)
            df_blank = reader_blank.to_dataframe()
            assert isinstance(df_blank, pl.DataFrame)
            assert len(df_blank) == 0
            assert df_blank.shape == (0, 0)

        # Test file with only header
        header_file = tmp_path / "header.csv"
        header_file.write_text("column1,column2\n")

        for engine in ALL_ENGINES:
            reader_header = CSVReader(str(header_file), engine=engine)
            df_header = reader_header.to_dataframe()
            assert isinstance(df_header, pl.DataFrame)
            assert len(df_header) == 0

        # Verify that the method returns a new DataFrame instance each time (test with first engine)
        reader_empty = CSVReader(str(empty_file), engine=ALL_ENGINES[0])
        df1 = reader_empty.to_dataframe()
        df2 = reader_empty.to_dataframe()
        assert df1 is not df2

    def test_query_data_empty_and_blank_files(self, tmp_path):
        """Test query_data method for empty and blank files."""
        # Test empty file (0 bytes)
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")

        for engine in ALL_ENGINES:
            reader_empty = CSVReader(str(empty_file), engine=engine)
            result_empty = reader_empty.query_data("SELECT * FROM table")
            assert isinstance(result_empty, list)
            assert len(result_empty) == 0

        # Test blank file (only whitespace and newlines)
        blank_file = tmp_path / "blank.csv"
        blank_file.write_text("\n   \n  \n")

        for engine in ALL_ENGINES:
            reader_blank = CSVReader(str(blank_file), engine=engine)
            result_blank = reader_blank.query_data("SELECT * FROM table")
            assert isinstance(result_blank, list)
            assert len(result_blank) == 0
