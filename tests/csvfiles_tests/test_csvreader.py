import polars as pl
import pyarrow as pa
from duckdb import DuckDBPyRelation

from datagrunt import CSVReader

class TestCSVReader:
    """Test suite for CSVReader class."""

    def test_to_dataframe(self, sample_csv):
        """Test conversion to dataframe with both engines."""
        # Test with Polars engine
        reader_polars = CSVReader(sample_csv, engine='polars')
        df_polars = reader_polars.to_dataframe()
        assert isinstance(df_polars, pl.DataFrame)
        assert len(df_polars) == 2
        assert list(df_polars.columns) == ['name', 'age', 'city']

        # Test with DuckDB engine
        reader_duckdb = CSVReader(sample_csv, engine='duckdb')
        df_duckdb = reader_duckdb.to_dataframe()
        assert isinstance(df_duckdb, pl.DataFrame)
        assert len(df_duckdb) == 2
        assert list(df_duckdb.columns) == ['name', 'age', 'city']

    def test_to_arrow_table(self, sample_csv):
        """Test conversion to Arrow table with both engines."""
        reader_polars = CSVReader(sample_csv, engine='polars')
        table_polars = reader_polars.to_arrow_table()
        assert isinstance(table_polars, pa.Table)

        reader_duckdb = CSVReader(sample_csv, engine='duckdb')
        table_duckdb = reader_duckdb.to_arrow_table()
        assert isinstance(table_duckdb, pa.Table)

    def test_to_dicts(self, sample_csv):
        """Test conversion to list of dictionaries with both engines."""
        reader = CSVReader(sample_csv)
        dicts = reader.to_dicts()
        assert isinstance(dicts, list)
        assert len(dicts) == 2
        assert all(isinstance(d, dict) for d in dicts)
        assert dicts[0]['name'] == 'John'
        assert dicts[1]['name'] == 'Jane'

    def test_query_data(self, sample_csv):
        """Test querying data with both engines."""
        # Test with DuckDB engine
        reader_duckdb = CSVReader(sample_csv, engine='duckdb')
        result_duckdb = reader_duckdb.query_data(f"""SELECT * FROM {reader_duckdb.db_table} WHERE age > '25'""")
        assert isinstance(result_duckdb, DuckDBPyRelation)

        # Test with Polars engine
        reader_polars = CSVReader(sample_csv, engine='polars')
        result_polars = reader_polars.query_data(f"""SELECT * FROM {reader_duckdb.db_table} WHERE age > '25'""")
        assert isinstance(result_polars, pl.DataFrame)

    def test_empty_file_handling(self, empty_csv):
        """Test handling of empty files."""
        reader = CSVReader(empty_csv)
        assert isinstance(reader.to_dataframe(), pl.DataFrame)
        assert len(reader.to_dataframe()) == 0
        assert isinstance(reader.to_dicts(), list)
        assert len(reader.to_dicts()) == 0
        assert isinstance(reader.to_arrow_table(), pa.Table)

    def test_blank_file_handling(self, blank_csv):
        """Test handling of blank files (containing only whitespace)."""
        reader = CSVReader(blank_csv)
        assert isinstance(reader.to_dataframe(), pl.DataFrame)
        assert len(reader.to_dataframe()) == 0
        assert isinstance(reader.to_dicts(), list)
        assert len(reader.to_dicts()) == 0
        assert isinstance(reader.to_arrow_table(), pa.Table)

    def test_normalize_columns(self, tmp_path):
        """Test column name normalization."""
        # Create CSV with mixed case and spaces in column names
        csv_content = "First Name,Last Name,Age Group\nJohn,Doe,30-40\nJane,Smith,20-30"
        csv_file = tmp_path / "test_normalize.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        df = reader.to_dataframe(normalize_columns=True)

        expected_columns = ['first_name', 'last_name', 'age_group']
        assert list(df.columns) == expected_columns

    def test_get_sample(self, sample_csv, capsys):
        """Test get_sample method."""
        reader = CSVReader(sample_csv)
        reader.get_sample()
        captured = capsys.readouterr()
        assert captured.out  # Verify that something was printed
        assert 'John' in captured.out
        assert 'Jane' in captured.out

    def test_to_dataframe_empty_and_blank_files(self, tmp_path):
        """Test to_dataframe method specifically for empty and blank files."""

        # Test empty file (0 bytes)
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")
        reader_empty = CSVReader(str(empty_file))
        df_empty = reader_empty.to_dataframe()
        assert isinstance(df_empty, pl.DataFrame)
        assert len(df_empty) == 0
        assert df_empty.shape == (0, 0)

        # Test blank file (only whitespace and newlines)
        blank_file = tmp_path / "blank.csv"
        blank_file.write_text("\n   \n  \n")
        reader_blank = CSVReader(str(blank_file))
        df_blank = reader_blank.to_dataframe()
        assert isinstance(df_blank, pl.DataFrame)
        assert len(df_blank) == 0
        assert df_blank.shape == (0, 0)

        # Test file with only header
        header_file = tmp_path / "header.csv"
        header_file.write_text("column1,column2\n")
        reader_header = CSVReader(str(header_file))
        df_header = reader_header.to_dataframe()
        assert isinstance(df_header, pl.DataFrame)
        assert len(df_header) == 0

        # Verify that the method returns a new DataFrame instance each time
        df1 = reader_empty.to_dataframe()
        df2 = reader_empty.to_dataframe()
        assert df1 is not df2
