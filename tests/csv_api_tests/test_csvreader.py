"""This module contains tests for the CSVReader class."""

import polars as pl
import pyarrow as pa
from duckdb import DuckDBPyRelation

from datagrunt import CSVReader

# All supported engines for CSVReader
ALL_ENGINES = ["polars", "duckdb", "pyarrow"]

# Engine-specific expected return types for query_data method
ENGINE_QUERY_TYPES = {"duckdb": DuckDBPyRelation, "polars": pl.DataFrame, "pyarrow": pl.DataFrame}


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

    def test_get_sample(self, sample_csv):
        """Test get_sample method returns a sample DataFrame."""
        for engine in ALL_ENGINES:
            reader = CSVReader(sample_csv, engine=engine)
            sample = reader.get_sample()
            assert isinstance(sample, pl.DataFrame)
            assert len(sample) == 2
            names = sample["name"].to_list()
            assert "John" in names
            assert "Jane" in names

    def test_same_stem_files_do_not_collide(self, tmp_path):
        """Two files sharing a name stem must not overwrite each other's data.

        Regression test for the DuckDB table-name collision bug: previously
        both files mapped to a single global table named after the stem, so
        constructing the second reader silently corrupted the first.
        """
        dir_a = tmp_path / "dirA"
        dir_b = tmp_path / "dirB"
        dir_a.mkdir()
        dir_b.mkdir()
        (dir_a / "data.csv").write_text("col1,col2\n1,A\n2,B\n")
        (dir_b / "data.csv").write_text("col1,col2\n99,Z\n100,Y\n")

        for engine in ALL_ENGINES:
            reader_a = CSVReader(str(dir_a / "data.csv"), engine=engine)
            reader_b = CSVReader(str(dir_b / "data.csv"), engine=engine)

            # Distinct, deterministic table names per file path
            assert reader_a.db_table != reader_b.db_table

            # Each reader must still return its own data after the other exists
            a_vals = sorted(reader_a.to_dataframe()["col1"].to_list())
            b_vals = sorted(reader_b.to_dataframe()["col1"].to_list())
            assert a_vals == ["1", "2"]
            assert b_vals == ["100", "99"]

            # And SQL queries via DuckDB must hit the correct per-file table
            res_a = reader_a.query_data(f"SELECT col1 FROM {reader_a.db_table} ORDER BY col1")
            df_a = res_a.pl() if hasattr(res_a, "pl") else res_a
            assert df_a["col1"].to_list() == ["1", "2"]

    def test_duckdb_lazy_result_not_corrupted_by_later_same_stem_reader(self, tmp_path):
        """A deferred DuckDB result must keep its own file's data even after a
        second file sharing its name stem is loaded.

        This is the test shape that actually exposes the original bug. The
        eager paths (``to_dataframe``, or a ``query_data`` result materialized
        immediately) re-import and read in a single step, so they always see
        the right table and hide the collision. The bug only surfaces when a
        *lazy* ``DuckDBPyRelation`` from the first file is materialized *after*
        a second same-stem file is imported: on the old shared global
        connection with a stem-only table name, the second ``CREATE OR REPLACE
        TABLE`` overwrote the first file's table, so the deferred read returned
        the wrong file's rows.

        Lazy evaluation is DuckDB-specific here; the Polars and PyArrow engines
        read eagerly into memory and never share a mutable table.
        """
        dir_a = tmp_path / "dirA"
        dir_b = tmp_path / "dirB"
        dir_a.mkdir()
        dir_b.mkdir()
        (dir_a / "data.csv").write_text("col1,col2\n1,A\n2,B\n")
        (dir_b / "data.csv").write_text("col1,col2\n99,Z\n100,Y\n")

        reader_a = CSVReader(str(dir_a / "data.csv"), engine="duckdb")

        # Build a lazy relation over file A's table but DO NOT materialize it.
        lazy_a = reader_a.query_data(f"SELECT col1 FROM {reader_a.db_table} ORDER BY col1")

        # Now import a different file that shares the "data" stem. On the buggy
        # implementation this CREATE OR REPLACE TABLE clobbered the single
        # shared table that ``lazy_a`` still points at.
        reader_b = CSVReader(str(dir_b / "data.csv"), engine="duckdb")
        reader_b.to_dataframe()

        # Materialize A's deferred result only now. It must still be A's rows.
        assert lazy_a.pl()["col1"].to_list() == ["1", "2"]

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
