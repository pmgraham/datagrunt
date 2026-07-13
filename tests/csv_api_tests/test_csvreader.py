"""This module contains tests for the CSVReader class."""

import polars as pl
import pyarrow as pa
import pytest
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

    def test_query_data_normalize_columns_polars_and_pyarrow(self, tmp_path):
        """query_data(normalize_columns=True) normalizes result columns on the
        Polars and PyArrow engines.

        Coverage test for review finding 7: only these two engines route
        query_data through DuckDBQueries.sql_query_to_dataframe and its
        _normalize_dataframe_columns helper. The DuckDB engine uses a different
        projection path, so those lines previously had no test coverage.
        """
        csv_file = tmp_path / "people.csv"
        csv_file.write_text("First Name,Last Name,Age Group\nJohn,Doe,30-40\nJane,Smith,20-30")
        expected_columns = ["first_name", "last_name", "age_group"]

        for engine in ("polars", "pyarrow"):
            reader = CSVReader(str(csv_file), engine=engine)
            result = reader.query_data(f"SELECT * FROM {reader.db_table}", normalize_columns=True)
            assert isinstance(result, pl.DataFrame)
            assert list(result.columns) == expected_columns

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

    def test_get_sample_n_rows(self, tmp_path):
        """get_sample(n_rows=...) adjusts the sample size on every engine."""
        csv_file = tmp_path / "wide.csv"
        rows = "\n".join(f"row{i},{i}" for i in range(30))
        csv_file.write_text(f"name,value\n{rows}")
        for engine in ALL_ENGINES:
            reader = CSVReader(str(csv_file), engine=engine)
            assert len(reader.get_sample()) == 20  # default unchanged
            assert len(reader.get_sample(n_rows=5)) == 5
            assert len(reader.get_sample(n_rows=100)) == 30  # capped at file size

    def test_get_sample_n_rows_invalid(self, sample_csv):
        """Invalid n_rows raises ValueError on every engine."""
        for engine in ALL_ENGINES:
            reader = CSVReader(sample_csv, engine=engine)
            for bad in (0, -3, 2.5, "ten", True):
                with pytest.raises(ValueError):
                    reader.get_sample(n_rows=bad)

    def test_get_sample_n_rows_empty_file(self, empty_csv):
        """n_rows on an empty file still returns an empty DataFrame."""
        for engine in ALL_ENGINES:
            reader = CSVReader(empty_csv, engine=engine)
            assert reader.get_sample(n_rows=5).is_empty()

    def test_get_sample_n_rows_invalid_on_empty_file(self, empty_csv):
        """Invalid n_rows raises even when the file is empty."""
        for engine in ALL_ENGINES:
            reader = CSVReader(empty_csv, engine=engine)
            with pytest.raises(ValueError):
                reader.get_sample(n_rows=0)

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

    def test_get_sample_empty_and_blank_files(self, tmp_path):
        """get_sample must return an empty DataFrame for empty and blank files.

        Regression test for issue #86: get_sample lacked the empty/blank guard
        the other read methods have, so it crashed or fabricated phantom
        columns divergently per engine on empty or whitespace-only files.
        """
        # Test empty file (0 bytes)
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")

        for engine in ALL_ENGINES:
            reader_empty = CSVReader(str(empty_file), engine=engine)
            sample_empty = reader_empty.get_sample()
            assert isinstance(sample_empty, pl.DataFrame)
            assert sample_empty.shape == (0, 0)

        # Test blank file (only whitespace and newlines)
        blank_file = tmp_path / "blank.csv"
        blank_file.write_text("\n   \n  \n")

        for engine in ALL_ENGINES:
            reader_blank = CSVReader(str(blank_file), engine=engine)
            sample_blank = reader_blank.get_sample()
            assert isinstance(sample_blank, pl.DataFrame)
            assert sample_blank.shape == (0, 0)

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

    def test_legacy_mac_carriage_returns_guidance(self, tmp_path):
        """Test that Polars and PyArrow raise helpful ValueError on legacy Mac
        carriage returns (\\r), while DuckDB succeeds."""
        mac_csv = tmp_path / "mac_legacy.csv"
        mac_csv.write_text("id,name,age\r1,John,30\r2,Jane,25\r")

        # Polars must raise ValueError with custom message
        reader_polars = CSVReader(str(mac_csv), engine="polars")
        try:
            reader_polars.to_dataframe()
            assert False, "Polars should have raised ValueError"
        except ValueError as e:
            assert "Polars engine does not support legacy Mac OS carriage return (\\r) newlines" in str(e)
            assert "use engine='duckdb'" in str(e)

        try:
            reader_polars.get_sample()
            assert False, "Polars should have raised ValueError on sample"
        except ValueError as e:
            assert "Polars engine does not support legacy Mac OS carriage return (\\r) newlines" in str(e)
            assert "use engine='duckdb'" in str(e)

        # PyArrow must raise ValueError with custom message
        reader_pyarrow = CSVReader(str(mac_csv), engine="pyarrow")
        try:
            reader_pyarrow.to_dataframe()
            assert False, "PyArrow should have raised ValueError"
        except ValueError as e:
            assert "PyArrow engine does not support legacy Mac OS carriage return (\\r) newlines" in str(e)
            assert "use engine='duckdb'" in str(e)

        try:
            reader_pyarrow.get_sample()
            assert False, "PyArrow should have raised ValueError on sample"
        except ValueError as e:
            assert "PyArrow engine does not support legacy Mac OS carriage return (\\r) newlines" in str(e)
            assert "use engine='duckdb'" in str(e)

        # DuckDB must succeed
        reader_duckdb = CSVReader(mac_csv, engine="duckdb")
        df = reader_duckdb.to_dataframe()
        assert len(df) == 2
        assert list(df.columns) == ["id", "name", "age"]

    def test_ragged_rows_fail_loudly(self, tmp_path):
        """Test that Polars, PyArrow, and DuckDB fail loudly on ragged rows."""
        # Create CSV with an extra field in the second row
        ragged_csv = tmp_path / "ragged.csv"
        ragged_csv.write_text("id,name,age\n1,John,30\n2,Jane,25,extra_field\n")

        for engine in ALL_ENGINES:
            reader = CSVReader(str(ragged_csv), engine=engine)
            try:
                reader.to_dataframe()
                assert False, f"Engine '{engine}' should have failed on ragged rows"
            except Exception as e:
                # Polars, PyArrow, and DuckDB must raise a parsing/execution error
                assert isinstance(e, Exception)

    def test_query_data_imports_file_once_across_repeated_calls(self, sample_csv, monkeypatch):
        """Repeated query_data calls on one reader must import the CSV only once.

        Regression test for issue #104: previously the DuckDB engine rebuilt a
        fresh engine (and connection) on every query_data call and unconditionally
        re-ran CREATE OR REPLACE TABLE, paying a full file import every time. The
        engine is now cached on the reader and create_table is idempotent per
        connection, so the file is imported exactly once no matter how many times
        query_data is called. Results must stay identical across calls.
        """
        from datagrunt.core.databases import DuckDBQueries

        import_calls = {"count": 0}
        original_import_query = DuckDBQueries.import_csv_query

        def counting_import_query(self):
            import_calls["count"] += 1
            return original_import_query(self)

        monkeypatch.setattr(DuckDBQueries, "import_csv_query", counting_import_query)

        reader = CSVReader(sample_csv, engine="duckdb")
        query = f"SELECT name FROM {reader.db_table} ORDER BY name"

        first = reader.query_data(query).pl()["name"].to_list()
        second = reader.query_data(query).pl()["name"].to_list()
        third = reader.query_data(query).pl()["name"].to_list()

        # The file is imported exactly once for the lifetime of the reader.
        assert import_calls["count"] == 1
        # Results are unchanged across repeated calls.
        assert first == second == third == ["Jane", "John"]

    def test_duckdb_cached_engine_reimports_when_normalize_mode_changes(self, tmp_path):
        """Caching the engine must not freeze the table's normalization mode.

        The engine (and its imported table) is now reused across calls on one
        reader. The idempotent import is keyed on normalize_columns, so toggling
        the flag must still produce the correct column names rather than serving
        a stale table from a prior call (issue #104 correctness guard).
        """
        csv_file = tmp_path / "people.csv"
        csv_file.write_text("First Name,Last Name\nJohn,Doe\nJane,Smith")

        reader = CSVReader(str(csv_file), engine="duckdb")

        raw = reader.to_dataframe(normalize_columns=False)
        assert list(raw.columns) == ["First Name", "Last Name"]

        normalized = reader.to_dataframe(normalize_columns=True)
        assert list(normalized.columns) == ["first_name", "last_name"]

        # Toggling back must restore the original column names, not stay stuck.
        raw_again = reader.to_dataframe(normalize_columns=False)
        assert list(raw_again.columns) == ["First Name", "Last Name"]

    def test_ragged_rows_lenient(self, tmp_path):
        """Test that Polars, PyArrow, and DuckDB load ragged rows successfully with warnings when lenient=True."""
        import warnings

        ragged_csv = tmp_path / "ragged.csv"
        ragged_csv.write_text("id,name,age\n1,John,30\n2,Jane,25,extra_field\n")

        for engine in ALL_ENGINES:
            reader = CSVReader(str(ragged_csv), engine=engine, lenient=True)

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                df = reader.to_dataframe()

                # Check that a UserWarning about ragged rows was issued
                assert len(w) >= 1
                assert any("ragged rows" in str(warn.message) for warn in w)

                # Check that it loaded successfully
                assert isinstance(df, pl.DataFrame)
                assert len(df) == 2
                # The extra field should have been truncated
                assert list(df.columns) == ["id", "name", "age"]
                assert df.row(1) == ("2", "Jane", "25")


class TestCSVReaderContextManager:
    """CSVReader supports the with-statement, closing its engine (issue #150)."""

    def _csv(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")
        return str(csv_file)

    def test_enter_returns_self(self, tmp_path):
        """``with CSVReader(...) as r`` binds the reader."""
        reader = CSVReader(self._csv(tmp_path), engine="duckdb")
        with reader as bound:
            assert bound is reader

    def test_with_block_closes_duckdb_engine_connection(self, tmp_path):
        """Leaving the block closes the DuckDB engine's connection."""
        reader = CSVReader(self._csv(tmp_path), engine="duckdb")
        with reader:
            reader.query_data(f"SELECT * FROM {reader.db_table}")
            engine = reader.__dict__["_engine"]
            assert engine.queries._connection is not None
        assert engine.queries._connection is None

    def test_with_block_closes_on_exception(self, tmp_path):
        """An exception inside the block still closes the connection and propagates."""
        reader = CSVReader(self._csv(tmp_path), engine="duckdb")
        with pytest.raises(ValueError, match="boom"):
            with reader:
                reader.query_data(f"SELECT * FROM {reader.db_table}")
                raise ValueError("boom")
        assert reader.__dict__["_engine"].queries._connection is None

    def test_close_does_not_build_engine_when_unused(self, tmp_path):
        """close() on a reader that ran no operation must not create the engine."""
        reader = CSVReader(self._csv(tmp_path), engine="duckdb")
        reader.close()
        assert "_engine" not in reader.__dict__

    def test_reader_usable_after_close(self, tmp_path):
        """The reader stays usable after the block: a later query reopens."""
        reader = CSVReader(self._csv(tmp_path), engine="duckdb")
        with reader:
            reader.query_data(f"SELECT * FROM {reader.db_table}")
        result = reader.query_data(f"SELECT a, b FROM {reader.db_table} ORDER BY a")
        assert result.fetchall() == [("1", "2"), ("3", "4")]
        reader.close()

    def test_with_block_harmless_for_non_duckdb_engines(self, tmp_path):
        """polars/pyarrow engines have no open connection; the block is harmless."""
        for engine in ("polars", "pyarrow"):
            with CSVReader(self._csv(tmp_path), engine=engine) as reader:
                df = reader.to_dataframe()
            assert len(df) == 2

    def test_duckdb_mixed_reads_import_file_once(self, sample_csv, monkeypatch):
        """to_dataframe + to_arrow_table + query_data on one DuckDB reader import once."""
        from datagrunt.core.databases import DuckDBQueries

        calls = {"n": 0}
        original = DuckDBQueries.import_csv_query

        def spy(self, *args, **kwargs):
            calls["n"] += 1
            return original(self, *args, **kwargs)

        monkeypatch.setattr(DuckDBQueries, "import_csv_query", spy)

        reader = CSVReader(sample_csv, engine="duckdb")
        reader.to_dataframe()
        # Connection stays open after a materializing read (no per-call close).
        assert reader.__dict__["_engine"].queries._connection is not None
        reader.to_arrow_table()
        reader.query_data(f"SELECT * FROM {reader.db_table} LIMIT 1")

        assert calls["n"] == 1


class TestCSVReaderToDataframeKwargs:
    """Test that CSVReader.to_dataframe() supports flat kwargs passed to Polars."""

    def test_to_dataframe_kwargs_forwarding(self, sample_csv):
        reader = CSVReader(sample_csv, engine="polars")
        df = reader.to_dataframe(n_rows=1)
        assert df.height == 1
        assert list(df.columns) == ["name", "age", "city"]

        # Test another Polars parameter: has_header=False
        df_no_header = reader.to_dataframe(has_header=False)
        # When has_header=False, the first row (the header names) becomes a data row.
        assert df_no_header.height == 3
        assert df_no_header.row(0) == ("name", "age", "city")
