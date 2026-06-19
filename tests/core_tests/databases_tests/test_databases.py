"""Tests for the DuckDBQueries connection lifecycle."""

import duckdb
import pytest

from datagrunt.core.databases import DuckDBQueries


class TestDuckDBQueriesClose:
    """Explicit, deterministic disposal of the per-instance connection."""

    def test_close_releases_connection(self, tmp_path):
        """close() must dispose the live per-instance in-memory connection.

        DuckDBQueries owns a private connection; close() gives callers a
        deterministic disposal path so they need not rely on garbage
        collection or a __del__ (intentionally not implemented).

        The connection is now opened lazily, so close() resets the backing
        handle to None. We therefore capture the live handle before closing and
        assert THAT handle is dead afterwards; the property itself transparently
        reopens on the next access (covered by TestDuckDBQueriesLazyConnection).
        """
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n2,B\n")
        queries = DuckDBQueries(str(csv))

        # Usable before close; capture the live handle to verify disposal.
        live_handle = queries.connection
        assert live_handle.sql("SELECT 1").fetchone() == (1,)

        queries.close()

        # The disposed handle is unusable; the backing attribute is reset.
        assert queries._connection is None
        with pytest.raises(duckdb.Error):
            live_handle.sql("SELECT 1")

    def test_close_is_idempotent(self, tmp_path):
        """Calling close() more than once must not raise."""
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n2,B\n")
        queries = DuckDBQueries(str(csv))

        queries.close()
        queries.close()  # second call must be a safe no-op


class TestDuckDBQueriesLazyConnection:
    """The per-instance DuckDB connection must open lazily, not eagerly."""

    def test_connection_not_opened_until_first_access(self, tmp_path):
        """Constructing DuckDBQueries must not open a connection.

        Readers/writers build a DuckDBQueries just to read the table name; the
        polars/pyarrow paths never touch DuckDB. Opening a connection eagerly
        in __init__ wastes a connection on every such construction.
        """
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n")
        queries = DuckDBQueries(str(csv))

        # No connection should exist before the property is first accessed.
        assert queries._connection is None

        # Accessing the public attribute opens it (backward-compatible usage).
        assert queries.connection.sql("SELECT 1").fetchone() == (1,)
        assert queries._connection is not None

    def test_close_allows_reopen(self, tmp_path):
        """After close(), a later access must transparently reopen.

        close() resets the backing attribute to None so the connection can be
        lazily reopened, matching the old eager attribute's always-usable
        contract for any caller that touches it again.
        """
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n")
        queries = DuckDBQueries(str(csv))

        assert queries.connection.sql("SELECT 1").fetchone() == (1,)
        queries.close()
        assert queries._connection is None

        # A fresh access reopens a working connection.
        assert queries.connection.sql("SELECT 2").fetchone() == (2,)
        queries.close()


def test_csv_quotes_after_sniffer_sample(tmp_path):
    csv_file = tmp_path / "delayed_quotes.csv"
    # 5 rows without quotes, 6th row has a quoted field with a comma
    content = 'col1,col2,col3\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n13,"hello, world",15\n'
    csv_file.write_text(content)

    # This should parse successfully without raising column count/parsing errors
    queries = DuckDBQueries(str(csv_file))
    queries.connection.execute(queries.import_csv_query())
    res = queries.connection.execute(queries.select_from_duckdb_table()).fetchall()
    assert len(res) == 5
    # The 5th row's second column should be "hello, world"
    assert res[4][1] == "hello, world"
    queries.close()


class TestCreateTableIdempotent:
    """create_table imports each file once per connection (issue #104)."""

    def test_repeated_create_table_imports_once(self, tmp_path, monkeypatch):
        """Repeated create_table calls must import the CSV only once.

        The table name is deterministic per file path, so once the table
        exists on this instance's connection there is no need to re-run the
        expensive CREATE OR REPLACE TABLE import. Subsequent calls reuse the
        already-imported table.
        """
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n2,B\n")
        queries = DuckDBQueries(str(csv))

        import_calls = {"count": 0}
        original_import_query = queries.import_csv_query

        def counting_import_query():
            import_calls["count"] += 1
            return original_import_query()

        monkeypatch.setattr(queries, "import_csv_query", counting_import_query)

        first = queries.create_table().fetchall()
        second = queries.create_table().fetchall()

        assert import_calls["count"] == 1
        assert first == second == [("1", "A"), ("2", "B")]
        queries.close()

    def test_create_table_reimports_after_close(self, tmp_path):
        """close() destroys the in-memory database, so the import cache must
        reset: a later create_table must re-import instead of reusing a table
        that no longer exists on the reopened connection."""
        csv_file = tmp_path / "reimport.csv"
        csv_file.write_text("id,name\n1,A\n2,B\n")

        queries = DuckDBQueries(csv_file)
        queries.create_table()
        queries.close()

        relation = queries.create_table()
        assert relation.fetchall() == [("1", "A"), ("2", "B")]
        queries.close()


class TestSqlQueryToDataframeReusesImport:
    """sql_query_to_dataframe must reuse the cached table, not re-import (#189)."""

    def test_repeated_queries_import_once(self, tmp_path, monkeypatch):
        """Two SQL queries on one instance must import the CSV only once."""
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n2,B\n")
        queries = DuckDBQueries(str(csv))

        import_calls = {"count": 0}
        original_import_query = queries.import_csv_query

        def counting_import_query():
            import_calls["count"] += 1
            return original_import_query()

        monkeypatch.setattr(queries, "import_csv_query", counting_import_query)

        sql = f"SELECT col1, col2 FROM {queries.database_table_name} ORDER BY col1"
        first = queries.sql_query_to_dataframe(sql)
        second = queries.sql_query_to_dataframe(sql)

        assert import_calls["count"] == 1
        assert first.rows() == second.rows() == [("1", "A"), ("2", "B")]
        queries.close()

    def test_reimports_with_original_names_after_normalized_table(self, tmp_path):
        """A prior normalized import must be rebuilt with original column names.

        sql_query_to_dataframe queries against the original headers, so if the
        cached table was last imported normalized, it must re-import (not reuse
        the normalized table) so the user's SQL can reference original names.
        """
        csv = tmp_path / "data.csv"
        csv.write_text("First Name,Age\nJohn,30\n")
        queries = DuckDBQueries(str(csv))

        # Prime the cache with a NORMALIZED table (columns: first_name, age).
        queries.create_table(normalize_columns=True)

        # Querying original names must still work (forces a fresh import).
        sql = f'SELECT "First Name", "Age" FROM {queries.database_table_name}'
        result = queries.sql_query_to_dataframe(sql)
        assert result.columns == ["First Name", "Age"]
        assert result.rows() == [("John", "30")]
        queries.close()


class TestNumericLeadingFilename:
    """Numeric-leading filenames must yield a valid unquoted table name (#149).

    DuckDB rejects unquoted identifiers that start with a digit, and the table
    name is interpolated bare into every query. A file like ``123_data.csv``
    must therefore still import, query, and export on the duckdb engine.
    """

    def test_table_name_starts_with_letter(self, tmp_path):
        """The generated table name must not start with a digit."""
        csv_file = tmp_path / "123_data.csv"
        csv_file.write_text("name,age\nalice,30\nbob,25\n")

        queries = DuckDBQueries(str(csv_file))
        assert not queries.database_table_name[0].isdigit()

    def test_numeric_leading_filename_imports_and_queries(self, tmp_path):
        """A numeric-leading filename must import and SELECT without a ParserError."""
        csv_file = tmp_path / "2026_sales.csv"
        csv_file.write_text("name,age\nalice,30\nbob,25\n")

        queries = DuckDBQueries(str(csv_file))
        queries.connection.execute(queries.import_csv_query())
        res = queries.connection.execute(queries.select_from_duckdb_table()).fetchall()
        assert res == [("alice", "30"), ("bob", "25")]
        queries.close()

    def test_numeric_leading_filename_exports(self, tmp_path):
        """A numeric-leading filename must export via a COPY builder."""
        csv_file = tmp_path / "01_export.csv"
        csv_file.write_text("name,age\nalice,30\n")

        queries = DuckDBQueries(str(csv_file))
        queries.create_table()
        out = tmp_path / "out.csv"
        queries.connection.execute(queries.export_csv_query(str(out)))
        assert out.exists()
        assert "alice" in out.read_text()
        queries.close()

    def test_table_name_deterministic_across_instances(self, tmp_path):
        """Two instances for the same numeric-leading path agree on the name."""
        csv_file = tmp_path / "123_data.csv"
        csv_file.write_text("name,age\nalice,30\n")

        first = DuckDBQueries(str(csv_file))
        second = DuckDBQueries(str(csv_file))
        assert first.database_table_name == second.database_table_name


class TestDuckDBQueriesContextManager:
    """DuckDBQueries supports the with-statement, closing on exit (issue #150)."""

    def test_enter_returns_self(self, tmp_path):
        """__enter__ returns the instance so ``with ... as q`` binds it."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        queries = DuckDBQueries(str(csv_file))
        with queries as bound:
            assert bound is queries

    def test_with_block_closes_connection_on_normal_exit(self, tmp_path):
        """Leaving the block closes the live connection."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        queries = DuckDBQueries(str(csv_file))
        with queries:
            queries.create_table()
            assert queries._connection is not None
        assert queries._connection is None

    def test_with_block_closes_connection_on_exception(self, tmp_path):
        """An exception inside the block still closes the connection and propagates."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n")
        queries = DuckDBQueries(str(csv_file))
        with pytest.raises(ValueError, match="boom"):
            with queries:
                queries.create_table()
                raise ValueError("boom")
        assert queries._connection is None

    def test_reuse_after_block_reopens(self, tmp_path):
        """The instance stays usable after the block: a later use reopens."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")
        queries = DuckDBQueries(str(csv_file))
        with queries:
            queries.create_table()
        # Connection was closed on exit; reuse transparently reopens and re-imports.
        relation = queries.create_table()
        assert relation.fetchall() == [("1", "2"), ("3", "4")]
        queries.close()


class TestDuckDBQueriesLazySkipRows:
    """The leading-line scan must be deferred until ``skip_rows`` is needed."""

    def test_skip_rows_not_scanned_at_construction(self, tmp_path, monkeypatch):
        """Constructing DuckDBQueries must not scan the file for leading lines.

        ``skip_rows`` is only consumed by the DuckDB import path. Polars/PyArrow
        engines build a DuckDBQueries (for the table name) but never touch
        DuckDB, so scanning leading physical lines in __init__ wastes a full
        leading-block read on every such construction.
        """
        import datagrunt.core.databases.databases as dbmod

        calls = {"count": 0}
        original = dbmod._count_leading_physical_lines_before_header

        def counting(path):
            calls["count"] += 1
            return original(path)

        monkeypatch.setattr(dbmod, "_count_leading_physical_lines_before_header", counting)

        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n2,B\n")
        queries = DuckDBQueries(str(csv))

        # Construction must not have triggered the leading-line scan.
        assert calls["count"] == 0

        # First access computes it once; the result is cached thereafter.
        first = queries.skip_rows
        assert calls["count"] == 1
        assert queries.skip_rows == first
        assert calls["count"] == 1

    def test_skip_rows_value_matches_leading_blank_lines(self, tmp_path):
        """Lazy skip_rows must still yield the correct DuckDB skip count."""
        csv = tmp_path / "leading.csv"
        # Two leading blank lines before the header; DuckDB skips up to the header.
        csv.write_text("\n\ncol1,col2\n1,A\n")
        queries = DuckDBQueries(str(csv))
        assert queries.skip_rows == 2


class TestSpatialExtensionInstallOnce:
    """Spatial DDL must live outside the export query and INSTALL once/process."""

    def test_export_excel_query_has_no_spatial_ddl(self, tmp_path):
        csv = tmp_path / "d.csv"
        csv.write_text("a,b\n1,2\n")
        queries = DuckDBQueries(str(csv))
        sql = queries.export_excel_query("output.xlsx").upper()
        assert "INSTALL" not in sql
        assert "LOAD" not in sql
        assert "COPY" in sql
        queries.close()

    def test_install_runs_once_load_runs_each_call(self, tmp_path):
        """INSTALL spatial at most once per process; LOAD on every connection."""
        # Reset the process-level guard so the assertion is deterministic.
        DuckDBQueries._spatial_installed = False

        executed = []

        class _FakeConn:
            def execute(self, sql, *args, **kwargs):
                executed.append(sql.strip())
                return self

        csv = tmp_path / "d.csv"
        csv.write_text("a,b\n1,2\n")
        queries = DuckDBQueries(str(csv))
        fake = _FakeConn()

        # Two exports in the same process each get a fresh connection.
        queries.load_spatial_extension(fake)
        queries.load_spatial_extension(fake)

        installs = [s for s in executed if s.upper().startswith("INSTALL SPATIAL")]
        loads = [s for s in executed if s.upper().startswith("LOAD SPATIAL")]
        assert len(installs) == 1
        assert len(loads) == 2


class TestSetExportFilename:
    """set_export_filename distinguishes None (use default) from '' (error)."""

    def test_none_uses_default(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text("col1\n1\n")
        queries = DuckDBQueries(str(csv))
        result = queries.set_export_filename("default.csv", None)
        assert result == "default.csv"

    def test_provided_filename_returned(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text("col1\n1\n")
        queries = DuckDBQueries(str(csv))
        result = queries.set_export_filename("default.csv", "custom.csv")
        assert result == "custom.csv"

    def test_empty_string_raises_value_error(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text("col1\n1\n")
        queries = DuckDBQueries(str(csv))
        with pytest.raises(ValueError, match="export_filename"):
            queries.set_export_filename("default.csv", "")

    def test_whitespace_only_raises_value_error(self, tmp_path):
        csv = tmp_path / "data.csv"
        csv.write_text("col1\n1\n")
        queries = DuckDBQueries(str(csv))
        with pytest.raises(ValueError, match="export_filename"):
            queries.set_export_filename("default.csv", "   ")
