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
    content = (
        "col1,col2,col3\n"
        "1,2,3\n"
        "4,5,6\n"
        "7,8,9\n"
        "10,11,12\n"
        '13,"hello, world",15\n'
    )
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
