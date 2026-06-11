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

