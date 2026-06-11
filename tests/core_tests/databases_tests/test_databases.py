"""Tests for the DuckDBQueries connection lifecycle."""

import duckdb
import pytest

from datagrunt.core.databases import DuckDBQueries


class TestDuckDBQueriesClose:
    """Explicit, deterministic disposal of the per-instance connection."""

    def test_close_releases_connection(self, tmp_path):
        """close() must close the per-instance in-memory connection.

        DuckDBQueries owns a private connection; close() gives callers a
        deterministic disposal path so they need not rely on garbage
        collection or a __del__ (intentionally not implemented).
        """
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n2,B\n")
        queries = DuckDBQueries(str(csv))

        # Usable before close.
        assert queries.connection.sql("SELECT 1").fetchone() == (1,)

        queries.close()

        # Unusable after close: the underlying connection is gone.
        with pytest.raises(duckdb.Error):
            queries.connection.sql("SELECT 1")

    def test_close_is_idempotent(self, tmp_path):
        """Calling close() more than once must not raise."""
        csv = tmp_path / "data.csv"
        csv.write_text("col1,col2\n1,A\n2,B\n")
        queries = DuckDBQueries(str(csv))

        queries.close()
        queries.close()  # second call must be a safe no-op


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

