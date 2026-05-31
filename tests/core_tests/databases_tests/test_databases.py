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
