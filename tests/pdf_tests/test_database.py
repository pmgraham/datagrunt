"""The v5 reader's DuckDB connection (#372, A8b): ``datagrunt.pdf._database``.

The connection is private and in memory, with spilling off
(``SET temp_directory=''``): a query past its memory fails rather than
writing DuckDB's default ``.tmp`` directory in the working directory.
"""

import duckdb
import pytest

from datagrunt.pdf import _database

# A sort of 1,000,000 md5 strings in 32 MB: DuckDB 2.0's default in-memory connection spills it to ./.tmp.
SPILLING_SORT = "SELECT count(*) FROM (SELECT md5(i::VARCHAR) AS s FROM range(1000000) t(i) ORDER BY s)"


class TestTheDatabaseWritesNothing:
    """The reader's DuckDB: a private in-memory connection that never spills to disk."""

    def test_the_connection_is_in_memory_with_spilling_off(self):
        connection = _database.private_connection()
        try:
            setting = connection.sql("SELECT current_setting('temp_directory')").fetchall()
            databases = connection.sql("SELECT path FROM duckdb_databases() WHERE NOT internal").fetchall()
        finally:
            connection.close()

        assert setting == [("",)]
        assert databases == [(None,)]  # in memory: no database file

    def test_a_query_past_its_memory_fails_rather_than_spilling(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # DuckDB's default temp_directory is .tmp in the working directory
        connection = _database.private_connection()
        try:
            for setting in ("memory_limit='32MB'", "threads=1", "preserve_insertion_order=false"):
                connection.execute(f"SET {setting}")
            with pytest.raises(duckdb.OutOfMemoryException):
                connection.sql(SPILLING_SORT).fetchall()
            assert list(tmp_path.iterdir()) == []
        finally:
            connection.close()

    def test_each_connection_is_its_own(self):
        first, second = _database.private_connection(), _database.private_connection()
        try:
            first.execute("CREATE TABLE only_here AS SELECT 1 AS a")

            assert second.sql("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'only_here'").fetchall() == [
                (0,)
            ]
        finally:
            first.close()
            second.close()
