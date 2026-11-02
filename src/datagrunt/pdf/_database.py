"""The v5 PDFReader's DuckDB connection (#372, A8b): private, in memory, never spilling to disk.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.

:func:`private_connection` opens an in-memory DuckDB database of its own
(``duckdb.connect(":memory:")``: two connections share nothing) and turns
spilling off with ``SET temp_directory=''``. An in-memory database's
default ``temp_directory`` is ``.tmp`` in the working directory, where
DuckDB writes what does not fit in its memory (a large sort, join or
aggregate). With it empty, such a query raises
``duckdb.OutOfMemoryException`` instead, and nothing is written. Checked
on DuckDB 2.0.0: a sort of 1,000,000 md5 strings in 32 MB spills to
``./.tmp`` by default, and with the setting fails, writing nothing. The
setting is read back: a connection that does not report it empty is
closed and refused.

Nothing else here writes. What a caller's SQL does is the caller's: SQL
that reads or writes files (``COPY ... TO``), or that needs an extension
DuckDB installs on first use (the ones a reader's frame needs, ``json``,
``parquet`` and ``icu``, are built into DuckDB's Python package), is the
application's to allow or refuse, as for the CSV readers (preserve, not
transform).
"""

import duckdb

__all__ = ["private_connection"]

_SPILL_OFF = "SET temp_directory=''"
_SPILL_SETTING = "SELECT current_setting('temp_directory')"


def private_connection() -> duckdb.DuckDBPyConnection:
    """A new in-memory DuckDB connection of its own, with spilling to disk off (see the module notes).

    Raises:
        RuntimeError: If DuckDB does not report ``temp_directory`` empty
            once it is set so: the connection is closed first.
        duckdb.Error: As DuckDB raises it; the connection is closed first.
    """
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(_SPILL_OFF)
        setting = connection.sql(_SPILL_SETTING).fetchall()
        if setting != [("",)]:
            raise RuntimeError(f"DuckDB reports temp_directory {setting!r} after {_SPILL_OFF}: it could spill to disk")
    except BaseException:
        connection.close()
        raise
    return connection
