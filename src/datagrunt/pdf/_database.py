"""The v5 PDFReader's DuckDB connection (#372, A8b): private, in memory, writing nothing to disk.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.

:func:`private_connection` opens an in-memory DuckDB database of its own
(``duckdb.connect(":memory:")``: two connections share nothing) and turns
off the two writes DuckDB would make of its own accord:

- Spilling: ``SET temp_directory=''``. An in-memory database's default
  ``temp_directory`` is ``.tmp`` in the working directory, where DuckDB
  writes what does not fit in its memory (a large sort, join or
  aggregate). With it empty, such a query raises
  ``duckdb.OutOfMemoryException`` instead, and nothing is written. Checked
  on DuckDB 2.0.0: a sort of 1,000,000 md5 strings in 32 MB spills to
  ``./.tmp`` by default, and with the setting fails, writing nothing.
- Installing an extension: ``SET autoinstall_known_extensions=false``. By
  default a query that needs a known extension DuckDB does not link in
  (``read_xlsx``, the ``excel`` extension's, say) downloads it into
  ``~/.duckdb/extensions`` as a side effect. With the setting off, the
  query raises ``duckdb.Error`` ("Install it first using ``INSTALL
  excel``") and nothing is written. Checked on DuckDB 2.0.0, with a fresh
  ``HOME``: ``read_xlsx`` installs ``excel`` there by default, and with
  the setting fails, writing nothing. The extensions a reader's frame
  needs (``json``, ``parquet``, ``icu``) are linked into DuckDB's Python
  package. ``autoload_known_extensions`` stays on: loading an extension
  already installed only reads it.

Both settings are read back: a connection that does not report
``temp_directory`` empty and ``autoinstall_known_extensions`` false is
closed and refused.

:func:`query_result` runs a caller's SQL on such a connection and reads
its result whole, as Arrow, into a :class:`QueryResult`: the shape of the
CSV readers' ``query_data`` result (``.pl()``), with ``.pa()`` and
``.dicts()``. It holds the rows, not the connection, so it outlives the
connection and its reader. A statement that returns no relation (DDL on
DuckDB 1.x) is an empty result of no columns.

Nothing else here writes. What a caller's SQL writes explicitly is the
caller's: ``COPY ... TO`` a file, or ``INSTALL`` of an extension. An
extension a query needs is the caller's to install, by an explicit
``INSTALL`` in their own SQL or in their environment (DuckDB's extension
directory); the application allows or refuses such SQL, as for the CSV
readers (preserve, not transform: the SQL is never rewritten).
"""

from typing import Any

import duckdb
import polars
import pyarrow

# Full module names: the result's methods are named pl and pa, the CSV readers' result shape.

__all__ = ["QueryResult", "private_connection", "query_result"]

# The writes DuckDB makes of its own accord, turned off (see the module notes), then read back.
_WRITES_OFF = ("SET temp_directory=''", "SET autoinstall_known_extensions=false")
_WRITE_SETTINGS = "SELECT current_setting('temp_directory'), current_setting('autoinstall_known_extensions')"
_WRITES_OFF_REPORTED = [("", False)]


def private_connection() -> duckdb.DuckDBPyConnection:
    """A new in-memory DuckDB connection of its own: no spilling to disk, no extension installed (module notes).

    Raises:
        RuntimeError: If DuckDB does not report ``temp_directory`` empty and
            ``autoinstall_known_extensions`` false once they are set so: the
            connection is closed first.
        duckdb.Error: As DuckDB raises it; the connection is closed first.
    """
    connection = duckdb.connect(":memory:")
    try:
        for statement in _WRITES_OFF:
            connection.execute(statement)
        reported = connection.sql(_WRITE_SETTINGS).fetchall()
        if reported != _WRITES_OFF_REPORTED:
            raise RuntimeError(
                f"DuckDB reports (temp_directory, autoinstall_known_extensions) {reported!r} after "
                f"{'; '.join(_WRITES_OFF)}: it could write to disk"
            )
    except BaseException:
        connection.close()
        raise
    return connection


class QueryResult:
    """A query's rows, read whole when it ran: as polars (``.pl()``), Arrow (``.pa()``) or dicts (``.dicts()``).

    It holds no connection: it outlives the reader that ran it.
    """

    __slots__ = ("_table",)

    def __init__(self, table: pyarrow.Table) -> None:
        if not isinstance(table, pyarrow.Table):
            raise TypeError(f"table must be a pyarrow.Table, got {type(table).__name__}")
        self._table = table

    def pl(self) -> polars.DataFrame:
        """The rows as a polars DataFrame (a new one each call)."""
        return polars.DataFrame(self._table)

    def pa(self) -> pyarrow.Table:
        """The rows as the Arrow table DuckDB returned (immutable)."""
        return self._table

    def dicts(self) -> list[dict[str, Any]]:
        """The rows as new dicts, one a row, in order."""
        return self._table.to_pylist()


def query_result(connection: duckdb.DuckDBPyConnection, sql: str) -> QueryResult:
    """Run ``sql`` on ``connection`` as it is written, and read its result whole (see the module notes).

    Raises:
        duckdb.Error: As DuckDB raises it (a syntax error, an unknown table, ...).
    """
    relation = connection.sql(sql)
    return QueryResult(pyarrow.table({}) if relation is None else relation.to_arrow_table())
