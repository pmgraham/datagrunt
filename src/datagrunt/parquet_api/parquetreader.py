"""Module for reading Parquet files into in-memory Python objects."""

# standard library
from pathlib import Path

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.parquet_io.engines import ParquetEngineProperties, resolve_sample_rows
from datagrunt.parquet_api._engine_backed import _ParquetEngineBacked


class ParquetReader(_ParquetEngineBacked):
    """Read Parquet files into Polars / PyArrow / dict objects, or query via SQL."""

    _engine_role = "reader"

    def __init__(self, filepath: str | Path, normalize_columns: bool = False, **read_options) -> None:
        """Initialize the Parquet Reader.

        Args:
            filepath (str or Path): Path to the Parquet file to read.
            normalize_columns (bool): Normalize column names for every operation
                on this reader. A per-call ``normalize_columns`` overrides it.
            **read_options: Polars ``read_parquet`` options applied to every read
                (e.g. ``columns=[...]``, ``n_rows=100``). Per-call options
                override these. The reserved key ``source`` is not allowed.
        """
        super().__init__(filepath, normalize_columns=normalize_columns, **read_options)

    @property
    def db_table(self) -> str:
        """The DuckDB table name ``query_data`` registers the file under."""
        return self._engine.db_table

    def get_sample(
        self, normalize_columns: bool | None = None, n_rows: int | None = None, **read_options
    ) -> pl.DataFrame:
        """Return the leading sample rows (empty frame if empty/blank).

        Args:
            normalize_columns (bool or None): Per-call override; ``None``
                (default) inherits the constructor-level setting.
            n_rows (int or None): Number of sample rows to return. ``None``
                (default) uses the standard sample size (20).
            **read_options: Polars ``read_parquet`` options for this call.

        Raises:
            ValueError: If ``n_rows`` is not ``None`` or a positive integer.
        """
        # Validate n_rows before the empty/blank short-circuit so a bad value
        # raises regardless of file contents, as the Raises clause promises.
        sample_rows = resolve_sample_rows(n_rows, ParquetEngineProperties.dataframe_sample_rows)
        if self.is_empty or self.is_blank:
            return pl.DataFrame()
        return self._engine.get_sample(normalize_columns, n_rows=sample_rows, **read_options)

    def to_dataframe(self, normalize_columns: bool | None = None, **read_options) -> pl.DataFrame:
        """Convert to a Polars DataFrame (empty frame if empty/blank)."""
        if self.is_empty or self.is_blank:
            return pl.DataFrame()
        return self._engine.to_dataframe(normalize_columns, **read_options)

    def to_arrow_table(self, normalize_columns: bool | None = None, **read_options) -> pa.Table:
        """Convert to a PyArrow Table (empty table if empty/blank)."""
        if self.is_empty or self.is_blank:
            return pa.Table.from_pydict({})
        return self._engine.to_arrow_table(normalize_columns, **read_options)

    def to_dicts(self, normalize_columns: bool | None = None, **read_options) -> list:
        """Convert to a list of row dicts (empty list if empty/blank)."""
        if self.is_empty or self.is_blank:
            return []
        return self._engine.to_dicts(normalize_columns, **read_options)

    def query_data(self, sql_query: str, normalize_columns: bool | None = None, **read_options):
        """Query the file via DuckDB.

        Security:
            ``sql_query`` is executed verbatim against the in-memory session.
            Concatenating untrusted input is a SQL-injection vector; validate /
            parameterize and sandbox at the application layer.

        Returns:
            A DuckDB ``DuckDBPyRelation``; an empty ``list`` for empty/blank
            files.

        Example:
            pq = ParquetReader('data.parquet')
            pq.query_data(f"SELECT * FROM {pq.db_table}")
        """
        if self.is_empty or self.is_blank:
            return []
        return self._engine.query_data(sql_query, normalize_columns, **read_options)
