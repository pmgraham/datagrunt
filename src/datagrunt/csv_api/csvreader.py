"""
Module for reading CSV files and converting to different in memory python
objects.
"""

# standard library
from functools import cached_property
from pathlib import Path

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core import CSVComponents, CSVEngineFactory, DuckDBQueries
from datagrunt.csv_api._compat import warn_per_call_normalize


class CSVReader(CSVComponents):
    """Class to unify the interface for reading CSV files."""

    def __init__(self, filepath, engine="polars", lenient=False, normalize_columns=False):
        """
        Initialize the CSV Reader class.

        Args:
            filepath (str or Path): Path to the file to read.
            engine (str, default 'polars'): Determines which reader engine
            class to instantiate.
            lenient (bool): Whether to run in lenient mode.
            normalize_columns (bool): Whether to normalize column names for
            every operation on this reader. With the DuckDB engine, queries
            are then written against the normalized names.
        """
        filepath = Path(filepath)
        self.lenient = lenient
        self.normalize_columns = normalize_columns
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath, lenient=self.lenient).database_table_name
        self.engine = engine.lower().replace(" ", "")
        CSVEngineFactory.validate_engine(self.engine)

    def _return_empty_file_object(self, object):
        """Return an empty object of the specified type."""
        return object

    def close(self):
        """Close the DuckDB connection held by this reader's engine, if any.

        Only the DuckDB engine holds a live connection, and only once an
        operation has built the cached engine; if no operation has run yet there
        is nothing to close, so this never forces the engine (and its import)
        into existence. ``close()`` is idempotent and the reader stays usable
        afterward - a later call transparently rebuilds/reopens (issue #150).
        """
        reader = self.__dict__.get("_reader")
        if reader is not None:
            reader.close()

    def __enter__(self):
        """Enter a ``with`` block, returning this reader."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the engine's connection on leaving the ``with`` block.

        Returns ``None`` so any in-flight exception propagates.
        """
        self.close()

    @cached_property
    def _reader(self):
        """Return this reader's engine, built once and reused.

        The engine owns the DuckDB connection (and, for the DuckDB engine, the
        imported table). Caching it here means repeated calls - notably
        ``query_data`` - reuse a single import instead of rebuilding a fresh
        engine and re-importing the file on every call (issue #104).
        """
        return CSVEngineFactory(
            self.filepath,
            self.engine,
            lenient=self.lenient,
            normalize_columns=self.normalize_columns,
        ).create_reader()

    def _create_reader(self):
        """Return this reader's cached engine.

        Retained for backward compatibility; delegates to the cached ``_reader``
        so callers share a single engine and a single CSV import.
        """
        return self._reader

    def get_sample(self, normalize_columns=None):
        """Return a sample of the CSV file.

        Args:
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.

        Returns:
            A Polars DataFrame containing the sample rows.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame())
        return self._create_reader().get_sample(warn_per_call_normalize(normalize_columns))

    def to_dataframe(self, normalize_columns=None):
        """Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.

        Returns:
            A Polars dataframe.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame())
        return self._create_reader().to_dataframe(warn_per_call_normalize(normalize_columns))

    def to_arrow_table(self, normalize_columns=None):
        """Converts CSV to a PyArrow table.

        Args:
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.

        Returns:
            A PyArrow table.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pa.Table.from_pydict({}))
        return self._create_reader().to_arrow_table(warn_per_call_normalize(normalize_columns))

    def to_dicts(self, normalize_columns=None):
        """Converts CSV to a list of dictionaries.

        Args:
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.

        Returns:
            A list of dictionaries.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(list())
        return self._create_reader().to_dicts(warn_per_call_normalize(normalize_columns))

    def query_data(self, sql_query, normalize_columns=None):
        """
        Queries a CSV file after importing into DuckDB.

        Security:
            ``sql_query`` is executed verbatim against the in-memory session.
            This is intentional for analytics, but it means concatenating
            untrusted input into ``sql_query`` is a SQL-injection vector
            against that session. Callers that build queries from user input
            must validate/parameterize and sandbox at the application layer.

        Args:
            sql_query (str): Query to run against DuckDB.
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.

        Returns:
            A DuckDB DuckDBPyRelation with the query results (DuckDB engine)
            or a Polars DataFrame (polars/pyarrow engines). NOTE: for an empty
            or blank file this returns an empty ``list`` regardless of engine,
            rather than an empty relation/DataFrame — check ``is_empty`` /
            ``is_blank`` first if you require a consistent result type.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv', normalize_columns=True)
            query = f"SELECT col_one, col_two FROM {dg.db_table}"
            dg.query_data(query)

        If the reader was constructed with normalize_columns=True, the table
        is imported with normalized column names, so write your SQL against
        those names (see ``columns_normalized`` for the list). The deprecated
        per-call argument keeps the legacy behavior instead: the query runs
        against the original column names and only the result is renamed.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(list())
        return self._create_reader().query_data(sql_query, warn_per_call_normalize(normalize_columns))
