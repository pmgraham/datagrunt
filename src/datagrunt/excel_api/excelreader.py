"""Module for reading Excel workbooks into in-memory Python objects."""

# standard library
from pathlib import Path

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.excel_io.engines import ExcelEngineProperties, resolve_sample_rows
from datagrunt.excel_api._engine_backed import _ExcelEngineBacked


class ExcelReader(_ExcelEngineBacked):
    """Read Excel workbooks, selecting a worksheet per call (default first)."""

    _engine_role = "reader"

    def __init__(self, filepath: str | Path, normalize_columns: bool = False, **read_options) -> None:
        """Initialize the Excel Reader.

        Args:
            filepath (str or Path): Path to the workbook to read.
            normalize_columns (bool): Normalize column names for every
                operation on this reader. A per-call ``normalize_columns``
                argument overrides it.
            **read_options: Polars ``read_excel`` options applied to every read
                (e.g. ``has_header=False``, ``read_options={"skip_rows": 2,
                "n_rows": 100}``). Per-call options override these. The reserved
                keys ``source``, ``sheet_id``, ``sheet_name`` are not allowed.
        """
        if "read_options" in read_options:
            import warnings

            warnings.warn(
                "Passing 'read_options' as a dictionary is deprecated and will be removed in a future release. "
                "Pass the options as keyword arguments directly instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(filepath, normalize_columns=normalize_columns, **read_options)

    @property
    def db_table(self) -> str:
        """The DuckDB table name ``query_data`` registers the sheet under."""
        return self._engine.db_table

    def get_sample(
        self, sheet=None, normalize_columns: bool | None = None, n_rows: int | None = None, **read_options
    ) -> pl.DataFrame:
        """Return the leading sample rows of a sheet (empty frame if empty/blank).

        Args:
            sheet (str or int or None): Sheet to sample; ``None`` reads the
                first sheet.
            normalize_columns (bool or None): Per-call override; ``None``
                (default) inherits the constructor-level setting.
            n_rows (int or None): Number of sample rows to return. ``None``
                (default) uses the standard sample size (20).
            **read_options: Polars ``read_excel`` options for this call.

        Raises:
            ValueError: If ``n_rows`` is not ``None`` or a positive integer.
        """
        # Validate n_rows before the empty/blank short-circuit so a bad value
        # raises regardless of workbook contents, as the Raises clause promises.
        sample_rows = resolve_sample_rows(n_rows, ExcelEngineProperties.dataframe_sample_rows)
        if self.is_empty or self.is_blank:
            return pl.DataFrame()
        return self._engine.get_sample(sheet, normalize_columns, n_rows=sample_rows, **read_options)

    def to_dataframe(self, sheet=None, normalize_columns: bool | None = None, **read_options) -> pl.DataFrame:
        """Convert a sheet to a Polars DataFrame (empty frame if empty/blank)."""
        if self.is_empty or self.is_blank:
            return pl.DataFrame()
        if "read_options" in read_options:
            import warnings

            warnings.warn(
                "Passing 'read_options' as a dictionary is deprecated and will be removed in a future release. "
                "Pass the options as keyword arguments directly instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        return self._engine.to_dataframe(sheet, normalize_columns, **read_options)

    def to_arrow_table(self, sheet=None, normalize_columns: bool | None = None, **read_options) -> pa.Table:
        """Convert a sheet to a PyArrow Table (empty table if empty/blank)."""
        if self.is_empty or self.is_blank:
            return pa.Table.from_pydict({})
        return self._engine.to_arrow_table(sheet, normalize_columns, **read_options)

    def to_dicts(self, sheet=None, normalize_columns: bool | None = None, **read_options) -> list:
        """Convert a sheet to a list of row dicts (empty list if empty/blank)."""
        if self.is_empty or self.is_blank:
            return []
        return self._engine.to_dicts(sheet, normalize_columns, **read_options)

    def query_data(self, sql_query: str, sheet=None, normalize_columns: bool | None = None, **read_options):
        """Query a sheet via DuckDB.

        Security:
            ``sql_query`` is executed verbatim against the in-memory session.
            Concatenating untrusted input is a SQL-injection vector; validate /
            parameterize and sandbox at the application layer.

        Returns:
            A DuckDB ``DuckDBPyRelation``; an empty ``list`` for empty/blank
            workbooks.

        Example:
            xl = ExcelReader('book.xlsx')
            xl.query_data(f"SELECT * FROM {xl.db_table}", sheet="Sheet2")
        """
        if self.is_empty or self.is_blank:
            return []
        return self._engine.query_data(sql_query, sheet, normalize_columns, **read_options)
