"""Polars/calamine-backed engines for the Excel subsystem."""

# standard library
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import ClassVar

# third party libraries
import duckdb
import polars as pl

# local libraries
from datagrunt.core.excel_io.excelcomponents import (
    ExcelComponents,
    excel_table_name,
    normalize_excel_columns,
    resolve_sheet,
)


@dataclass
class ExcelEngineProperties:
    """Default output filenames and tuning constants for Excel engines.

    ``dataframe_sample_rows`` is a class-level constant so callers can
    reference ``ExcelEngineProperties.dataframe_sample_rows`` without
    constructing an instance.
    """

    filepath: Path
    dataframe_sample_rows: ClassVar[int] = 20
    csv_export_filename: str = "output.csv"
    excel_export_filename: str = "output.xlsx"
    json_export_filename: str = "output.json"
    json_newline_export_filename: str = "output.jsonl"
    parquet_export_filename: str = "output.parquet"


def _resolve_normalize(instance_value, per_call_value):
    """Return the per-call override when given, else the instance default."""
    return instance_value if per_call_value is None else per_call_value


def _freeze(value):
    """Recursively convert dicts/lists into a hashable key for caching."""
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(v) for v in value)
    return value


# Keys that select the sheet/source — controlled by the engine, never the caller.
_RESERVED_READ_OPTIONS = ("source", "sheet_id", "sheet_name")


class ExcelReaderEngine:
    """Read a workbook's sheets into Polars frames and convert/query them.

    Parses each (sheet, normalize, read_options) combination at most once and
    caches the frame. Holds a DuckDB connection only when ``query_data`` is
    used; it is opened lazily and released by ``close()``.

    ``**read_options`` are forwarded verbatim to ``pl.read_excel``, giving the
    full Polars read surface (``has_header``, ``columns``, ``schema_overrides``,
    the calamine ``read_options={"skip_rows": ..., "n_rows": ...}`` dict, etc.).
    Constructor options are defaults; per-call options merge over them.
    """

    def __init__(self, filepath, normalize_columns=False, **read_options):
        self.filepath = Path(filepath)
        self.normalize_columns = normalize_columns
        self._read_options = read_options
        self._components = ExcelComponents(self.filepath)
        self._frame_cache = {}
        self._connection = None

    @property
    def sheets(self):
        """Worksheet names in workbook order."""
        return self._components.sheets

    @cached_property
    def db_table(self):
        """Sheet-independent table name used by ``query_data`` registration."""
        return excel_table_name(self.filepath)

    def _merge_read_options(self, call_options):
        """Merge constructor + per-call options; reject reserved keys."""
        merged = {**self._read_options, **call_options}
        reserved = [k for k in _RESERVED_READ_OPTIONS if k in merged]
        if reserved:
            raise ValueError(
                f"Sheet/source selection is controlled via the sheet= argument; do not pass {reserved} as read options."
            )
        return merged

    def _read_sheet(self, sheet, normalize_columns, read_options):
        """Return the cached frame for (sheet, normalize, read_options)."""
        resolved_normalize = _resolve_normalize(self.normalize_columns, normalize_columns)
        merged_options = self._merge_read_options(read_options)
        name = resolve_sheet(self.sheets, sheet)
        key = (name, resolved_normalize, _freeze(merged_options))
        if key not in self._frame_cache:
            df = pl.read_excel(self.filepath, sheet_name=name, **merged_options)
            if resolved_normalize:
                mapping = dict(zip(df.columns, normalize_excel_columns(df.columns)))
                df = df.rename(mapping)
            self._frame_cache[key] = df
        return self._frame_cache[key]

    def get_sample(self, sheet=None, normalize_columns=None, **read_options):
        """Return the leading sample rows of a sheet as a Polars frame."""
        return self._read_sheet(sheet, normalize_columns, read_options).head(
            ExcelEngineProperties.dataframe_sample_rows
        )

    def to_dataframe(self, sheet=None, normalize_columns=None, **read_options):
        """Return a sheet as a Polars DataFrame."""
        return self._read_sheet(sheet, normalize_columns, read_options)

    def to_arrow_table(self, sheet=None, normalize_columns=None, **read_options):
        """Return a sheet as a PyArrow Table."""
        return self._read_sheet(sheet, normalize_columns, read_options).to_arrow()

    def to_dicts(self, sheet=None, normalize_columns=None, **read_options):
        """Return a sheet as a list of row dicts."""
        return self._read_sheet(sheet, normalize_columns, read_options).to_dicts()

    def query_data(self, sql_query, sheet=None, normalize_columns=None, **read_options):
        """Register the selected sheet under ``db_table`` and run SQL.

        Security:
            ``sql_query`` runs verbatim against the in-memory session — a
            SQL-injection vector if built from untrusted input. Validate /
            parameterize at the application layer.

        Returns:
            A live ``DuckDBPyRelation``. Re-registering replaces the prior
            sheet, so the table name stays stable across calls.
        """
        df = self._read_sheet(sheet, normalize_columns, read_options)
        if self._connection is None:
            self._connection = duckdb.connect(":memory:")
        self._connection.register(self.db_table, df.to_arrow())
        return self._connection.sql(sql_query)

    def close(self):
        """Release the DuckDB connection if one was opened. Idempotent."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
