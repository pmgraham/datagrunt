"""Polars/calamine-backed engines for the Excel subsystem."""

# standard library
import re
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import ClassVar, Optional

# third party libraries
import duckdb
import polars as pl
import xlsxwriter

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


def resolve_sample_rows(n_rows: Optional[int], default: int) -> int:
    """Resolve a per-call sample size against the engine default.

    ``None`` inherits ``default``. Anything else must be a positive int;
    bools are rejected because they are ints in Python but never a row count.
    """
    if n_rows is None:
        return default
    if isinstance(n_rows, bool) or not isinstance(n_rows, int) or n_rows < 1:
        raise ValueError(f"n_rows must be a positive integer, got {n_rows!r}.")
    return n_rows


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
        if "read_options" in read_options:
            import warnings

            warnings.warn(
                "Passing 'read_options' as a dictionary is deprecated and will be removed in a future release. "
                "Pass the options as keyword arguments directly instead.",
                DeprecationWarning,
                stacklevel=2,
            )
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

    def get_sample(self, sheet=None, normalize_columns=None, n_rows=None, **read_options):
        """Return the leading sample rows of a sheet as a Polars frame.

        ``n_rows`` is the sample size (default 20). It is consumed here, not
        forwarded to ``read_excel`` — pass row limits for full reads via
        ``to_dataframe(n_rows=...)`` instead.
        """
        sample_rows = resolve_sample_rows(n_rows, ExcelEngineProperties.dataframe_sample_rows)
        return self._read_sheet(sheet, normalize_columns, read_options).head(sample_rows)

    def to_dataframe(self, sheet=None, normalize_columns=None, **read_options):
        """Return a sheet as a Polars DataFrame."""
        if "read_options" in read_options:
            import warnings

            warnings.warn(
                "Passing 'read_options' as a dictionary is deprecated and will be removed in a future release. "
                "Pass the options as keyword arguments directly instead.",
                DeprecationWarning,
                stacklevel=2,
            )
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


def set_excel_export_filename(default_filename, export_filename=None):
    """Return ``export_filename`` if provided (non-blank), else the default.

    Raises:
        ValueError: If ``export_filename`` is a non-``None`` empty/whitespace string.
    """
    if export_filename is not None:
        if not export_filename.strip():
            raise ValueError(
                f"out_filename must not be empty or whitespace-only; "
                f"got {export_filename!r}. Pass None to use the default."
            )
        return export_filename
    return default_filename


def sanitize_sheet_for_filename(sheet_name):
    """Return a filesystem-safe token for a worksheet name."""
    return re.sub(r"[^0-9A-Za-z._-]", "_", sheet_name) or "sheet"


class ExcelWriterEngine:
    """Export a workbook's sheet(s) to CSV/JSON/JSONL/Parquet/Excel via Polars."""

    # Map format key -> (default filename attr, Polars writer method name).
    _SINGLE_TABLE_WRITERS = {
        "csv": ("csv_export_filename", "write_csv"),
        "json": ("json_export_filename", "write_json"),
        "jsonl": ("json_newline_export_filename", "write_ndjson"),
        "parquet": ("parquet_export_filename", "write_parquet"),
        "excel": ("excel_export_filename", "write_excel"),
    }

    def __init__(self, filepath, normalize_columns=False, **read_options):
        self.filepath = Path(filepath)
        self.normalize_columns = normalize_columns
        self._reader = ExcelReaderEngine(self.filepath, normalize_columns=normalize_columns, **read_options)

    @property
    def sheets(self):
        """Worksheet names in workbook order."""
        return self._reader.sheets

    def close(self):
        """Release any resources held by the backing reader engine."""
        self._reader.close()

    def _per_sheet_filename(self, base_filename, sheet_name):
        """Return ``<stem>_<sheet><suffix>`` next to ``base_filename``."""
        base = Path(base_filename)
        token = sanitize_sheet_for_filename(sheet_name)
        return str(base.with_name(f"{base.stem}_{token}{base.suffix}"))

    def _write_one(self, fmt, df, filename):
        """Write a single Polars frame in ``fmt`` to ``filename``."""
        _, writer_method = self._SINGLE_TABLE_WRITERS[fmt]
        getattr(df, writer_method)(filename)

    def _write_all_sheets_excel(self, filename, normalize_columns, read_options):
        """Write every sheet into one multi-tab .xlsx workbook."""
        with xlsxwriter.Workbook(filename) as workbook:
            for name in self.sheets:
                df = self._reader.to_dataframe(name, normalize_columns, **read_options)
                df.write_excel(workbook=workbook, worksheet=name)

    def _write(self, fmt, out_filename, sheet, all_sheets, normalize_columns, read_options):
        """Shared dispatch for all public write_* methods."""
        if all_sheets and sheet is not None:
            raise ValueError("Pass either sheet= or all_sheets=True, not both.")
        default_attr = self._SINGLE_TABLE_WRITERS[fmt][0]
        default_filename = getattr(ExcelEngineProperties, default_attr)
        base = set_excel_export_filename(default_filename, out_filename)
        if all_sheets and fmt == "excel":
            self._write_all_sheets_excel(base, normalize_columns, read_options)
            return
        if all_sheets:
            for name in self.sheets:
                df = self._reader.to_dataframe(name, normalize_columns, **read_options)
                self._write_one(fmt, df, self._per_sheet_filename(base, name))
            return
        df = self._reader.to_dataframe(sheet, normalize_columns, **read_options)
        self._write_one(fmt, df, base)

    def write_csv(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet (or all sheets) to CSV."""
        self._write("csv", out_filename, sheet, all_sheets, normalize_columns, read_options)

    def write_json(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet (or all sheets) to JSON."""
        self._write("json", out_filename, sheet, all_sheets, normalize_columns, read_options)

    def write_json_newline_delimited(
        self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options
    ):
        """Export a sheet (or all sheets) to newline-delimited JSON."""
        self._write("jsonl", out_filename, sheet, all_sheets, normalize_columns, read_options)

    def write_parquet(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet (or all sheets) to Parquet."""
        self._write("parquet", out_filename, sheet, all_sheets, normalize_columns, read_options)

    def write_excel(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet to .xlsx, or all sheets to one multi-tab workbook."""
        self._write("excel", out_filename, sheet, all_sheets, normalize_columns, read_options)
