"""Polars-backed engines for the Parquet subsystem."""

# standard library
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import ClassVar

# third party libraries
import duckdb
import polars as pl

# local libraries
from datagrunt.core.parquet_io.parquetcomponents import (
    ParquetComponents,
    normalize_parquet_columns,
    parquet_table_name,
)


@dataclass
class ParquetEngineProperties:
    """Default output filenames and tuning constants for Parquet engines."""

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


def set_parquet_export_filename(default_filename, export_filename=None):
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


# Keys that select the source — controlled by the engine, never the caller.
_RESERVED_READ_OPTIONS = ("source",)


class ParquetReaderEngine:
    """Read a Parquet file into a Polars frame and convert/query it.

    Parses each (normalize, read_options) combination at most once and caches
    the frame. Holds a DuckDB connection only when ``query_data`` is used; it is
    opened lazily and released by ``close()``.

    ``**read_options`` are forwarded verbatim to ``pl.read_parquet`` (``columns``,
    ``n_rows``, ``row_index_name``, etc.). Constructor options are defaults;
    per-call options merge over them.
    """

    def __init__(self, filepath, normalize_columns=False, **read_options):
        self.filepath = Path(filepath)
        self.normalize_columns = normalize_columns
        self._read_options = read_options
        self._components = ParquetComponents(self.filepath)
        self._frame_cache = {}
        self._connection = None

    @cached_property
    def db_table(self):
        """Stable table name used by ``query_data`` registration."""
        return parquet_table_name(self.filepath)

    def _merge_read_options(self, call_options):
        """Merge constructor + per-call options; reject reserved keys."""
        merged = {**self._read_options, **call_options}
        reserved = [k for k in _RESERVED_READ_OPTIONS if k in merged]
        if reserved:
            raise ValueError(
                f"The source path is controlled via the constructor; do not pass {reserved} as read options."
            )
        return merged

    def _read_frame(self, normalize_columns, read_options):
        """Return the cached frame for (normalize, read_options)."""
        resolved_normalize = _resolve_normalize(self.normalize_columns, normalize_columns)
        merged_options = self._merge_read_options(read_options)
        key = (resolved_normalize, _freeze(merged_options))
        if key not in self._frame_cache:
            df = pl.read_parquet(self.filepath, **merged_options)
            if resolved_normalize:
                mapping = dict(zip(df.columns, normalize_parquet_columns(df.columns)))
                df = df.rename(mapping)
            self._frame_cache[key] = df
        return self._frame_cache[key]

    def get_sample(self, normalize_columns=None, **read_options):
        """Return the leading sample rows as a Polars frame."""
        return self._read_frame(normalize_columns, read_options).head(
            ParquetEngineProperties.dataframe_sample_rows
        )

    def to_dataframe(self, normalize_columns=None, **read_options):
        """Return the file as a Polars DataFrame."""
        return self._read_frame(normalize_columns, read_options)

    def to_arrow_table(self, normalize_columns=None, **read_options):
        """Return the file as a PyArrow Table."""
        return self._read_frame(normalize_columns, read_options).to_arrow()

    def to_dicts(self, normalize_columns=None, **read_options):
        """Return the file as a list of row dicts."""
        return self._read_frame(normalize_columns, read_options).to_dicts()

    def query_data(self, sql_query, normalize_columns=None, **read_options):
        """Register the frame under ``db_table`` and run SQL.

        Security:
            ``sql_query`` runs verbatim against the in-memory session — a
            SQL-injection vector if built from untrusted input. Validate /
            parameterize at the application layer.

        Returns:
            A live ``DuckDBPyRelation``. Re-registering replaces the prior
            table, so the table name stays stable across calls.
        """
        df = self._read_frame(normalize_columns, read_options)
        if self._connection is None:
            self._connection = duckdb.connect(":memory:")
        self._connection.register(self.db_table, df.to_arrow())
        return self._connection.sql(sql_query)

    def close(self):
        """Release the DuckDB connection if one was opened. Idempotent."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None


class ParquetWriterEngine:
    """Export a Parquet file to CSV/JSON/JSONL/Parquet/Excel via Polars."""

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
        self._reader = ParquetReaderEngine(self.filepath, normalize_columns=normalize_columns, **read_options)

    def close(self):
        """Release any resources held by the backing reader engine."""
        self._reader.close()

    def _write(self, fmt, out_filename, normalize_columns, write_options):
        """Shared dispatch for all public write_* methods."""
        default_attr, writer_method = self._SINGLE_TABLE_WRITERS[fmt]
        default_filename = getattr(ParquetEngineProperties, default_attr)
        filename = set_parquet_export_filename(default_filename, out_filename)
        df = self._reader.to_dataframe(normalize_columns)
        getattr(df, writer_method)(filename, **write_options)

    def write_csv(self, out_filename=None, normalize_columns=None, **write_options):
        """Export to CSV."""
        self._write("csv", out_filename, normalize_columns, write_options)

    def write_json(self, out_filename=None, normalize_columns=None, **write_options):
        """Export to JSON."""
        self._write("json", out_filename, normalize_columns, write_options)

    def write_json_newline_delimited(self, out_filename=None, normalize_columns=None, **write_options):
        """Export to newline-delimited JSON."""
        self._write("jsonl", out_filename, normalize_columns, write_options)

    def write_parquet(self, out_filename=None, normalize_columns=None, **write_options):
        """Export to Parquet (re-encode; ``compression=`` etc. pass through)."""
        self._write("parquet", out_filename, normalize_columns, write_options)

    def write_excel(self, out_filename=None, normalize_columns=None, **write_options):
        """Export to .xlsx."""
        self._write("excel", out_filename, normalize_columns, write_options)
