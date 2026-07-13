"""Structural protocols and shared helpers for CSV reader/writer engines."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Protocol, Union, runtime_checkable

import polars as pl
import pyarrow as pa
from duckdb import DuckDBPyRelation


def resolve_normalize_columns(instance_default: bool, per_call_value: Optional[bool]) -> bool:
    """Resolve a per-call normalize flag against the engine's instance default."""
    return instance_default if per_call_value is None else per_call_value


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


def arrow_to_polars(table: pa.Table) -> pl.DataFrame:
    """Convert a PyArrow table to a Polars DataFrame, never a bare Series."""
    df = pl.from_arrow(table)
    return df.to_frame() if isinstance(df, pl.Series) else df


@runtime_checkable
class CSVReaderEngineProtocol(Protocol):
    """Structural interface every CSV reader engine must satisfy."""

    filepath: Path
    lenient: bool
    normalize_columns: bool
    db_table: str
    delimiter: str

    @property
    def queries(self):
        """DuckDB query helper bound to this reader's source file."""
        ...

    def close(self) -> None:
        """Release engine-held resources."""
        ...

    def get_sample(self, normalize_columns: Optional[bool] = None, n_rows: Optional[int] = None) -> pl.DataFrame:
        """Return a sample of the data as a Polars DataFrame."""
        ...

    def to_dataframe(self, normalize_columns: Optional[bool] = None) -> pl.DataFrame:
        """Convert the full CSV to a Polars DataFrame."""
        ...

    def to_arrow_table(self, normalize_columns: Optional[bool] = None) -> pa.Table:
        """Convert the full CSV to a PyArrow table."""
        ...

    def to_dicts(self, normalize_columns: Optional[bool] = None) -> List[Dict]:
        """Convert the full CSV to a list of row dicts."""
        ...

    def query_data(
        self, sql_query: str, normalize_columns: Optional[bool] = None
    ) -> Union[DuckDBPyRelation, pl.DataFrame]:
        """Run SQL against the imported CSV."""
        ...


@runtime_checkable
class CSVWriterEngineProtocol(Protocol):
    """Structural interface every CSV writer engine must satisfy."""

    filepath: Path
    lenient: bool
    normalize_columns: bool
    db_table: str

    @property
    def queries(self):
        """DuckDB query helper bound to this writer's source file."""
        ...

    def write_csv(self, export_filename=None, normalize_columns=None) -> None:
        """Export to CSV."""
        ...

    def write_excel(self, export_filename=None, normalize_columns=None) -> None:
        """Export to Excel."""
        ...

    def write_json(self, export_filename=None, normalize_columns=None) -> None:
        """Export to JSON."""
        ...

    def write_json_newline_delimited(self, export_filename=None, normalize_columns=None) -> None:
        """Export to JSON Lines."""
        ...

    def write_parquet(self, export_filename=None, normalize_columns=None) -> None:
        """Export to Parquet."""
        ...


class DataFrameDerivedReaderMixin:
    """Default Arrow/dict conversions for engines centered on Polars DataFrames."""

    def to_arrow_table(self, normalize_columns=None):
        return self.to_dataframe(normalize_columns).to_arrow()

    def to_dicts(self, normalize_columns=None):
        return self.to_dataframe(normalize_columns).to_dicts()
