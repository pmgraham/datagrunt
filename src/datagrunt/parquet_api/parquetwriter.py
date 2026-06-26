"""Module for writing Parquet files to other file formats."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core import ParquetEngineProperties
from datagrunt.core.parquet_io import set_parquet_export_filename
from datagrunt.parquet_api._engine_backed import _ParquetEngineBacked


class ParquetWriter(_ParquetEngineBacked):
    """Convert Parquet files to CSV/JSON/JSONL/Parquet/Excel.

    Security:
        Values are written verbatim — datagrunt preserves data, it does not
        sanitize it. For spreadsheet-interpreted formats (``write_csv`` and
        ``write_excel``), a value beginning with ``=``, ``+``, ``-``, ``@`` (or
        a leading tab/carriage return) is treated as a FORMULA when opened in a
        spreadsheet application (CWE-1236). Sanitize at the application layer if
        your source data is untrusted; datagrunt will not mutate values for you.
    """

    _engine_role = "writer"

    def __init__(self, filepath: str | Path, normalize_columns: bool = False, **read_options) -> None:
        """Initialize the Parquet Writer.

        Args:
            filepath (str or Path): Path to the Parquet file to read.
            normalize_columns (bool): Normalize column names in every file this
                writer exports. A per-call argument overrides it.
            **read_options: Polars ``read_parquet`` options applied when reading
                the source before export (per-call options override these).
        """
        super().__init__(filepath, normalize_columns=normalize_columns, **read_options)

    def _write_empty_output(self, default_filename, out_filename=None):
        """Write a single 0-byte file for an empty/blank source."""
        filename = set_parquet_export_filename(default_filename, out_filename)
        Path(filename).write_bytes(b"")

    def write_csv(
        self, out_filename: str | None = None, normalize_columns: bool | None = None, **write_options
    ) -> None:
        """Export to CSV."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ParquetEngineProperties.csv_export_filename, out_filename)
        return self._engine.write_csv(out_filename, normalize_columns=normalize_columns, **write_options)

    def write_json(
        self, out_filename: str | None = None, normalize_columns: bool | None = None, **write_options
    ) -> None:
        """Export to JSON."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ParquetEngineProperties.json_export_filename, out_filename)
        return self._engine.write_json(out_filename, normalize_columns=normalize_columns, **write_options)

    def write_json_newline_delimited(
        self, out_filename: str | None = None, normalize_columns: bool | None = None, **write_options
    ) -> None:
        """Export to newline-delimited JSON."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ParquetEngineProperties.json_newline_export_filename, out_filename)
        return self._engine.write_json_newline_delimited(
            out_filename, normalize_columns=normalize_columns, **write_options
        )

    def write_parquet(
        self, out_filename: str | None = None, normalize_columns: bool | None = None, **write_options
    ) -> None:
        """Export to Parquet (re-encode; ``compression=`` etc. pass through)."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ParquetEngineProperties.parquet_export_filename, out_filename)
        return self._engine.write_parquet(out_filename, normalize_columns=normalize_columns, **write_options)

    def write_excel(
        self, out_filename: str | None = None, normalize_columns: bool | None = None, **write_options
    ) -> None:
        """Export to .xlsx."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ParquetEngineProperties.excel_export_filename, out_filename)
        return self._engine.write_excel(out_filename, normalize_columns=normalize_columns, **write_options)
