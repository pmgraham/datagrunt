"""Module for writing Excel workbook sheets to other file formats."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core import ExcelEngineProperties
from datagrunt.core.excel_io import set_excel_export_filename
from datagrunt.excel_api._engine_backed import _ExcelEngineBacked


class ExcelWriter(_ExcelEngineBacked):
    """Convert Excel workbook sheets to CSV/JSON/JSONL/Parquet/Excel.

    Security:
        Cell values are written verbatim — datagrunt preserves data, it does
        not sanitize it. For spreadsheet-interpreted formats (``write_csv`` and
        ``write_excel``), a value beginning with ``=``, ``+``, ``-``, ``@`` (or
        a leading tab/carriage return) is treated as a FORMULA when opened in a
        spreadsheet application (CWE-1236). Sanitize at the application layer if
        your source data is untrusted; datagrunt will not mutate values for you.
    """

    _engine_role = "writer"

    def __init__(self, filepath: str | Path, normalize_columns: bool = False, **read_options) -> None:
        """Initialize the Excel Writer.

        Args:
            filepath (str or Path): Path to the workbook to read.
            normalize_columns (bool): Normalize column names in every file this
                writer exports. A per-call argument overrides it.
            **read_options: Polars ``read_excel`` options applied when reading
                the source before export (per-call options override these).
        """
        super().__init__(filepath, normalize_columns=normalize_columns, **read_options)

    def _write_empty_output(self, default_filename, out_filename=None):
        """Write a single 0-byte file for an empty/blank source workbook."""
        filename = set_excel_export_filename(default_filename, out_filename)
        Path(filename).write_bytes(b"")

    def write_csv(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet (or all sheets, one file each) to CSV."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ExcelEngineProperties.csv_export_filename, out_filename)
        return self._engine.write_csv(
            out_filename, sheet=sheet, all_sheets=all_sheets, normalize_columns=normalize_columns, **read_options
        )

    def write_json(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet (or all sheets, one file each) to JSON."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ExcelEngineProperties.json_export_filename, out_filename)
        return self._engine.write_json(
            out_filename, sheet=sheet, all_sheets=all_sheets, normalize_columns=normalize_columns, **read_options
        )

    def write_json_newline_delimited(
        self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options
    ):
        """Export a sheet (or all sheets, one file each) to newline-delimited JSON."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ExcelEngineProperties.json_newline_export_filename, out_filename)
        return self._engine.write_json_newline_delimited(
            out_filename, sheet=sheet, all_sheets=all_sheets, normalize_columns=normalize_columns, **read_options
        )

    def write_parquet(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet (or all sheets, one file each) to Parquet."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ExcelEngineProperties.parquet_export_filename, out_filename)
        return self._engine.write_parquet(
            out_filename, sheet=sheet, all_sheets=all_sheets, normalize_columns=normalize_columns, **read_options
        )

    def write_excel(self, out_filename=None, sheet=None, all_sheets=False, normalize_columns=None, **read_options):
        """Export a sheet to .xlsx, or all sheets to one multi-tab workbook."""
        if self.is_empty or self.is_blank:
            return self._write_empty_output(ExcelEngineProperties.excel_export_filename, out_filename)
        return self._engine.write_excel(
            out_filename, sheet=sheet, all_sheets=all_sheets, normalize_columns=normalize_columns, **read_options
        )
