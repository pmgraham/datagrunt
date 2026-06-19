"""Module for writing CSV files and converting to different file formats."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core import CSVEngineProperties, DuckDBQueries
from datagrunt.csv_api._compat import warn_per_call_normalize
from datagrunt.csv_api._engine_backed import _CSVEngineBacked


class CSVWriter(_CSVEngineBacked):
    """
    Class to unify the interface for converting CSV files to various other
    supported file types.

    Security:
        Cell values are written verbatim — datagrunt preserves data, it does
        not transform or sanitize it. For the spreadsheet-interpreted formats
        (``write_csv`` and ``write_excel``), a value beginning with ``=``,
        ``+``, ``-``, ``@`` (or a leading tab/carriage return) is treated as a
        FORMULA by Excel / LibreOffice / Google Sheets when the file is opened
        — the classic CSV/formula-injection vector (CWE-1236). If your source
        data is untrusted and the output may be opened in a spreadsheet
        application, sanitize at the application layer before export; datagrunt
        will not silently mutate the values for you.
    """

    _engine_role = "writer"

    def __init__(
        self,
        filepath: str | Path,
        engine: str = "duckdb",
        lenient: bool = False,
        normalize_columns: bool = False,
    ) -> None:
        """
        Initialize the CSV Writer class.

        Args:
            filepath (str or Path): Path to the file to write.
            engine (str, default 'duckdb'): Determines which writer engine
            class to instantiate.
            lenient (bool): Whether to run in lenient mode.
            normalize_columns (bool): Whether to normalize column names in
            every file this writer exports.
        """
        super().__init__(filepath, engine, lenient=lenient, normalize_columns=normalize_columns)
        self.queries = DuckDBQueries(self.filepath, lenient=self.lenient)
        self.db_table = self.queries.database_table_name

    def _write_empty_output(self, default_filename, out_filename=None):
        """Write a truly empty output file for an empty/blank source.

        Mirrors CSVReader's empty/blank guard: rather than fabricating a
        ``column0`` header (duckdb) or raising an engine-specific error
        (polars ``NoDataError`` / pyarrow ``ArrowInvalid``), every engine
        produces an identical 0-byte file.
        """
        filename = self.queries.set_export_filename(default_filename, out_filename)
        Path(filename).write_bytes(b"")

    def write_csv(self, out_filename: str | None = None, normalize_columns: bool | None = None) -> None:
        """
        Query to export a DuckDB table to a CSV file.

        Args:
            out_filename (str | None): The name of the output file.
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.
        """
        if self.is_empty or self.is_blank:
            return self._write_empty_output(CSVEngineProperties.csv_export_filename, out_filename)
        return self._engine.write_csv(out_filename, warn_per_call_normalize(normalize_columns))

    def write_excel(self, out_filename: str | None = None, normalize_columns: bool | None = None) -> None:
        """
        Query to export a DuckDB table to an Excel file.

        Args:
            out_filename (str | None): The name of the output file.
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.
        """
        if self.is_empty or self.is_blank:
            return self._write_empty_output(CSVEngineProperties.excel_export_filename, out_filename)
        return self._engine.write_excel(out_filename, warn_per_call_normalize(normalize_columns))

    def write_json(self, out_filename: str | None = None, normalize_columns: bool | None = None) -> None:
        """
        Query to export a DuckDB table to a JSON file.

        Args:
            out_filename (str | None): The name of the output file.
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.
        """
        if self.is_empty or self.is_blank:
            return self._write_empty_output(CSVEngineProperties.json_export_filename, out_filename)
        return self._engine.write_json(out_filename, warn_per_call_normalize(normalize_columns))

    def write_json_newline_delimited(
        self, out_filename: str | None = None, normalize_columns: bool | None = None
    ) -> None:
        """
        Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename (str | None): The name of the output file.
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.
        """
        if self.is_empty or self.is_blank:
            return self._write_empty_output(CSVEngineProperties.json_newline_export_filename, out_filename)
        return self._engine.write_json_newline_delimited(out_filename, warn_per_call_normalize(normalize_columns))

    def write_parquet(self, out_filename: str | None = None, normalize_columns: bool | None = None) -> None:
        """
        Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename (str | None): The name of the output file.
            normalize_columns (bool or None): Deprecated per-call override.
            ``None`` (default) inherits the constructor-level setting.
        """
        if self.is_empty or self.is_blank:
            return self._write_empty_output(CSVEngineProperties.parquet_export_filename, out_filename)
        return self._engine.write_parquet(out_filename, warn_per_call_normalize(normalize_columns))
