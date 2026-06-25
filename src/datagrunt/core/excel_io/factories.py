"""Factory for creating Excel reader and writer engine instances."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core.excel_io.engines import ExcelReaderEngine, ExcelWriterEngine


class ExcelEngineFactory:
    """Create the canonical Polars/calamine-backed Excel engines."""

    def __init__(self, filepath, normalize_columns=False, **read_options):
        """Initialize the factory.

        Args:
            filepath (str or Path): Path to the Excel file.
            normalize_columns (bool): Instance-level normalization forwarded to
                the engines this factory creates.
            **read_options: Polars ``read_excel`` options forwarded to the
                engines this factory creates.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        self.filepath = Path(filepath)
        self.normalize_columns = normalize_columns
        self.read_options = read_options
        if not self.filepath.exists():
            raise FileNotFoundError(f"Excel file not found: {self.filepath}")

    def create_reader(self):
        """Return a new ``ExcelReaderEngine``."""
        return ExcelReaderEngine(self.filepath, normalize_columns=self.normalize_columns, **self.read_options)

    def create_writer(self):
        """Return a new ``ExcelWriterEngine``."""
        return ExcelWriterEngine(self.filepath, normalize_columns=self.normalize_columns, **self.read_options)
