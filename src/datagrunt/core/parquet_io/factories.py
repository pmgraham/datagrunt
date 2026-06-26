"""Factory for creating Parquet reader and writer engine instances."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core.parquet_io.engines import ParquetReaderEngine, ParquetWriterEngine


class ParquetEngineFactory:
    """Create the canonical Polars-backed Parquet engines."""

    def __init__(self, filepath, normalize_columns=False, **read_options):
        """Initialize the factory.

        Args:
            filepath (str or Path): Path to the Parquet file.
            normalize_columns (bool): Instance-level normalization forwarded to
                the engines this factory creates.
            **read_options: Polars ``read_parquet`` options forwarded to the
                engines this factory creates.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        self.filepath = Path(filepath)
        self.normalize_columns = normalize_columns
        self.read_options = read_options
        if not self.filepath.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.filepath}")

    def create_reader(self):
        """Return a new ``ParquetReaderEngine``."""
        return ParquetReaderEngine(self.filepath, normalize_columns=self.normalize_columns, **self.read_options)

    def create_writer(self):
        """Return a new ``ParquetWriterEngine``."""
        return ParquetWriterEngine(self.filepath, normalize_columns=self.normalize_columns, **self.read_options)
