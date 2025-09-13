"""Factory module for creating CSV factory instances."""

# standard library
from pathlib import Path

# third party libraries
# local libraries
from datagrunt.core.csv_io.engines import (
    CSVEngineProperties,
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
    CSVReaderPyArrowEngine,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine,
    CSVWriterPyArrowEngine,
)
from datagrunt.core.databases import DuckDBQueries


class CSVEngineFactory:
    """Factory class for creating CSV reader and writer engine instances."""

    READER_ENGINES = {
        "duckdb": CSVReaderDuckDBEngine,
        "polars": CSVReaderPolarsEngine,
        "pyarrow": CSVReaderPyArrowEngine,
    }

    WRITER_ENGINES = {
        "duckdb": CSVWriterDuckDBEngine,
        "polars": CSVWriterPolarsEngine,
        "pyarrow": CSVWriterPyArrowEngine,
    }

    def __init__(self, filepath, engine):
        """
        Initialize the Engine Factory class.

        Args:
            filepath (str or Path): Path to the file to read.
            engine (str): type of engine to create by the factory.
        """
        self.filepath = Path(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        if not self.filepath.exists():
            raise FileNotFoundError
        if self.engine not in CSVEngineProperties.valid_engines:
            raise ValueError(CSVEngineProperties.value_error_message.format(engine=self.engine))

    def create_reader(self):
        """Create a reader engine instance.

        Args:
            filepath (str): Path to the input file
            engine (str): Engine type ('duckdb' or 'polars')

        Returns:
            An instance of BaseReaderEngine
        """
        engine_class = self.READER_ENGINES.get(self.engine)
        if engine_class:
            return engine_class(self.filepath)
        else:
            raise ValueError(f"Unsupported reader engine: {self.engine}")

    def create_writer(self):
        """Create a writer engine instance.

        Args:
            filepath (str): Path to the input file
            engine (str): Engine type ('duckdb' or 'polars')

        Returns:
            An instance of BaseWriterEngine
        """
        engine_class = self.WRITER_ENGINES.get(self.engine)
        if engine_class:
            return engine_class(self.filepath)
        else:
            raise ValueError(f"Unsupported reader engine: {self.engine}")
