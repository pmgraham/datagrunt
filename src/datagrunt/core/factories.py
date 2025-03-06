"""Factory module for creating factory instances."""

# standard library
import os

# third party libraries

# local libraries
from src.datagrunt.core.engines import EngineProperties
from src.datagrunt.core.engines import (
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine
)

from src.datagrunt.core.queries import DuckDBQueries

class CSVEngineFactory:
    """Factory class for creating CSV reader and writer engine instances."""

    READER_ENGINES = {
                'duckdb': CSVReaderDuckDBEngine,
                'polars': CSVReaderPolarsEngine,
            }

    WRITER_ENGINES = {
                'duckdb': CSVWriterDuckDBEngine,
                'polars': CSVWriterPolarsEngine,
            }

    def __init__(self, filepath, engine):
        """
        Initialize the Engine Factory class.

        Args:
            filepath (str): Path to the file to read.
            engine (str): type of engine to create by the factory.
        """
        self.filepath = filepath
        self.engine = engine.lower().replace(' ', '')
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        if not os.path.exists(self.filepath):
            raise FileNotFoundError
        if self.engine not in EngineProperties.valid_engines:
            raise ValueError(EngineProperties.value_error_message.format(engine=self.engine))

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
