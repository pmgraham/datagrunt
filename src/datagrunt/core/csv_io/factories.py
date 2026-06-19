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

    def __init__(self, filepath, engine, lenient=False, normalize_columns=False):
        """
        Initialize the Engine Factory class.

        Args:
            filepath (str or Path): Path to the file to read.
            engine (str): type of engine to create by the factory.
            lenient (bool): Whether to run in lenient mode.
            normalize_columns (bool): Instance-level column normalization
            setting forwarded to the engines this factory creates.
        """
        self.filepath = Path(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.lenient = lenient
        self.normalize_columns = normalize_columns
        if not self.filepath.exists():
            raise FileNotFoundError(f"CSV file not found: {self.filepath}")
        self.validate_engine(self.engine)

    @staticmethod
    def validate_engine(engine):
        """Validate a normalized engine name against the supported engines.

        Args:
            engine (str): Normalized engine name (lowercased, spaces stripped).

        Raises:
            ValueError: If the engine name is not a supported engine.
        """
        if engine not in CSVEngineProperties.valid_engines:
            raise ValueError(CSVEngineProperties.value_error_message.format(engine=engine))

    @property
    def db_table(self):
        """Return the database table name."""
        return DuckDBQueries(self.filepath, lenient=self.lenient).database_table_name

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
            return engine_class(self.filepath, lenient=self.lenient, normalize_columns=self.normalize_columns)
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
            return engine_class(self.filepath, lenient=self.lenient, normalize_columns=self.normalize_columns)
        else:
            raise ValueError(f"Unsupported writer engine: {self.engine}")
