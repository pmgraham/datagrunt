"""Module for writing CSV files and converting to different file formats."""

# standard library

# third party libraries

# local libraries
from src.datagrunt.core import (
    CSVProperties,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine,
    DuckDBQueries
)

class CSVWriter(CSVProperties):
    """Class to unify the interface for converting CSV files to various other supported file types."""

    WRITER_ENGINES = ['duckdb', 'polars']
    VALUE_ERROR_MESSAGE = """Writer engine '{engine}' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

    def __init__(self, filepath, engine='duckdb'):
        """Initialize the CSV Writer class.

        Args:
            filepath (str): Path to the file to write.
            engine (str, default 'duckdb'): Determines which writer engine class to instantiate.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.engine = engine.lower().replace(' ', '')
        if self.engine not in self.WRITER_ENGINES:
            raise ValueError(self.VALUE_ERROR_MESSAGE.format(engine=self.engine))

    def _set_writer_engine(self):
        """Sets the CSV reader engine as either DuckDB or Polars.
           Default engine is Polars.
        """
        if self.engine != 'polars':
            engine = CSVWriterDuckDBEngine(self.filepath)
        else:
            engine = CSVWriterPolarsEngine(self.filepath)
        return engine

    def write_csv(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
        return self._set_writer_engine().write_csv(out_filename, normalize_columns)

    def write_excel(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_excel(out_filename, normalize_columns)

    def write_json(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_json(out_filename, normalize_columns)

    def write_json_newline_delimited(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_json_newline_delimited(out_filename, normalize_columns)

    def write_parquet(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_parquet(out_filename, normalize_columns)
