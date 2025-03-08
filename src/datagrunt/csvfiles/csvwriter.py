"""Module for writing CSV files and converting to different file formats."""

# standard library

# third party libraries

# local libraries
from datagrunt.core import CSVComponents
from datagrunt.core import DuckDBQueries
from datagrunt.core import CSVEngineFactory

class CSVWriter(CSVComponents):
    """Class to unify the interface for converting CSV files to various other supported file types."""

    def __init__(self, filepath, engine='duckdb'):
        """Initialize the CSV Writer class.

        Args:
            filepath (str): Path to the file to write.
            engine (str, default 'duckdb'): Determines which writer engine class to instantiate.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.engine = engine.lower().replace(' ', '')

    def _create_writer(self):
        """Create a reader object."""
        return CSVEngineFactory(self.filepath, self.engine).create_writer()

    def write_csv(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
                normalize_columns optional, bool: Whether to normalize column names.
            """
        return self._create_writer().write_csv(out_filename, normalize_columns)

    def write_excel(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename str: The name of the output file.
            normalize_columns optional, bool: Whether to normalize column names.
        """
        return self._create_writer().write_excel(out_filename, normalize_columns)

    def write_json(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename str: The name of the output file.
            normalize_columns optional, bool: Whether to normalize column names.
        """
        return self._create_writer().write_json(out_filename, normalize_columns)

    def write_json_newline_delimited(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
            normalize_columns optional, bool: Whether to normalize column names.
        """
        return self._create_writer().write_json_newline_delimited(out_filename, normalize_columns)

    def write_parquet(self, out_filename=None, normalize_columns=False):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename str: The name of the output file.
            normalize_columns optional, bool: Whether to normalize column names.
        """
        return self._create_writer().write_parquet(out_filename, normalize_columns)
