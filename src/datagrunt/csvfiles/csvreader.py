"""Module for reading CSV files and converting to different in memory python objects."""

# standard library

# third party libraries
import polars as pl

# local libraries
from src.datagrunt.core import CSVComponents
from src.datagrunt.core import DuckDBQueries
from src.datagrunt.core import CSVEngineFactory

class CSVReader(CSVComponents):
    """Class to unify the interface for reading CSV files."""

    def __init__(self, filepath, engine='polars'):
        """Initialize the CSV Reader class.

        Args:
            filepath (str): Path to the file to read.
            engine (str, default 'polars'): Determines which reader engine class to instantiate.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.engine = engine.lower().replace(' ', '')

    def _return_empty_file_object(self, object):
        """Return an empty file object."""
        return object

    def _create_reader(self):
        """Create a reader object."""
        return CSVEngineFactory(self.filepath, self.engine).create_reader()

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        self._create_reader().get_sample(normalize_columns)

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Args:
            normalize_columns (bool): Whether to normalize column names.

        Returns:
            A Polars dataframe.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame())
        return self._create_reader().to_dataframe(normalize_columns)

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Args:
            normalize_columns (bool): Whether to normalize column names.

        Returns:
            A PyArrow table.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame().to_arrow())
        return self._create_reader().to_arrow_table(normalize_columns)

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of dictionaries.

        Args:
            normalize_columns (bool): Whether to normalize column names.

        Returns:
            A list of dictionaries.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(list())
        return self._create_reader().to_dicts(normalize_columns)

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.
            normalize_columns (optional, bool): Whether to normalize column names.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)

        If you set normalize_columns=True, the column names will be normalized to lowercase
        and spaces will be replaced with underscores, and you must reference the new column names
        in your query.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(list())
        return self._create_reader().query_data(sql_query, normalize_columns)
