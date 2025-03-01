"""Module for reading CSV files and converting to different in memory python objects."""

# standard library

# third party libraries
import polars as pl

# local libraries
from src.datagrunt.core import (
    CSVProperties,
    CSVReaderDuckDBEngine,
    CSVReaderPolarsEngine,
    DuckDBQueries
)

class CSVReader(CSVProperties):
    """Class to unify the interface for reading CSV files."""

    READER_ENGINES = ['duckdb', 'polars']
    VALUE_ERROR_MESSAGE = """Reader engine '{engine}' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

    def __init__(self, filepath, engine='polars'):
        """Initialize the CSV Reader class.

        Args:
            filepath (str): Path to the file to read.
            engine (str, default 'polars'): Determines which reader engine class to instantiate.
        """
        super().__init__(filepath)
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.engine = engine.lower().replace(' ', '')
        if self.engine not in self.READER_ENGINES:
            raise ValueError(self.VALUE_ERROR_MESSAGE.format(engine=self.engine))

    def _set_reader_engine(self):
        """Sets the CSV reader engine as either DuckDB or Polars.
           Default engine is Polars.
        """
        if self.engine != 'polars':
            engine = CSVReaderDuckDBEngine(self.filepath)
        else:
            engine = CSVReaderPolarsEngine(self.filepath)
        return engine

    def _return_empty_file_object(self, object):
        """Return an empty file object."""
        return object

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        self._set_reader_engine().get_sample(normalize_columns)

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame())
        return self._set_reader_engine().to_dataframe(normalize_columns)

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame().to_arrow())
        return self._set_reader_engine().to_arrow_table(normalize_columns)

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of dictionaries.

        Returns:
            A list of dictionaries.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(list())
        return self._set_reader_engine().to_dicts(normalize_columns)

    def query_data(self, sql_query, normalize_columns=False):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.

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
        return self._set_reader_engine().query_data(sql_query, normalize_columns)
