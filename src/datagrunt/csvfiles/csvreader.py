"""Module for reading CSV files and converting to different in memory python objects."""

# standard library

# third party libraries
import polars as pl

# local libraries

from src.datagrunt.core.queries import DuckDBQueries
from src.datagrunt.core.fileproperties import FileProperties
from src.datagrunt.core.engines import EngineProperties, EngineFactory

class CSVReader:
    """Class to unify the interface for reading CSV files."""

    READER_ENGINES = ['duckdb', 'polars']
    VALUE_ERROR_MESSAGE = """Reader engine '{engine}' is not 'duckdb' or 'polars'. Pass either 'duckdb' or 'polars' as valid engine params."""

    def __init__(self, filepath, engine='polars'):
        """Initialize the CSV Reader class.

        Args:
            filepath (str): Path to the file to read.
            engine (str, default 'polars'): Determines which reader engine class to instantiate.
        """
        self.filepath = filepath
        self.db_table = DuckDBQueries(self.filepath).database_table_name
        self.engine = engine.lower().replace(' ', '')
        self.file_properties = FileProperties(self.filepath)
        if self.engine not in EngineProperties.valid_engines:
            raise ValueError(EngineProperties.value_error_message.format(engine=self.engine))

    def _return_empty_file_object(self, object):
        """Return an empty file object."""
        return object

    def _create_reader(self):
        return EngineFactory(self.filepath, self.engine).create_reader()

    def get_sample(self, normalize_columns=False):
        """Return a sample of the CSV file."""
        self._create_reader().get_sample(normalize_columns)

    def to_dataframe(self, normalize_columns=False):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.file_properties.is_empty or self.file_properties.is_blank:
            return self._return_empty_file_object(pl.DataFrame())
        return self._create_reader().to_dataframe(normalize_columns)

    def to_arrow_table(self, normalize_columns=False):
        """Converts CSV to a PyArrow table.

        Returns:
            A PyArrow table.
        """
        if self.file_properties.is_empty or self.file_properties.is_blank:
            return self._return_empty_file_object(pl.DataFrame().to_arrow())
        return self._create_reader().to_arrow_table(normalize_columns)

    def to_dicts(self, normalize_columns=False):
        """Converts CSV to a list of dictionaries.

        Returns:
            A list of dictionaries.
        """
        if self.file_properties.is_empty or self.file_properties.is_blank:
            return self._return_empty_file_object(list())
        return self._create_reader().to_dicts(normalize_columns)

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
        if self.file_properties.is_empty or self.file_properties.is_blank:
            return self._return_empty_file_object(list())
        return self._create_reader().query_data(sql_query, normalize_columns)
