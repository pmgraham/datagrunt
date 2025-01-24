"""Module for reading CSV files and converting CSV files to different standard file formats."""

# standard library

# third party libraries
import duckdb
import polars as pl

# local libraries
from .core.fileproperties import CSVProperties
from .core.engines import CSVReaderDuckDBEngine, CSVReaderPolarsEngine
from .core.engines import CSVWriterDuckDBEngine, CSVWriterPolarsEngine
from .core.queries import DuckDBQueries

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

    def get_sample(self):
        """Return a sample of the CSV file."""
        self._set_reader_engine().get_sample()

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame())
        return self._set_reader_engine().to_dataframe()

    def to_arrow_table(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A PyArrow table.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(pl.DataFrame().to_arrow())
        return self._set_reader_engine().to_arrow_table()

    def to_dicts(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A list of dictionaries.
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(list())
        return self._set_reader_engine().to_dicts()

    def query_data(self, sql_query):
        """Queries as CSV file after importing into DuckDB.

        Args:
            sql_query (str): Query to run against DuckDB.

        Returns:
            A DuckDB DuckDBPyRelation with the query results.

        Example if DuckDB Engine:
            dg = CSVReader('myfile.csv')
            query = "SELECT col1, col2 FROM {dg.db_table}" # f string assumed
            dg.query_csv_data(query)
        """
        if self.is_empty or self.is_blank:
            return self._return_empty_file_object(list())
        queries = DuckDBQueries(self.filepath)
        duckdb.sql(queries.import_csv_query(self.delimiter))
        return duckdb.sql(sql_query)

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

    def write_csv(self, out_filename=None):
        """Query to export a DuckDB table to a CSV file.

            Args:
                out_filename str: The name of the output file.
            """
        return self._set_writer_engine().write_csv(out_filename)

    def write_excel(self, out_filename=None):
        """Query to export a DuckDB table to an Excel file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_excel(out_filename)

    def write_json(self, out_filename=None):
        """Query to export a DuckDB table to a JSON file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_json(out_filename)

    def write_json_newline_delimited(self, out_filename=None):
        """Query to export a DuckDB table to a JSON newline delimited file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_json_newline_delimited(out_filename)

    def write_parquet(self, out_filename=None):
        """Query to export a DuckDB table to a Parquet file.

        Args:
            out_filename str: The name of the output file.
        """
        return self._set_writer_engine().write_parquet(out_filename)
