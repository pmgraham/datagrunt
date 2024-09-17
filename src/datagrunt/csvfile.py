"""Module for reading CSV files and converting CSV files to different standard file formats."""

# standard library
import re

# third party libraries
import duckdb

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

    def get_sample(self):
        """Return a sample of the CSV file."""
        self._set_reader_engine().get_sample()

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A Polars dataframe.
        """
        return self._set_reader_engine().to_dataframe()

    def to_arrow_table(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A PyArrow table.
        """
        return self._set_reader_engine().to_arrow_table()

    def to_dicts(self):
        """Converts CSV to a Polars dataframe.

        Returns:
            A list of dictionaries.
        """
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

class CSVCleaner(CSVProperties):
    """Class to unify the interface for cleaning CSV files."""
    
    def _remove_invalid_chars(self, column_name):
        """Leaves only alphanumeric, underscores, and whitespace characters in the string.
        
        Args:
            column_name str: String to be formatted.
        
        Returns:
            str: String with only alphanumeric, underscores, and whitespace characters.
        
        """
        return re.sub(r'[^a-zA-Z0-9_\s]', '', column_name)

    def _ensure_valid_prefix(self, column_name):
        """Ensures that the string always starts with either a letter or an underscore
        
        Args:
            column_name str: String to be formatted.
        
        Returns:
            str: String that always starts with either a letter or an underscore.
        """
        return column_name if re.match(r'^[a-zA-Z_]', column_name) else f'_{column_name}'

    def _replace_spaces(self, column_name):
        """Converts any spaces or other whitespace characters to an underscore.
        
        Args:
            column_name str: String to be formatted.

        Returns:
            str: String with spaces replaced with underscores.
        """
        return re.sub(r'\s+', '_', column_name)

    def _remove_consecutive_duplicates(self, column_name):
        """Removes duplicate consecutive characters from the string.

        Args:
            column_name str: String to be formatted.

        Returns:
            str: String with duplicate consecutive characters removed.

        Examples:
            - hello__world would become hello_world
            - apple111orange would become apple1orange
            - AABBCC would become ABC
        """
        return re.sub(r'([a-zA-Z0-9])\1+', r'\1', column_name)

    def _normalize_column_name(self):
        """Normalize a column name by applying cleaning methods.
        
        Returns:
            column_name str: Cleaned and normalized version of the column name string.
        """
        column_name = column_name.lower()
        column_name = self._remove_invalid_chars(column_name)
        column_name = self._ensure_valid_prefix(column_name)
        column_name = self._replace_spaces(column_name)
        column_name = self._remove_consecutive_duplicates(column_name)
        return column_name

    def normalize_columns(self):
        """Normalize all CSV column names and return a dict of old names to new names.
        
        Returns:
            dict: old column name as the key with new column name as the value.
        """
        return {col: self._normalize_column_name(col) for col in self.columns}
    
    def update_csv_column_headers(self,
                                  csv_output_file,
                                  new_headers_dict,
                                  delimiter,
                                  line_terminator):
        """Function does the following:
        1. Reads existing CSV
        2. Replaces headers with values passed in via a dictionary
        3. Writes out the values to a new file
        Originally the function was going to handle the file management and offer a way to
        delete the original file and replace with the output file. However, it's best to allow
        that to be handled downstream in order to give the most flexibility during operations.

        Args:
            csv_source_file (string): CSV file with the original headers
            csv_output_file (string): CSV file with the updated headers
            new_headers_dict (dict): Dictionary with the new header values
            delimiter (string): CSV file delimiter
            line_terminator (string): Line terminator characters
        """
        with open(self.filepath, 'r') as f, open(csv_output_file, 'w') as o:
            f.readline() # and discard
            vals = f"{delimiter}".join(new_headers_dict.values())
            vals += line_terminator
            o.write(vals)
            shutil.copyfileobj(f, o)

    def change_encoding():
        pass

    def isolate_problem_rows():
        pass

    def isolate_good_rows():
        pass

    def remove_hidden_characters():
        pass

    