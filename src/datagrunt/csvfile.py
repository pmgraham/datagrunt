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

class CSVColumnFormatter(CSVProperties):
    """Class for formatting CSV files."""

    REGEX_PATTERNS = {
        'invalid_chars': r'[^a-zA-Z0-9_.\s-]',
        'valid_prefix': r'^[a-zA-Z_]',
        'spaces_periods': r'[\s.]+',
        'consecutive_duplicates': r'([^a-zA-Z0-9])\1+'
    }

    @staticmethod
    def _apply_string_functions_in_sequence(value, *funcs):
        """
        Apply a series of string functions to a value in sequence.

        This method takes an initial value and applies each function in the
        provided sequence of functions to that value. Each function is applied
        to the result of the previous function application.

        Args:
            value (str): The initial value to which the functions will be applied.
            *funcs (Callable): A variable number of functions to apply to the value.
                Each function should take a single argument and return a value.

        Returns:
            Any: The result after applying all functions in sequence to the initial value.

        Example:
            result = self._apply_string_functions_in_sequence("HELLO WORLD",
                                                              str.lower,
                                                              str.strip,
                                                              lambda s: s.replace(" ", "_")
                                                              )
            # result will be "hello_world"
        """
        for func in funcs:
            value = func(value)
        return value

    def _remove_invalid_chars(self, column_name):
        """
        Remove invalid characters from the column name.

        This method leaves only alphanumeric characters, underscores, periods, hyphens
        and spaces in the string. Hyphens are then replaced with underscores.

        Args:
            column_name: The original column name to be cleaned.

        Returns:
            The column name with only alphanumeric characters, underscores, and periods.

        Example:
            >>> self._remove_invalid_chars("Hello, World! 123")
            "Hello_World_123"
        """
        return re.sub(self.REGEX_PATTERNS['invalid_chars'], '', column_name).replace('-', '_')

    def _add_valid_prefix(self, column_name):
        """
        Ensure the column name starts with a letter or underscore.

        If the column name doesn't start with a letter or underscore,
        an underscore is prepended to it.

        Args:
            column_name: The column name to be checked and possibly modified.

        Returns:
            The column name with a valid prefix.

        Examples:
            >>> self._ensure_valid_prefix("123column")
            "_123column"
            >>> self._ensure_valid_prefix("column")
            "column"
        """
        return column_name if re.match(self.REGEX_PATTERNS['valid_prefix'], column_name) else f'_{column_name}'

    def _replace_spaces_periods_with_underscores(self, column_name):
        """
        Replace spaces, periods, and other whitespace characters with underscores.

        This method converts any sequence of whitespace characters or periods to a single underscore.

        Args:
            column_name: The column name to be processed.

        Returns:
            The column name with spaces and periods replaced by underscores.

        Example:
            >>> self._replace_spaces_periods_with_underscores("Hello   World.csv")
            "Hello_World_csv"
        """
        return re.sub(self.REGEX_PATTERNS['spaces_periods'], '_', column_name)

    def _remove_consecutive_non_alphanumeric_duplicates(self, column_name):
        """
        Remove consecutive duplicate non-alphanumeric characters from the column name.
        This method keeps only the first occurrence of consecutive identical non-alphanumeric characters.

        Args:
            column_name: The column name to be processed.

        Returns:
            The column name with consecutive non-alphanumeric duplicates removed.

        Examples:
            >>> self.remove_consecutive_non_alphanumeric_duplicates("hello__world")
            "hello_world"
            >>> self.remove_consecutive_non_alphanumeric_duplicates("apple!!!orange")
            "apple!orange"
            >>> self.remove_consecutive_non_alphanumeric_duplicates("AABBCC")
            "AABBCC"
            >>> self.remove_consecutive_non_alphanumeric_duplicates("user@@@gmail.com")
            "user@gmail.com"
        """
        return re.sub(self.REGEX_PATTERNS['consecutive_duplicates'], r'\1', column_name)

    def normalize_column_name(self, column_name):
        """
        Normalize a column name by applying a series of cleaning methods.

        This method applies the following transformations in order:
        1. Convert to lowercase
        2. Remove invalid characters
        3. Ensure a valid prefix
        4. Replace spaces and periods with underscores
        5. Remove consecutive duplicates

        Args:
            column_name: The original column name to be normalized.

        Returns:
            The normalized column name.

        Example:
            >>> self.normalize_column_name("  Hello, World! 123.csv  ")
            "hello_world_123_csv"
        """
        return self._apply_string_functions_in_sequence(
            column_name,
            str.lower,
            self._remove_invalid_chars,
            self._add_valid_prefix,
            self._replace_spaces_periods_with_underscores,
            self._remove_consecutive_non_alphanumeric_duplicates
        )

    def normalize_column_names(self):
        """
        Normalize all CSV column names and return a mapping of old names to new names.

        This method applies the normalization process to all columns in the CSV file.

        Returns:
            A dictionary where keys are original column names and
            values are the corresponding normalized names.

        Example:
            >>> self.normalize_columns()
            {"Original Name": "original_name", "Another Column!": "another_column"}
        """
        return {col: self.normalize_column_name(col) for col in self.columns}

    def normalize_dataframe_column_names(self, dataframe):
        """
        Update the column names of a Polars DataFrame using the normalized column names.

        This method applies the column name normalization rules defined in the CSVCleaner
        class to rename the columns of the provided Polars DataFrame. It uses the
        `normalize_columns` method to generate a mapping of original column names to
        their normalized versions, and then applies this mapping to rename the DataFrame
        columns.

        Args:
            df (polars.DataFrame): The input Polars DataFrame whose column names
                are to be updated.

        Returns:
            polars.DataFrame: A new Polars DataFrame with updated column names.
                The original DataFrame is not modified.

        Note:
            This method does not modify the input DataFrame in-place. Instead, it
            returns a new DataFrame with the updated column names.

        Example:
            >>> formatter = CSVColumnFormatter()
            >>> df = pl.DataFrame({"Original Name": [1, 2, 3], "Another Column!": [4, 5, 6]})
            >>> updated_df = formatter.normalize_dataframe_columns(df)
            >>> print(updated_df.columns)
            ['original_name', 'another_column']
        """
        return dataframe.rename(self.normalize_column_names())
