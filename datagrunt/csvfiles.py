"""Module for data processing from files. Uses duckdb for file processing
   and Polars for dataframe objects.
"""

# standard library
from collections import Counter
import csv
import json
from operator import itemgetter
import re

# third party libraries
import pandas as pd

# local libraries
from core.databases import DuckDBDatabase
from core.filehelpers import FileEvaluator

# TODO CSVParser is a mess. Need to clean it up so that methods have a single idempotent responsibility and not be intertwined with state.

class CSVParser(FileEvaluator):
    """Class for parsing CSV files. Mostly determining the delimiter."""

    COMMON_DELIMITERS = [',', ';', '|', '\t', ' ', '^']
    COMMON_DELIMTERS_STRING = ''.join(COMMON_DELIMITERS)
    DELIMITER_REGEX_PATTERN = r'[^0-9a-zA-Z]'

    def __init__(self, filepath):
        super().__init__(filepath)
        self.first_row = self._get_first_line_from_file()
        self.first_row_no_spaces = self._remove_spaces_from_string()

    def _get_first_line_from_file(self):
        """Reads and returns the first line of a file.

        Args:
            filename: The path to the file.

        Returns:
            The first line of the file, stripped of leading/trailing whitespace,
            or None if the file is empty.
        """
        with open(self.filepath, 'r') as csv_file:
            first_line = csv_file.readline().strip()
        return first_line

    def _remove_spaces_from_string(self):
        """Remove all spaces from a given string.
        
        Args:
            text (str): The string to remove spaces from.

        Returns:
            str: The string with all spaces removed.
        """
        return self.first_row.replace(" ", "")

    def get_last_character_from_string(self):
        """Get the last character of a given string.
        
        Args:
            text (str): The string to get the last character from.

        Returns:
            str: The last character of the string.
        """
        return self.first_row[-1]

    def return_string_without_last_non_alpha_numeric_character(self):
        """Return the string without the last non alpha numeric character.
        
        Args:
            text (str): The string to return without the last non alpha numeric character.

        Returns:
            str: The string without the last non alpha numeric character.
        """
        # headers = self.remove_spaces_from_string()
        if not self.get_last_character_from_string().isalnum():
            headers = self.first_row[0:len(self.first_row)-1]
        else:
            headers = self.first_row
        return headers

    def remove_trailing_non_alpha_numeric_character_from_string(self):
        """Clean the headers of a given string.
        
        Args:
            text (str): The string to clean.

        Returns:
            str: The cleaned string.
        """
        headers_no_spaces = self.remove_spaces_from_string()
        return self.return_string_without_last_non_alpha_numeric_character(headers_no_spaces)

    def get_most_common_non_alpha_numeric_character_from_string(self):
        """Get the most common non-alpha-numeric character from a given string.
        
        Args:
            text (str): The string to get the most common non-alpha-numeric character from.

        Returns:
            str: The most common non-alpha-numeric character from the string.
        """
        regex = re.compile(self.DELIMITER_REGEX_PATTERN)
        counts = Counter(char for char in regex.findall(self.first_row))
        most_common = counts.most_common()
        return most_common

    def infer_csv_file_delimiter(self):
        """Infer the delimiter of a CSV file.
        
        Args:
            csv_file (str): The path to the CSV file.

        Returns:
            str: The delimiter of the CSV file.
        """
        clean_headers = self.remove_trailing_non_alpha_numeric_character_from_string()
        delimiter_candidates = self.get_most_common_non_alpha_numeric_character_from_string(clean_headers)
        if len(delimiter_candidates) < 1:
            delimiter = ' '
        else:
            delimiter = delimiter_candidates[0][0]
        return delimiter


class CSVFile(CSVParser):

    DEFAULT_ENCODING = 'utf-8'
    DEFAULT_DELIMITER = ','
    DEFAULT_SAMPLE_ROWS = 1

    QUOTING_MAP = {
        0: 'no quoting',
        1: 'quote all',
        2: 'quote minimal',
        3: 'quote non-numeric'
    }

    def __init__(self, filepath):
        """
        Initialize the CSVFile class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.duckdb_instance = DuckDBDatabase(self.filepath)
        if self.extension_string.lower() not in self.CSV_FILE_EXTENSIONS:
            raise ValueError(f"File extension '{self.extension_string}' is not a valid CSV file extension.")
    
    def _infer_csv_delimiter(self, number_of_sample_rows=5):
        """Function to infer the delimiter of a CSV file.

        Args:
            number_of_sample_rows (int): Number of rows to be sampled from the file.

        Returns:
            str: Delimiter
        """
        df = pd.read_csv(self.filepath, nrows=number_of_sample_rows, sep=self.DEFAULT_DELIMITER)
        columns = df.columns.to_list()

        # The assumption is if only one column then the delimiter isn't correct
        # The next steps will attempt to derive the delimiter even if not a commonly used one
        if len(columns) <= 1:

            # Regular expression to match non-alphanumeric characters. Excludes spaces.
            regex = re.compile('[^0-9a-zA-Z ]')

            # Count occurrences of each non-alphanumeric character
            # Assumption is highest count non-alphanumeric will be the delimiter
            counts = Counter(char for string in columns for char in regex.findall(string))
            # Find the most common non-alphanumeric character(s) and attempt to use as a delimiter
            most_common = counts.most_common()
            delimiter = max(most_common, key=itemgetter(1))[0]
        else:
            delimiter = self.DEFAULT_DELIMITER
    
        return delimiter

    @property
    def delimiter(self):
        """Return the delimiter used in the CSV file."""
        return self._infer_csv_delimiter()

    def _csv_import_table_statement(self):
        """Default CSV import table statement."""
        df = pd.read_csv(self.filepath, sep=self.delimiter, nrows=5)
        columns = {c: 'VARCHAR' for c in df.columns.tolist()}
        return f"""CREATE OR REPLACE TABLE {self.duckdb_instance.database_table_name}
                 AS SELECT * 
                 FROM read_csv('{self.filepath}', 
                                auto_detect=false,
                                delim='{self.delimiter}',
                                header = true,
                                columns = {columns}, 
                                null_padding=true,
                                all_varchar=True)
                """ # preserve integrity of data by importing as strings
    
    def select_from_table(self, sql_statement):
        """Select from duckdb table. This method gives the user an option to
           write a data transformation as a SQL statement. Results returned
           as a Polars dataframe.
        
        Args:
            sql_statement (str): SQL statement to import data.
        
        Return:
            Polars dataframe.
        """
        with self.duckdb_instance.database_connection as con:
            con.sql(self._csv_import_table_statement())
            return con.query(sql_statement).pl()
    
    def attributes(self):
        """Generate CSV attributes."""
        delimiter = self._infer_csv_delimiter()
        df = pd.read_csv(self.filepath, sep=delimiter, nrows=5)
        columns = {c: 'VARCHAR' for c in df.columns.tolist()}
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csvfile:
            # Sniff the file to detect parameters
            dialect = csv.Sniffer().sniff(csvfile.read(5))
            csvfile.seek(0)  # Reset file pointer to the beginning

            attributes = {
                'delimiter': self.delimiter,
                'quotechar': dialect.quotechar,
                'escapechar': dialect.escapechar,
                'doublequote': dialect.doublequote,
                'newline_delimiter': dialect.lineterminator,
                'skipinitialspace': dialect.skipinitialspace,
                'quoting': self.QUOTING_MAP.get(dialect.quoting),
                'columns': columns
            }

        return attributes

    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'r', encoding=self.duckdb_instance.DEFAULT_ENCODING) as csv_file:
            return sum(1 for _ in csv_file)

    def row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.row_count_with_header() - 1

    def columns(self):
        """Return the schema of the columns in the CSV file."""
        attributes = self.attributes()
        return attributes['columns']

    def columns_string(self):
        """Return the first row of the CSV file."""
        with open(self.filepath, 'r', encoding=self.duckdb_instance.DEFAULT_ENCODING) as csv_file:
            return csv_file.readline().strip()

    def columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        with open(self.filepath, 'rb') as csv_file:
            return csv_file.readline().strip()

    def column_count(self):
        """Return the number of columns in the CSV file."""
        return len(self.columns_string().split(','))

    def quotechar(self):
        """Return the quote character used in the CSV file."""
        return self.attributes()['quotechar']

    def escapechar(self):
        """Return the escape character used in the CSV file."""
        return self.attributes()['escapechar']

    def newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self.attributes()['newline_delimiter']

    def to_dicts(self):
        """Converts Dataframe to list of dictionaries."""
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=self.delimiter)
            return list(csv_reader)

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe."""
        return self.duckdb_instance.to_dataframe(self._csv_import_table_statement())

    def to_json(self):
         """Converts CSV to a JSON string."""
         if self.is_large:
             print("Warning: File is large and may not load into memory properly.")
         return json.loads(self.to_dataframe().write_json())

    def to_json_newline_delimited(self):
        """Converts CSV to a JSON string with newline delimited."""
        return self.to_dataframe().write_ndjson()

    def write_json(self):
        """Writes JSON to a file."""
        self.duckdb_instance.to_json(self._csv_import_table_statement())

    def write_json_newline_delimited(self):
        """Writes JSON to a file with newline delimited."""
        self.duckdb_instance.to_json_newline_delimited(self._csv_import_table_statement())

    def write_parquet(self):
        """Writes data to a Parquet file."""
        self.duckdb_instance.to_parquet(self._csv_import_table_statement())

    def write_excel(self):
        """Writes data to an Excel file."""
        self.duckdb_instance.to_excel(self._csv_import_table_statement())
