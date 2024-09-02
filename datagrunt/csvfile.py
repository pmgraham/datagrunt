"""Module for defining properties, reading, and writing CSV files."""

# standard library
from collections import Counter
import csv
import re

# third party libraries
from duckdb import read_csv, sql
import polars as pl

# local libraries
from datagrunt.core.fileproperties import FileProperties
from datagrunt.core.queries import DuckDBQueries
from datagrunt.core.logger import show_warning, show_large_file_warning

class CSVProperties(FileProperties):
    """Class for parsing CSV files. Mostly determining the delimiter."""

    DELIMITER_REGEX_PATTERN = r'[^0-9a-zA-Z_-]'
    DEFAULT_DELIMITER = ','
    DEFAULT_SAMPLE_ROWS = 1
    CSV_SNIFF_SAMPLE_ROWS = 5
    DATAFRAME_SAMPLE_ROWS = 5

    def __init__(self, filepath):
        """
        Initialize the CSVParser class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.first_row = self._get_first_row_from_file()
        self.delimiter = self._infer_csv_file_delimiter()
        if not self.is_csv:
            raise ValueError(f"File extension '{self.extension_string}' is not a valid CSV file extension.")

    def _get_first_row_from_file(self):
        """Reads and returns the first line of a file.

        Args:
            filename: The path to the file.

        Returns:
            The first line of the file, stripped of leading/trailing whitespace,
            or None if the file is empty.
        """
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            first_line = csv_file.readline().strip()
        return first_line

    def _get_most_common_non_alpha_numeric_character_from_string(self):
        """Get the most common non-alpha-numeric character from a given string.

        Args:
            text (str): The string to get the most common non-alpha-numeric character from.

        Returns:
            str: The most common non-alpha-numeric character from the string.
        """
        columns_no_spaces = self.first_row.replace(' ', '')
        regex = re.compile(self.DELIMITER_REGEX_PATTERN)
        counts = Counter(char for char in regex.findall(columns_no_spaces))
        most_common = counts.most_common()
        return most_common

    def _infer_csv_file_delimiter(self):
        """Infer the delimiter of a CSV file.

        Args:
            csv_file (str): The path to the CSV file.

        Returns:
            str: The delimiter of the CSV file.
        """
        delimiter_candidates = self._get_most_common_non_alpha_numeric_character_from_string()

        if self.is_empty:
            delimiter = self.DEFAULT_DELIMITER
        elif len(delimiter_candidates) == 0:
            delimiter = ' '
        else:
            delimiter = delimiter_candidates[0][0]
        return delimiter

    def get_row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            return sum(1 for _ in csv_file)

    def get_row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.get_row_count_with_header() - 1

    def get_attributes(self):
        """Generate CSV attributes."""
        columns_list = self.first_row.split(self.delimiter)
        columns = {c: 'VARCHAR' for c in columns_list}
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csvfile:
            # Sniff the file to detect parameters
            dialect = csv.Sniffer().sniff(csvfile.read(self.CSV_SNIFF_SAMPLE_ROWS))
            csvfile.seek(0)  # Reset file pointer to the beginning

            attributes = {
                'delimiter': self.delimiter,
                'quotechar': dialect.quotechar,
                'escapechar': dialect.escapechar,
                'doublequote': dialect.doublequote,
                'newline_delimiter': dialect.lineterminator,
                'skipinitialspace': dialect.skipinitialspace,
                'quoting': self.QUOTING_MAP.get(dialect.quoting),
                'row_count_with_header': self.get_row_count_with_header(),
                'row_count_without_header': self.get_row_count_without_header(),
                'columns_schema': columns,
                'columns_original_format': self.first_row,
                'columns_list': columns_list,
                'columns_string': ", ".join(columns_list),
                'columns_byte_string': ", ".join(columns_list).encode(),
                'column_count': len(columns_list)
            }

        return attributes

    def get_columns(self):
        """Return the schema of the columns in the CSV file."""
        return self.get_attributes()['columns_list']

    def get_columns_string(self):
        """Return the first row of the CSV file."""
        return self.get_attributes()['columns_string']

    def get_columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        return self.get_attributes()['columns_byte_string']

    def get_column_count(self):
        """Return the number of columns in the CSV file."""
        return self.get_attributes()['column_count']

    def get_quotechar(self):
        """Return the quote character used in the CSV file."""
        return self.get_attributes()['quotechar']

    def get_escapechar(self):
        """Return the escape character used in the CSV file."""
        return self.get_attributes()['escapechar']

    def get_newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self.get_attributes()['newline_delimiter']

class CSVReader(CSVProperties):

    QUOTING_MAP = {
        0: 'no quoting',
        1: 'quote all',
        2: 'quote minimal',
        3: 'quote non-numeric'
    }

    def __init__(self, filepath):
        """
        Initialize the CSVReader class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.dataframe = self._to_dataframe()

    def _to_dataframe(self):
        """Converts CSV to a Polars dataframe."""
        return pl.read_csv(self.filepath, separator=self.delimiter)

    def to_dicts(self):
        """Converts Dataframe to list of dictionaries."""
        if self.is_large:
             show_large_file_warning()
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=self.delimiter)
            return list(csv_reader)

    def to_json(self):
         """Converts CSV to a JSON string."""
         if self.is_large:
             show_large_file_warning()
         return self.to_dataframe().write_json()

    def to_json_newline_delimited(self):
        """Converts CSV to a JSON string with newline delimited."""
        if self.is_large:
             show_large_file_warning()
        return self.to_dataframe().write_ndjson()

class CSVWriter(CSVProperties):
    """Class to write CSV files to various file types."""

    def __init__(self, filepath):
        """
        Initialize the CSVWriter class.

        Args:
            filepath (str): Path to the file to write.
        """
        super().__init__(filepath)

    def write_avro(self, out_filename=None):
        """Writes data to an Avro file.

        Args:
            out_filename (str): The name of the output file.
        """
        if out_filename:
            filename = out_filename
        else:
            filename = DuckDBQueries(self.filepath).AVRO_OUT_FILENAME
        self.to_dataframe().write_avro(filename)

    def write_csv(self, out_filename=None):
        """Writes CSV to a file.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).export_csv_query(out_filename))

    def write_json(self, out_filename=None):
        """Writes JSON to a file.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).export_json_query(out_filename))

    def write_json_newline_delimited(self, out_filename=None):
        """Writes JSON to a file with newline delimited.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).export_json_newline_delimited_query(out_filename))

    def write_parquet(self, out_filename=None):
        """Writes data to a Parquet file.

        Args:
            out_filename (str): The name of the output file.
        """
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).export_parquet_query(out_filename))

    def write_excel(self, out_filename=None):
        """Writes data to an Excel file.

        Args:
            out_filename (str): The name of the output file.
        """
        if self.get_row_count_with_header() > self.EXCEL_ROW_LIMIT:
            show_warning(f"Data will be lost: file contains {self.get_row_count_with_header()} rows and Excel supports a max of {self.EXCEL_ROW_LIMIT} rows.")
        sql(self._csv_import_table_statement())
        sql(DuckDBQueries(self.filepath).export_excel_query(out_filename))
