"""Module for data processing from files. Uses duckdb for file processing
   and Polars for dataframe objects.
"""

# standard library
from collections import Counter
import csv
import json
import re

# third party libraries

# local libraries
from core.databases import DuckDBDatabase
from core.filehelpers import FileEvaluator

# TODO need evaluate first row in CSVParser without spaces. Spaces are errenously counted as the delimiter in certain scenarios.
# TODO reinstate the remove spaces logic from the first line of the CSV file and re-evaluate the delimiter that way.

class CSVParser(FileEvaluator):
    """Class for parsing CSV files. Mostly determining the delimiter."""

    DELIMITER_REGEX_PATTERN = r'[^0-9a-zA-Z_-]'
    DEFAULT_DELIMITER = ','

    def __init__(self, filepath):
        """
        Initialize the CSVParser class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.first_row = self._get_first_line_from_file()
        self.delimiter = self.infer_csv_file_delimiter()

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

    def get_most_common_non_alpha_numeric_character_from_string(self):
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

    def infer_csv_file_delimiter(self):
        """Infer the delimiter of a CSV file.

        Args:
            csv_file (str): The path to the CSV file.

        Returns:
            str: The delimiter of the CSV file.
        """
        delimiter_candidates = self.get_most_common_non_alpha_numeric_character_from_string()

        if self.is_empty:
            delimiter = self.DEFAULT_DELIMITER
        elif len(delimiter_candidates) == 0:
            delimiter = ' '
        else:
            delimiter = delimiter_candidates[0][0]
        return delimiter

class CSVFile(CSVParser):

    DEFAULT_ENCODING = 'utf-8'
    DEFAULT_DELIMITER = ','
    DEFAULT_SAMPLE_ROWS = 1
    CSV_SNIFF_SAMPLE_ROWS = 5
    DATAFRAME_SAMPLE_ROWS = 5

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

    def _csv_import_table_statement(self):
        """Default CSV import table statement."""
        # all_varchar=True is set to preserve integrity of data by importing as strings.
        return f"""CREATE OR REPLACE TABLE {self.duckdb_instance.database_table_name}
                 AS SELECT *
                 FROM read_csv('{self.filepath}',
                                auto_detect=true,
                                delim='{self.delimiter}',
                                header = true,
                                null_padding=true,
                                all_varchar=True)
                """

    def select_from_table(self, sql_statement):
        """Select from duckdb table. This method gives the user an option to
           write a data transformation as a SQL statement. Results returned
           as a Polars dataframe.

        Args:
            sql_statement (str): SQL statement to import data.

        Return:
            Polars dataframe.
        """
        with self.duckdb_instance.set_database_connection as con:
            con.sql(self._csv_import_table_statement())
            return con.query(sql_statement).pl()

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
                'columns': columns
            }

        return attributes

    def get_row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'r', encoding=self.duckdb_instance.DEFAULT_ENCODING) as csv_file:
            return sum(1 for _ in csv_file)

    def get_row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.get_row_count_with_header() - 1

    def get_columns(self):
        """Return the schema of the columns in the CSV file."""
        attributes = self.get_attributes()
        return attributes['columns']

    def get_columns_string(self):
        """Return the first row of the CSV file."""
        with open(self.filepath, 'r', encoding=self.duckdb_instance.DEFAULT_ENCODING) as csv_file:
            return csv_file.readline().strip()

    def get_columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        with open(self.filepath, 'rb') as csv_file:
            return csv_file.readline().strip()

    def get_column_count(self):
        """Return the number of columns in the CSV file."""
        return len(self.get_columns_string().split(','))

    def get_quotechar(self):
        """Return the quote character used in the CSV file."""
        return self.get_attributes()['quotechar']

    def get_escapechar(self):
        """Return the escape character used in the CSV file."""
        return self.get_attributes()['escapechar']

    def get_newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self.get_attributes()['newline_delimiter']

    def to_dicts(self):
        """Converts Dataframe to list of dictionaries."""
        delimiter = self.infer_csv_file_delimiter()
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
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
    
    def write_avro(self):
        """Writes data to an Avro file."""
        self.to_dataframe().write_avro(self.duckdb_instance.AVRO_OUT_FILENAME)

    def write_csv(self):
        """Writes CSV to a file."""
        # self.duckdb_instance.to_csv(self._csv_import_table_statement())
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(),
                                           self.duckdb_instance.export_to_csv_statement()
                                           )

    def write_json(self):
        """Writes JSON to a file."""
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(),
                                           self.duckdb_instance.export_to_json_array_statement()
                                           )

    def write_json_newline_delimited(self):
        """Writes JSON to a file with newline delimited."""
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(),
                                           self.duckdb_instance.export_to_json_new_line_delimited_statement()
                                           )

    def write_parquet(self):
        """Writes data to a Parquet file."""
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(),
                                           self.duckdb_instance.export_to_parquet_statement()
                                           )

    def write_excel(self):
        """Writes data to an Excel file."""
        self.duckdb_instance.write_to_file(self._csv_import_table_statement(),
                                           self.duckdb_instance.export_to_excel_statement()
                                           )
