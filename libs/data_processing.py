"""Module for data processing from files. Uses duckdb for file processing
   and Polars for dataframe objects.
"""

# standard library
import csv
import json

# third party libraries

# local libraries
from . databases import DuckDBDatabase
from . file_utils import FileEvaluator

class CSVFile(FileEvaluator):

    DEFAULT_ENCODING = 'utf-8'

    def __init__(self, filepath):
        """
        Initialize the CSVFile class.

        Args:
            filepath (str): Path to the file to read.
        """
        super().__init__(filepath)
        self.duckdb_instance = DuckDBDatabase(self.filepath)

    def csv_import_table_statement(self):
        """Default CSV import table statement."""
        return f"""CREATE OR REPLACE TABLE {self.duckdb_instance.database_table_name}
                 AS SELECT * FROM read_csv('{self.filepath}', all_varchar=True)""" # preserve integrity of data by importing as strings

    def attributes(self):
        """Return the attributes of the CSV file."""
        with self.duckdb_instance.database_connection as con:
            df = con.sql(f"FROM sniff_csv('{self.filepath}')").pl()
            return df.to_dicts()[0]

    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'r', encoding=self.duckdb_instance.DEFAULT_ENCODING) as csv_file:
            return sum(1 for _ in csv_file)

    def row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.row_count_with_header() - 1

    def columns_schema(self):
        """Return the schema of the columns in the CSV file."""
        attributes = self.attributes()
        return attributes['Columns']

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

    def delimiter(self):
        """Return the delimiter used in the CSV file."""
        attributes = self.attributes()
        return attributes['Delimiter']

    def quotechar(self):
        """Return the quote character used in the CSV file."""
        attributes = self.attributes()
        return attributes['Quote']

    def escapechar(self):
        """Return the escape character used in the CSV file."""
        attributes = self.attributes()
        return attributes['Escape']

    def newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        attributes = self.attributes()
        return attributes['NewLineDelimiter']

    def has_header(self):
        """Return True if the CSV file has a header, False otherwise."""
        attributes = self.attributes()
        return attributes['HasHeader']

    def to_dicts(self):
        """Converts Dataframe to list of dictionaries."""
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=self.delimiter())
            return list(csv_reader)

    def to_dataframe(self):
        """Converts CSV to a Polars dataframe."""
        return self.duckdb_instance.to_dataframe(self.csv_import_table_statement())

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
        self.duckdb_instance.to_json(self.csv_import_table_statement())

    def write_json_newline_delimited(self):
        """Writes JSON to a file with newline delimited."""
        self.duckdb_instance.to_json_new_line_delimited(self.csv_import_table_statement())

    def write_parquet(self):
        """Writes data to a Parquet file."""
        self.duckdb_instance.to_parquet(self.csv_import_table_statement())

    def write_excel(self):
        """Writes data to an Excel file."""
        self.duckdb_instance.to_excel(self.csv_import_table_statement())
