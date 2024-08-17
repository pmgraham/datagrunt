"""Module for file wrangling tasks. Uses duckdb for file processing
   and Polars for dataframe objects.
"""

# standard library
import csv
import json
import os
from pathlib import Path

# third party libraries

# local libraries
from . databases import DuckDBDatabase

class FileBase:
    """Base class for file objects."""

    def __init__(self, filepath):
        """
        Initialize the FileBase class.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath
        self.filename = Path(filepath).name
        self.extension = Path(filepath).suffix
        self.extension_string = self.extension.replace('.', '')
        self.size_in_bytes = os.path.getsize(filepath)
        self.size_in_kb = self.size_in_bytes / 1024
        self.size_in_mb = self.size_in_kb / 1024
        self.size_in_gb = self.size_in_mb / 1024
        self.size_in_tb = self.size_in_gb / 1024

class FileEvaluator(FileBase):
    """Class to evaluate file types and to instantiate the right class."""

    EXCEL_FILE_EXTENSIONS = [
        'xlsx',
        'xlsm',
        'xlsb',
        'xltx',
        'xltm',
        'xls',
        'xlt',
        'xls'
    ]

    CSV_FILE_EXTENSIONS = ['csv', 'tsv', 'txt']
    APACHE_FILE_EXTENSIONS = ['parquet', 'avro']
    STRUCTURED_FILE_EXTENSIONS = list(set(CSV_FILE_EXTENSIONS + EXCEL_FILE_EXTENSIONS +
                                          APACHE_FILE_EXTENSIONS))
    STRUCTURED_FILE_EXTENSIONS.sort()
    SEMI_STRUCTURED_FILE_EXTENSIONS = ['json']
    UNSTRUCTURED_FILE_EXTENSIONS = ['pdf']
    STANDARD_FILE_EXTENSIONS = ['csv', 'tsv', 'txt', 'json']
    PROPRIETARY_FILE_EXTENSIONS = EXCEL_FILE_EXTENSIONS
    JSON_OUT_FILENAME = 'output.json'
    JSON_NEWLINE_OUT_FILENAME = 'output.jsonl'

    def __init__(self, filepath):
        """
        Initialize the FileEvaluator class.

        Args:
            filepath (str): Path to the file.
        """
        super().__init__(filepath)

    @property
    def is_structured(self):
        """Check if the file is structured."""
        return self.extension_string.lower() in self.STRUCTURED_FILE_EXTENSIONS

    @property
    def is_semi_structured(self):
        """Check if the file is semi-structured."""
        return self.extension_string.lower() in self.SEMI_STRUCTURED_FILE_EXTENSIONS

    @property
    def is_unstructured(self):
        """Check if the file is unstructured."""
        match self.extension_string.lower():
            case extension if extension in self.UNSTRUCTURED_FILE_EXTENSIONS:
                return True
            case extension if extension not in self.STRUCTURED_FILE_EXTENSIONS:
                return True
            case extension if extension not in self.SEMI_STRUCTURED_FILE_EXTENSIONS:
                return True
            case _:
                return True

    @property
    def is_standard(self):
        """Check if the file is standard."""
        return self.extension_string.lower() in self.STANDARD_FILE_EXTENSIONS

    @property
    def is_proprietary(self):
        """Check if the file is proprietary."""
        return self.extension_string.lower() in self.PROPRIETARY_FILE_EXTENSIONS

    @property
    def is_csv(self):
        """Check if the file is a CSV file."""
        return self.extension_string.lower() in self.CSV_FILE_EXTENSIONS

    @property
    def is_excel(self):
        """Check if the file is an Excel file."""
        return self.extension_string.lower() in self.EXCEL_FILE_EXTENSIONS

    @property
    def is_apache(self):
        """Check if the file is an Excel file."""
        return self.extension_string.lower() in self.APACHE_FILE_EXTENSIONS

    @property
    def is_empty(self):
        """Check if the file is empty."""
        return self.size_in_bytes == 0

    @property
    def is_large(self):
        """Check if the file is large."""
        return self.size_in_gb >= 1

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
        with open(self.filepath, 'r', encoding=self.duckdb_instance.DEFAULT_ENCODING) as file:
            return sum(1 for _ in file)

    def row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.row_count_with_header() - 1

    def columns_schema(self):
        """Return the schema of the columns in the CSV file."""
        attributes = self.attributes()
        return attributes['Columns']

    def columns_string(self):
        """Return the first row of the CSV file."""
        with open(self.filepath, 'r', encoding=self.duckdb_instance.DEFAULT_ENCODING) as file:
            return file.readline().strip()

    def columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        with open(self.filepath, 'rb') as file:
            return file.readline().strip()

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
    
    def to_json_new_line_delimited(self):
        """Converts CSV to a JSON string with new line delimited."""
        return self.to_dataframe().write_ndjson()

    def write_json(self):
        """Writes JSON to a file."""
        self.duckdb_instance.to_json(self.csv_import_table_statement())

    def write_json_new_line_delimited(self):
        """Writes JSON to a file with new line delimited."""
        self.duckdb_instance.to_json_new_line_delimited(self.csv_import_table_statement())

    def write_parquet(self):
        """Writes data to a Parquet file."""
        self.duckdb_instance.to_parquet(self.csv_import_table_statement())

    def write_excel(self):
        """Writes data to an Excel file."""
        self.duckdb_instance.to_excel(self.csv_import_table_statement())
