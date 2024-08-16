"""Module for file wrangling tasks."""

# standard library
import csv
import json
from io import StringIO
import os
from pathlib import Path

# third party libraries
import duckdb
import polars as pl

# local libraries

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
    STRUCTURED_FILE_EXTENSIONS = list(set(CSV_FILE_EXTENSIONS + EXCEL_FILE_EXTENSIONS + APACHE_FILE_EXTENSIONS))
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
            file_path (str): Path to the file.
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
    DEFAULT_DUCK_DB_THREAD_COUNT = 8
    DEFAULT_DUCK_DB_CSV_IMPORT_TABLE_NAME = 'csv_import_table'
    DEFAULT_DUCK_DB_FILE = 'file.db'

    def __init__(self, filepath):
        """
        Initialize the CSVFile class.

        Args:
            file_path (str): Path to the file to read.
        """
        super().__init__(filepath)

    def _establish_duckdb_database_connection(self, threads=None):
        """Establish a connection with duckdb."""
        threads = self.DEFAULT_DUCK_DB_THREAD_COUNT
        return duckdb.connect(self.DEFAULT_DUCK_DB_FILE, config = {'threads': threads})

    def _duckdb_default_csv_import_table_statement(self):
        """Default CSV import table statement."""
        return f"CREATE OR REPLACE TABLE {self.DEFAULT_DUCK_DB_CSV_IMPORT_TABLE_NAME} AS SELECT * FROM read_csv('{self.filepath}', all_varchar=True)";

    def _duckdb_select_from_default_csv_import_table_statement(self):
        """Select from default CSV import table statement."""
        return f"SELECT * FROM {self.DEFAULT_DUCK_DB_CSV_IMPORT_TABLE_NAME}";

    def attributes(self):
        """Return the attributes of the CSV file."""
        with self._establish_duckdb_database_connection() as con:
            df = con.sql(f"FROM sniff_csv('{self.filepath}')").pl()
            return df.to_dicts()[0]

    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as file:
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
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as file:
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

    def to_dataframe(self):
        """Return the CSV file as a DataFrame."""
        with self._establish_duckdb_database_connection() as con:
            con.sql(self._duckdb_default_csv_import_table_statement());
            return con.query(self._duckdb_select_from_default_csv_import_table_statement()).pl()

    def to_dicts(self):
        """Converts Dataframe to list of dictionaries."""
        with open(self.filepath, 'r', encoding=self.DEFAULT_ENCODING) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=self.delimiter())
            return [row for row in csv_reader]

    def to_json(self, write_json_file=False):
        """Converts CSV to JSON."""
        if not write_json_file:
            return self.to_dataframe().write_json()
        else: 
            return self.to_dataframe().write_json(self.JSON_OUT_FILENAME)

    def to_json_new_line_delimited(self, write_json_file=False):
        """Converts CSV to new line delimited JSON."""
        if not write_json_file:
            return self.to_dataframe().write_ndjson()
        else: 
            return self.to_dataframe().write_ndjson(self.JSON_NEWLINE_OUT_FILENAME)