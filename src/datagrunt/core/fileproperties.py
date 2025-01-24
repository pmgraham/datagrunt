"""Module for deriving and evaluating file properties."""

# standard library
from collections import Counter
import csv
from functools import lru_cache
import os
from pathlib import Path
import re

class FileProperties:
    """Base class for file objects."""

    FILE_SIZE_DIVISOR = 1024
    DEFAULT_ENCODING = 'utf-8'
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

    CSV_FILE_EXTENSIONS = ['csv']
    TAB_SEPARATED_FILES = ['tsv']

    TABULAR_FILES = list(set(CSV_FILE_EXTENSIONS +
                             EXCEL_FILE_EXTENSIONS +
                             TAB_SEPARATED_FILES
                             )
                        )
    TABULAR_FILES.sort()

    APACHE_FILE_EXTENSIONS = ['parquet', 'avro']

    STRUCTURED_FILE_EXTENSIONS = list(set(CSV_FILE_EXTENSIONS +
                                          EXCEL_FILE_EXTENSIONS +
                                          TABULAR_FILES +
                                          TAB_SEPARATED_FILES +
                                          APACHE_FILE_EXTENSIONS
                                          )
                                    )
    STRUCTURED_FILE_EXTENSIONS.sort()

    SEMI_STRUCTURED_FILE_EXTENSIONS = ['json', 'jsonl']

    STANDARD_FILE_EXTENSIONS = list(set(CSV_FILE_EXTENSIONS +
                                        TAB_SEPARATED_FILES +
                                        SEMI_STRUCTURED_FILE_EXTENSIONS +
                                        APACHE_FILE_EXTENSIONS
                                        )
                                    )

    STANDARD_FILE_EXTENSIONS.sort()

    PROPRIETARY_FILE_EXTENSIONS = EXCEL_FILE_EXTENSIONS

    EXCEL_ROW_LIMIT = 1_048_576

    JSON_OUT_FILENAME = 'output.json'
    JSON_NEWLINE_OUT_FILENAME = 'output.jsonl'
    CSV_OUT_FILENAME = 'output.csv'
    EXCEL_OUT_FILENAME = 'output.xlsx'
    PARQUET_OUT_FILENAME = 'output.parquet'
    AVRO_OUT_FILENAME = 'output.avro'

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
        self.size_in_kb = round((self.size_in_bytes / self.FILE_SIZE_DIVISOR), 5)
        self.size_in_mb = round((self.size_in_kb / self.FILE_SIZE_DIVISOR), 5)
        self.size_in_gb = round((self.size_in_mb / self.FILE_SIZE_DIVISOR), 5)
        self.size_in_tb = round((self.size_in_gb / self.FILE_SIZE_DIVISOR), 5)

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
        return self.extension_string.lower() not in self.STRUCTURED_FILE_EXTENSIONS and \
               self.extension_string.lower() not in self.SEMI_STRUCTURED_FILE_EXTENSIONS

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
        """Check if the file is an Apache formatted file."""
        return self.extension_string.lower() in self.APACHE_FILE_EXTENSIONS

    @property
    def is_empty(self):
        """Check if the file is empty. Empty files have a size of 0 bytes."""
        return self.size_in_bytes == 0

    @property
    def is_blank(self):
        """Check if the file is blank. Blank files contain only whitespace."""
        # Read the file as text first
        with open(self.filepath, 'r') as f:
            # Remove whitespace, newlines, and other invisible characters
            content = f.read().strip()
            if not content:  # If file is completely empty
                return True
        return False

    @property
    def is_large(self):
        """Check if the file is greater than or equal to 1 GB."""
        return self.size_in_gb >= 1.0

    @property
    def is_tabular(self):
        """Check if the file is tabular."""
        return self.extension_string.lower() in self.TABULAR_FILES

class CSVProperties(FileProperties):
    """Class for parsing CSV files. Mostly determining the delimiter."""

    DELIMITER_REGEX_PATTERN = r'[^0-9a-zA-Z_ "-]'
    DEFAULT_DELIMITER = ','
    DEFAULT_SAMPLE_ROWS = 1
    CSV_SNIFF_SAMPLE_ROWS = 5
    DATAFRAME_SAMPLE_ROWS = 20

    QUOTING_MAP = {
        0: 'no quoting',
        1: 'quote all',
        2: 'quote minimal',
        3: 'quote non-numeric'
    }

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
            raise ValueError(
                f"File extension '{self.extension_string}' is not a valid CSV file extension."
                             )

    def _return_empty_file_attributes(self):
        """Return an empty file object."""
        return {
            'delimiter': self.DEFAULT_DELIMITER,
            'quotechar': '"',
            'escapechar': None,
            'doublequote': True,
            'newline_delimiter': '\n',
            'skipinitialspace': False,
            'quoting': 'quote all',
            'columns_schema': {},
            'columns_original_format': '',
            'columns_list': [],
            'columns_string': '',
            'columns_byte_string': b'',
            'column_count': 0
        }

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

        if self.is_empty or self.is_blank:
            delimiter = self.DEFAULT_DELIMITER
        elif len(delimiter_candidates) == 0:
            delimiter = ' '
        else:
            delimiter = delimiter_candidates[0][0]
        return delimiter

    def _get_attributes(self):
        """Generate a dictionary of CSV attributes."""
        if self.is_empty or self.is_blank:
            attributes = self._return_empty_file_attributes()
        else:
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
                        'columns_schema': columns,
                        'columns_original_format': self.first_row,
                        'columns_list': columns_list,
                        'columns_string': ", ".join(columns_list),
                        'columns_byte_string': ", ".join(columns_list).encode(),
                        'column_count': len(columns_list)
                    }

        return attributes

    @property
    @lru_cache()
    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'rb') as csv_file:
            return sum(1 for _ in csv_file)

    @property
    def row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.row_count_with_header - 1

    @property
    def columns(self):
        """Return the schema of the columns in the CSV file."""
        return self._get_attributes()['columns_list']

    @property
    def columns_string(self):
        """Return the first row of a CSV file as a string."""
        return self._get_attributes()['columns_string']

    @property
    def columns_byte_string(self):
        """Return the first row of the CSV file as bytes."""
        return self._get_attributes()['columns_byte_string']

    @property
    def column_count(self):
        """Return the number of columns in the CSV file."""
        return self._get_attributes()['column_count']

    @property
    def quotechar(self):
        """Return the quote character used in the CSV file."""
        return self._get_attributes()['quotechar']

    @property
    def escapechar(self):
        """Return the escape character used in the CSV file."""
        return self._get_attributes()['escapechar']

    @property
    def newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self._get_attributes()['newline_delimiter']
