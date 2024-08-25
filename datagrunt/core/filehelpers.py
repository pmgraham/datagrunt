"""Module for deriving and evaluating file properties."""

# standard library
from collections import Counter
import os
from pathlib import Path
import re

class FileBase:
    """Base class for file objects."""

    FILE_SIZE_DIVISOR = 1024
    DEFAULT_ENCODING = 'utf-8'

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
    EXCEL_ROW_LIMIT = 1_048_576

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

class CSVParser(FileEvaluator):
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
        self.delimiter = self.infer_csv_file_delimiter()

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
