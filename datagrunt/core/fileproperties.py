"""Module for deriving and evaluating file properties."""

# standard library
import os
from pathlib import Path

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
    TABULAR_FILES = ['csv', 'tsv']
    APACHE_FILE_EXTENSIONS = ['parquet', 'avro']
    STRUCTURED_FILE_EXTENSIONS = list(set(CSV_FILE_EXTENSIONS +
                                          EXCEL_FILE_EXTENSIONS +
                                          TABULAR_FILES +
                                          APACHE_FILE_EXTENSIONS)
                                          )
    STRUCTURED_FILE_EXTENSIONS.sort()
    SEMI_STRUCTURED_FILE_EXTENSIONS = ['json']
    UNSTRUCTURED_FILE_EXTENSIONS = ['pdf']
    STANDARD_FILE_EXTENSIONS = ['csv', 'tsv', 'txt', 'json', 'jsonl', 'parquet', 'avro']
    PROPRIETARY_FILE_EXTENSIONS = EXCEL_FILE_EXTENSIONS
    JSON_OUT_FILENAME = 'output.json'
    JSON_NEWLINE_OUT_FILENAME = 'output.jsonl'
    EXCEL_ROW_LIMIT = 1_048_576

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
