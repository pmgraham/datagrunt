"""Module for deriving and evaluating file properties."""

# standard library
import os
from pathlib import Path

# third party libraries

# local libraries

class FileBase:
    """Base class for file objects."""

    FILE_SIZE_DIVISOR = 1024

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
        self.size_in_kb = self.size_in_bytes / self.FILE_SIZE_DIVISOR
        self.size_in_mb = self.size_in_kb / self.FILE_SIZE_DIVISOR
        self.size_in_gb = self.size_in_mb / self.FILE_SIZE_DIVISOR
        self.size_in_tb = self.size_in_gb / self.FILE_SIZE_DIVISOR

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
