"""Module for file wrangling tasks."""

# standard library
import os
from pathlib import Path

# third party libraries

# local libraries

class FileBase:
    """Base class for file objects."""

    EXCEL_FILE_TYPES = [
        'xlsx',
        'xlsm',
        'xlsb',
        'xltx',
        'xltm',
        'xls',
        'xlt',
        'xls'
    ]

    CSV_FILE_TYPES = ['csv', 'tsv', 'txt']

    csv_string = ', '.join(CSV_FILE_TYPES)
    excel_string = ', '.join(EXCEL_FILE_TYPES)

    STRUCTURED_FILE_EXTENSIONS = [csv_string, excel_string]
    SEMI_STRUCTURED_FILE_EXTENSIONS = ['json', 'xml', 'yaml', 'yml']
    UNSTRUCTURED_FILE_EXTENSIONS = ['pdf']
    STANDARD_FILE_EXTENSIONS = ['csv', 'tsv', 'txt', 'json', 'xml', 'yaml', 'yml', 'pdf']
    PROPRIETARY_FILE_EXTENSIONS = EXCEL_FILE_TYPES

    def __init__(self, filepath):
        """
        Initialize the FileBase class.

        Args:
            file_path (str): Path to the file.
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

    @property
    def is_structured(self):
        """Check if the file is unstructured."""
        return self.extension_string.lower() in self.STRUCTURED_FILE_EXTENSIONS

    @property
    def is_semi_structured(self):
        """Check if the file is semi-structured."""
        return self.extension_string.lower() in self.SEMI_STRUCTURED_FILE

    @property
    def is_unstructured(self):
        """Check if the file is unstructured."""
        return self.extension_string.lower() in self.UNSTRUCTURED_FILE_EXTENSIONS

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
        return self.extension_string.lower() in self.CSV_FILE_TYPES

    @property
    def is_excel(self):
        """Check if the file is an Excel file."""
        return self.extension_string.lower() in self.EXCEL_FILE_TYPES

    @property
    def is_empty(self):
        """Check if the file is empty."""
        return self.size_in_bytes == 0

    @property
    def is_large(self):
        """Check if the file is large."""
        return self.size_in_gb >= 1
