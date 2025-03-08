"""Module for deriving and evaluating file properties."""

# standard library
import os
from pathlib import Path

class FileExtensions:
    """Class for getting file extensions."""
    def __init__(self, filepath):
        """Initialize the FileExtensions object.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath

    @property
    def extension(self):
        """Get the file extensions."""
        return Path(self.filepath).suffixes

    @property
    def csv_extensions(self):
        """Define CSV extensions."""
        return ['csv']

    @property
    def tsv_extensions(self):
        """Define TSV extensions."""
        return ['tsv']

    @property
    def excel_extensions(self):
        """Define Excel extensions."""
        return [
            'xlsx',
            'xlsm',
            'xlsb',
            'xltx',
            'xltm',
            'xls',
            'xlt'
        ]

    @property
    def tabular_extensions(self):
        """Define tabular extensions."""
        return list(set(self.csv_extensions + self.tsv_extensions + self.excel_extensions))

    @property
    def apache_extensions(self):
        """Define Apache extensions."""
        return ['parquet', 'avro']

    @property
    def semi_structured_extensions(self):
        """Define semi-structured extensions."""
        return list(set(['json', 'jsonl']))

    @property
    def standard_extensions(self):
        """Define standard (non proprietary) extensions."""
        return list(set(self.csv_extensions +
                        self.tsv_extensions +
                        self.apache_extensions +
                        self.semi_structured_extensions))

    @property
    def structured_extensions(self):
        """Define structured extensions."""
        return list(set(self.csv_extensions +
                        self.tsv_extensions +
                        self.excel_extensions +
                        self.apache_extensions +
                        self.tabular_extensions))

    @property
    def proprietary_extensions(self):
        """Define proprietary extensions."""
        return list(set(self.excel_extensions))

class FileStatistics:
    """Class for getting file statistics."""

    FILE_SIZE_DIVISOR = 1_000
    EXCEL_ROW_LIMIT = 1_048_576
    FILE_SIZE_ROUND_FACTOR = 5
    LARGE_FILE_FACTOR = 1.0 # size in GB

    def __init__(self, filepath):
        """Initialize the FileStatistics object.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath
        self.size_in_bytes = os.path.getsize(self.filepath)
        self.size_in_kb = round((self.size_in_bytes / self.FILE_SIZE_DIVISOR), self.FILE_SIZE_ROUND_FACTOR)
        self.size_in_mb = round((self.size_in_kb / self.FILE_SIZE_DIVISOR), self.FILE_SIZE_ROUND_FACTOR)
        self.size_in_gb = round((self.size_in_mb / self.FILE_SIZE_DIVISOR), self.FILE_SIZE_ROUND_FACTOR)
        self.size_in_tb = round((self.size_in_gb / self.FILE_SIZE_DIVISOR), self.FILE_SIZE_ROUND_FACTOR)

    @property
    def modified_time(self):
        """Get the file modified time."""
        return os.path.getmtime(self.filepath)

    @property
    def is_large(self):
        """Check if the file is at least one gigabyte or larger."""
        return self.size_in_gb >= self.LARGE_FILE_FACTOR

class BlankFile:
    """Class for checking if a file is blank."""
    def __init__(self, filepath):
        """Initialize the BlankFile object.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath

    @property
    def is_blank(self):
        """Check if the file is blank. Blank files contain only whitespace."""
        with open(self.filepath, 'r') as f:
            content = f.read().strip()
            if not content:
                return True
        return False

class EmptyFile:
    """Class for checking if a file is empty."""
    def __init__(self, filepath):
        """Initialize the EmptyFile object.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath

    @property
    def is_empty(self):
        """Check if the file is empty."""
        return FileStatistics(self.filepath).size_in_bytes == 0

class FileProperties:
    """Base class for file objects."""

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
        self.size_in_bytes = FileStatistics(self.filepath).size_in_bytes
        self.size_in_kb = FileStatistics(self.filepath).size_in_kb
        self.size_in_mb = FileStatistics(self.filepath).size_in_mb
        self.size_in_gb = FileStatistics(self.filepath).size_in_gb
        self.size_in_tb = FileStatistics(self.filepath).size_in_tb

    @property
    def is_structured(self):
        """Check if the file is structured."""
        return self.extension_string.lower() in FileExtensions(self.filepath).structured_extensions

    @property
    def is_semi_structured(self):
        """Check if the file is semi-structured."""
        return self.extension_string.lower() in FileExtensions(self.filepath).semi_structured_extensions

    @property
    def is_unstructured(self):
        """Check if the file is unstructured."""
        return self.extension_string.lower() not in FileExtensions(self.filepath).standard_extensions and \
               self.extension_string.lower() not in FileExtensions(self.filepath).semi_structured_extensions

    @property
    def is_standard(self):
        """Check if the file is standard."""
        return self.extension_string.lower() in FileExtensions(self.filepath).standard_extensions

    @property
    def is_proprietary(self):
        """Check if the file is proprietary."""
        return self.extension_string.lower() in FileExtensions(self.filepath).proprietary_extensions

    @property
    def is_csv(self):
        """Check if the file is a CSV file."""
        return self.extension_string.lower() in FileExtensions(self.filepath).csv_extensions

    @property
    def is_excel(self):
        """Check if the file is an Excel file."""
        return self.extension_string.lower() in FileExtensions(self.filepath).excel_extensions

    @property
    def is_apache(self):
        """Check if the file is an Apache formatted file."""
        return self.extension_string.lower() in FileExtensions(self.filepath).apache_extensions

    @property
    def is_empty(self):
        """Check if the file is empty. Empty files have a size of 0 bytes."""
        return EmptyFile(self.filepath).is_empty

    @property
    def is_blank(self):
        """Check if the file is blank. Blank files contain only whitespace."""
        return BlankFile(self.filepath).is_blank

    @property
    def is_large(self):
        """Check if the file is greater than or equal to 1 GB."""
        return FileStatistics(self.filepath).is_large

    @property
    def is_tabular(self):
        """Check if the file is tabular."""
        return self.extension_string.lower() in FileExtensions(self.filepath).tabular_extensions
