"""Module for deriving and evaluating file properties."""

# standard library
import os
from pathlib import Path

class FileProperties:
    """Base class for file objects."""

    FILE_SIZE_DIVISOR = 1000
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
