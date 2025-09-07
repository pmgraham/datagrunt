"""Module for deriving and evaluating file properties."""

# standard library
import os
from functools import cached_property
from pathlib import Path


class FileExtensions:
    """Class for getting file extensions."""

    def __init__(self, filepath):
        """Initialize the FileExtensions object.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath

    @cached_property
    def extension(self):
        """Get the file extensions."""
        return Path(self.filepath).suffixes

    @cached_property
    def csv_extensions(self):
        """Define CSV extensions."""
        return ["csv"]

    @cached_property
    def tsv_extensions(self):
        """Define TSV extensions."""
        return ["tsv"]

    @cached_property
    def excel_extensions(self):
        """Define Excel extensions."""
        return ["xlsx", "xlsm", "xlsb", "xltx", "xltm", "xls", "xlt"]

    @cached_property
    def tabular_extensions(self):
        """Define tabular extensions."""
        return list(set(self.csv_extensions + self.tsv_extensions + self.excel_extensions))

    @cached_property
    def apache_extensions(self):
        """Define Apache extensions."""
        return ["parquet", "avro"]

    @cached_property
    def semi_structured_extensions(self):
        """Define semi-structured extensions."""
        return list(set(["json", "jsonl"]))

    @cached_property
    def standard_extensions(self):
        """Define standard (non proprietary) extensions."""
        return list(
            set(self.csv_extensions + self.tsv_extensions + self.apache_extensions + self.semi_structured_extensions)
        )

    @cached_property
    def structured_extensions(self):
        """Define structured extensions."""
        return list(
            set(
                self.csv_extensions
                + self.tsv_extensions
                + self.excel_extensions
                + self.apache_extensions
                + self.tabular_extensions
            )
        )

    @cached_property
    def proprietary_extensions(self):
        """Define proprietary extensions."""
        return list(set(self.excel_extensions))


class FileStatistics:
    """Class for getting file statistics."""

    FILE_SIZE_DIVISOR = 1_000
    EXCEL_ROW_LIMIT = 1_048_576
    FILE_SIZE_ROUND_FACTOR = 5
    LARGE_FILE_FACTOR = 1.0  # size in GB

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

    @cached_property
    def modified_time(self):
        """Get the file modified time."""
        return os.path.getmtime(self.filepath)

    @cached_property
    def is_large(self):
        """Check if the file is at least one gigabyte or larger in size."""
        return self.size_in_gb >= self.LARGE_FILE_FACTOR


class BlankFile:
    """Class for checking if a file is blank."""

    FILE_SIZE_MB_FACTOR = 10.0

    def __init__(self, filepath):
        """Initialize the BlankFile object.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath

    @cached_property
    def is_blank(self):
        """Check if the file is blank. Blank files contain only whitespace."""

        filestats = FileStatistics(self.filepath)

        # Very low probability of being blank if file is 10MB or larger in size
        if filestats.size_in_mb >= self.FILE_SIZE_MB_FACTOR:
            return False
        with open(self.filepath, "r") as f:
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

    @cached_property
    def is_empty(self):
        """Check if the file is empty."""
        return FileStatistics(self.filepath).size_in_bytes == 0


class FileProperties:
    """Base class for file objects."""

    DEFAULT_ENCODING = "utf-8"

    def __init__(self, filepath):
        """
        Initialize the FileBase class.

        Args:
            filepath (str): Path to the file.
        """
        self.filepath = filepath
        self.filename = Path(filepath).name
        self.extension = Path(filepath).suffix
        self.extension_string = self.extension.replace(".", "")

        # Instantiate helper classes
        self._stats = FileStatistics(self.filepath)
        self._ext = FileExtensions(self.filepath)
        self._empty = EmptyFile(self.filepath)
        self._blank = BlankFile(self.filepath)

        self.size_in_bytes = self._stats.size_in_bytes
        self.size_in_kb = self._stats.size_in_kb
        self.size_in_mb = self._stats.size_in_mb
        self.size_in_gb = self._stats.size_in_gb
        self.size_in_tb = self._stats.size_in_tb

    @cached_property
    def is_structured(self):
        """Check if the file is structured."""
        return self.extension_string.lower() in self._ext.structured_extensions  # noqa: E501

    @cached_property
    def is_semi_structured(self):
        """Check if the file is semi-structured."""
        return self.extension_string.lower() in self._ext.semi_structured_extensions  # noqa: E501

    @cached_property
    def is_unstructured(self):
        """Check if the file is unstructured."""
        is_not_standard = self.extension_string.lower() not in self._ext.standard_extensions
        is_not_semi_structured = self.extension_string.lower() not in self._ext.semi_structured_extensions
        return is_not_standard and is_not_semi_structured

    @cached_property
    def is_standard(self):
        """Check if the file is standard."""
        return self.extension_string.lower() in self._ext.standard_extensions

    @cached_property
    def is_proprietary(self):
        """Check if the file is proprietary."""
        return self.extension_string.lower() in self._ext.proprietary_extensions  # noqa: E501

    @cached_property
    def is_csv(self):
        """Check if the file is a CSV file."""
        return self.extension_string.lower() in self._ext.csv_extensions

    @cached_property
    def is_excel(self):
        """Check if the file is an Excel file."""
        return self.extension_string.lower() in self._ext.excel_extensions

    @cached_property
    def is_apache(self):
        """Check if the file is an Apache formatted file."""
        return self.extension_string.lower() in self._ext.apache_extensions

    @cached_property
    def is_empty(self):
        """Check if the file is empty. Empty files have a size of 0 bytes."""
        return self._empty.is_empty

    @cached_property
    def is_blank(self):
        """Check if the file is blank. Blank files contain only whitespace."""
        return self._blank.is_blank

    @cached_property
    def is_large(self):
        """Check if the file is greater than or equal to 1 GB."""
        return self._stats.is_large

    @cached_property
    def is_tabular(self):
        """Check if the file is tabular."""
        return self.extension_string.lower() in self._ext.tabular_extensions

    @cached_property
    def is_tsv(self):
        """Check if the file is tabular."""
        return self.extension_string.lower() in self._ext.tsv_extensions
