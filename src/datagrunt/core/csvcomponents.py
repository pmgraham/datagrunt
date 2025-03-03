# standard library
from collections import Counter, OrderedDict
import csv
from functools import lru_cache
import re

# third party libraries

# local libraries
from src.datagrunt.core.fileproperties import FileProperties

class CSVDelimiter:
    """Class to infer and derive the CSV delimiter."""

    DELIMITER_REGEX_PATTERN = r'[^0-9a-zA-Z_ "-]'
    DEFAULT_DELIMITER = ','

    def __init__(self, filepath):
        self.file_properties = FileProperties(filepath)
        self.first_row = CSVRows(filepath).first_row
        self.delimiter = self.infer_csv_file_delimiter()
        self.delimiter_byte_string = self.delimiter.encode()

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

    def infer_csv_file_delimiter(self):
        """Infer the delimiter of a CSV file.

        Args:
            csv_file (str): The path to the CSV file.

        Returns:
            str: The delimiter of the CSV file.
        """
        delimiter_candidates = self._get_most_common_non_alpha_numeric_character_from_string()

        if self.file_properties.is_empty or self.file_properties.is_blank:
            delimiter = self.DEFAULT_DELIMITER
        elif len(delimiter_candidates) == 0:
            delimiter = ' '
        else:
            delimiter = delimiter_candidates[0][0]
        return delimiter

class CSVColumnNameNormalizer:
    """Class to normalize CSV columns names."""

    SPECIAL_CHARS_PATTERN = re.compile(r'[^a-z0-9]+')
    MULTI_UNDERSCORE_PATTERN = re.compile(r'_+')

    def __init__(self, filename):
        """Initialize the CSVColumnNameNormalizer with a filename."""
        self.filename = filename
        self.columns_normalized = self.normalize_column_names(self._columns())

    def _columns(self):
        """Return the list of columns from the CSV file."""
        return CSVColumns(self.filename).columns

    def _normalize_single_column_name(self, column_name):
        """Normalize a single column name by converting to lowercase, replacing spaces and special
        characters with underscores, and removing extra underscores.

        Replace special characters and spaces with underscore
        Remove leading and trailing underscores
        Replace multiple underscores with single underscore
        Add a leading underscore if the name starts with a digit

        Args:
            column_name (str): The column name to normalize

        Returns:
            str: The normalized column name
        """
        name = column_name.lower()
        name = self.SPECIAL_CHARS_PATTERN.sub('_', name)
        name = name.strip('_')
        name = self.MULTI_UNDERSCORE_PATTERN.sub('_', name)
        return f'_{name}' if name and name[0].isdigit() else name

    def _make_unique_column_names(self, columns_list):
        """
        Make unique column names by appending a number to duplicate names.

        Args:
            columns (list): List of column names to make unique

        Returns:
            list: List of unique column names
        """
        name_count = {}
        unique_names = []

        for name in columns_list:
            if name in name_count:
                name_count[name] += 1
                unique_names.append(f"{name}_{name_count[name]}")
            else:
                name_count[name] = 0
                unique_names.append(name)

        return unique_names

    def normalize_column_names(self, columns):
        """
        Normalize column names by converting to lowercase, replacing spaces and special
        characters with underscores, and removing extra underscores.

        Args:
            columns (list): List of column names to normalize

        Returns:
            list: List of normalized column names
        """
        normalized_columns = [self._normalize_single_column_name(col) for col in columns]
        return self._make_unique_column_names(normalized_columns)

class CSVDialect:
    """Class for inferring the CSV dialect."""

    CSV_SNIFF_SAMPLE_ROWS = 5
    QUOTING_MAP = {
        0: 'no quoting',
        1: 'quote all',
        2: 'quote minimal',
        3: 'quote non-numeric'
    }

    def __init__(self, filepath):
        self.filepath = filepath
        self.dialect = self._get_csv_dialect()

    def _get_csv_dialect(self):
        """Get the CSV dialect from the file.

        Returns:
            csv.Dialect: The CSV dialect inferred from the file.
        """
        with open(self.filepath, 'r', encoding=FileProperties(self.filepath).DEFAULT_ENCODING) as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(self.CSV_SNIFF_SAMPLE_ROWS))
            csvfile.seek(0)  # Reset file pointer to the beginning
        return dialect

    @property
    def quotechar(self):
        return self.dialect.quotechar

    @property
    def escapechar(self):
        return self.dialect.escapechar

    @property
    def doublequote(self):
        return self.dialect.doublequote

    @property
    def newline_delimiter(self):
        return self.dialect.lineterminator

    @property
    def skipinitialspace(self):
        return self.dialect.skipinitialspace

    @property
    def quoting(self):
        return self.QUOTING_MAP.get(self.dialect.quoting)

class CSVRows:
    """Class for parsing CSV rows."""

    def __init__(self, filepath):
        self.filepath = filepath
        self.first_row = self._get_first_row_from_file()

    def _get_first_row_from_file(self):
        """Reads and returns the first line of a file.

        Args:
            filename: The path to the file.

        Returns:
            The first line of the file, stripped of leading/trailing whitespace,
            or None if the file is empty.
        """
        with open(self.filepath, 'r', encoding=FileProperties(self.filepath).DEFAULT_ENCODING) as csv_file:
            first_line = csv_file.readline().strip()
        return first_line

    @property
    @lru_cache()
    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, 'rb', encoding=FileProperties(self.filepath).DEFAULT_ENCODING) as csv_file:
            return sum(1 for _ in csv_file)

    @property
    def row_count_without_header(self):
        """Return the number of lines in the CSV file excluding the header."""
        return self.row_count_with_header - 1

class CSVColumns:
    """Class for parsing CSV columns."""

    def __init__(self, filepath):
        """Initialize the CSVColumns class."""
        self.filepath = filepath
        self.columns = CSVRows(filepath).first_row.split(CSVDelimiter(filepath).delimiter)
        self.columns_normalized = CSVColumnNameNormalizer(filepath).columns_normalized

    @property
    def columns_string(self):
        """Return a string representation of the columns."""
        return ', '.join(self.columns)

    @property
    def columns_normalized_string(self):
        """Return a list representation of the normalized columns."""
        return ', '.join(self.columns_normalized)

    @property
    def columns_byte_string(self):
        """Return a byte string representation of the columns."""
        return ', '.join(self.columns).encode()

    @property
    def columns_normalized_byte_string(self):
        """Return a byte string representation of the normalized columns."""
        return ', '.join(self.columns_normalized).encode()

    @property
    def columns_to_normalized_mapping(self):
        """Return the mapping of original column names to normalized column names."""
        return dict(OrderedDict(zip(self.columns, self.columns_normalized)))

    @property
    def columns_count(self):
        """Return the number of columns."""
        return len(self.columns)
