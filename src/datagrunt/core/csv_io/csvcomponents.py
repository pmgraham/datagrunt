"""Module for CSV components."""

# standard library
import csv
import re
from collections import Counter, OrderedDict
from functools import cached_property

# third party libraries
import polars as pl

# local libraries
from datagrunt.core.file_io import FileProperties


class CSVStringSample:
    """Base class for creating a string sample of a CSV file."""

    SAMPLE_ROWS = 2
    SAMPLE_ROWS_BY_QUALITY = 50_000

    def __init__(self, filepath):
        """Initialize the CSVString object.

        Args:
            filepath (str): The path to the CSV file.
        """
        self.filepath = filepath

    @cached_property
    def csv_string_sample(self):
        """
        Convert a Polars DataFrame to a CSV string.

        Returns:
            str: The CSV string representation of the DataFrame.
        """
        df = pl.read_csv(self.filepath, separator=CSVDelimiter(self.filepath).delimiter, n_rows=self.SAMPLE_ROWS)
        return df.write_csv(file=None)

    @cached_property
    def csv_string_sample_by_quality(self):
        """
        Convert a Polars DataFrame to a CSV string, prioritizing rows with
        the fewest null values.

        Returns:
            str: The CSV string representation of the DataFrame.
        """
        df = pl.read_csv(
            self.filepath,
            separator=CSVDelimiter(self.filepath).delimiter,
            n_rows=self.SAMPLE_ROWS_BY_QUALITY,
        )
        df = df.with_columns(pl.sum_horizontal(pl.all().is_null()).alias("null_count"))
        df = df.sort("null_count")
        df = df.drop("null_count")
        return df.head(self.SAMPLE_ROWS).write_csv(file=None)


class CSVDelimiter:
    """Class to infer and derive the CSV delimiter."""

    DELIMITER_REGEX_PATTERN = r'[^0-9a-zA-Z_ "-]'
    DEFAULT_DELIMITER = ","
    DEFAULT_TAB_DELIMITER = "\t"

    def __init__(self, filepath):
        super().__init__()
        """Initialize the CSVDelimiter class.

        Args:
            filepath (str): The path to the CSV file.
        """
        self.file_properties = FileProperties(filepath)
        self.first_row = CSVRows(filepath).first_row
        self.delimiter = self.infer_csv_file_delimiter()
        self.delimiter_byte_string = self.delimiter.encode()

    def _get_most_common_non_alpha_numeric_character_from_string(self):
        """
        Get the most common non-alpha-numeric character from a given string.

        Returns:
            str: The most common non-alpha-numeric character from the string.
        """
        columns_no_spaces = self.first_row.replace(" ", "")
        regex = re.compile(self.DELIMITER_REGEX_PATTERN)
        counts = Counter(char for char in regex.findall(columns_no_spaces))  # noqa: E501
        most_common = counts.most_common()
        return most_common

    def infer_csv_file_delimiter(self):
        """Infer the delimiter of a CSV file.

        Returns:
            str: The delimiter of the CSV file.
        """
        delimiter_candidates = self._get_most_common_non_alpha_numeric_character_from_string()

        if self.file_properties.is_empty or self.file_properties.is_blank:
            delimiter = self.DEFAULT_DELIMITER
        elif self.file_properties.is_tsv:
            delimiter = self.DEFAULT_TAB_DELIMITER
        elif len(delimiter_candidates) == 0:
            delimiter = " "
        else:
            delimiter = delimiter_candidates[0][0]
        return delimiter


class CSVDialect:
    """Class for inferring the CSV dialect."""

    CSV_SNIFF_SAMPLE_ROWS = 5
    QUOTING_MAP = {0: "no quoting", 1: "quote all", 2: "quote minimal", 3: "quote non-numeric"}

    def __init__(self, filepath):
        """Initialize the CSVDialect object.

        Args:
            filepath (str): The path to the CSV file.
        """
        self.filepath = filepath
        self.dialect = self._get_csv_dialect()

    def _get_csv_dialect(self):
        """Get the CSV dialect from the file.

        Returns:
            csv.Dialect: The CSV dialect inferred from the file.
        """
        if FileProperties(self.filepath).is_empty or FileProperties(self.filepath).is_blank:
            return None
        with open(self.filepath, "r", encoding=FileProperties(self.filepath).DEFAULT_ENCODING) as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(self.CSV_SNIFF_SAMPLE_ROWS))  # noqa: E501
            csvfile.seek(0)  # Reset file pointer to the beginning
        return dialect

    @cached_property
    def quotechar(self):
        """The character used to quote fields in the CSV file."""
        return self.dialect.quotechar if self.dialect else '"'

    @cached_property
    def escapechar(self):
        """The character used to escape characters in the CSV file."""
        return self.dialect.escapechar if self.dialect else None

    @cached_property
    def doublequote(self):
        """
        Whether double quotes are used to escape quotes in the CSV file.
        """
        return self.dialect.doublequote if self.dialect else False

    @cached_property
    def newline_delimiter(self):
        """The newline delimiter used in the CSV file."""
        return self.dialect.lineterminator if self.dialect else "\r\n"

    @cached_property
    def skipinitialspace(self):
        """
        Whether spaces are skipped at the beginning of fields in the CSV file.
        """
        return self.dialect.skipinitialspace if self.dialect else False

    @cached_property
    def quoting(self):
        """The quoting style used in the CSV file."""
        return self.QUOTING_MAP.get(self.dialect.quoting) if self.dialect else "quote minimal"


class CSVRows:
    """Class for parsing CSV rows."""

    def __init__(self, filepath):
        """Initialize the CSVRows object.

        Args:
            filepath: The path to the CSV file.
        """
        self.filepath = filepath
        self.first_row = self._get_first_row_from_file()

    def _get_first_row_from_file(self):
        """Reads and returns the first line of a file.

        Returns:
            The first line of the file, stripped of leading/trailing
            whitespace, or None if the file is empty.
        """
        with open(self.filepath, "r", encoding=FileProperties(self.filepath).DEFAULT_ENCODING) as csv_file:  # noqa: E501
            first_line = csv_file.readline().strip()
        return first_line

    @cached_property
    def row_count_with_header(self):
        """Return the number of lines in the CSV file including the header."""
        with open(self.filepath, "rb") as csv_file:
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
        self.columns = self._get_columns()

    def _get_columns(self):
        """Return the columns."""
        if FileProperties(self.filepath).is_empty or FileProperties(self.filepath).is_blank:
            return []
        df = pl.read_csv(
            self.filepath,
            separator=CSVDelimiter(self.filepath).delimiter,
            truncate_ragged_lines=True,
            infer_schema=False,
            n_rows=5,
        )
        return df.columns

    @cached_property
    def columns_string(self):
        """Return a string representation of the columns."""
        return ", ".join(self.columns)

    @cached_property
    def columns_byte_string(self):
        """Return a byte string representation of the columns."""
        return ", ".join(self.columns).encode()

    @cached_property
    def columns_count(self):
        """Return the number of columns."""
        return len(self.columns)


class CSVColumnNameNormalizer:
    """Class to normalize CSV columns names."""

    SPECIAL_CHARS_PATTERN = re.compile(r"[^a-z0-9]+")
    MULTI_UNDERSCORE_PATTERN = re.compile(r"_+")

    def __init__(self, filepath):
        """Initialize the CSVColumnNameNormalizer with a filepath."""
        self.filepath = filepath
        self.columns_normalized = self._normalize_column_names(CSVColumns(filepath).columns)

    def _normalize_single_column_name(self, column_name):
        """
        Normalize a single column name by converting to lowercase, replacing
        spaces and special characters with underscores, and removing extra
        underscores.

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
        name = self.SPECIAL_CHARS_PATTERN.sub("_", name)
        name = name.strip("_")
        name = self.MULTI_UNDERSCORE_PATTERN.sub("_", name)
        return f"_{name}" if name and name[0].isdigit() else name

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

    def _normalize_column_names(self, columns):
        """
        Normalize column names by converting to lowercase, replacing spaces
        and special characters with underscores, and removing extra
        underscores.

        Args:
            columns (list): List of column names to normalize

        Returns:
            list: List of normalized column names
        """
        normalized_columns = [self._normalize_single_column_name(col) for col in columns]
        return self._make_unique_column_names(normalized_columns)

    @cached_property
    def columns_normalized_string(self):
        """Return a list representation of the normalized columns."""
        return ", ".join(self.columns_normalized)

    @cached_property
    def columns_normalized_byte_string(self):
        """Return a byte string representation of the normalized columns."""
        return ", ".join(self.columns_normalized).encode()

    @cached_property
    def columns_to_normalized_mapping(self):
        """
        Return the mapping of original column names to normalized column names.
        """
        return dict(OrderedDict(zip(CSVColumns(self.filepath).columns, self.columns_normalized)))


class CSVComponents(FileProperties):
    """A class that combines all CSV components into a single interface."""

    def __init__(self, filepath):
        """Initialize the CSVComponents object.

        Args:
            filepath (str): Path to the CSV file.
        """
        super().__init__(filepath)
        self._delimiter = CSVDelimiter(filepath)
        self._dialect = CSVDialect(filepath)
        self._rows = CSVRows(filepath)
        self._columns = CSVColumns(filepath)
        self._normalizer = CSVColumnNameNormalizer(filepath)
        self._sample = CSVStringSample(filepath)
        self.delimiter = self._delimiter.delimiter

    @cached_property
    def quotechar(self):
        """Return the quote character used in the CSV file."""
        return self._dialect.quotechar

    @cached_property
    def escapechar(self):
        """Return the escape character used in the CSV file."""
        return self._dialect.escapechar

    @cached_property
    def doublequote(self):
        """Return the double quote character used in the CSV file."""
        return self._dialect.doublequote

    @cached_property
    def newline_delimiter(self):
        """Return the newline delimiter used in the CSV file."""
        return self._dialect.newline_delimiter

    @cached_property
    def skipinitialspace(self):
        """Return the skipinitialspace flag used in the CSV file."""
        return self._dialect.skipinitialspace

    @cached_property
    def quoting(self):
        """Return the quoting flag used in the CSV file."""
        return self._dialect.quoting

    @cached_property
    def row_count_with_header(self):
        """
        Return the number of rows in the CSV file including the header row.
        """
        return self._rows.row_count_with_header

    @cached_property
    def row_count_without_header(self):
        """
        Return the number of rows in the CSV file excluding the header row.
        """
        return self._rows.row_count_without_header

    @cached_property
    def columns(self):
        """Return the columns of the CSV file."""
        return self._columns.columns

    @cached_property
    def columns_string(self):
        """Return the columns of the CSV file as a string."""
        return self._columns.columns_string

    @cached_property
    def columns_byte_string(self):
        """Return the columns of the CSV file as a byte string."""
        return self._columns.columns_byte_string

    @cached_property
    def columns_count(self):
        """Count the columns of the CSV file."""
        return self._columns.columns_count

    @cached_property
    def columns_normalized(self):
        """Normalize the columns of the CSV file."""
        return self._normalizer.columns_normalized

    @cached_property
    def columns_normalized_string(self):
        """Normalize the columns of the CSV file."""
        return self._normalizer.columns_normalized_string

    @cached_property
    def columns_normalized_byte_string(self):
        """Normalize the columns of the CSV file."""
        return self._normalizer.columns_normalized_byte_string

    @cached_property
    def columns_to_normalized_mapping(self):
        """Normalize the columns of the CSV file."""
        return self._normalizer.columns_to_normalized_mapping

    @cached_property
    def csv_string_sample(self):
        """Return a sample of the CSV file as a string."""
        return self._sample.csv_string_sample

    @cached_property
    def csv_string_sample_by_quality(self):
        """
        Return a sample of the CSV file as a string, prioritizing rows with
        the fewest null values.
        """
        return self._sample.csv_string_sample_by_quality
