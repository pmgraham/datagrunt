"""Module for CSV components."""

# standard library
import csv
import re
from collections import Counter, OrderedDict
from functools import cached_property
from pathlib import Path

# third party libraries
import polars as pl

# local libraries
from datagrunt.core.file_io import FileProperties


def _count_leading_comments(filepath):
    count = 0
    with open(filepath, "r", encoding=FileProperties(filepath).DEFAULT_ENCODING) as f:
        for line in f:
            stripped = line.strip()
            if stripped.startswith("#") or not stripped:
                count += 1
            else:
                break
    return count


def _is_legacy_mac_newlines(filepath):
    """Check if the file uses legacy Mac OS carriage returns (\\r) as line endings."""
    try:
        with open(filepath, "rb") as f:
            chunk = f.read(4096)
        return b"\r" in chunk and b"\n" not in chunk
    except Exception:
        return False


def _check_csv_ragged_and_warn(filepath, delimiter):
    """Check if the CSV is ragged and issue a warning if lenient loading is enabled."""
    import csv
    import warnings

    try:
        is_legacy_mac = _is_legacy_mac_newlines(filepath)
        newline_param = None if is_legacy_mac else ""
        encoding = FileProperties(filepath).DEFAULT_ENCODING
        with open(filepath, "r", encoding=encoding, newline=newline_param, errors="ignore") as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = None
            for row in reader:
                if row and not row[0].startswith("#"):
                    header = row
                    break
            if not header:
                return False

            expected_cols = len(header)
            row_count = 0
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                row_count += 1
                if len(row) != expected_cols:
                    warnings.warn(
                        f"CSV file contains ragged rows. Row {row_count} has {len(row)} columns, "
                        f"expected {expected_cols}. Some fields will be truncated or padded with nulls.",
                        UserWarning,
                    )
                    return True
                if row_count >= 10000:
                    break
    except Exception:
        pass
    return False


class CSVStringSample:
    """Base class for creating a string sample of a CSV file."""

    SAMPLE_ROWS = 2
    SAMPLE_ROWS_BY_QUALITY = 50_000

    def __init__(self, filepath, delimiter=None):
        """Initialize the CSVString object.

        Args:
            filepath (str or Path): The path to the CSV file.
            delimiter (str, optional): The delimiter of the CSV file.
        """
        self.filepath = Path(filepath)
        self._delimiter = delimiter

    @cached_property
    def delimiter(self):
        """Get the delimiter."""
        if self._delimiter is not None:
            return self._delimiter
        return CSVDelimiter(self.filepath).delimiter

    @cached_property
    def csv_string_sample(self):
        """
        Convert a Polars DataFrame to a CSV string.

        Returns:
            str: The CSV string representation of the DataFrame.
        """
        if _is_legacy_mac_newlines(self.filepath):
            try:
                lines = []
                encoding = FileProperties(self.filepath).DEFAULT_ENCODING
                with open(self.filepath, "r", encoding=encoding, newline=None) as f:
                    for line in f:
                        stripped = line.strip()
                        if not stripped.startswith("#") and stripped:
                            lines.append(line)
                            if len(lines) >= self.SAMPLE_ROWS + 1:
                                break
                return "".join(lines)
            except Exception:
                pass
        df = pl.read_csv(self.filepath, separator=self.delimiter, n_rows=self.SAMPLE_ROWS)
        return df.write_csv(file=None)

    @cached_property
    def csv_string_sample_by_quality(self):
        """
        Convert a Polars DataFrame to a CSV string, prioritizing rows with
        the fewest null values.

        Returns:
            str: The CSV string representation of the DataFrame.
        """
        if _is_legacy_mac_newlines(self.filepath):
            return self.csv_string_sample
        df = pl.read_csv(
            self.filepath,
            separator=self.delimiter,
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

    def __init__(self, filepath, first_row=None):
        """Initialize the CSVDelimiter class.

        Args:
            filepath (str or Path): The path to the CSV file.
            first_row (str, optional): The first row of the CSV file.
        """
        filepath = Path(filepath)
        self.file_properties = FileProperties(filepath)
        if first_row is not None:
            self.first_row = first_row
        else:
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

        if self.file_properties.is_tsv:
            delimiter = self.DEFAULT_TAB_DELIMITER
        elif self.file_properties.is_empty or self.file_properties.is_blank:
            delimiter = self.DEFAULT_DELIMITER
        elif len(delimiter_candidates) == 0:
            delimiter = " "
        else:
            delimiter = delimiter_candidates[0][0]
        return delimiter


class CSVDialect:
    """Class for inferring the CSV dialect."""

    CSV_SNIFF_SAMPLE_ROWS = 5
    QUOTING_MAP = {0: "no quoting", 1: "quote all", 2: "quote minimal", 3: "quote non-numeric"}

    def __init__(self, filepath, delimiter=None, is_empty=None, is_blank=None):
        """Initialize the CSVDialect object.

        Args:
            filepath (str or Path): The path to the CSV file.
            delimiter (str, optional): The delimiter of the CSV file.
            is_empty (bool, optional): Whether the file is empty.
            is_blank (bool, optional): Whether the file is blank.
        """
        self.filepath = Path(filepath)
        self._delimiter = delimiter
        self._is_empty = is_empty
        self._is_blank = is_blank
        self.dialect = self._get_csv_dialect()

    def _get_csv_dialect(self):
        """Get the CSV dialect from the file.

        Returns:
            csv.Dialect: The CSV dialect inferred from the file.
        """
        is_empty = self._is_empty if self._is_empty is not None else FileProperties(self.filepath).is_empty
        is_blank = self._is_blank if self._is_blank is not None else FileProperties(self.filepath).is_blank
        if is_empty or is_blank:
            return None
        with open(self.filepath, "r", encoding=FileProperties(self.filepath).DEFAULT_ENCODING) as csvfile:
            # Read exactly CSV_SNIFF_SAMPLE_ROWS lines to avoid diluting sniff results
            lines = []
            for line in csvfile:
                if line.strip().startswith("#"):
                    continue
                lines.append(line)
                if len(lines) >= self.CSV_SNIFF_SAMPLE_ROWS:
                    break
            sample = "".join(lines)
        try:
            if self._delimiter:
                dialect = csv.Sniffer().sniff(sample, delimiters=self._delimiter)
            else:
                dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = None
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
            filepath (str or Path): The path to the CSV file.
        """
        self.filepath = Path(filepath)

    @cached_property
    def first_row(self):
        """Reads and returns the first line of a file.

        Returns:
            The first line of the file, stripped of leading/trailing
            whitespace, or None if the file is empty.
        """
        return self._get_first_row_from_file()

    def _get_first_row_from_file(self):
        """Reads and returns the first line of a file.

        Returns:
            The first line of the file, stripped of leading/trailing
            whitespace, or None if the file is empty.
        """
        with open(self.filepath, "r", encoding=FileProperties(self.filepath).DEFAULT_ENCODING) as csv_file:  # noqa: E501
            for line in csv_file:
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    return stripped
        return ""

    @cached_property
    def row_count_with_header(self):
        """Return the number of CSV records in the file including the header.

        Counts parsed CSV records rather than physical lines so that quoted
        fields containing embedded newlines are counted as a single record,
        matching what the parsing engines report. Comment and blank records
        are still excluded, mirroring ``_check_csv_ragged_and_warn``.
        """
        is_legacy_mac = _is_legacy_mac_newlines(self.filepath)
        newline_param = None if is_legacy_mac else ""
        encoding = FileProperties(self.filepath).DEFAULT_ENCODING
        delimiter = CSVDelimiter(self.filepath).delimiter
        count = 0
        with open(self.filepath, "r", encoding=encoding, newline=newline_param) as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter)
            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                count += 1
        return count

    @property
    def row_count_without_header(self):
        """Return the number of CSV records in the file excluding the header."""
        return self.row_count_with_header - 1


class CSVColumns:
    """Class for parsing CSV columns."""

    def __init__(self, filepath, delimiter=None):
        """Initialize the CSVColumns class.

        Args:
            filepath (str or Path): The path to the CSV file.
            delimiter (str, optional): The delimiter of the CSV file.
        """
        self.filepath = Path(filepath)
        self._delimiter = delimiter

    @cached_property
    def delimiter(self):
        """Get the delimiter."""
        if self._delimiter is not None:
            return self._delimiter
        return CSVDelimiter(self.filepath).delimiter

    @cached_property
    def columns(self):
        """Return the columns."""
        return self._get_columns()

    def _get_columns(self):
        """Return the columns."""
        if FileProperties(self.filepath).is_empty or FileProperties(self.filepath).is_blank:
            return []
        if _is_legacy_mac_newlines(self.filepath):
            try:
                encoding = FileProperties(self.filepath).DEFAULT_ENCODING
                with open(self.filepath, "r", encoding=encoding, newline=None) as f:
                    for line in f:
                        stripped = line.strip()
                        if not stripped.startswith("#") and stripped:
                            import csv

                            reader = csv.reader([line], delimiter=self.delimiter)
                            return next(reader)
            except Exception:
                pass
        df = pl.read_csv(
            self.filepath,
            separator=self.delimiter,
            truncate_ragged_lines=True,
            infer_schema=False,
            n_rows=5,
            skip_rows=_count_leading_comments(self.filepath),
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

    def __init__(self, filepath, columns=None):
        """Initialize the CSVColumnNameNormalizer with a filepath.

        Args:
            filepath (str or Path): The path to the CSV file.
            columns (list, optional): Pre-extracted column names.
        """
        self.filepath = Path(filepath)
        self._columns = columns

    @cached_property
    def columns_list(self):
        """Get the original columns list."""
        if self._columns is not None:
            return self._columns
        return CSVColumns(self.filepath).columns

    @cached_property
    def columns_normalized(self):
        """Get the normalized columns list."""
        return self._normalize_column_names(self.columns_list)

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
            columns_list (list): List of column names to make unique

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
        return dict(OrderedDict(zip(self.columns_list, self.columns_normalized)))


class CSVComponents(FileProperties):
    """A class that combines all CSV components into a single interface."""

    def __init__(self, filepath):
        """Initialize the CSVComponents object.

        Args:
            filepath (str or Path): Path to the CSV file.
        """
        super().__init__(filepath)  # Parent class handles Path conversion

    @cached_property
    def _rows(self):
        return CSVRows(self.filepath)

    @cached_property
    def first_row(self):
        """Return the first row of the CSV file."""
        return self._rows.first_row

    @cached_property
    def _delimiter(self):
        return CSVDelimiter(self.filepath, first_row=self.first_row)

    @property
    def delimiter(self):
        """Return the delimiter used in the CSV file."""
        return self._delimiter.delimiter

    @cached_property
    def _dialect(self):
        return CSVDialect(
            self.filepath,
            delimiter=self.delimiter,
            is_empty=self.is_empty,
            is_blank=self.is_blank,
        )

    @cached_property
    def _columns(self):
        return CSVColumns(self.filepath, delimiter=self.delimiter)

    @cached_property
    def columns(self):
        """Return the columns of the CSV file."""
        return self._columns.columns

    @cached_property
    def _normalizer(self):
        return CSVColumnNameNormalizer(self.filepath, columns=self.columns)

    @cached_property
    def _sample(self):
        return CSVStringSample(self.filepath, delimiter=self.delimiter)

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
