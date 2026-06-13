"""Module for CSV components."""

# standard library
import csv
import logging
import re
import warnings
from collections import OrderedDict
from functools import cached_property
from pathlib import Path

# third party libraries
import polars as pl

# local libraries
from datagrunt.core.file_io import FileProperties
from datagrunt.core.csv_io import _compute

logger = logging.getLogger(__name__)


def _count_leading_comments(filepath):
    """Count leading ``#``-prefixed comment lines before the header.

    Delegates to the active compute backend (Rust by default; pure Python when
    the hidden toggle is on).
    """
    return _compute.backend().count_leading_comments(str(filepath))


def _count_leading_physical_lines_before_header(filepath):
    """Count every leading physical line up to and including the header line.

    Delegates to the active compute backend.
    """
    return _compute.backend().count_leading_physical_lines_before_header(str(filepath))


def _is_legacy_mac_newlines(filepath):
    """Check if the file uses legacy Mac OS carriage returns (\\r) as line endings.

    Delegates to the active compute backend.
    """
    return _compute.backend().is_legacy_mac_newlines(str(filepath))


def _check_csv_ragged_and_warn(filepath, delimiter):
    """Warn if the CSV has ragged rows; return whether it does.

    Ragged detection runs in the active compute backend; the ``UserWarning`` is
    emitted here. The compute layer returns only a boolean, so the warning no
    longer names the specific offending row and column counts (a documented
    behavior delta) — but it still contains the substring "ragged rows", which
    is the only thing any test asserts.
    """
    is_ragged = _compute.backend().check_ragged(str(filepath), delimiter)
    if is_ragged:
        warnings.warn(
            "CSV file contains ragged rows. Some fields will be truncated or "
            "padded with nulls.",
            UserWarning,
        )
    return is_ragged


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
                with open(self.filepath, "r", encoding=encoding, newline=None, errors="ignore") as f:
                    for line in f:
                        stripped = line.strip()
                        if not stripped.startswith("#") and stripped:
                            lines.append(line)
                            if len(lines) >= self.SAMPLE_ROWS + 1:
                                break
                return "".join(lines)
            except Exception:  # noqa: BLE001 - best-effort legacy-mac sample; fall back to polars
                logger.debug("Legacy-mac sample read failed for %s; using polars", self.filepath, exc_info=True)
        df = pl.read_csv(
            self.filepath,
            separator=self.delimiter,
            n_rows=self.SAMPLE_ROWS,
            comment_prefix="#",
        )
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
            comment_prefix="#",
        )
        # Namespaced internal name avoids clobbering a user column named "null_count".
        null_count_column = "__datagrunt_null_count__"
        df = df.with_columns(
            pl.sum_horizontal(pl.all().is_null()).alias(null_count_column)
        )
        df = df.sort(null_count_column)
        df = df.drop(null_count_column)
        return df.head(self.SAMPLE_ROWS).write_csv(file=None)


class CSVDelimiter:
    """Class to infer and derive the CSV delimiter."""

    # --- Single source of truth for delimiter characters ---
    # Every delimiter character is named exactly once here; the regex, the
    # defaults, and the inference passes all derive from these collections so
    # no delimiter character is repeated anywhere else in the library.
    #
    # Unambiguous delimiters: accepted as soon as one is the most frequent
    # candidate in the header, with no row-consistency check.
    COMMA, SEMICOLON, PIPE, TAB = SAFE_DELIMITERS = (",", ";", "|", "\t")
    # The whitespace delimiter is ambiguous - incidental spaces in values look
    # identical to a real space delimiter - so it is accepted only when the
    # sampled rows split on it consistently.
    SPACE_DELIMITER = " "
    # Characters that can appear in a header/value but are never the delimiter,
    # so they are dropped from candidate counting. The dot (``user.id``) and
    # apostrophe (``User's Name``) are deliberately absent: they ARE counted,
    # then validated by row consistency rather than blindly picked (issue #74).
    NON_DELIMITER_CHARS = ('"', "-")

    DEFAULT_DELIMITER = COMMA
    DEFAULT_TAB_DELIMITER = TAB
    # Candidates are non-word, non-space characters except those that are never
    # delimiters; built from NON_DELIMITER_CHARS so the set lives in one place.
    DELIMITER_REGEX_PATTERN = "[^0-9a-zA-Z_ " + "".join(re.escape(c) for c in NON_DELIMITER_CHARS) + "]"

    CANDIDATE_SAMPLE_ROWS = 5
    # A candidate must split rows into at least this many fields to win. Two
    # fields are indistinguishable from a single text column with one separator
    # per value (e.g. ``John Smith``), so they do not count.
    MIN_CONSISTENT_FIELDS = 3

    def __init__(self, filepath, first_row=None):
        """Initialize the CSVDelimiter class.

        Args:
            filepath (str or Path): The path to the CSV file.
            first_row (str, optional): The first row of the CSV file.
        """
        self.filepath = Path(filepath)
        self.file_properties = FileProperties(self.filepath)
        if first_row is not None:
            self.first_row = first_row
        else:
            self.first_row = CSVRows(self.filepath).first_row
        self.delimiter = self.infer_csv_file_delimiter()
        self.delimiter_byte_string = self.delimiter.encode()

    def infer_csv_file_delimiter(self):
        """Infer the delimiter of the CSV file via the active compute backend.

        Routes to ``_compute.backend().infer_delimiter`` (Rust by default; pure
        Python when the toggle is on), reproducing the same precedence: safe
        delimiter > consistent punctuation > space > comma, with TSV-extension
        and empty/blank handling.
        """
        return _compute.backend().infer_delimiter(str(self.filepath))


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
        # errors="ignore" so non-UTF-8 bytes can't crash dialect sniffing.
        with open(self.filepath, "r", encoding=FileProperties(self.filepath).DEFAULT_ENCODING, errors="ignore") as csvfile:  # noqa: E501
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
        """The first non-comment row, stripped, or "" — via the compute backend."""
        return _compute.backend().first_row(str(self.filepath))

    def leading_rows(self, limit):
        """Up to ``limit`` leading non-comment rows, each stripped — via backend.

        Args:
            limit (int): The maximum number of rows to return.

        Returns:
            list[str]: The leading non-comment rows, in file order.
        """
        return _compute.backend().leading_rows(str(self.filepath), limit)

    @cached_property
    def row_count_with_header(self):
        """Number of CSV records including the header — via the compute backend.

        Counts parsed records (quoted embedded newlines = one record), skipping
        comment and blank records, matching the parsing engines.
        """
        delimiter = CSVDelimiter(self.filepath).delimiter
        return _compute.backend().row_count_with_header(str(self.filepath), delimiter)

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
                with open(self.filepath, "r", encoding=encoding, newline=None, errors="ignore") as f:
                    for line in f:
                        stripped = line.strip()
                        if not stripped.startswith("#") and stripped:
                            import csv

                            reader = csv.reader([line], delimiter=self.delimiter)
                            return next(reader)
            except Exception:  # noqa: BLE001 - best-effort legacy-mac header; fall back to polars
                logger.debug("Legacy-mac column read failed for %s; using polars", self.filepath, exc_info=True)
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
    # Used when a header normalizes to the empty string so it can still be
    # uniquified into a valid, non-empty identifier.
    EMPTY_NAME_PLACEHOLDER = "column"

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
        # A header of only special characters (e.g. "%" or "()") normalizes to
        # the empty string, which is an invalid zero-length SQL identifier and
        # is silently dropped by some engines. Fall back to a placeholder so it
        # can be uniquified into a valid column name.
        if not name:
            return self.EMPTY_NAME_PLACEHOLDER
        return f"_{name}" if name[0].isdigit() else name

    def _make_unique_column_names(self, columns_list):
        """
        Make unique column names by appending a number to duplicate names.

        A naive ``name_N`` suffix can itself collide with a real column (e.g.
        ``col_a, col_a, col_a_1`` would emit two ``col_a_1``). To guarantee a
        unique result, this tracks every name already emitted and keeps
        incrementing the suffix until the candidate is unused across the whole
        list. Downstream SQL projections and Arrow renames rely on this: a
        duplicate name breaks DuckDB's ``AS`` projection and silently drops a
        column in PyArrow.

        Args:
            columns_list (list): List of column names to make unique

        Returns:
            list: List of unique column names
        """
        emitted = set()
        unique_names = []

        for name in columns_list:
            candidate = name
            suffix = 0
            while candidate in emitted:
                suffix += 1
                candidate = f"{name}_{suffix}"
            emitted.add(candidate)
            unique_names.append(candidate)

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
