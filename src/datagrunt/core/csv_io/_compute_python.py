"""Pure-Python reference implementation of datagrunt's CSV compute.

LEADING reference implementation. It mirrors the function API of the Rust
extension ``datagrunt._native`` exactly (names, arguments, return shapes) and
serves two roles as one body of code:

1. Runtime FALLBACK when Rust is disabled via the hidden toggle in
   ``datagrunt.core.csv_io._compute`` (env ``DATAGRUNT_DISABLE_RUST`` /
   ``set_disable_rust(True)`` / ``rust_disabled()``).
2. The differential-parity ORACLE the test suite compares Rust against.

Workflow: change CSV-compute behavior HERE first, validate against the test
suite, then port the change to Rust and let the parity suite confirm they agree.
This module must stand alone in pure Python — standard library + FileProperties
only; it must never import ``datagrunt._native`` or ``_compute``.
"""

# standard library
import csv
import re
from collections import Counter

# local libraries
from datagrunt.core.file_io import FileProperties

# --- delimiter inference constants (from CSVDelimiter) ---
COMMA, SEMICOLON, PIPE, TAB = SAFE_DELIMITERS = (",", ";", "|", "\t")
SPACE_DELIMITER = " "
DEFAULT_DELIMITER = COMMA
DEFAULT_TAB_DELIMITER = TAB
NON_DELIMITER_CHARS = ('"', "-")
DELIMITER_REGEX_PATTERN = "[^0-9a-zA-Z_ " + "".join(re.escape(c) for c in NON_DELIMITER_CHARS) + "]"
DELIMITER_REGEX = re.compile(DELIMITER_REGEX_PATTERN)
CANDIDATE_SAMPLE_ROWS = 5
MIN_CONSISTENT_FIELDS = 3

# --- dialect constants (from CSVDialect) ---
CSV_SNIFF_SAMPLE_ROWS = 5

# --- normalization constants (from CSVColumnNameNormalizer) ---
SPECIAL_CHARS_PATTERN = re.compile(r"[^a-z0-9]+")
MULTI_UNDERSCORE_PATTERN = re.compile(r"_+")
EMPTY_NAME_PLACEHOLDER = "column"


def is_legacy_mac_newlines(filepath):
    """True if the file uses legacy Mac OS carriage returns (\\r) as line endings."""
    try:
        with open(filepath, "rb") as f:
            chunk = f.read(4096)
        return b"\r" in chunk and b"\n" not in chunk
    except OSError:
        return False


def count_leading_comments(filepath):
    """Count leading ``#``-prefixed comment lines before the header (blanks skipped)."""
    count = 0
    with open(filepath, "r", encoding=FileProperties(filepath).DEFAULT_ENCODING, errors="ignore") as f:
        for line in f:
            stripped = line.strip()
            if stripped.startswith("#"):
                count += 1
            elif not stripped:
                continue
            else:
                break
    return count


def count_leading_physical_lines_before_header(filepath):
    """Count every leading physical line up to and including the header line."""
    count = 0
    with open(filepath, "r", encoding=FileProperties(filepath).DEFAULT_ENCODING, errors="ignore") as f:
        for line in f:
            stripped = line.strip()
            count += 1
            if stripped and not stripped.startswith("#"):
                break
    return count


def leading_rows(filepath, limit):
    """Up to ``limit`` leading non-blank, non-comment rows, each stripped."""
    rows = []
    with open(filepath, "r", encoding=FileProperties(filepath).DEFAULT_ENCODING, errors="ignore") as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                rows.append(stripped)
                if len(rows) >= limit:
                    break
    return rows


def first_row(filepath):
    """The first non-comment row, stripped, or "" if none."""
    rows = leading_rows(filepath, 1)
    return rows[0] if rows else ""


def check_ragged(filepath, delimiter):
    """Return True if the CSV has ragged rows in the first 10,000 data rows.

    Bool only (no warning — that stays in the csvcomponents wrapper). Best-effort:
    any error returns False.
    """
    try:
        newline_param = None if is_legacy_mac_newlines(filepath) else ""
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
                    return True
                if row_count >= 10000:
                    break
    except Exception:  # noqa: BLE001 - best-effort, mirrors the original
        return False
    return False


def row_count_with_header(filepath, delimiter):
    """Number of CSV records including the header (quoted newlines = one record)."""
    newline_param = None if is_legacy_mac_newlines(filepath) else ""
    encoding = FileProperties(filepath).DEFAULT_ENCODING
    count = 0
    with open(filepath, "r", encoding=encoding, newline=newline_param, errors="ignore") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            count += 1
    return count


def _candidates_most_common(first_row_str):
    """Non-alphanumeric header characters, most common first (Counter.most_common)."""
    columns_no_spaces = first_row_str.replace(" ", "")
    counts = Counter(DELIMITER_REGEX.findall(columns_no_spaces))
    return [char for char, _ in counts.most_common()]


def _split_row(row, char):
    return row.split() if char == SPACE_DELIMITER else row.split(char)


def _splits_rows_consistently(sample_rows, char):
    if len(sample_rows) < 2:
        return False
    field_counts = {len(_split_row(r, char)) for r in sample_rows}
    return len(field_counts) == 1 and field_counts.pop() >= MIN_CONSISTENT_FIELDS


def infer_delimiter(filepath):
    """Infer the delimiter (safe > consistent punctuation > space > comma)."""
    props = FileProperties(filepath)
    if props.is_tsv:
        return DEFAULT_TAB_DELIMITER
    if props.is_empty or props.is_blank:
        return DEFAULT_DELIMITER
    candidates = _candidates_most_common(first_row(filepath))
    for char in candidates:
        if char in SAFE_DELIMITERS:
            return char
    sample = leading_rows(filepath, CANDIDATE_SAMPLE_ROWS)
    for char in candidates:
        if _splits_rows_consistently(sample, char):
            return char
    if _splits_rows_consistently(sample, SPACE_DELIMITER):
        return SPACE_DELIMITER
    return DEFAULT_DELIMITER


def sniff_dialect(filepath, delimiter=None):
    """Sniff the CSV dialect, returning the native dict shape (or None).

    None for empty/blank files and undeterminable samples. The constant fields
    (``escapechar`` None, ``lineterminator`` "\\r\\n", ``quoting`` 0) mirror what
    ``csv.Sniffer().sniff`` and ``datagrunt._native.sniff_dialect`` produce.
    """
    props = FileProperties(filepath)
    if props.is_empty or props.is_blank:
        return None
    lines = []
    with open(filepath, "r", encoding=props.DEFAULT_ENCODING, errors="ignore") as f:
        for line in f:
            if line.strip().startswith("#"):
                continue
            lines.append(line)
            if len(lines) >= CSV_SNIFF_SAMPLE_ROWS:
                break
    sample = "".join(lines)
    try:
        if delimiter:
            dialect = csv.Sniffer().sniff(sample, delimiters=delimiter)
        else:
            dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        return None
    return {
        "delimiter": dialect.delimiter,
        "quotechar": dialect.quotechar,
        "escapechar": None,
        "doublequote": dialect.doublequote,
        "lineterminator": "\r\n",
        "skipinitialspace": dialect.skipinitialspace,
        "quoting": 0,
    }


def _normalize_single(name):
    name = name.lower()
    name = SPECIAL_CHARS_PATTERN.sub("_", name)
    name = name.strip("_")
    name = MULTI_UNDERSCORE_PATTERN.sub("_", name)
    if not name:
        return EMPTY_NAME_PLACEHOLDER
    return f"_{name}" if name[0].isdigit() else name


def _make_unique(columns_list):
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


def normalize_columns(names):
    """Normalize then collision-safe-uniquify a list of column names."""
    return _make_unique([_normalize_single(c) for c in names])
