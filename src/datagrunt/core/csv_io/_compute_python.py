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
This module must stand alone in pure Python — standard library + FileProperties,
plus the typing-only ``_compute_protocol`` import below (it imports nothing back,
so this is not a cycle); it must never import ``datagrunt._native`` or ``_compute``.
"""

# standard library
import csv
import re
import sys
from collections import Counter
from collections.abc import Iterator
from typing import TextIO

# local libraries
from datagrunt.core.csv_io._compute_protocol import HeaderProbe, SniffedDialect, StrPath
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

# --- dialect sniffing: a mirror of CPython's csv.Sniffer (issue #378) ---
# Ported from Lib/csv.py at CPython v3.10.22, which behaves identically to
# v3.11.18 / v3.13.18 / v3.14.10: the releases carrying the 2026 Sniffer fixes
# gh-103925 (line endings normalised before sniffing), gh-98820 (a quoted-field
# body stops at the first undoubled quote) and gh-109638 (doubled quotes are
# sought in whole fields). Mirrored rather than delegated so the oracle no
# longer changes with the interpreter's patch level. 3.11+ spells both bodies
# with possessive quantifiers, which 3.10's `re` lacks; the 3.10.22 unrolled
# spellings used here are unambiguous, so they match the same text.
# rust/datagrunt-core/src/dialect.rs is the Rust port of this algorithm.
SNIFF_PREFERRED_DELIMITERS = (",", "\t", ";", " ", ":")
SNIFF_ASCII = tuple(chr(c) for c in range(127))  # code points 0..126, as CPython scans
SNIFF_CHUNK_LINES = 10
SNIFF_CONSISTENCY_THRESHOLD = 0.9
_QUOTE_OTHER = r"(?:(?!(?P=quote)).)*"
_QUOTE_BODY = r"%s(?:(?P=quote){2}%s)*" % (_QUOTE_OTHER, _QUOTE_OTHER)
# Tried in this order; the first pattern with any match wins.
SNIFF_QUOTE_PATTERNS = tuple(
    re.compile(restr % _QUOTE_BODY, re.DOTALL | re.MULTILINE)
    for restr in (
        r'(?P<delim>[^\w\n"\'])(?P<space> ?)(?P<quote>["\'])%s(?P=quote)(?P=delim)',  # ,"...",
        r'(?:^|\n)(?P<quote>["\'])%s(?P=quote)(?P<delim>[^\w\n"\'])(?P<space> ?)',  #  "...",
        r'(?P<delim>[^\w\n"\'])(?P<space> ?)(?P<quote>["\'])%s(?P=quote)(?:$|\n)',  # ,"..."
        r'(?:^|\n)(?P<quote>["\'])%s(?P=quote)(?:$|\n)',  #  "..." (no delim, no space)
    )
)

# --- normalization constants (from CSVColumnNameNormalizer) ---
SPECIAL_CHARS_PATTERN = re.compile(r"[^a-z0-9]+")
MULTI_UNDERSCORE_PATTERN = re.compile(r"_+")
EMPTY_NAME_PLACEHOLDER = "column"

# Bound per-physical-line length so a malformed newline-free file (one giant
# line) cannot force unbounded buffering (issue #222). 2Mi chars is far above
# any realistic CSV line/header, so this only ever truncates pathological input.
MAX_LINE_CHARS = 2 * 1024 * 1024


def _capped_lines(f: TextIO) -> Iterator[str]:
    """Yield each line from a text file, truncated to ``MAX_LINE_CHARS`` chars.

    Mirrors the Rust line readers' per-line cap so the two backends agree on
    pathological (multi-MB single-line) input. The cap is by character count
    (identical to Rust's), keeping parity decode-agnostic.
    """
    for line in f:
        yield line if len(line) <= MAX_LINE_CHARS else line[:MAX_LINE_CHARS]


def is_legacy_mac_newlines(filepath: StrPath) -> bool:
    """True if the file uses legacy Mac OS carriage returns (\\r) as line endings."""
    try:
        with open(filepath, "rb") as f:
            chunk = f.read(4096)
        return b"\r" in chunk and b"\n" not in chunk
    except OSError:
        return False


def count_leading_comments(filepath: StrPath) -> int:
    """Count leading ``#``-prefixed comment lines before the header (blanks skipped)."""
    count = 0
    with open(filepath, "r", encoding=FileProperties(filepath).DEFAULT_ENCODING, errors="ignore") as f:
        for line in _capped_lines(f):
            stripped = line.strip()
            if stripped.startswith("#"):
                count += 1
            elif not stripped:
                continue
            else:
                break
    return count


def count_leading_physical_lines_before_header(filepath: StrPath) -> int:
    """Count every leading physical line up to and including the header line."""
    count = 0
    with open(filepath, "r", encoding=FileProperties(filepath).DEFAULT_ENCODING, errors="ignore") as f:
        for line in _capped_lines(f):
            stripped = line.strip()
            count += 1
            if stripped and not stripped.startswith("#"):
                break
    return count


def leading_rows(filepath: StrPath, limit: int) -> list[str]:
    """Up to ``limit`` leading non-blank, non-comment rows, each stripped."""
    rows: list[str] = []
    with open(filepath, "r", encoding=FileProperties(filepath).DEFAULT_ENCODING, errors="ignore") as f:
        # A non-positive limit asks for nothing. The check sits inside the
        # `with` so an unreadable path still raises here, exactly as it does
        # for every other limit (issue #325).
        if limit <= 0:
            return rows
        for line in _capped_lines(f):
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                rows.append(stripped)
                if len(rows) >= limit:
                    break
    return rows


def first_row(filepath: StrPath) -> str:
    """The first non-comment row, stripped, or "" if none."""
    rows = leading_rows(filepath, 1)
    return rows[0] if rows else ""


def _nul_safe_lines(f: TextIO) -> Iterator[str]:
    """Line stream safe for ``csv.reader`` on every supported Python.

    Python < 3.11's csv module rejects NUL characters ("line contains NUL");
    3.11+ parses them as ordinary field content, as does the Rust backend.
    Substituting U+FFFD (never empty-string stripping) keeps comment
    detection, row emptiness, and column counts identical to 3.11+.
    """
    if sys.version_info >= (3, 11):
        return f
    return (line.replace("\0", "�") for line in f)


def check_ragged(filepath: StrPath, delimiter: str) -> bool:
    """Return True if the CSV has ragged rows in the first 10,000 data rows.

    Bool only (no warning — that stays in the csvcomponents wrapper). Best-effort:
    any error returns False.
    """
    try:
        newline_param = None if is_legacy_mac_newlines(filepath) else ""
        encoding = FileProperties(filepath).DEFAULT_ENCODING
        with open(filepath, "r", encoding=encoding, newline=newline_param, errors="ignore") as f:
            reader = csv.reader(_nul_safe_lines(f), delimiter=delimiter)
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


def row_count_with_header(filepath: StrPath, delimiter: str) -> int:
    """Number of CSV records including the header (quoted newlines = one record)."""
    newline_param = None if is_legacy_mac_newlines(filepath) else ""
    encoding = FileProperties(filepath).DEFAULT_ENCODING
    count = 0
    with open(filepath, "r", encoding=encoding, newline=newline_param, errors="ignore") as f:
        reader = csv.reader(_nul_safe_lines(f), delimiter=delimiter)
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            count += 1
    return count


def _candidates_most_common(first_row_str: str) -> list[str]:
    """Non-alphanumeric header characters, most common first (Counter.most_common)."""
    columns_no_spaces = first_row_str.replace(" ", "")
    counts = Counter(DELIMITER_REGEX.findall(columns_no_spaces))
    return [char for char, _ in counts.most_common()]


def _split_row(row: str, char: str) -> list[str]:
    return row.split() if char == SPACE_DELIMITER else row.split(char)


def _splits_rows_consistently(sample_rows: list[str], char: str) -> bool:
    if len(sample_rows) < 2:
        return False
    field_counts = {len(_split_row(r, char)) for r in sample_rows}
    return len(field_counts) == 1 and field_counts.pop() >= MIN_CONSISTENT_FIELDS


def probe_csv_header(filepath: StrPath) -> HeaderProbe:
    """Single-pass header probe shared by delimiter + dialect inference.

    One read captures everything both inference paths need, replacing the
    separate ``is_blank`` / ``first_row`` / ``leading_rows`` / sniff-sample reads.

    Returns a dict:
      - ``empty``: file has zero bytes.
      - ``blank``: file has no non-whitespace content.
      - ``first_row``: first stripped, non-blank, non-comment row ("" if none).
      - ``sample_rows``: up to ``CANDIDATE_SAMPLE_ROWS`` stripped non-blank
        non-comment rows (reproduces ``leading_rows(CANDIDATE_SAMPLE_ROWS)``).
      - ``sample_lines``: up to ``CSV_SNIFF_SAMPLE_ROWS`` raw non-comment lines,
        blanks kept (reproduces ``sniff_dialect``'s sample).
    """
    props = FileProperties(filepath)
    if props.is_empty:
        return {"empty": True, "blank": False, "first_row": "", "sample_rows": [], "sample_lines": []}

    sample_lines: list[str] = []
    sample_rows: list[str] = []
    saw_nonblank = False
    # NOTE on blankness semantics: this probe derives ``blank`` from
    # ``saw_nonblank`` over lines decoded with ``errors="ignore"``. An
    # all-invalid-UTF-8 file therefore reads as ``blank=True`` here, whereas
    # ``FileProperties.is_blank`` treats undecodable bytes as content and
    # returns ``blank=False`` (issue #146). Do NOT align them: aligning would
    # require a Rust+parity change for a negligible edge case — all-invalid
    # input yields DEFAULT_DELIMITER/None from both paths anyway. The probe
    # intentionally uses a lighter "errors=ignore" blankness notion.
    with open(filepath, "r", encoding=props.DEFAULT_ENCODING, errors="ignore") as f:
        for line in _capped_lines(f):
            stripped = line.strip()
            if stripped:
                saw_nonblank = True
            is_comment = stripped.startswith("#")
            if not is_comment and len(sample_lines) < CSV_SNIFF_SAMPLE_ROWS:
                sample_lines.append(line)
            if stripped and not is_comment and len(sample_rows) < CANDIDATE_SAMPLE_ROWS:
                sample_rows.append(stripped)
            if (
                saw_nonblank
                and len(sample_lines) >= CSV_SNIFF_SAMPLE_ROWS
                and len(sample_rows) >= CANDIDATE_SAMPLE_ROWS
            ):
                break

    return {
        "empty": False,
        "blank": not saw_nonblank,
        "first_row": sample_rows[0] if sample_rows else "",
        "sample_rows": sample_rows,
        "sample_lines": sample_lines,
    }


def infer_delimiter(filepath: StrPath) -> str:
    """Infer the delimiter (safe > consistent punctuation > space > comma)."""
    props = FileProperties(filepath)
    if props.is_tsv:
        return DEFAULT_TAB_DELIMITER
    probe = probe_csv_header(filepath)
    if probe["empty"] or probe["blank"]:
        return DEFAULT_DELIMITER
    candidates = _candidates_most_common(probe["first_row"])
    for char in candidates:
        if char in SAFE_DELIMITERS:
            return char
    sample = probe["sample_rows"]
    for char in candidates:
        if _splits_rows_consistently(sample, char):
            return char
    if _splits_rows_consistently(sample, SPACE_DELIMITER):
        return SPACE_DELIMITER
    return DEFAULT_DELIMITER


def _first_quote_matches(data: str) -> tuple[re.Pattern[str], list[re.Match[str]]]:
    """Every match of the first quote pattern that matches at all (CPython's order)."""
    for regexp in SNIFF_QUOTE_PATTERNS:
        matches = list(regexp.finditer(data))
        if matches:
            return regexp, matches
    return SNIFF_QUOTE_PATTERNS[-1], []


def _tally_quote_matches(
    regexp: re.Pattern[str], matches: list[re.Match[str]], delimiters: str | None
) -> tuple[dict[str, int], dict[str, int], int]:
    """Count quote chars, allowed delimiters, and matches with a space after the delimiter.

    The quote and delimiter groups each match exactly one character, so
    CPython's emptiness checks on them can never fire. The last pattern has no
    ``delim``/``space`` groups: CPython's ``KeyError`` ``continue`` skips both
    tallies for it, as ``has_delim`` does here. ``delim in delimiters`` is a
    substring test on purpose, exactly as CPython's is on a str restriction.
    """
    quotes: dict[str, int] = {}
    delims: dict[str, int] = {}
    spaces = 0
    has_delim = "delim" in regexp.groupindex
    for match in matches:
        quote = match["quote"]
        quotes[quote] = quotes.get(quote, 0) + 1
        if not has_delim:
            continue
        delim = match["delim"]
        if delimiters is None or delim in delimiters:
            delims[delim] = delims.get(delim, 0) + 1
        if match["space"]:
            spaces += 1
    return quotes, delims, spaces


def _has_doubled_quote(data: str, quotechar: str, delim: str) -> bool:
    """gh-109638: a doubled quote inside a whole quoted field means ``doublequote``.

    The field body is CPython 3.10.22's unrolled loop; the quotechar is only
    ever ``"`` or ``'``, which need no escaping (CPython does not escape it).
    """
    dq_regexp = re.compile(
        r"(?:(?<=%(delim)s)|^)%(space)s%(quote)s"  # ,"
        r"([^%(quote)s]*(?:%(quote)s%(quote)s[^%(quote)s]*)*)"  # the body
        r"%(quote)s(?:%(delim)s|$)"  # ",
        % {
            "delim": re.escape(delim),
            "quote": quotechar,
            # Skipping spaces after a space rescans them.
            "space": " *" if delim != " " else "",
        },
        re.MULTILINE,
    )
    doubled = quotechar * 2
    return any(doubled in match[1] for match in dq_regexp.finditer(data))


def _guess_quote_and_delimiter(data: str, delimiters: str | None) -> tuple[str, bool, str, bool]:
    """Mirror of ``Sniffer._guess_quote_and_delimiter``.

    Returns ``(quotechar, doublequote, delimiter, skipinitialspace)``; an empty
    delimiter means "not determinable from quoting". CPython's ``delim == '\\n'``
    reset is omitted: both delimiter groups exclude ``\\n``, so it is unreachable.
    """
    regexp, matches = _first_quote_matches(data)
    if not matches:
        return "", False, "", False
    quotes, delims, spaces = _tally_quote_matches(regexp, matches, delimiters)
    # max() keeps the first key reaching the maximum, as max(d, key=d.get) does.
    quotechar = max(quotes, key=quotes.__getitem__)
    if not delims:
        # A single column of quoted data. Doubled quotes are only sought once a
        # delimiter is known (gh-109638).
        return quotechar, False, "", False
    delim = max(delims, key=delims.__getitem__)
    skipinitialspace = delims[delim] == spaces
    return quotechar, _has_doubled_quote(data, quotechar, delim), delim, skipinitialspace


def _count_char_frequencies(lines: list[str], char_frequency: dict[str, dict[int, int]]) -> None:
    """Add each line's per-character count to ``char_frequency[char][count]`` (zeros included)."""
    for line in lines:
        for char in SNIFF_ASCII:
            meta_frequency = char_frequency.setdefault(char, {})
            freq = line.count(char)
            meta_frequency[freq] = meta_frequency.get(freq, 0) + 1


def _char_modes(char_frequency: dict[str, dict[int, int]]) -> dict[str, tuple[int, int]]:
    """Each seen character's modal frequency, its row count reduced by every other frequency's."""
    modes: dict[str, tuple[int, int]] = {}
    for char, meta_frequency in char_frequency.items():
        items = list(meta_frequency.items())
        if len(items) == 1 and items[0][0] == 0:
            continue
        if len(items) == 1:
            modes[char] = items[0]
            continue
        mode_freq, mode_count = max(items, key=lambda item: item[1])
        others = sum(count for freq, count in items if freq != mode_freq)
        modes[char] = (mode_freq, mode_count - others)
    return modes


def _consistent_delimiters(
    modes: dict[str, tuple[int, int]], total: float, delimiters: str | None
) -> dict[str, tuple[int, int]]:
    """Characters meeting their mode on the most rows, relaxing 1.0 down to the 0.9 threshold.

    At most 11 passes. The repeated float subtraction is CPython's, kept so the
    boundary values compare identically.
    """
    consistency = 1.0
    while consistency >= SNIFF_CONSISTENCY_THRESHOLD:
        found = {
            char: mode
            for char, mode in modes.items()
            if mode[0] > 0
            and mode[1] > 0
            and mode[1] / total >= consistency
            and (delimiters is None or char in delimiters)
        }
        if found:
            return found
        consistency -= 0.01
    return {}


def _skips_initial_space(first_line: str, delim: str) -> bool:
    return first_line.count(delim) == first_line.count(delim + " ")


def _pick_delimiter(delims: dict[str, tuple[int, int]], first_line: str) -> tuple[str, bool]:
    """The first preferred delimiter present, else the dominant one (CPython sorts ``(mode, char)``)."""
    preferred = [d for d in SNIFF_PREFERRED_DELIMITERS if d in delims]
    delim = preferred[0] if preferred else max((mode, char) for char, mode in delims.items())[1]
    return delim, _skips_initial_space(first_line, delim)


def _guess_delimiter(data: str, delimiters: str | None) -> tuple[str, bool]:
    """Mirror of ``Sniffer._guess_delimiter``: ``(delimiter, skipinitialspace)``, "" if undetermined.

    CPython never revisits a non-empty ``delims``, so later chunks cannot change
    the outcome once one is found; stopping there returns what CPython returns.
    A lone candidate needs no tie-break, so ``_pick_delimiter`` returns it as is.
    """
    lines = [line for line in data.split("\n") if line]
    chunk_length = min(SNIFF_CHUNK_LINES, len(lines))
    char_frequency: dict[str, dict[int, int]] = {}
    delims: dict[str, tuple[int, int]] = {}
    iteration = 0
    # Bounded: each pass advances `start` by chunk_length (>= 1 when lines exist).
    for start in range(0, len(lines), max(chunk_length, 1)):
        iteration += 1
        _count_char_frequencies(lines[start : start + chunk_length], char_frequency)
        total = float(min(chunk_length * iteration, len(lines)))
        delims = _consistent_delimiters(_char_modes(char_frequency), total, delimiters)
        if delims:
            break
    if not delims:
        return "", False
    return _pick_delimiter(delims, lines[0])


def _sniff_sample(sample: str, delimiters: str | None) -> SniffedDialect | None:
    """Mirror of ``csv.Sniffer().sniff(sample, delimiters)``; None where CPython raises ``csv.Error``.

    The constant fields (``escapechar`` None, ``lineterminator`` "\\r\\n",
    ``quoting`` 0 = QUOTE_MINIMAL) are the ones CPython's sniffed dialect carries.
    """
    sample = sample.replace("\r\n", "\n").replace("\r", "\n")  # gh-103925
    quotechar, doublequote, delimiter, skipinitialspace = _guess_quote_and_delimiter(sample, delimiters)
    if not delimiter:
        delimiter, skipinitialspace = _guess_delimiter(sample, delimiters)
    if not delimiter:
        return None
    return {
        "delimiter": delimiter,
        "quotechar": quotechar or '"',  # _csv.reader won't accept a quotechar of ''
        "escapechar": None,
        "doublequote": doublequote,
        "lineterminator": "\r\n",
        "skipinitialspace": skipinitialspace,
        "quoting": 0,
    }


def sniff_dialect(filepath: StrPath, delimiter: str | None = None) -> SniffedDialect | None:
    """Sniff the CSV dialect, returning the native dict shape (or None).

    None for empty/blank files and undeterminable samples. Sniffing is this
    module's mirror of the fixed CPython ``csv.Sniffer`` (see ``_sniff_sample``),
    never the running interpreter's, so the result does not depend on it.
    An empty ``delimiter`` means no restriction, like ``None``.
    """
    probe = probe_csv_header(filepath)
    if probe["empty"] or probe["blank"]:
        return None
    return _sniff_sample("".join(probe["sample_lines"]), delimiter or None)


def _normalize_single(name: str) -> str:
    name = name.lower()
    name = SPECIAL_CHARS_PATTERN.sub("_", name)
    name = name.strip("_")
    name = MULTI_UNDERSCORE_PATTERN.sub("_", name)
    if not name:
        return EMPTY_NAME_PLACEHOLDER
    return f"_{name}" if name[0].isdigit() else name


def _make_unique(columns_list: list[str]) -> list[str]:
    emitted: set[str] = set()
    unique_names: list[str] = []
    for name in columns_list:
        candidate = name
        suffix = 0
        while candidate in emitted:
            suffix += 1
            candidate = f"{name}_{suffix}"
        emitted.add(candidate)
        unique_names.append(candidate)
    return unique_names


def normalize_columns(names: list[str]) -> list[str]:
    """Normalize then collision-safe-uniquify a list of column names."""
    return _make_unique([_normalize_single(c) for c in names])
