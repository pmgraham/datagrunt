"""Hypothesis strategies and primitives for differential parity testing.

Generates CSV-shaped bytes aimed at the seams both backends special-case, and
carries a label set describing which seams each example actually hit so the
suite can prove it is not testing one shape ten thousand times.
"""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass

from hypothesis import strategies as st

# Delimiters the inference logic ranks and tie-breaks between.
DELIMITERS = [",", ";", "|", "\t", ":", ".", "'", " "]
LINE_TERMINATORS = ["\n", "\r\n", "\r"]
EXTENSIONS = [".csv", ".tsv", ".TSV", ".txt"]

# Python's str.split() treats these as whitespace; Rust's char::is_whitespace
# does not. rust/datagrunt-core/src/io.rs (is_python_whitespace) calls this the
# ONLY divergence between the two, bridged by py_split_whitespace — so
# generated input must reach it.
C0_WHITESPACE = ["\x1c", "\x1d", "\x1e", "\x1f"]

BOM = b"\xef\xbb\xbf"
INVALID_UTF8 = b"\xe9\xff\xfe"

SEAM_LABELS = frozenset(
    {
        "bom",
        "lf",
        "crlf",
        "legacy-mac",
        "quoted",
        "embedded-delimiter",
        "embedded-newline",
        "comments",
        "blank-lines",
        "ragged",
        "invalid-utf8",
        "c0-controls",
        "no-trailing-newline",
        "tsv-extension",
        "empty",
        "single-column",
    }
)


@dataclass(frozen=True)
class GeneratedCSV:
    """One generated example: the bytes, the filename suffix, and the seams hit."""

    data: bytes
    suffix: str
    seams: frozenset[str]


@contextmanager
def csv_file(data: bytes, suffix: str = ".csv"):
    """Write `data` to a temp file, yield its path as str, always clean up.

    A context manager rather than a pytest fixture: Hypothesis re-runs a test
    body many times against one function-scoped fixture instance, which trips
    HealthCheck.function_scoped_fixture. Owning the file here sidesteps that
    without suppressing a health check that exists for good reason.
    """
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        yield path
    finally:
        os.unlink(path)


def outcome(fn, *args):
    """Collapse a call into a comparable ('ok', value) or ('raised', name).

    Comparing outcomes rather than return values is what lets one primitive
    cover both value parity and panic-freedom: a Rust panic crossing PyO3
    arrives as PanicException, a name the Python oracle can never produce, so
    any reachable panic becomes a parity failure with a minimized repro.

    OSError subclasses are normalized to the family name because PyO3 maps io
    errors to bare OSError while CPython raises specific subclasses; without
    this, every missing-file example would read as a false divergence.
    PanicException is not an OSError, so this narrowing cannot hide a panic.
    """
    try:
        return ("ok", fn(*args))
    except OSError:
        return ("raised", "OSError")
    except Exception as exc:  # noqa: BLE001 - comparing behavior, not handling it
        return ("raised", type(exc).__name__)


# exclude_categories, not the blacklist_categories alias: the old spelling is
# still accepted by 6.164.0 but is a documented deprecated alias that warns
# about nothing today and can disappear in any minor.
_TEXT = st.text(
    alphabet=st.characters(min_codepoint=32, max_codepoint=0x2FFF, exclude_categories=("Cs",)),
    max_size=12,
)


@st.composite
def csv_bytes(draw) -> GeneratedCSV:
    """Build a CSV-shaped example plus the set of seams it exercises."""
    seams: set[str] = set()

    delimiter = draw(st.sampled_from(DELIMITERS))
    terminator = draw(st.sampled_from(LINE_TERMINATORS))
    seams.add({"\n": "lf", "\r\n": "crlf", "\r": "legacy-mac"}[terminator])

    suffix = draw(st.sampled_from(EXTENSIONS))
    if suffix.lower() == ".tsv":
        seams.add("tsv-extension")

    n_fields = draw(st.integers(min_value=1, max_value=5))
    if n_fields == 1:
        seams.add("single-column")
    n_rows = draw(st.integers(min_value=0, max_value=6))

    def make_field() -> str:
        text = draw(_TEXT)
        if draw(st.booleans()) and draw(st.booleans()):
            text += draw(st.sampled_from(C0_WHITESPACE))
            seams.add("c0-controls")
        if draw(st.booleans()) and draw(st.booleans()):
            text += delimiter
            seams.add("embedded-delimiter")
            text = f'"{text}"'
            seams.add("quoted")
        elif draw(st.booleans()) and draw(st.booleans()):
            text = f'"{text}\n more"'
            seams.add("embedded-newline")
            seams.add("quoted")
        elif draw(st.booleans()):
            text = f'"{text}"'
            seams.add("quoted")
        return text

    lines: list[str] = []

    for _ in range(draw(st.integers(min_value=0, max_value=3))):
        lines.append("# " + draw(_TEXT))
        seams.add("comments")

    header = [make_field() for _ in range(n_fields)]
    lines.append(delimiter.join(header))

    for _ in range(n_rows):
        if draw(st.booleans()) and draw(st.booleans()):
            lines.append("")
            seams.add("blank-lines")
        width = n_fields
        if draw(st.booleans()) and draw(st.booleans()):
            width = draw(st.integers(min_value=1, max_value=n_fields + 2))
            if width != n_fields:
                seams.add("ragged")
        lines.append(delimiter.join(make_field() for _ in range(width)))

    text = terminator.join(lines)
    if lines and draw(st.booleans()):
        text += terminator
    else:
        seams.add("no-trailing-newline")

    data = text.encode("utf-8")

    if draw(st.booleans()) and draw(st.booleans()):
        data = BOM + data
        seams.add("bom")
    if draw(st.booleans()) and draw(st.booleans()) and draw(st.booleans()):
        data += INVALID_UTF8
        seams.add("invalid-utf8")
    if not data:
        seams.add("empty")

    return GeneratedCSV(data=data, suffix=suffix, seams=frozenset(seams))


def raw_bytes():
    """Arbitrary bytes. Most examples are garbage both backends reject the same
    way; the job here is finding input that crashes one of them."""
    return st.binary(max_size=200)


def column_names():
    """Column-name lists for normalize_columns: duplicates, empties, unicode,
    and names that collide only after normalization."""
    name = st.one_of(
        _TEXT,
        st.sampled_from(["", " ", "col a", "COL-B", "col_a", "col_a_1", "123abc", "%", "näme", "城市"]),
    )
    return st.lists(name, max_size=8)
