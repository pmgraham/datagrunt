"""Differential properties: the two compute backends must agree on generated input.

The fixed corpus in corpus.py proves the cases someone thought to write down.
These prove the cases nobody did. Every property compares *outcomes* — returned
value or raised exception — so a Rust panic crossing PyO3 is a parity failure
with a minimized repro attached, not a mysterious crash.

On divergence: minimize, add the bytes to corpus.py as a named case, open an
issue, and mark that property xfail(strict=True) with the issue link. Do not
weaken a property to make it pass.
"""

import pytest
from hypothesis import event, example, given
from hypothesis import strategies as st
from strategies import GeneratedCSV, column_names, csv_bytes, csv_file, outcome, raw_bytes

from datagrunt import _native as rs
from datagrunt.core.csv_io import _compute_python as py

# Functions taking only a path.
PATH_ONLY = [
    "is_legacy_mac_newlines",
    "first_row",
    "count_leading_comments",
    "count_leading_physical_lines_before_header",
    "infer_delimiter",
    "probe_csv_header",
]


def _record(gen):
    """Surface which seams this example hit, for --hypothesis-show-statistics."""
    for seam in gen.seams:
        event(seam)


@given(gen=csv_bytes(), fn_name=st.sampled_from(PATH_ONLY))
def test_path_only_functions_agree(gen, fn_name):
    _record(gen)
    event(f"fn:{fn_name}")
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(getattr(rs, fn_name), path) == outcome(getattr(py, fn_name), path), fn_name


@given(gen=csv_bytes(), limit=st.integers(min_value=0, max_value=200))
def test_leading_rows_agrees(gen, limit):
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(rs.leading_rows, path, limit) == outcome(py.leading_rows, path, limit)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "sniff_dialect's delimiter class is [^\\w\\n\"'], and CPython's \\w (str.isalnum()-based) "
        "disagrees with the Rust regex crate's \\w on category No (U+00B2) and on marks (U+0301), "
        "so the two sniff different delimiters. Corpus case sniff_delimiter_word_class.csv. See #NNN"
    ),
)
@example(
    # The minimized falsifying example, pinned so this fails at every profile
    # rather than only where the search happens to reach it (it was found at the
    # deep profile, not at ci's 100 examples). Keep as a regression case once
    # #NNN is fixed and the xfail comes off.
    gen=GeneratedCSV(
        data=b'"\'"\'\n"\xc2\xb2\'"',
        suffix=".csv",
        seams=frozenset({"embedded-delimiter", "lf", "no-trailing-newline", "quoted", "ragged"}),
    ),
    delimiter=None,
)
@given(gen=csv_bytes(), delimiter=st.one_of(st.none(), st.sampled_from([",", ";", "|", "\t", ":", " "])))
def test_sniff_dialect_agrees(gen, delimiter):
    """Compares the RAW dicts, including `delimiter`.

    The corpus suite normalizes both sides through dialect_properties_from_rust,
    which drops `delimiter` — so sniff_dialect's own delimiter output has never
    been compared. This closes that gap.
    """
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        args = (path,) if delimiter is None else (path, delimiter)
        assert outcome(rs.sniff_dialect, *args) == outcome(py.sniff_dialect, *args)


@given(gen=csv_bytes(), delimiter=st.sampled_from([",", ";", "|", "\t", ":", " ", "x"]))
def test_row_count_with_header_agrees(gen, delimiter):
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(rs.row_count_with_header, path, delimiter) == outcome(py.row_count_with_header, path, delimiter)


@given(gen=csv_bytes(), delimiter=st.sampled_from([",", ";", "|", "\t", ":", " ", "x"]))
def test_check_ragged_agrees(gen, delimiter):
    _record(gen)
    with csv_file(gen.data, gen.suffix) as path:
        assert outcome(rs.check_ragged, path, delimiter) == outcome(py.check_ragged, path, delimiter)


@given(names=column_names())
def test_normalize_columns_agrees(names):
    """Takes a list, not a path — duplicates and post-normalization collisions
    are the interesting cases."""
    event(f"n_names:{len(names)}")
    assert outcome(rs.normalize_columns, names) == outcome(py.normalize_columns, names)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "count_leading_physical_lines_before_header diverges on bytes that decode to '' under "
        "errors='ignore' with no line terminator (b'\\x80'): Rust counts a final empty line (1), "
        "CPython iterates decoded text and yields none (0). Corpus case "
        "invalid_utf8_only_no_newline.csv. See #NNN"
    ),
)
@example(
    # The minimized falsifying example, pinned so the failure does not depend on
    # the search reaching it under a given profile or Hypothesis version. Keep as
    # a regression case once #NNN is fixed and the xfail comes off.
    data=b"\x80",
    fn_name="count_leading_physical_lines_before_header",
)
@given(data=raw_bytes(), fn_name=st.sampled_from(PATH_ONLY))
def test_arbitrary_bytes_do_not_diverge(data, fn_name):
    """Crash hunt. Most examples are garbage both backends reject identically;
    the value is the one that does not."""
    with csv_file(data, ".csv") as path:
        assert outcome(getattr(rs, fn_name), path) == outcome(getattr(py, fn_name), path), fn_name
