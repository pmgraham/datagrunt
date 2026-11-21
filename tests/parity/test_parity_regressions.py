"""Targeted regressions for sniffer parity bugs fixed for 4.0.0.

These cases don't fit the shared corpus: the adversarial input is multi-MB, and
the empty-delimiter case needs an explicit argument the corpus tests don't pass.
"""

import time

import pytest

from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py

# #378: adversarial lines for the quoted-field body (gh-98820) and the
# doubled-quote search (gh-109638). The first four are the ReDoS probe's shapes;
# `mixed_quote_openers` is where the pre-fix Rust patterns were super-linear.
REDOS_SHAPES = {
    "unterminated_quote": lambda n: "id,note\n1," + '"' + "a" * n + "\n",
    "quote_run": lambda n: "a,b\n" + '"' * n + "\n",
    "comma_quote_openers": lambda n: "a,b\n" + ',"' * (n // 2) + "\n",
    "doubled_unterminated": lambda n: 'a,"' + '""' * (n // 2) + "\n",
    "mixed_quote_openers": lambda n: "a,b\n" + ",'\"" * (n // 3) + "\n",
}
REDOS_LINE_CHARS = 64 * 1024
REDOS_MAX_SECONDS = 2.0  # generous: both backends take milliseconds when linear


def test_adversarial_long_line_does_not_panic_and_matches_python(tmp_path):
    """A ~1.8 MB newline-free line of alternating quotes/delimiters used to drive
    fancy_regex past its backtrack limit and PANIC across the FFI boundary
    (pyo3_runtime.PanicException), where CPython's csv.Sniffer succeeds. The Rust
    sniffer must now degrade gracefully and match the Python oracle exactly.
    """
    path = tmp_path / "adversarial.csv"
    path.write_text(",'a'" + ",a'" * 600_000 + "\n")

    # Must not raise (regression: this previously raised PanicException, a
    # BaseException that ordinary `except Exception` handlers do not catch).
    rust = datagrunt_rs.sniff_dialect(str(path), None)
    python = py.sniff_dialect(str(path), None)

    assert rust is not None
    assert rust == python
    assert rust["delimiter"] == ","


def test_empty_delimiter_is_treated_as_unrestricted(tmp_path):
    """An empty delimiter string is Python-falsy (`if delimiter:`), i.e. "no
    restriction". Previously Rust passed Some("") down, whose substring guard
    rejected every candidate and returned None; it must now behave like None and
    match the Python oracle.
    """
    path = tmp_path / "semicolon.csv"
    path.write_text("a;b;c\n1;2;3\n4;5;6\n")

    rust_empty = datagrunt_rs.sniff_dialect(str(path), "")
    rust_none = datagrunt_rs.sniff_dialect(str(path), None)
    python_empty = py.sniff_dialect(str(path), "")

    assert rust_empty == rust_none == python_empty
    assert rust_empty is not None
    assert rust_empty["delimiter"] == ";"


@pytest.mark.parametrize("shape", sorted(REDOS_SHAPES))
def test_redos_shapes_agree_in_bounded_time(tmp_path, shape):
    """Both backends give the same dialect on each shape, each within the bound.

    A super-linear pattern would blow the bound (CPython's pre-fix Sniffer was
    quadratic on `comma_quote_openers`), or, in Rust, stop at fancy-regex's
    backtrack limit and answer differently from the oracle.
    """
    path = tmp_path / f"{shape}.csv"
    path.write_text(REDOS_SHAPES[shape](REDOS_LINE_CHARS))
    results = {}
    for name, sniff in (("rust", datagrunt_rs.sniff_dialect), ("python", py.sniff_dialect)):
        start = time.perf_counter()
        results[name] = sniff(str(path))
        elapsed = time.perf_counter() - start
        assert elapsed < REDOS_MAX_SECONDS, f"{name} took {elapsed:.2f}s on {shape}"
    assert results["rust"] == results["python"], shape


# #381: fancy-regex aborted searches at its 1M backtrack limit or 1M-entry stack
# on very long lines (250k-1.2M+ chars), making Rust diverge from the Python
# oracle on real files whose header probe can reach 5 lines x 2 MiB.
LONG_LINE_PARITY_CASES = {
    "late_quoted_field_1200k": lambda: "a;b\n" + "x" * 1_200_000 + ',"hi",1\n',
    "late_doubled_quote_500k": lambda: 'a,b\n"x",' + "a" * 500_000 + ',"say ""hi"""\n',
    "pattern1_long_body_1200k": lambda: 'a,b\n1,"' + "a" * 1_200_000 + '",2\n',
    "pattern1_long_body_doubled_1200k": lambda: 'a,b\n1,"' + "a" * 1_200_000 + '""",2\n',
    "pattern2_long_body_1200k": lambda: "a;b\n'" + "a" * 1_200_000 + "',1\n",
    "pattern3_long_body_1200k": lambda: "a;b\n1,'" + "a" * 1_200_000 + "'\n",
    "pattern4_long_body_1200k": lambda: "'" + "a" * 1_200_000 + "'\n",
    "unterminated_500k_then_valid": lambda: 'id,note\n1,"' + "a" * 500_000 + '\n2,"ok",3\n',
    "doubled_unterminated_500k_then_valid": lambda: 'a,"' + '""' * 250_000 + '\n1,"x",2\n',
    "comma_quote_openers_500k": lambda: REDOS_SHAPES["comma_quote_openers"](500_000),
    "mixed_quote_openers_500k": lambda: REDOS_SHAPES["mixed_quote_openers"](500_000),
    "quote_run_500k": lambda: REDOS_SHAPES["quote_run"](500_000),
}


@pytest.mark.parametrize("case", sorted(LONG_LINE_PARITY_CASES))
def test_long_line_sniff_and_probe_parity(tmp_path, case):
    """#381: Rust and Python oracle must agree on very long lines (500k-1.2M chars)."""
    path = tmp_path / f"{case}.csv"
    path.write_text(LONG_LINE_PARITY_CASES[case]())

    assert datagrunt_rs.probe_csv_header(str(path)) == py.probe_csv_header(str(path)), case
    assert datagrunt_rs.sniff_dialect(str(path)) == py.sniff_dialect(str(path)), case
