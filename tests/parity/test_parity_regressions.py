"""Targeted regressions for sniffer parity bugs fixed for 4.0.0.

These cases don't fit the shared corpus: the adversarial input is multi-MB, and
the empty-delimiter case needs an explicit argument the corpus tests don't pass.
"""

from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py


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
