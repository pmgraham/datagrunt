import pytest

from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py

# Corpus cases with a known, tracked backend divergence, mapped to the reason.
# Applied per-case as a STRICT xfail: fixing the divergence turns it into an
# XPASS failure, which forces the entry (and this table) out again.
COUNT_LEADING_PHYSICAL_LINES_DIVERGENCES = {
    "invalid_utf8_only_no_newline.csv": (
        "bytes that decode to '' under errors='ignore' with no terminator: Rust counts a final empty line "
        "(1), CPython iterates decoded text and yields none (0), see #NNN"
    ),
}


def test_is_legacy_mac_newlines(corpus_file):
    assert datagrunt_rs.is_legacy_mac_newlines(str(corpus_file)) == py.is_legacy_mac_newlines(str(corpus_file))


def test_count_leading_comments(corpus_file):
    assert datagrunt_rs.count_leading_comments(str(corpus_file)) == py.count_leading_comments(str(corpus_file))


def test_count_leading_physical_lines_before_header(corpus_file, request):
    reason = COUNT_LEADING_PHYSICAL_LINES_DIVERGENCES.get(corpus_file.name)
    if reason:
        request.applymarker(pytest.mark.xfail(strict=True, reason=reason))
    assert datagrunt_rs.count_leading_physical_lines_before_header(
        str(corpus_file)
    ) == py.count_leading_physical_lines_before_header(str(corpus_file))


def test_first_row(corpus_file):
    assert datagrunt_rs.first_row(str(corpus_file)) == py.first_row(str(corpus_file))


def test_leading_rows(corpus_file):
    for limit in (1, 2, 5, 100):
        assert datagrunt_rs.leading_rows(str(corpus_file), limit) == py.leading_rows(str(corpus_file), limit)


def test_probe_csv_header(corpus_file):
    rust = datagrunt_rs.probe_csv_header(str(corpus_file))
    python = py.probe_csv_header(str(corpus_file))
    assert rust == python, corpus_file.name
