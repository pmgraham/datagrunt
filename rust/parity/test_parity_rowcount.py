"""Parity tests for csv-record-aware row counting (CSVRows.row_count_with_header)."""

from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py


def test_row_count_with_header(corpus_file):
    # Pass the Python-inferred delimiter to BOTH sides so this test isolates
    # row counting (delimiter parity is proven separately).
    delimiter = py.infer_delimiter(str(corpus_file))
    assert datagrunt_rs.row_count_with_header(
        str(corpus_file), delimiter
    ) == py.row_count_with_header(str(corpus_file), delimiter)


def test_row_count_without_header_is_count_minus_one(corpus_file):
    delimiter = py.infer_delimiter(str(corpus_file))
    rust_count = datagrunt_rs.row_count_with_header(str(corpus_file), delimiter)
    py_count = py.row_count_with_header(str(corpus_file), delimiter)
    assert rust_count - 1 == py_count - 1, "row_count_without_header parity"
