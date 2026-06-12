"""Parity tests for csv-record-aware row counting (CSVRows.row_count_with_header)."""

import datagrunt_rs
from datagrunt.core.csv_io.csvcomponents import CSVDelimiter, CSVRows


def test_row_count_with_header(corpus_file):
    # Pass the Python-inferred delimiter to BOTH sides so this test isolates
    # row counting (delimiter parity is proven separately).
    delimiter = CSVDelimiter(corpus_file).delimiter
    assert datagrunt_rs.row_count_with_header(
        str(corpus_file), delimiter
    ) == CSVRows(corpus_file).row_count_with_header


def test_row_count_without_header_is_count_minus_one(corpus_file):
    delimiter = CSVDelimiter(corpus_file).delimiter
    rust_count = datagrunt_rs.row_count_with_header(str(corpus_file), delimiter)
    assert rust_count - 1 == CSVRows(corpus_file).row_count_without_header
