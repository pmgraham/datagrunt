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


# Note: there is intentionally no separate ``row_count_without_header`` parity
# test. Both backends expose only ``row_count_with_header``; the without-header
# count is ``with_header - 1`` at the CSVRows layer (covered in the core tests),
# so a backend-level ``(x-1) == (y-1)`` assertion is just the test above.
