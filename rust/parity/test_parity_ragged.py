"""Parity tests for ragged-row detection (_check_csv_ragged_and_warn, bool only)."""

from datagrunt import _native as datagrunt_rs
from datagrunt.core.csv_io import _compute_python as py


def test_check_ragged(corpus_file):
    delimiter = py.infer_delimiter(str(corpus_file))
    assert datagrunt_rs.check_ragged(str(corpus_file), delimiter) == py.check_ragged(
        str(corpus_file), delimiter
    )
