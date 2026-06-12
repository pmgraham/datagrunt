"""Parity tests for ragged-row detection (_check_csv_ragged_and_warn, bool only)."""

import warnings

import datagrunt_rs
from datagrunt.core.csv_io import csvcomponents as py
from datagrunt.core.csv_io.csvcomponents import CSVDelimiter


def check_ragged_py(path, delimiter):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return py._check_csv_ragged_and_warn(path, delimiter)


def test_check_ragged(corpus_file):
    delimiter = CSVDelimiter(corpus_file).delimiter
    assert datagrunt_rs.check_ragged(str(corpus_file), delimiter) == check_ragged_py(
        corpus_file, delimiter
    )
