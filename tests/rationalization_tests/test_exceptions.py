"""Tests for ``RationalizationError`` (``datagrunt.rationalization.exceptions``)."""

import pickle

import pytest

from datagrunt.pdf import DatagruntPDFError
from datagrunt.rationalization import RationalizationError, exceptions


def test_is_a_datagrunt_pdf_error():
    assert issubclass(RationalizationError, DatagruntPDFError)


def test_the_pdf_family_catches_it():
    with pytest.raises(DatagruntPDFError, match="schema has no Key field"):
        raise RationalizationError("schema has no Key field")


def test_message_is_preserved():
    error = RationalizationError("max_retries must be <= 10, got 11")

    assert str(error) == "max_retries must be <= 10, got 11"
    assert error.args == ("max_retries must be <= 10, got 11",)


def test_package_name_is_the_defining_modules_class():
    assert RationalizationError is exceptions.RationalizationError
    assert RationalizationError.__module__ == "datagrunt.rationalization.exceptions"


def test_pickle_round_trip_keeps_the_message():
    # Same-process round trip of an error built here (no untrusted bytes): errors cross thread and process
    # boundaries.
    restored = pickle.loads(pickle.dumps(RationalizationError("bad schema")))

    assert isinstance(restored, RationalizationError)
    assert str(restored) == "bad schema"
