"""Tests that an invalid engine name fails fast at construction.

Regression coverage for issue #92: the engine name was validated lazily (on
first read) and never at all for empty/blank source files.
"""

import pytest

from datagrunt import CSVReader, CSVWriter


class TestEngineNameValidationAtConstruction:
    """An invalid engine name must raise ValueError when the object is built."""

    def test_reader_invalid_engine_raises_at_construction(self, sample_csv):
        """A typo engine name raises ValueError immediately for a normal file."""
        with pytest.raises(ValueError):
            CSVReader(sample_csv, engine="polrs")

    def test_writer_invalid_engine_raises_at_construction(self, sample_csv):
        """A typo engine name raises ValueError immediately for a normal file."""
        with pytest.raises(ValueError):
            CSVWriter(sample_csv, engine="polrs")

    def test_reader_invalid_engine_raises_for_empty_file(self, empty_csv):
        """The typo must surface even when the source file is empty."""
        with pytest.raises(ValueError):
            CSVReader(empty_csv, engine="polrs")

    def test_writer_invalid_engine_raises_for_empty_file(self, empty_csv):
        """The typo must surface even when the source file is empty."""
        with pytest.raises(ValueError):
            CSVWriter(empty_csv, engine="polrs")

    def test_reader_invalid_engine_raises_for_blank_file(self, blank_csv):
        """The typo must surface even when the source file is only whitespace."""
        with pytest.raises(ValueError):
            CSVReader(blank_csv, engine="polrs")

    def test_writer_invalid_engine_raises_for_blank_file(self, blank_csv):
        """The typo must surface even when the source file is only whitespace."""
        with pytest.raises(ValueError):
            CSVWriter(blank_csv, engine="polrs")

    def test_reader_valid_engine_still_constructs(self, sample_csv):
        """A valid engine name constructs without raising."""
        for engine in ("duckdb", "polars", "pyarrow"):
            assert CSVReader(sample_csv, engine=engine) is not None

    def test_writer_valid_engine_still_constructs(self, sample_csv):
        """A valid engine name constructs without raising."""
        for engine in ("duckdb", "polars", "pyarrow"):
            assert CSVWriter(sample_csv, engine=engine) is not None
