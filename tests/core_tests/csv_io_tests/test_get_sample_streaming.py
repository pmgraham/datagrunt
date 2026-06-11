"""Tests that ``get_sample`` streams the first N rows instead of materializing
the entire file on the pyarrow and duckdb engines.

See https://github.com/pmgraham/datagrunt/issues/105.
"""

import polars as pl
import pyarrow.csv as pacsv
import pytest

from datagrunt.core import (
    CSVEngineFactory,
    CSVEngineProperties,
)
from datagrunt.core.databases import databases as databases_module

ALL_ENGINES = ["duckdb", "polars", "pyarrow"]
SAMPLE_ROWS = CSVEngineProperties.dataframe_sample_rows


@pytest.fixture
def large_csv(tmp_path):
    """A CSV with many more rows than the sample size.

    The row ``id`` equals its position so we can assert ordering precisely.
    """
    total_rows = SAMPLE_ROWS * 50
    lines = ["id,name,city"]
    for i in range(total_rows):
        lines.append(f"{i},name_{i},city_{i}")
    csv_file = tmp_path / "large.csv"
    csv_file.write_text("\n".join(lines))
    return str(csv_file)


def _sample(engine, filepath, normalize_columns=False):
    reader = CSVEngineFactory(filepath, engine).create_reader()
    return reader.get_sample(normalize_columns=normalize_columns)


def test_get_sample_returns_same_first_n_rows_across_engines(large_csv):
    """All three engines return identical first-N rows for the sample."""
    samples = {engine: _sample(engine, large_csv) for engine in ALL_ENGINES}

    for engine, sample in samples.items():
        assert isinstance(sample, pl.DataFrame), engine
        assert len(sample) == SAMPLE_ROWS, engine
        # First-N rows means ids 0..N-1 in order.
        assert sample["id"].to_list() == [str(i) for i in range(SAMPLE_ROWS)], engine

    reference = samples["polars"]
    for engine, sample in samples.items():
        assert sample.columns == reference.columns, engine
        assert sample.to_dicts() == reference.to_dicts(), engine


def test_get_sample_normalized_same_first_n_rows_across_engines(large_csv):
    """Normalized samples are also identical first-N rows across engines."""
    samples = {engine: _sample(engine, large_csv, normalize_columns=True) for engine in ALL_ENGINES}

    reference = samples["polars"]
    for engine, sample in samples.items():
        assert len(sample) == SAMPLE_ROWS, engine
        assert sample.columns == reference.columns, engine
        assert sample.to_dicts() == reference.to_dicts(), engine


def test_pyarrow_get_sample_does_not_fully_read_the_file(large_csv, monkeypatch):
    """The pyarrow sample must not call ``pacsv.read_csv`` (whole-file read)."""
    calls = []
    original_read_csv = pacsv.read_csv

    def spy_read_csv(*args, **kwargs):
        calls.append((args, kwargs))
        return original_read_csv(*args, **kwargs)

    # Patch the symbol as referenced inside the engines module.
    import datagrunt.core.csv_io.engines as engines_module

    monkeypatch.setattr(engines_module.pacsv, "read_csv", spy_read_csv)

    sample = _sample("pyarrow", large_csv)
    assert len(sample) == SAMPLE_ROWS
    assert calls == [], "pyarrow get_sample fully materialized the file via pacsv.read_csv"


def test_duckdb_get_sample_does_not_create_full_table(large_csv, monkeypatch):
    """The duckdb sample must not import the whole file into a table."""
    calls = []
    original_create_table = databases_module.DuckDBQueries.create_table

    def spy_create_table(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original_create_table(self, *args, **kwargs)

    monkeypatch.setattr(databases_module.DuckDBQueries, "create_table", spy_create_table)

    sample = _sample("duckdb", large_csv)
    assert len(sample) == SAMPLE_ROWS
    assert calls == [], "duckdb get_sample materialized the full table via create_table"


def test_get_sample_preserves_comment_handling(tmp_path):
    """Leading comment lines are skipped consistently across engines."""
    content = "# a leading comment\n# another comment\nid,name\n0,zero\n1,one\n2,two"
    csv_file = tmp_path / "commented.csv"
    csv_file.write_text(content)
    filepath = str(csv_file)

    samples = {engine: _sample(engine, filepath) for engine in ALL_ENGINES}
    reference = samples["polars"]
    for engine, sample in samples.items():
        assert sample.columns == reference.columns, engine
        assert sample.to_dicts() == reference.to_dicts(), engine
