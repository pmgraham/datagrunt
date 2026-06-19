"""Unit tests for the _polars_read_csv and _polars_read_to_string_arrow helpers.

These helpers are the single source of truth for the parity-critical Polars
read options shared by all three Polars-backed CSV read paths. Tests verify:
  - Leading comment lines are skipped (not treated as data).
  - Ragged rows are truncated when truncate_ragged_lines=True.
  - n_rows limits the result to the requested count.
  - All returned columns are typed as pa.string() (arrow helper).
"""

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.core.csv_io.engines import (
    _polars_read_csv,
    _polars_read_to_string_arrow,
)


@pytest.fixture()
def comment_csv(tmp_path):
    """CSV with two leading comment lines before the header."""
    p = tmp_path / "comments.csv"
    p.write_text("# first comment\n# second comment\nname,color\nalice,#FF0000\nbob,#00FF00\n")
    return p


@pytest.fixture()
def ragged_csv(tmp_path):
    """CSV with a ragged (extra-field) row."""
    p = tmp_path / "ragged.csv"
    p.write_text("a,b\n1,2\n3,4,EXTRA\n5,6\n")
    return p


class TestPolarsReadCsv:
    def test_leading_comments_are_skipped(self, comment_csv):
        df = _polars_read_csv(comment_csv, delimiter=",", truncate_ragged_lines=False)
        # Header must be the real header, not a comment line.
        assert df.columns == ["name", "color"]
        # Data rows only — comments are not treated as data rows.
        assert len(df) == 2
        assert df["name"].to_list() == ["alice", "bob"]

    def test_hash_value_in_data_is_preserved(self, comment_csv):
        """A '#' value in a data column must NOT be silently dropped."""
        df = _polars_read_csv(comment_csv, delimiter=",", truncate_ragged_lines=False)
        assert "#FF0000" in df["color"].to_list()

    def test_ragged_rows_truncated(self, ragged_csv):
        df = _polars_read_csv(ragged_csv, delimiter=",", truncate_ragged_lines=True)
        assert df.columns == ["a", "b"]
        assert len(df) == 3

    def test_n_rows_limits_result(self, comment_csv):
        df = _polars_read_csv(comment_csv, delimiter=",", truncate_ragged_lines=False, n_rows=1)
        assert len(df) == 1
        assert df["name"][0] == "alice"

    def test_returns_polars_dataframe(self, comment_csv):
        result = _polars_read_csv(comment_csv, delimiter=",", truncate_ragged_lines=False)
        assert isinstance(result, pl.DataFrame)

    def test_infer_schema_false_all_string_dtypes(self, comment_csv):
        df = _polars_read_csv(comment_csv, delimiter=",", truncate_ragged_lines=False)
        for dtype in df.dtypes:
            assert dtype == pl.String


class TestPolarsReadToStringArrow:
    def test_returns_pyarrow_table(self, comment_csv):
        table = _polars_read_to_string_arrow(comment_csv, delimiter=",", truncate_ragged_lines=False)
        assert isinstance(table, pa.Table)

    def test_all_columns_are_string_type(self, comment_csv):
        table = _polars_read_to_string_arrow(comment_csv, delimiter=",", truncate_ragged_lines=False)
        for field in table.schema:
            assert field.type == pa.string(), f"Column '{field.name}' has type {field.type}, expected string"

    def test_leading_comments_skipped(self, comment_csv):
        table = _polars_read_to_string_arrow(comment_csv, delimiter=",", truncate_ragged_lines=False)
        assert table.column_names == ["name", "color"]
        assert table.num_rows == 2

    def test_hash_data_value_preserved(self, comment_csv):
        table = _polars_read_to_string_arrow(comment_csv, delimiter=",", truncate_ragged_lines=False)
        colors = table.column("color").to_pylist()
        assert "#FF0000" in colors

    def test_ragged_rows_truncated(self, ragged_csv):
        table = _polars_read_to_string_arrow(ragged_csv, delimiter=",", truncate_ragged_lines=True)
        assert table.column_names == ["a", "b"]
        assert table.num_rows == 3

    def test_n_rows_limits_result(self, comment_csv):
        table = _polars_read_to_string_arrow(comment_csv, delimiter=",", truncate_ragged_lines=False, n_rows=1)
        assert table.num_rows == 1
        assert table.column("name").to_pylist() == ["alice"]
