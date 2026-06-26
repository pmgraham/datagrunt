"""Tests for Parquet components and helpers."""

import pytest

from datagrunt.core.parquet_io.parquetcomponents import (
    ParquetComponents,
    normalize_parquet_columns,
    parquet_table_name,
)


def test_normalize_parquet_columns_matches_compute_backend():
    assert normalize_parquet_columns(["First Name!", "#Age@"]) == ["first_name", "age"]


def test_parquet_table_name_is_sql_safe(sample_parquet):
    name = parquet_table_name(sample_parquet)
    assert name.startswith("tbl_")
    assert all(c.isalnum() or c == "_" for c in name)


def test_components_accepts_parquet(sample_parquet):
    components = ParquetComponents(sample_parquet)
    assert components.is_parquet


def test_components_rejects_non_parquet(sample_files):
    with pytest.raises(ValueError, match="not a Parquet file"):
        ParquetComponents(sample_files["data.csv"])
