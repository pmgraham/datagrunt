"""Regression tests for issue #85.

Leading blank lines before the header must not corrupt column metadata on
any engine, and must not silently corrupt data on the pyarrow engine.
"""

import pytest

from datagrunt import CSVReader
from datagrunt.core import CSVColumns, CSVComponents

ENGINES = ("polars", "duckdb", "pyarrow")

# Two leading blank lines, then header, then two data rows.
LEADING_BLANK_LINES_CONTENT = "\n\nname,age\nalice,30\nbob,25\n"


@pytest.fixture
def leading_blank_lines_csv(tmp_path):
    """A CSV file whose header is preceded by leading blank lines."""
    csv_file = tmp_path / "leading_blank_lines.csv"
    csv_file.write_text(LEADING_BLANK_LINES_CONTENT)
    return str(csv_file)


class TestLeadingBlankLinesColumns:
    """Column metadata must be correct regardless of leading blank lines."""

    def test_csvcolumns_header_not_taken_from_data(self, leading_blank_lines_csv):
        columns = CSVColumns(leading_blank_lines_csv)
        assert columns.columns == ["name", "age"]
        assert columns.columns_count == 2

    def test_components_columns_metadata(self, leading_blank_lines_csv):
        components = CSVComponents(leading_blank_lines_csv)
        assert components.columns == ["name", "age"]
        assert components.columns_normalized == ["name", "age"]
        assert components.columns_count == 2

    @pytest.mark.parametrize("engine", ENGINES)
    def test_reader_columns_metadata(self, leading_blank_lines_csv, engine):
        reader = CSVReader(leading_blank_lines_csv, engine=engine)
        assert reader.columns == ["name", "age"]
        assert reader.columns_normalized == ["name", "age"]
        assert reader.columns_count == 2


class TestLeadingBlankLinesData:
    """Data must be parsed correctly regardless of leading blank lines."""

    @pytest.mark.parametrize("engine", ENGINES)
    def test_dataframe_header_and_data(self, leading_blank_lines_csv, engine):
        reader = CSVReader(leading_blank_lines_csv, engine=engine)
        df = reader.to_dataframe()

        # Header must come from the real header row, not a data row.
        assert list(df.columns) == ["name", "age"]

        # All engines return a Polars dataframe from to_dataframe().
        records = df.to_dicts()
        names = [str(r["name"]) for r in records]
        ages = [str(r["age"]) for r in records]
        assert names == ["alice", "bob"]
        assert ages == ["30", "25"]
