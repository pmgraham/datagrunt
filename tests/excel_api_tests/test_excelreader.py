import polars as pl
import pyarrow as pa
import pytest

from datagrunt import ExcelReader


def test_sheets_property(sample_xlsx):
    assert ExcelReader(sample_xlsx).sheets == ["People", "Products", "Messy"]


def test_to_dataframe_default_first_sheet(sample_xlsx):
    df = ExcelReader(sample_xlsx).to_dataframe()
    assert df.columns == ["name", "age", "city"]


def test_to_dataframe_by_name(sample_xlsx):
    assert ExcelReader(sample_xlsx).to_dataframe(sheet="Products").columns == ["product", "price"]


def test_constructor_normalize(sample_xlsx):
    assert ExcelReader(sample_xlsx, normalize_columns=True).to_dataframe(sheet="Messy").columns == [
        "first_name",
        "age",
    ]


def test_to_dicts(sample_xlsx):
    assert ExcelReader(sample_xlsx).to_dicts()[0]["name"] == "John"


def test_to_arrow_table(sample_xlsx):
    assert isinstance(ExcelReader(sample_xlsx).to_arrow_table(), pa.Table)


def test_query_data(sample_xlsx):
    xl = ExcelReader(sample_xlsx)
    rel = xl.query_data(f"SELECT count(*) AS n FROM {xl.db_table}")
    assert rel.fetchone()[0] == 2


def test_read_options_passthrough(sample_xlsx):
    assert ExcelReader(sample_xlsx).to_dataframe(read_options={"n_rows": 1}).height == 1


def test_reserved_read_option_raises(sample_xlsx):
    with pytest.raises(ValueError, match="controlled via"):
        ExcelReader(sample_xlsx).to_dataframe(sheet_name="People")


def test_invalid_sheet_raises(sample_xlsx):
    with pytest.raises(ValueError, match="not found"):
        ExcelReader(sample_xlsx).to_dataframe(sheet="Nope")


def test_non_excel_raises(sample_csv):
    with pytest.raises(ValueError, match="not an Excel file"):
        ExcelReader(sample_csv)


def test_empty_workbook_returns_empty(empty_xlsx):
    xl = ExcelReader(empty_xlsx)
    assert xl.sheets == []
    assert xl.to_dataframe().is_empty()
    assert xl.to_dicts() == []


def test_context_manager_closes(sample_xlsx):
    with ExcelReader(sample_xlsx) as xl:
        xl.query_data(f"SELECT 1 FROM {xl.db_table}")
    # No assertion needed beyond clean exit; close() must be idempotent.
    xl.close()


def test_empty_workbook_to_arrow_table_returns_empty(empty_xlsx):
    """to_arrow_table() on an empty workbook returns an empty pa.Table."""
    table = ExcelReader(empty_xlsx).to_arrow_table()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 0
    assert table.num_columns == 0


def test_get_sample_normal_returns_dataframe(sample_xlsx):
    """get_sample() on a normal workbook returns a pl.DataFrame."""
    sample = ExcelReader(sample_xlsx).get_sample()
    assert isinstance(sample, pl.DataFrame)
    assert sample.height > 0


def test_get_sample_empty_workbook_returns_empty_dataframe(empty_xlsx):
    """get_sample() on an empty workbook returns an empty pl.DataFrame."""
    sample = ExcelReader(empty_xlsx).get_sample()
    assert isinstance(sample, pl.DataFrame)
    assert sample.is_empty()


def test_excelreader_to_dataframe_flat_kwargs(sample_xlsx):
    """Verify that flat kwargs are passed to pl.read_excel."""
    df = ExcelReader(sample_xlsx).to_dataframe(has_header=False)
    assert df.height == 3  # Header is treated as data row
    assert df.row(0) == ("name", "age", "city")


def test_excelreader_to_dataframe_read_options_deprecation(sample_xlsx):
    """Verify that passing read_options as a dict raises a DeprecationWarning."""
    reader = ExcelReader(sample_xlsx)
    with pytest.warns(DeprecationWarning, match="Passing 'read_options' as a dictionary is deprecated"):
        df = reader.to_dataframe(read_options={"n_rows": 1})

    assert df.height == 1
