import warnings

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
    assert rel.fetchall()[0][0] == 2


def test_read_options_passthrough(sample_xlsx):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error", DeprecationWarning)
        df = ExcelReader(sample_xlsx).to_dataframe(read_options={"n_rows": 1})
    assert caught == []
    assert df.height == 1


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


def test_constructor_read_options_preamble_no_deprecation_warning(preamble_xlsx):
    """ExcelReader(..., read_options={'header_row': 3}) emits zero DeprecationWarnings across all read methods."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error", DeprecationWarning)
        with ExcelReader(preamble_xlsx, read_options={"header_row": 3}) as reader:
            sample = reader.get_sample()
            sample_capped = reader.get_sample(n_rows=2)
            df = reader.to_dataframe()
            table = reader.to_arrow_table()
            rows = reader.to_dicts()
            count = reader.query_data(f"SELECT count(*) FROM {reader.db_table}").fetchall()[0][0]
            names = [r[0] for r in reader.query_data(f"SELECT name FROM {reader.db_table}").fetchall()]

    assert caught == []
    assert sample.columns == ["name", "age", "city"]
    assert sample.height == 3
    assert sample_capped.height == 2
    assert df.columns == ["name", "age", "city"]
    assert df.height == 3
    assert df["name"].to_list() == ["Alice", "Bob", "Cara"]
    assert table.column_names == ["name", "age", "city"]
    assert table.num_rows == 3
    assert len(rows) == 3
    assert rows[0] == {"name": "Alice", "age": 30, "city": "New York"}
    assert count == 3
    assert names == ["Alice", "Bob", "Cara"]


def test_per_call_read_options_preamble_no_deprecation_warning(preamble_xlsx):
    """Per-call read_options={'header_row': 3, 'n_rows': 2} emits zero DeprecationWarnings across all read methods."""
    opts = {"header_row": 3, "n_rows": 2}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error", DeprecationWarning)
        with ExcelReader(preamble_xlsx) as reader:
            sample = reader.get_sample(read_options=opts)
            df = reader.to_dataframe(read_options=opts)
            table = reader.to_arrow_table(read_options=opts)
            rows = reader.to_dicts(read_options=opts)
            count = reader.query_data(f"SELECT count(*) FROM {reader.db_table}", read_options=opts).fetchall()[0][0]

    assert caught == []
    assert sample.columns == ["name", "age", "city"]
    assert sample.height == 2
    assert df.columns == ["name", "age", "city"]
    assert df.height == 2
    assert df["name"].to_list() == ["Alice", "Bob"]
    assert table.column_names == ["name", "age", "city"]
    assert table.num_rows == 2
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert count == 2


def test_read_options_constructor_and_per_call_merge(preamble_xlsx):
    """Constructor and per-call options merge cleanly without DeprecationWarnings."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error", DeprecationWarning)
        reader = ExcelReader(preamble_xlsx, read_options={"header_row": 3})
        # Merge constructor read_options with a per-call top-level Polars kwarg.
        df_subset_cols = reader.to_dataframe(columns=["name", "city"])
        # Per-call read_options overrides constructor read_options.
        df_limited = reader.to_dataframe(read_options={"header_row": 3, "n_rows": 1})

    assert caught == []
    assert df_subset_cols.columns == ["name", "city"]
    assert df_subset_cols.height == 3
    assert df_limited.columns == ["name", "age", "city"]
    assert df_limited.height == 1
    assert df_limited["name"].to_list() == ["Alice"]


def test_get_sample_n_rows(tmp_path):
    import polars as pl
    import xlsxwriter

    path = tmp_path / "wide.xlsx"
    with xlsxwriter.Workbook(str(path)) as wb:
        pl.DataFrame({"n": list(range(30))}).write_excel(workbook=wb, worksheet="Wide")
    reader = ExcelReader(str(path))
    assert reader.get_sample().height == 20  # default unchanged
    assert reader.get_sample(n_rows=5).height == 5
    assert reader.get_sample(n_rows=100).height == 30  # capped at sheet size


def test_get_sample_n_rows_invalid(sample_xlsx):
    reader = ExcelReader(sample_xlsx)
    for bad in (0, -3, 2.5, "ten", True):
        with pytest.raises(ValueError):
            reader.get_sample(n_rows=bad)


def test_get_sample_n_rows_invalid_on_empty_file(empty_xlsx):
    """Invalid n_rows raises even when the workbook is empty."""
    with pytest.raises(ValueError):
        ExcelReader(empty_xlsx).get_sample(n_rows=0)
