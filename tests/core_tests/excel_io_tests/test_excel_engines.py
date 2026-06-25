"""Tests for ExcelReaderEngine."""

import pytest
import polars as pl
import pyarrow as pa

from datagrunt.core.excel_io import ExcelReaderEngine


def test_to_dataframe_defaults_to_first_sheet(sample_xlsx):
    df = ExcelReaderEngine(sample_xlsx).to_dataframe()
    assert df.columns == ["name", "age", "city"]
    assert df.height == 2


def test_to_dataframe_by_sheet_name(sample_xlsx):
    df = ExcelReaderEngine(sample_xlsx).to_dataframe(sheet="Products")
    assert df.columns == ["product", "price"]


def test_to_dataframe_by_sheet_index(sample_xlsx):
    df = ExcelReaderEngine(sample_xlsx).to_dataframe(sheet=1)
    assert df.columns == ["product", "price"]


def test_normalize_columns_constructor_flag(sample_xlsx):
    df = ExcelReaderEngine(sample_xlsx, normalize_columns=True).to_dataframe(sheet="Messy")
    assert df.columns == ["first_name", "age"]


def test_normalize_columns_per_call_override(sample_xlsx):
    df = ExcelReaderEngine(sample_xlsx).to_dataframe(sheet="Messy", normalize_columns=True)
    assert df.columns == ["first_name", "age"]


def test_get_sample_caps_rows(sample_xlsx):
    sample = ExcelReaderEngine(sample_xlsx).get_sample()
    assert isinstance(sample, pl.DataFrame)
    assert sample.height <= 20


def test_to_arrow_table(sample_xlsx):
    table = ExcelReaderEngine(sample_xlsx).to_arrow_table(sheet="Products")
    assert isinstance(table, pa.Table)
    assert table.column_names == ["product", "price"]


def test_to_dicts(sample_xlsx):
    rows = ExcelReaderEngine(sample_xlsx).to_dicts()
    assert rows[0]["name"] == "John"


def test_query_data_returns_relation(sample_xlsx):
    engine = ExcelReaderEngine(sample_xlsx)
    rel = engine.query_data(f"SELECT count(*) AS n FROM {engine.db_table}")
    assert rel.fetchone()[0] == 2
    engine.close()


def test_query_data_targets_selected_sheet(sample_xlsx):
    engine = ExcelReaderEngine(sample_xlsx)
    rel = engine.query_data(f"SELECT product FROM {engine.db_table}", sheet="Products")
    assert rel.fetchone()[0] == "A"
    engine.close()


def test_db_table_is_sheet_independent(sample_xlsx):
    engine = ExcelReaderEngine(sample_xlsx)
    assert engine.db_table == "tbl_test"


def test_read_options_per_call_n_rows(sample_xlsx):
    df = ExcelReaderEngine(sample_xlsx).to_dataframe(read_options={"n_rows": 1})
    assert df.height == 1


def test_read_options_has_header_false(sample_xlsx):
    # has_header=False turns the header row into data: 1 header + 2 rows = 3.
    df = ExcelReaderEngine(sample_xlsx).to_dataframe(sheet="Products", has_header=False)
    assert df.height == 3


def test_read_options_constructor_default(sample_xlsx):
    df = ExcelReaderEngine(sample_xlsx, read_options={"n_rows": 1}).to_dataframe()
    assert df.height == 1


def test_reserved_read_option_raises(sample_xlsx):
    with pytest.raises(ValueError, match="controlled via"):
        ExcelReaderEngine(sample_xlsx).to_dataframe(sheet_name="People")


# ---------------------------------------------------------------------------
# ExcelWriterEngine tests
# ---------------------------------------------------------------------------

from datagrunt.core.excel_io import ExcelWriterEngine


def test_writer_write_csv_first_sheet(sample_xlsx, tmp_path):
    out = str(tmp_path / "out.csv")
    ExcelWriterEngine(sample_xlsx).write_csv(out)
    assert pl.read_csv(out).columns == ["name", "age", "city"]


def test_writer_write_parquet_by_sheet(sample_xlsx, tmp_path):
    out = str(tmp_path / "out.parquet")
    ExcelWriterEngine(sample_xlsx).write_parquet(out, sheet="Products")
    assert pl.read_parquet(out).columns == ["product", "price"]


def test_writer_all_sheets_one_file_per_sheet(sample_xlsx, tmp_path):
    out = str(tmp_path / "out.csv")
    ExcelWriterEngine(sample_xlsx).write_csv(out, all_sheets=True)
    assert (tmp_path / "out_People.csv").exists()
    assert (tmp_path / "out_Products.csv").exists()
    assert (tmp_path / "out_Messy.csv").exists()
    assert not (tmp_path / "out.csv").exists()


def test_writer_all_sheets_excel_is_multi_tab(sample_xlsx, tmp_path):
    out = str(tmp_path / "out.xlsx")
    ExcelWriterEngine(sample_xlsx).write_excel(out, all_sheets=True)
    sheets = pl.read_excel(out, sheet_id=0)
    assert set(sheets) == {"People", "Products", "Messy"}


def test_writer_all_sheets_with_explicit_sheet_raises(sample_xlsx, tmp_path):
    with pytest.raises(ValueError, match="not both"):
        ExcelWriterEngine(sample_xlsx).write_csv(str(tmp_path / "o.csv"), sheet="People", all_sheets=True)


def test_writer_write_excel_single_sheet(sample_xlsx, tmp_path):
    out = str(tmp_path / "single.xlsx")
    ExcelWriterEngine(sample_xlsx).write_excel(out, sheet="Products")
    assert pl.read_excel(out).columns == ["product", "price"]


# ---------------------------------------------------------------------------
# ExcelEngineFactory tests
# ---------------------------------------------------------------------------

from datagrunt.core import ExcelEngineFactory as ExcelEngineFactoryFromCore
from datagrunt.core.excel_io import ExcelEngineFactory


def test_factory_creates_reader(sample_xlsx):
    engine = ExcelEngineFactory(sample_xlsx).create_reader()
    assert engine.to_dataframe().height == 2


def test_factory_creates_writer(sample_xlsx, tmp_path):
    engine = ExcelEngineFactory(sample_xlsx).create_writer()
    out = str(tmp_path / "f.csv")
    engine.write_csv(out)
    assert pl.read_csv(out).height == 2


def test_factory_missing_file_raises(nonexistent_xlsx):
    with pytest.raises(FileNotFoundError):
        ExcelEngineFactory(nonexistent_xlsx)


def test_core_reexports_factory(sample_xlsx):
    assert ExcelEngineFactoryFromCore is ExcelEngineFactory
