import os

import polars as pl
import pytest

from datagrunt import ExcelWriter


def test_write_csv_first_sheet(sample_xlsx, tmp_path):
    out = str(tmp_path / "o.csv")
    ExcelWriter(sample_xlsx).write_csv(out)
    assert pl.read_csv(out).columns == ["name", "age", "city"]


def test_write_json_and_jsonl(sample_xlsx, tmp_path):
    w = ExcelWriter(sample_xlsx)
    j, jl = str(tmp_path / "o.json"), str(tmp_path / "o.jsonl")
    w.write_json(j, sheet="Products")
    w.write_json_newline_delimited(jl, sheet="Products")
    assert os.path.exists(j) and os.path.exists(jl)


def test_write_parquet(sample_xlsx, tmp_path):
    out = str(tmp_path / "o.parquet")
    ExcelWriter(sample_xlsx).write_parquet(out)
    assert pl.read_parquet(out).height == 2


def test_write_excel_single_and_multi(sample_xlsx, tmp_path):
    w = ExcelWriter(sample_xlsx)
    single, multi = str(tmp_path / "single.xlsx"), str(tmp_path / "multi.xlsx")
    w.write_excel(single, sheet="Products")
    w.write_excel(multi, all_sheets=True)
    assert pl.read_excel(single).columns == ["product", "price"]
    assert set(pl.read_excel(multi, sheet_id=0)) == {"People", "Products", "Messy"}


def test_all_sheets_one_file_per_sheet(sample_xlsx, tmp_path):
    out = str(tmp_path / "o.parquet")
    ExcelWriter(sample_xlsx).write_parquet(out, all_sheets=True)
    assert (tmp_path / "o_People.parquet").exists()
    assert (tmp_path / "o_Products.parquet").exists()


def test_normalize_columns(sample_xlsx, tmp_path):
    out = str(tmp_path / "o.csv")
    ExcelWriter(sample_xlsx, normalize_columns=True).write_csv(out, sheet="Messy")
    assert pl.read_csv(out).columns == ["first_name", "age"]


def test_conflicting_args_raise(sample_xlsx, tmp_path):
    with pytest.raises(ValueError, match="not both"):
        ExcelWriter(sample_xlsx).write_csv(str(tmp_path / "o.csv"), sheet="People", all_sheets=True)


def test_read_options_passthrough(sample_xlsx, tmp_path):
    out = str(tmp_path / "o.csv")
    ExcelWriter(sample_xlsx).write_csv(out, read_options={"n_rows": 1})
    assert pl.read_csv(out).height == 1


def test_empty_source_writes_empty_file(empty_xlsx, tmp_path):
    out = str(tmp_path / "o.csv")
    ExcelWriter(empty_xlsx).write_csv(out)
    assert os.path.exists(out) and os.path.getsize(out) == 0


def test_non_excel_raises(sample_csv):
    with pytest.raises(ValueError, match="not an Excel file"):
        ExcelWriter(sample_csv)
