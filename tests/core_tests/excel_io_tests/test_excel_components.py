import pytest

from datagrunt.core.excel_io import (
    ExcelComponents,
    excel_table_name,
    normalize_excel_columns,
    resolve_sheet,
)


def test_sheets_lists_workbook_order(sample_xlsx):
    assert ExcelComponents(sample_xlsx).sheets == ["People", "Products", "Messy"]


def test_sheets_empty_for_empty_file(empty_xlsx):
    assert ExcelComponents(empty_xlsx).sheets == []


def test_non_excel_path_raises(sample_csv):
    with pytest.raises(ValueError, match="not an Excel file"):
        ExcelComponents(sample_csv)


def test_normalize_excel_columns_matches_compute():
    assert normalize_excel_columns(["First Name!", "#Age@"]) == ["first_name", "age"]


def test_excel_table_name_is_sanitized(tmp_path):
    f = tmp_path / "my data.xlsx"
    f.touch()
    assert excel_table_name(str(f)) == "tbl_my_data"


def test_resolve_sheet_default_first():
    assert resolve_sheet(["A", "B"], None) == "A"


def test_resolve_sheet_by_index():
    assert resolve_sheet(["A", "B"], 1) == "B"


def test_resolve_sheet_by_name():
    assert resolve_sheet(["A", "B"], "B") == "B"


def test_resolve_sheet_bad_name_raises():
    with pytest.raises(ValueError, match="not found"):
        resolve_sheet(["A", "B"], "C")


def test_resolve_sheet_bad_index_raises():
    with pytest.raises(ValueError, match="out of range"):
        resolve_sheet(["A", "B"], 5)


def test_resolve_sheet_no_sheets_raises():
    with pytest.raises(ValueError, match="no sheets"):
        resolve_sheet([], None)
