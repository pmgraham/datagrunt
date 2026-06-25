import polars as pl


def test_sample_xlsx_fixture_has_three_sheets(sample_xlsx):
    sheets = pl.read_excel(sample_xlsx, sheet_id=0)  # dict of all sheets
    assert set(sheets) == {"People", "Products", "Messy"}
