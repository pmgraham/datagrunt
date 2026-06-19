"""Regression tests for issue #224 robustness fixes (items 4, 6, 7)."""

import json

import pytest


def test_load_spatial_flag_not_set_when_load_fails(monkeypatch, tmp_path):
    """Item 4: if INSTALL succeeds but LOAD fails, _spatial_installed stays False
    so the next connection retries instead of LOADing a never-installed extension.
    """
    from datagrunt.core.databases.databases import DuckDBQueries

    monkeypatch.setattr(DuckDBQueries, "_spatial_installed", False)  # auto-restored
    csv = tmp_path / "x.csv"
    csv.write_text("a,b\n1,2\n")
    q = DuckDBQueries(str(csv))

    class FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql):
            self.calls.append(sql)
            if sql.startswith("LOAD"):
                raise RuntimeError("load failed")

    conn = FakeConn()
    with pytest.raises(RuntimeError, match="load failed"):
        q.load_spatial_extension(conn)

    assert DuckDBQueries._spatial_installed is False  # NOT set after LOAD failure
    assert conn.calls == ["INSTALL spatial;", "LOAD spatial;"]  # INSTALL ran, LOAD attempted


def test_json_top_level_non_object_raises_valueerror(tmp_path):
    """Item 6: a .json document that deserializes to a non-dict (e.g. a list)
    raises a clear ValueError instead of a bare AttributeError deep in .get(...).
    """
    from datagrunt.core.pdf_io.pdfcomponents import PDFComponents

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps([1, 2, 3]))
    with pytest.raises(ValueError, match="Expected a JSON object"):
        PDFComponents(str(bad))


def test_pdfium_page_closes_page_when_textpage_acquisition_fails():
    """Item 7: PdfiumPage closes the page handle if get_textpage() raises in
    __init__, so a corrupt page does not leak the handle (close()/__exit__ would
    otherwise never run because no PdfiumPage object is returned).
    """
    from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumPage

    closed = {"n": 0}

    class FakePage:
        def get_textpage(self):
            raise RuntimeError("corrupt page")

        def close(self):
            closed["n"] += 1

    with pytest.raises(RuntimeError, match="corrupt page"):
        PdfiumPage(FakePage(), None)

    assert closed["n"] == 1  # page handle closed despite textpage failure


def test_pdf_set_export_filename_empty_raises_consistent_with_csv():
    """Item 3+5 consistency: the PDF set_export_filename mirrors the CSV one —
    empty/whitespace raises ValueError; None uses the default; a real name returns."""
    from datagrunt.core.pdf_io.engines import set_export_filename

    assert set_export_filename("output.json", None) == "output.json"
    assert set_export_filename("output.json", "custom.json") == "custom.json"
    with pytest.raises(ValueError, match="must not be empty"):
        set_export_filename("output.json", "")
    with pytest.raises(ValueError, match="must not be empty"):
        set_export_filename("output.json", "   ")
