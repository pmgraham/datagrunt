"""Table cells read as written on /Rotate pages (#387).

pdfplumber lays a page out with /Rotate applied and reads each table's grid
and cell text in display order. When the text stands upright on the
unrotated page (as when a viewer's rotate tool sets /Rotate), display order
is not the order it was written: /Rotate 90 and 270 transpose the grid, 180
and 270 reverse each cell's glyphs ("Part" reads "traP"), and multi-word or
multi-line cells are broken up and re-joined in another order.

The written grid is the one pdfplumber reads from the same page without
/Rotate. Table placement (#385) must not move, and cell text must still not
leak out as body text.
"""

from collections import Counter

import pytest

from datagrunt.core.pdf_io.extraction.tables import PdfPlumberTableExtractor
from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler

from .test_table_frame import _BACKENDS, _RULE_RGB, _rendered_rule_box, _xyxy

_ROTATIONS = (0, 90, 180, 270)
# "cropbox" exposes only (50, 40, 530, 720) of the 612x792 MediaBox.
_KINDS = ("none", "cropbox")

# Multi-word, multi-line and numeric cells: 4 rows x 3 columns, so a
# transposed grid (3 x 4) cannot pass for the written one.
_WRITTEN = [
    ["Part No", "Qty", "Unit price"],
    ["Hex Bolt", "12", "0.25"],
    ["Nut", "40", "0.05"],
    ["Spring\nwasher", "7", "1.10"],
]
_CELL_WORDS = {word for row in _WRITTEN for cell in row for word in cell.split()}
_ROWS, _COLS = len(_WRITTEN), len(_WRITTEN[0])
# A merged cell reads into its first slot; the slots it covers read None.
_MERGED_WRITTEN = [["Item", "Spec range", None], ["Bolt M12", "min 10", "max 14"], [None, "4.5", "6"]]
# PyMuPDF top-left page points: the table's left/top edge, column width, row height.
_TABLE_X, _TABLE_Y, _COL_W, _ROW_H = 100, 150, 120, 36
# Body text above the table's right end, well clear of the first row.
_TITLE, _TITLE_AT = "Inventory", (380, 100)


def _draw_table(page, with_text):
    right, bottom = _TABLE_X + _COLS * _COL_W, _TABLE_Y + _ROWS * _ROW_H
    for r in range(_ROWS + 1):
        y = _TABLE_Y + r * _ROW_H
        page.draw_line((_TABLE_X, y), (right, y), color=_RULE_RGB)
    for c in range(_COLS + 1):
        x = _TABLE_X + c * _COL_W
        page.draw_line((x, _TABLE_Y), (x, bottom), color=_RULE_RGB)
    if not with_text:
        return
    page.insert_text(_TITLE_AT, _TITLE, fontsize=12)
    for r, row in enumerate(_WRITTEN):
        for c, cell in enumerate(row):
            for i, line in enumerate(cell.split("\n")):
                page.insert_text((_TABLE_X + c * _COL_W + 6, _TABLE_Y + r * _ROW_H + 14 + i * 12), line, fontsize=10)


def _table_pdf(tmp_path, kind, rotation, *, with_text=True):
    """One page: the ruled table written upright, then turned by /Rotate ``rotation``."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    _draw_table(page, with_text)
    if kind == "cropbox":
        # PyMuPDF takes a top-left-based rect; this writes /CropBox [50 40 530 720].
        page.set_cropbox(pymupdf.Rect(50, 72, 530, 752))
    page.set_rotation(rotation)
    path = tmp_path / f"rotated_table_{kind}_{rotation}_{int(with_text)}.pdf"
    doc.save(str(path))
    doc.close()
    return str(path)


def _upright_display_pdf(tmp_path, rotation):
    """A /Rotate ``rotation`` page whose content is turned the other way, so it displays upright.

    Landscape pages made this way (e.g. LaTeX ``pdflscape``) already read as
    written in pdfplumber's display order; turning them back would break them.
    """
    import pymupdf

    displayed = (792, 612) if rotation in (90, 270) else (612, 792)
    src = pymupdf.open()
    _draw_table(src.new_page(width=displayed[0], height=displayed[1]), with_text=True)
    doc = pymupdf.open()
    page = doc.new_page(width=displayed[1], height=displayed[0]) if rotation in (90, 270) else doc.new_page()
    page.show_pdf_page(page.rect, src, 0, rotate={90: 90, 180: 180, 270: -90}[rotation])
    page.set_rotation(rotation)
    path = tmp_path / f"upright_display_{rotation}.pdf"
    doc.save(str(path))
    doc.close()
    src.close()
    return str(path)


def _merged_cell_table_pdf(tmp_path, rotation):
    """A 3x3 ruled table with merged cells: row 0 spans columns 1-2, column 0 spans rows 1-2.

    pdfplumber reads a merged cell into its first slot and ``None`` into the
    slots it covers, giving ``_MERGED_WRITTEN`` on the unrotated page.
    """
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    x, y, w, h = _TABLE_X, _TABLE_Y, _COL_W, 30
    for x0, y0, x1, y1 in (
        (x, y, x + 3 * w, y),
        (x, y + h, x + 3 * w, y + h),
        (x + w, y + 2 * h, x + 3 * w, y + 2 * h),
        (x, y + 3 * h, x + 3 * w, y + 3 * h),
        (x, y, x, y + 3 * h),
        (x + w, y, x + w, y + 3 * h),
        (x + 2 * w, y + h, x + 2 * w, y + 3 * h),
        (x + 3 * w, y, x + 3 * w, y + 3 * h),
    ):
        page.draw_line((x0, y0), (x1, y1), color=_RULE_RGB)
    for r, row in enumerate(_MERGED_WRITTEN):
        for c, cell in enumerate(row):
            if cell:
                page.insert_text((x + c * w + 6, y + r * h + 18), cell, fontsize=10)
    page.set_rotation(rotation)
    path = tmp_path / f"merged_cells_{rotation}.pdf"
    doc.save(str(path))
    doc.close()
    return str(path)


def _parse_elements(pdf, engine):
    return DocumentAssembler(pdf, backend=_BACKENDS[engine](pdf)).parse_page(0)["elements"]


class TestExtractorReadsCellsAsWritten:
    """The extractor's grid is the written grid, whatever the /Rotate."""

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    @pytest.mark.parametrize("kind", _KINDS)
    def test_grid_matches_the_written_grid(self, tmp_path, kind, rotation):
        (table,) = PdfPlumberTableExtractor(_table_pdf(tmp_path, kind, rotation)).extract(0)
        assert table.data == _WRITTEN
        assert (table.rows, table.columns, table.has_header_row) == (_ROWS, _COLS, True)

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    def test_merged_cells_keep_their_written_slots(self, tmp_path, rotation):
        (table,) = PdfPlumberTableExtractor(_merged_cell_table_pdf(tmp_path, rotation)).extract(0)
        assert table.data == _MERGED_WRITTEN

    @pytest.mark.parametrize("rotation", (90, 180, 270))
    def test_content_that_displays_upright_is_left_in_display_order(self, tmp_path, rotation):
        # Guard: pdfplumber already reads these as written (green before the fix).
        (table,) = PdfPlumberTableExtractor(_upright_display_pdf(tmp_path, rotation)).extract(0)
        assert table.data == _WRITTEN

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    def test_table_without_text_keeps_its_displayed_shape(self, tmp_path, rotation):
        # Guard: with no glyphs there is no evidence of how it was written,
        # so the grid stays as displayed (green before the fix).
        (table,) = PdfPlumberTableExtractor(_table_pdf(tmp_path, "cropbox", rotation, with_text=False)).extract(0)
        displayed = (_COLS, _ROWS) if rotation in (90, 270) else (_ROWS, _COLS)
        assert (table.rows, table.columns) == displayed
        assert {cell for row in table.data for cell in row} == {""}


@pytest.mark.parametrize("rotation", _ROTATIONS)
@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("engine", list(_BACKENDS))
class TestAssembledTablesReadAsWritten:
    """Per engine, crop and rotation: the written grid, and #385's guarantees still hold."""

    def test_table_content_and_shape(self, tmp_path, engine, kind, rotation):
        elements = _parse_elements(_table_pdf(tmp_path, kind, rotation), engine)
        (table,) = [e for e in elements if e["type"] == "table"]
        assert table["content"] == _WRITTEN
        assert table["metadata"] == {"rows": _ROWS, "columns": _COLS, "has_header_row": True}

    def test_no_cell_text_outside_the_table(self, tmp_path, engine, kind, rotation):
        elements = _parse_elements(_table_pdf(tmp_path, kind, rotation), engine)
        outside = [e["content"] for e in elements if e["type"] != "table"]
        assert sorted({word for text in outside for word in str(text).split()} & _CELL_WORDS) == []
        assert Counter(outside)[_TITLE] == 1

    def test_table_position_matches_where_it_renders(self, tmp_path, engine, kind, rotation):
        elements = _parse_elements(_table_pdf(tmp_path, kind, rotation), engine)
        (table,) = [e for e in elements if e["type"] == "table"]
        # pdfium reports the page as displayed; PyMuPDF reports its text on the
        # unrotated page, so its truth is the same page rendered without /Rotate.
        truth = _rendered_rule_box(_table_pdf(tmp_path, kind, rotation if engine == "pdfium" else 0))
        assert _xyxy(table["position"]) == pytest.approx(truth, abs=1.5)
