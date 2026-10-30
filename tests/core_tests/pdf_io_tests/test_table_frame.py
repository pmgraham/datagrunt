"""Table boxes share the frame of the text they de-duplicate (#385).

v4 drops body text that lies inside a detected table
(``DocumentAssembler.parse_page``) and reports each table's ``position``.
Tables come from pdfplumber, which measures from the displayed MediaBox's
top-left corner (offset by its own origin). The engines report text elsewhere:

- pdfium: the display frame, i.e. the visible box (CropBox intersected with
  MediaBox) as the page renders, /Rotate applied (#384).
- PyMuPDF: the visible box with /Rotate NOT applied (its unrotated text frame).

On offset CropBoxes, non-zero MediaBox origins and (for PyMuPDF) rotated pages
the frames disagreed, so table cell text was emitted a second time as body
text. Ground truth is rendered pixels at 72 dpi, one pixel per point.
"""

import json
import logging
import re
from collections import Counter
from dataclasses import replace

import pytest

from datagrunt.core.pdf_io.extraction.pdfium_backend import PdfiumBackend
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument
from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend
from datagrunt.core.pdf_io.extraction.shapes import BBox, OcrBlock
from datagrunt.core.pdf_io.extraction.tables import PdfPlumberTableExtractor, _visible_box
from datagrunt.core.pdf_io.pdfcomponents import DocumentAssembler

# "none" is a plain 612x792 page. The next three all expose the visible box
# (50, 40, 530, 720), like #384's fixtures: an offset CropBox, a MediaBox with
# a non-zero origin, and that MediaBox inherited from the /Pages node.
# "overhanging_cropbox" has a CropBox that runs past the MediaBox's top and
# right, so only their intersection (40, 30, 612, 792) is visible.
_KINDS = ("none", "cropbox", "mediabox", "inherited_mediabox", "overhanging_cropbox")
_ROTATIONS = (0, 90, 180, 270)
_BACKENDS = {"pdfium": PdfiumBackend, "pymupdf": PyMuPDFBackend}
_OFFSET_BOX_PDF = "[50 40 530 720]"
_OVERHANGING_CROPBOX_PDF = "[40 30 700 850]"

_CELLS = (("Part", "Qty"), ("Bolt", "12"), ("Nut", "40"), ("Washer", "7"))
_CELL_TEXTS = {text for row in _CELLS for text in row}
# Body text outside the table, placed diagonally to it so no engine groups it
# into a table row or column on any rotation. "Note" sits just past the
# table's bottom-right corner, where a table box shifted by the crop offset
# would swallow it.
_OUTSIDE_TEXT = (("Header note", 380, 100, 12), ("Note", 352, 264, 10), ("Footer note", 380, 420, 12))
# PyMuPDF top-left page points: the table's left/top edge, column width, row height.
_TABLE_X, _TABLE_Y, _COL_W, _ROW_H = 100, 150, 120, 24
_RULE_RGB = (0, 0, 1)


def _draw_ruled_table(page, with_text):
    rows, cols = len(_CELLS), len(_CELLS[0])
    for r in range(rows + 1):
        y = _TABLE_Y + r * _ROW_H
        page.draw_line((_TABLE_X, y), (_TABLE_X + cols * _COL_W, y), color=_RULE_RGB)
    for c in range(cols + 1):
        x = _TABLE_X + c * _COL_W
        page.draw_line((x, _TABLE_Y), (x, _TABLE_Y + rows * _ROW_H), color=_RULE_RGB)
    if not with_text:
        return
    for r, row in enumerate(_CELLS):
        for c, text in enumerate(row):
            page.insert_text((_TABLE_X + c * _COL_W + 6, _TABLE_Y + r * _ROW_H + 16), text, fontsize=10)


def _apply_box_layout(doc, page, kind):
    """Give the page the ``kind`` box layout (see ``_KINDS``)."""
    if kind == "mediabox":
        doc.xref_set_key(page.xref, "MediaBox", _OFFSET_BOX_PDF)
    elif kind == "inherited_mediabox":
        parent_xref = int(doc.xref_get_key(page.xref, "Parent")[1].split()[0])
        doc.xref_set_key(parent_xref, "MediaBox", _OFFSET_BOX_PDF)
        page_obj = doc.xref_object(page.xref, compressed=True)
        doc.update_object(page.xref, re.sub(r"/MediaBox\s*\[[^\]]*\]", "", page_obj))
    elif kind == "overhanging_cropbox":
        # Written raw: PyMuPDF's set_cropbox() refuses a box outside the MediaBox.
        doc.xref_set_key(page.xref, "CropBox", _OVERHANGING_CROPBOX_PDF)


def _ruled_table_pdf(tmp_path, kind, rotation, *, with_text=True, with_image=False):
    """One page: a blue-ruled 4x2 table (and outside text), under ``kind`` and ``rotation``."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    _draw_ruled_table(page, with_text)
    if with_text:
        for text, x, y, size in _OUTSIDE_TEXT:
            page.insert_text((x, y), text, fontsize=size)
    if with_image:
        pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
        pix.set_rect(pix.irect, (255, 0, 0))
        page.insert_image(pymupdf.Rect(380, 500, 430, 550), stream=pix.tobytes("png"))
    if kind == "cropbox":
        # PyMuPDF takes a top-left-based rect; this writes /CropBox [50 40 530 720].
        page.set_cropbox(pymupdf.Rect(50, 72, 530, 752))
    page.set_rotation(rotation)
    _apply_box_layout(doc, page, kind)
    path = tmp_path / f"table_{kind}_{rotation}_{int(with_text)}{int(with_image)}.pdf"
    doc.save(str(path))
    doc.close()
    return str(path)


def _rendered_rule_box(pdf):
    """``(x0, y0, x1, y1)`` of the blue table rules as the page renders at 72 dpi."""
    from PIL import ImageChops

    with PdfiumDocument(pdf) as doc, doc.page(0) as page:
        img = page.render_pil(dpi=72)
    red, green, blue = img.split()
    mask = ImageChops.multiply(
        ImageChops.multiply(red.point(lambda v: 255 if v < 80 else 0), green.point(lambda v: 255 if v < 80 else 0)),
        blue.point(lambda v: 255 if v > 180 else 0),
    )
    box = mask.getbbox()
    assert box is not None, "no table rules rendered"
    return box


def _xyxy(position):
    return (position["x"], position["y"], position["x"] + position["w"], position["y"] + position["h"])


def _parse_elements(pdf, engine):
    return DocumentAssembler(pdf, backend=_BACKENDS[engine](pdf)).parse_page(0)["elements"]


class TestTableExtractorReportsDisplayFrame:
    """The extractor's boxes are where the table renders: the display frame."""

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    @pytest.mark.parametrize("kind", _KINDS)
    def test_table_bbox_matches_rendered_rules(self, tmp_path, kind, rotation):
        pdf = _ruled_table_pdf(tmp_path, kind, rotation)
        (table,) = PdfPlumberTableExtractor(pdf).extract(0)
        assert table.bbox.to_xyxy() == pytest.approx(_rendered_rule_box(pdf), abs=1.5)


@pytest.mark.parametrize("rotation", _ROTATIONS)
@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("engine", list(_BACKENDS))
class TestAssembledTablesShareTheTextFrame:
    """Per engine, layout and rotation: no duplicated cells, right position, no over-dropping."""

    def test_no_table_cell_text_outside_the_table(self, tmp_path, engine, kind, rotation):
        elements = _parse_elements(_ruled_table_pdf(tmp_path, kind, rotation), engine)
        # pdfplumber reads the grid as the page displays it: transposed on 90/270
        # and with glyphs in display order on 180/270 ("Part" reads "traP"). So
        # check only that the whole 4x2 grid was found, with text in every cell.
        (table,) = [e["content"] for e in elements if e["type"] == "table"]
        cells = [cell for row in table for cell in row]
        assert len(cells) == len(_CELL_TEXTS) and all(cells)
        outside = {word for e in elements if e["type"] != "table" for word in str(e["content"]).split()}
        assert sorted(outside & _CELL_TEXTS) == []

    def test_table_position_matches_where_it_renders(self, tmp_path, engine, kind, rotation):
        elements = _parse_elements(_ruled_table_pdf(tmp_path, kind, rotation), engine)
        (table,) = [e for e in elements if e["type"] == "table"]
        # pdfium reports the page as displayed. PyMuPDF reports its text on the
        # unrotated page, so its truth is the same page rendered without /Rotate.
        truth_rotation = rotation if engine == "pdfium" else 0
        truth = _rendered_rule_box(_ruled_table_pdf(tmp_path, kind, truth_rotation))
        assert _xyxy(table["position"]) == pytest.approx(truth, abs=1.5)

    def test_text_outside_the_table_is_kept(self, tmp_path, engine, kind, rotation):
        elements = _parse_elements(_ruled_table_pdf(tmp_path, kind, rotation), engine)
        emitted = Counter(e["content"] for e in elements if e["type"] != "table")
        assert {text: emitted[text] for text, *_ in _OUTSIDE_TEXT} == {text: 1 for text, *_ in _OUTSIDE_TEXT}


class TestOcrPageTablesStayInTheDisplayFrame:
    """Scanned pages de-duplicate OCR text, which comes from a render of the page.

    A render is the display frame for both engines, so on a page without a text
    layer tables stay there, even for PyMuPDF on a rotated page.
    """

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    @pytest.mark.parametrize("engine", list(_BACKENDS))
    def test_ocr_text_inside_the_table_is_dropped(self, tmp_path, monkeypatch, engine, rotation):
        pdf = _ruled_table_pdf(tmp_path, "cropbox", rotation, with_text=False, with_image=True)
        x0, y0, x1, y1 = _rendered_rule_box(pdf)
        inside = OcrBlock(text="inside", bbox=BBox(x=x0 + 10, y=y0 + 5, w=20, h=8), confidence=90.0, word_count=1)
        outside = OcrBlock(text="outside", bbox=BBox(x=x1 + 20, y=y1 + 20, w=20, h=8), confidence=90.0, word_count=1)
        backend = _BACKENDS[engine](pdf)
        monkeypatch.setattr(backend, "ocr_page", lambda page_index, dpi=300: [inside, outside])

        elements = DocumentAssembler(pdf, backend=backend).parse_page(0)["elements"]

        assert [e["content"] for e in elements if e["type"] == "body_text"] == ["outside"]
        (table,) = [e for e in elements if e["type"] == "table"]
        assert _xyxy(table["position"]) == pytest.approx((x0, y0, x1, y1), abs=1.5)


class TestPyMuPDFDisplayToTextFrame:
    """Edges of PyMuPDF's display-to-text-frame mapping.

    It works from the backend's own ``PageAnalysis`` (PyMuPDF's ``page.rect``
    size and ``page.rotation``), so it never reopens the page.
    """

    @staticmethod
    def _analysis(tmp_path, rotation):
        pdf = _ruled_table_pdf(tmp_path, "cropbox", rotation)
        backend = PyMuPDFBackend(pdf)
        return backend, backend.analyze_page(0)

    def test_empty_input_maps_to_empty_list(self, tmp_path):
        backend, analysis = self._analysis(tmp_path, 90)
        assert backend.display_to_text_frame(analysis, []) == []

    def test_unrotated_page_returns_boxes_unchanged_without_a_page_load(self, tmp_path, monkeypatch):
        backend, analysis = self._analysis(tmp_path, 0)

        def no_document_access():
            raise AssertionError("an unrotated page must not reopen the document")

        monkeypatch.setattr(backend, "_get_doc", no_document_access)
        box = BBox(x=-0.0, y=-0.0, w=10.0, h=10.0)
        (mapped,) = backend.display_to_text_frame(analysis, [box])
        # -0.0 == 0.0 in Python, so compare the serialized text the output carries.
        assert json.dumps(mapped.to_dict()) == json.dumps(box.to_dict())

    def test_rotated_page_does_not_reopen_the_document(self, tmp_path, monkeypatch):
        backend, analysis = self._analysis(tmp_path, 90)

        def no_document_access():
            raise AssertionError("the mapping must come from the PageAnalysis")

        monkeypatch.setattr(backend, "_get_doc", no_document_access)
        (mapped,) = backend.display_to_text_frame(analysis, [BBox(x=506.0, y=50.0, w=96.0, h=240.0)])
        assert mapped == BBox(x=50.0, y=78.0, w=240.0, h=96.0)

    def test_non_right_angle_rotation_raises(self, tmp_path):
        backend, analysis = self._analysis(tmp_path, 0)
        with pytest.raises(ValueError, match="right angle"):
            backend.display_to_text_frame(replace(analysis, rotation=45), [BBox(x=0, y=0, w=1, h=1)])


class TestVisibleBoxFallback:
    """A CropBox that misses the MediaBox falls back to the MediaBox, visibly."""

    def test_disjoint_cropbox_falls_back_to_mediabox_and_logs(self, caplog):
        mediabox, cropbox = (0.0, 0.0, 612.0, 792.0), (700.0, 800.0, 900.0, 900.0)
        with caplog.at_level(logging.DEBUG, logger="datagrunt.core.pdf_io.extraction.tables"):
            assert _visible_box(mediabox, cropbox) == mediabox
        assert "does not intersect" in caplog.text

    def test_overlapping_cropbox_is_intersected_without_logging(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="datagrunt.core.pdf_io.extraction.tables"):
            visible = _visible_box((0.0, 0.0, 612.0, 792.0), (40.0, 30.0, 700.0, 850.0))
        assert visible == (40.0, 30.0, 612.0, 792.0)
        assert caplog.text == ""
