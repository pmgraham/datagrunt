"""Tests for the v5 engine's object boxes (#400): the displayed box of every non-text object, from A2's walk.

The background check's gate draws a page when visible text lies over any
of them. Pages are written by hand (``hand_written_pdf``) for shadings,
curves and forms ReportLab cannot write, or built with the #363 toolkit.
Boxes are displayed, top-left ``(x0, y0, x1, y1)`` points on a 400 x 300 pt
page.
"""

import dataclasses
import pickle
import re
from pathlib import Path

import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import PDFCorruptError
from datagrunt.pdf.pdfium_engine import ObjectBoxes, RawPage, extract_page, open_document, page_object_boxes
from pdf_tests.builders import PageSpec, RasterImage, TextRun, build_pdf, hand_written_pdf, solid_png
from pdf_tests.test_pdfium_extract_page import failing

# An axial shading of one colour (0.2 grey) along x.
AXIAL = (
    b"<< /ShadingType 2 /ColorSpace /DeviceRGB /Coords [0 0 400 0] /Extend [true true] "
    b"/Function << /FunctionType 2 /Domain [0 1] /C0 [0.2 0.2 0.2] /C1 [0.2 0.2 0.2] /N 1 >> >>"
)
FORM_FILL = b"0 0 1 rg 0 0 50 50 re f"
# A disc of radius 50 at (200, 150): four Bezier quarters (0.5523 x 50 = 27.6 off each axis).
DISC = (
    b"250 150 m 250 177.6 227.6 200 200 200 c 172.4 200 150 177.6 150 150 c "
    b"150 122.4 172.4 100 200 100 c 227.6 100 250 122.4 250 150 c f"
)


def stream(body: bytes, entries: bytes = b"") -> bytes:
    return b"<< %s/Length %d >>\nstream\n%s\nendstream" % (entries, len(body), body)


def page_with(tmp_path: Path, content: bytes, *, rotate: int = 0, shading: bytes = AXIAL) -> Path:
    """A 400 x 300 pt page drawing ``content`` with font /F1, shading /Sh (AXIAL) and form /Fm (a 50 pt blue box)."""
    form = stream(FORM_FILL, b"/Type /XObject /Subtype /Form /BBox [0 0 50 50] ")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Rotate %d /Contents 4 0 R /Resources << "
        b"/Font << /F1 5 0 R >> /Shading << /Sh 6 0 R >> /XObject << /Fm 7 0 R >> >> >>" % rotate,
        stream(content),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        shading,
        form,
    ]
    return hand_written_pdf(tmp_path / "page.pdf", objects)


def boxes_of(path: Path) -> ObjectBoxes:
    with open_document(path) as handle:
        return page_object_boxes(handle, 0)


def rounded(boxes: ObjectBoxes) -> list[tuple[float, ...]]:
    return [tuple(round(value, 2) for value in box) for box in boxes.boxes]


class TestObjectBoxes:
    """Every path, image, shading and form object has its displayed box; text objects have none."""

    def test_an_unclipped_shading_covers_the_page(self, tmp_path):
        # sh with no clip paints everywhere: pdfium bounds it by the page.
        assert rounded(boxes_of(page_with(tmp_path, b"/Sh sh"))) == [(0.0, 0.0, 400.0, 300.0)]

    def test_a_clipped_shading_covers_its_clip(self, tmp_path):
        found = boxes_of(page_with(tmp_path, b"q 10 10 100 50 re W n /Sh sh Q"))

        assert rounded(found) == [(10.0, 240.0, 110.0, 290.0)]

    def test_a_stroke_covers_its_width(self, tmp_path):
        # A 20 pt stroke along y = 100 paints 10 pt each side: pdfium's box holds it (it pads by the whole width).
        (box,) = rounded(boxes_of(page_with(tmp_path, b"20 w 50 100 m 300 100 l S")))

        assert box[1] <= 190.0 and box[3] >= 210.0
        assert box[0] <= 40.0 and box[2] >= 310.0

    def test_a_curved_fill_covers_its_curves(self, tmp_path):
        assert rounded(boxes_of(page_with(tmp_path, DISC))) == [(150.0, 100.0, 250.0, 200.0)]

    def test_a_form_and_its_contents_each_have_a_box_on_the_page(self, tmp_path):
        # The form is drawn at (300, 50) scaled by 2: its 50 pt box is 100 pt on the page.
        found = boxes_of(page_with(tmp_path, b"q 2 0 0 2 300 50 cm /Fm Do Q"))

        assert rounded(found) == [(300.0, 150.0, 400.0, 250.0), (300.0, 150.0, 400.0, 250.0)]

    def test_an_image_covers_its_box(self, tmp_path):
        spec = PageSpec(
            width=400.0, height=300.0, items=(RasterImage(60.0, 40.0, 120.0, 80.0, solid_png(4, 4, (1, 0, 0))),)
        )

        assert rounded(boxes_of(build_pdf(tmp_path / "image.pdf", [spec]))) == [(60.0, 40.0, 180.0, 120.0)]

    def test_text_objects_have_no_box(self, tmp_path):
        found = boxes_of(page_with(tmp_path, b"BT /F1 12 Tf 40 150 Td (Text only) Tj ET"))

        assert found == ObjectBoxes(boxes=(), unboxed=0)

    def test_boxes_follow_the_object_walk(self, tmp_path):
        content = b"BT /F1 12 Tf 40 150 Td (Text) Tj ET q 10 10 100 50 re W n /Sh sh Q " + DISC

        assert rounded(boxes_of(page_with(tmp_path, content))) == [
            (10.0, 240.0, 110.0, 290.0),
            (150.0, 100.0, 250.0, 200.0),
        ]

    def test_boxes_are_displayed_on_a_turned_page(self, tmp_path):
        # /Rotate 90 shows the 400 x 300 page 300 wide: the clip (10, 10)-(110, 60) lies at its top-left.
        found = boxes_of(page_with(tmp_path, b"q 10 10 100 50 re W n /Sh sh Q", rotate=90))

        assert rounded(found) == [(10.0, 10.0, 60.0, 110.0)]


class TestUnboxed:
    """An object the walk cannot give a box is counted, never dropped: the gate counts it as covering the page."""

    def test_bounds_pdfium_cannot_read_are_counted(self, tmp_path, monkeypatch):
        path = page_with(tmp_path, b"/Sh sh " + DISC)
        monkeypatch.setattr(pdfium_c, "FPDFPageObj_GetBounds", failing(0))

        assert boxes_of(path) == ObjectBoxes(boxes=(), unboxed=2)

    def test_an_object_under_an_overflowed_matrix_is_counted(self, tmp_path):
        # Five 1e9 scales overflow pdfium's 32-bit CTM: the path's bounds are not finite.
        overflow = b"1000000000 0 0 1000000000 0 0 cm " * 5
        found = boxes_of(page_with(tmp_path, b"q " + overflow + DISC + b" Q " + DISC))

        assert (rounded(found), found.unboxed) == ([(150.0, 100.0, 250.0, 200.0)], 1)

    def test_a_form_matrix_pdfium_cannot_read_is_a_corrupt_page(self, tmp_path, monkeypatch):
        # Without its form's matrix no content box can be placed: the page fails, as A2's other stages fail.
        path = page_with(tmp_path, b"q 2 0 0 2 300 50 cm /Fm Do Q")
        monkeypatch.setattr(pdfium_c, "FPDFPageObj_GetMatrix", failing(0))

        with pytest.raises(PDFCorruptError, match=re.escape("page 1: pdfium cannot read the matrix of object 0")):
            boxes_of(path)


class TestRecord:
    """ObjectBoxes is a validated, frozen, picklable record; RawPage carries it."""

    def test_extract_page_carries_the_pages_object_boxes(self, tmp_path):
        path = page_with(tmp_path, b"/Sh sh")

        raw = extract_page(open_document(path), 0)

        assert isinstance(raw, RawPage) and raw.object_boxes == boxes_of(path)
        assert pickle.loads(pickle.dumps(raw.object_boxes)) == raw.object_boxes

    def test_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            ObjectBoxes(boxes=(), unboxed=0).unboxed = 1  # type: ignore[misc]

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"boxes": [(0.0, 0.0, 1.0, 1.0)]}, TypeError, "boxes must be a tuple, got list", id="list"),
            pytest.param({"boxes": ((1.0, 0.0, 0.0, 1.0),)}, ValueError, "boxes[0]", id="reversed"),
            pytest.param({"boxes": ((0.0, 0.0, float("inf"), 1.0),)}, ValueError, "boxes[0]", id="infinite"),
            pytest.param({"unboxed": -1}, ValueError, "unboxed must be >= 0, got -1", id="unboxed"),
            pytest.param({"unboxed": True}, TypeError, "unboxed must be an int", id="unboxed-bool"),
        ],
    )
    def test_invalid_fields_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            ObjectBoxes(**({"boxes": (), "unboxed": 0} | fields))

    def test_raw_page_needs_its_object_boxes(self, tmp_path):
        raw = extract_page(open_document(page_with(tmp_path, b"/Sh sh")), 0)

        with pytest.raises(TypeError, match=re.escape("object_boxes must be a ObjectBoxes, got NoneType")):
            dataclasses.replace(raw, object_boxes=None)

    def test_a_text_run_is_no_box(self, tmp_path):
        spec = PageSpec(width=400.0, height=300.0, items=(TextRun("Only text", x=40.0, y=100.0),))

        assert boxes_of(build_pdf(tmp_path / "text.pdf", [spec])) == ObjectBoxes(boxes=(), unboxed=0)
