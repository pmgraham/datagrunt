"""Clip-only text (render mode 7) and what pdfium exposes of its clip (#366, A3 review I6).

Mode-7 text paints nothing and adds its glyphs to the clipping path, so a
later paint in the same ``q`` ... ``Q`` scope shows through the glyphs
(gradient-filled text). Flagging such text visible needs proof that a later
paint runs inside its clip, is visibly painted, and covers the glyphs. The
pins below record what pdfium 156.0.8076.0 (pypdfium2 5.14.0) exposes, and
why that proof cannot be made from it: mode-7 text stays ``invisible`` in
every case (maintainer decision, 2026-10-31, given this evidence: flagged
text is still extracted, while unflagged invisible text would not be).
"""

import ctypes
import math
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf.pdfium_engine import RawPage, extract_page, open_document
from datagrunt.pdf.quality import assess_page
from pdf_tests.test_pdfium_spans import content_page
from pdf_tests.test_quality import classified_page, texts

GRADIENT = b"BT /F1 24 Tf 7 Tr 30 150 Td (Quarterly revenue up 12 percent) Tj ET "
LINE = "Quarterly revenue up 12 percent"
FILL = b"0 0.3 0.8 rg 0 0 400 300 re f "
SHADING = (
    b"<< /ShadingType 2 /ColorSpace /DeviceRGB /Coords [0 0 400 0]"
    b" /Function << /FunctionType 2 /Domain [0 1] /C0 [1 0 0] /C1 [0 0 1] /N 1 >> >>"
)
# A 1 x 1 px image: its pixels are never decoded here.
ONE_PIXEL = (
    b"<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceGray /BitsPerComponent 8"
    b" /Length 1 >>\nstream\n\x80\nendstream"
)
FORM_FILL = b"0 0 400 300 re f"
FORM = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Length %d >>\nstream\n%s\nendstream" % (
    len(FORM_FILL),
    FORM_FILL,
)
CLEAR = b"<< /Type /ExtGState /ca 0 /CA 0 >>"


def clip_page(tmp_path: Path, content: bytes) -> Path:
    """A page drawing ``content`` with /Sh, /Im (1 x 1 px), /X (a form filling the page) and /Clear (alpha 0)."""
    resources = b"/Shading << /Sh 6 0 R >> /XObject << /Im 7 0 R /X 8 0 R >> /ExtGState << /Clear 9 0 R >>"
    return content_page(tmp_path, content, extra=(SHADING, ONE_PIXEL, FORM, CLEAR), resources=resources)


def clip_reads(path: Path) -> list[tuple[int, int]]:
    """Each top-level page object's ``(type, FPDFClipPath_CountPaths of its clip)``, in paint order."""
    page = pdfium.PdfDocument(path)[0]
    found = []
    for index in range(pdfium_c.FPDFPage_CountObjects(page.raw)):
        obj = pdfium_c.FPDFPage_GetObject(page.raw, index)
        found.append(
            (pdfium_c.FPDFPageObj_GetType(obj), pdfium_c.FPDFClipPath_CountPaths(pdfium_c.FPDFPageObj_GetClipPath(obj)))
        )
    return found


TEXT, PATH, IMAGE, SHADE, FORM_KIND = (
    pdfium_c.FPDF_PAGEOBJ_TEXT,
    pdfium_c.FPDF_PAGEOBJ_PATH,
    pdfium_c.FPDF_PAGEOBJ_IMAGE,
    pdfium_c.FPDF_PAGEOBJ_SHADING,
    pdfium_c.FPDF_PAGEOBJ_FORM,
)


class TestWhatPdfiumExposesOfATextClip:
    """FPDFClipPath_* count path clips only: a text clip shows, at best, as a clip with no path."""

    @pytest.mark.parametrize(
        "content,later",
        [
            # In the text clip's scope, the fill reads a clip with no path (count 0); after Q, none (-1).
            pytest.param(b"q " + GRADIENT + FILL + b"Q", 0, id="same-scope"),
            pytest.param(b"q " + GRADIENT + b"Q " + FILL, -1, id="after-q"),
            pytest.param(b"q " + GRADIENT + b"/Sh sh Q", 0, id="shading"),
            pytest.param(b"q " + GRADIENT + b"q 400 0 0 300 0 0 cm /Im Do Q Q", 0, id="image"),
        ],
    )
    def test_a_later_object_in_the_text_clips_scope_reads_a_clip_with_no_path(self, tmp_path, content, later):
        reads = clip_reads(clip_page(tmp_path, content))

        assert reads[0] == (TEXT, -1)
        assert reads[1][1] == later

    def test_beside_a_path_clip_the_text_clip_is_not_exposed(self, tmp_path):
        # A fill under a path clip and the text clip reads exactly as one under the path clip alone.
        under_both = b"q 10 10 50 50 re W n " + GRADIENT + FILL + b"Q"
        under_path = b"q 10 10 50 50 re W n " + GRADIENT + b"Q q 10 10 50 50 re W n " + FILL + b"Q"

        both, path_only = clip_reads(clip_page(tmp_path, under_both)), clip_reads(clip_page(tmp_path, under_path))

        assert both[-1] == path_only[-1] == (PATH, 1)

    def test_two_text_clips_read_alike(self, tmp_path):
        # A fill in the second text's scope reads as one in the first's: the clip names no text.
        other = b"q BT /F1 24 Tf 7 Tr 30 50 Td (Elsewhere) Tj ET " + FILL + b"Q"

        reads = clip_reads(clip_page(tmp_path, b"q " + GRADIENT + b"Q " + other))

        assert reads == [(TEXT, -1), (TEXT, -1), (PATH, 0)]

    def test_a_later_forms_contents_read_no_clip(self, tmp_path):
        # The form object carries the text clip; the fill inside it reads none.
        page = pdfium.PdfDocument(clip_page(tmp_path, b"q " + GRADIENT + b"/X Do Q"))[0]
        form = pdfium_c.FPDFPage_GetObject(page.raw, 1)
        inner = pdfium_c.FPDFFormObj_GetObject(form, 0)

        assert pdfium_c.FPDFClipPath_CountPaths(pdfium_c.FPDFPageObj_GetClipPath(form)) == 0
        assert pdfium_c.FPDFClipPath_CountPaths(pdfium_c.FPDFPageObj_GetClipPath(inner)) == -1

    def test_a_path_clip_that_holds_the_object_reads_as_none(self, tmp_path):
        # pdfium drops a clip that cannot cut the object: "no clip" is not "no clip in scope".
        reads = clip_reads(clip_page(tmp_path, b"q 0 0 400 300 re W n 10 10 50 50 re f Q"))

        assert reads == [(PATH, -1)]

    @pytest.mark.parametrize("content,kind", [(b"/Sh sh", SHADE), (b"400 0 0 300 0 0 cm /Im Do", IMAGE)])
    def test_the_alpha_of_a_shading_or_image_cannot_be_read(self, tmp_path, content, kind):
        page = pdfium.PdfDocument(clip_page(tmp_path, b"q /Clear gs " + content + b" Q"))[0]
        obj = pdfium_c.FPDFPage_GetObject(page.raw, 0)
        channels = [ctypes.c_uint() for _ in range(4)]

        assert pdfium_c.FPDFPageObj_GetType(obj) == kind
        assert not pdfium_c.FPDFPageObj_GetFillColor(obj, *channels)

    def test_a_shading_has_its_clips_bounds_and_no_matrix(self, tmp_path):
        page = pdfium.PdfDocument(clip_page(tmp_path, b"q " + GRADIENT + b"/Sh sh Q"))[0]
        text, shading = (pdfium_c.FPDFPage_GetObject(page.raw, index) for index in (0, 1))
        glyphs, edges = ([ctypes.c_float() for _ in range(4)] for _ in range(2))

        assert not pdfium_c.FPDFPageObj_GetMatrix(shading, pdfium_c.FS_MATRIX())
        assert pdfium_c.FPDFPageObj_GetBounds(text, *glyphs)
        assert pdfium_c.FPDFPageObj_GetBounds(shading, *edges)
        # The glyphs' box, rounded out to whole points: not where the shading paints. The
        # glyph box comes from the outlines of the font pdfium substitutes for Helvetica,
        # which differ by platform ([31, 144, 376, 168] on macOS, [30, 144, 375, 168] on Linux).
        left, bottom, right, top = (edge.value for edge in glyphs)
        rounded_out = [math.floor(left), math.floor(bottom), math.ceil(right), math.ceil(top)]
        assert [edge.value for edge in edges] == rounded_out


class TestClipOnlyTextIsInvisible:
    """Mode-7 text is invisible whatever is painted after it (no proof a paint shows through its glyphs)."""

    @pytest.mark.parametrize(
        "after",
        [
            pytest.param(FILL, id="fill-in-scope"),
            pytest.param(b"Q " + FILL + b"q ", id="fill-after-q"),
            pytest.param(b"/Clear gs " + FILL, id="clear-fill-in-scope"),
            pytest.param(b"2 w 20 140 360 30 re S ", id="stroke-only-in-scope"),
            pytest.param(b"/Sh sh ", id="shading-in-scope"),
            pytest.param(b"q 400 0 0 300 0 0 cm /Im Do Q ", id="image-in-scope"),
            pytest.param(b"/X Do ", id="form-in-scope"),
            pytest.param(b"", id="nothing-after"),
        ],
    )
    def test_mode_7_text_is_invisible(self, tmp_path, after):
        pieces = classified_page(clip_page(tmp_path, b"q " + GRADIENT + after + b"Q"))

        assert texts(pieces) == [(LINE, ("invisible",))]

    def test_a_page_of_gradient_text_routes_as_hidden_text(self, tmp_path):
        handle = open_document(clip_page(tmp_path, b"q " + GRADIENT + FILL + b"Q"))
        try:
            raw = extract_page(handle, 0)
        finally:
            handle.close()
        assert isinstance(raw, RawPage)

        result = assess_page(raw)

        assert (result.route, result.quality.hidden_spans_flagged) == ("vision", 1)
