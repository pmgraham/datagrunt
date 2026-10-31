"""Tests for the v5 guarded page render (#369, A5a): ``datagrunt.pdf.render``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``), or
written by hand (``hand_written_pdf``) for content ReportLab cannot write.
Expected pixel sizes are derived from pdfium's own page geometry in the same
run, never pinned from one platform's fonts.
"""

import math
import re
import zlib
from pathlib import Path

import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import DatagruntPDFError, PDFCorruptError, _pdfium_images, pdfium_engine, render
from datagrunt.pdf.pdfium_engine import image_refusal, loaded_page, open_document, page_images
from datagrunt.pdf.render import RenderRefused, effective_dpi, render_size
from pdf_tests.builders import FormXObject, PageSpec, RasterImage, break_page, build_pdf, hand_written_pdf
from pdf_tests.test_pdfium_images import (
    DCT_16,
    FLATE,
    JPX_16,
    OVERFLOW,
    PNG,
    RGB_4X2,
    SAMPLES,
    content_page,
    gray,
    image_xobject,
    j2k,
    jpeg_bytes,
    jpeg_with_frame,
    one_page,
    xobject_page,
)

# The spec's defaults (PDFReader / PDFWriter): vision_dpi and max_render_pixels.
VISION_DPI = 300
MAX_RENDER_PIXELS = 16_000_000

LETTER = (612.0, 792.0)
# A 36" x 48" engineering sheet, in points.
SHEET_36_BY_48 = (36 * 72.0, 48 * 72.0)


def spec_formula(target_dpi: int, max_render_pixels: int, width_pt: float, height_pt: float) -> int:
    """The spec's effective DPI, written as the spec writes it."""
    return min(target_dpi, int(72.0 * math.sqrt(max_render_pixels / (width_pt * height_pt))))


class TestEffectiveDpi:
    """effective_dpi bounds a render's pixels by max_render_pixels (spec §3.3 step 4)."""

    def test_a_letter_page_at_300_dpi_keeps_300(self):
        assert effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, *LETTER) == 300

    def test_a_36_by_48_inch_sheet_at_300_dpi_stays_within_max_render_pixels(self):
        dpi = effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, *SHEET_36_BY_48)
        width_px, height_px = render_size(*SHEET_36_BY_48, dpi)

        assert dpi == spec_formula(VISION_DPI, MAX_RENDER_PIXELS, *SHEET_36_BY_48) == 96
        assert width_px * height_px <= MAX_RENDER_PIXELS
        # At the target DPI the sheet would be ~155.5 M pixels.
        unbounded = render_size(*SHEET_36_BY_48, VISION_DPI)
        assert unbounded[0] * unbounded[1] > 9 * MAX_RENDER_PIXELS

    @pytest.mark.parametrize("target_dpi", [36, 72, 150, 200, 300, 600, 1200])
    @pytest.mark.parametrize("max_render_pixels", [262_144, 4_000_000, 16_000_000, 100_000_000])
    def test_every_size_renders_within_max_render_pixels_or_is_refused(self, target_dpi, max_render_pixels):
        # Sides from 0.01 pt to 1e8 pt: pdfium reads a MediaBox of any size (1 x 5,000,000 and 1e8 x 1 load).
        sides = [10.0**exponent for exponent in range(-2, 9)] + [612.0, 792.0, 2592.0, 3456.0, 14400.0]
        for width_pt in sides:
            for height_pt in sides:
                try:
                    dpi = effective_dpi(target_dpi, max_render_pixels, width_pt, height_pt)
                except RenderRefused:
                    continue
                width_px, height_px = render_size(width_pt, height_pt, dpi)
                assert dpi == spec_formula(target_dpi, max_render_pixels, width_pt, height_pt)
                assert 1 <= dpi <= target_dpi
                assert width_px >= 1 and height_px >= 1
                assert width_px * height_px <= max_render_pixels, (width_pt, height_pt, dpi)

    def test_the_lowest_dpi_returned_is_1(self):
        # 72 * sqrt(262144 / 24576**2) = 1.5: one DPI renders the page at 341 x 341 px.
        assert effective_dpi(VISION_DPI, 262_144, 24576.0, 24576.0) == 1
        assert render_size(24576.0, 24576.0, 1) == (341, 341)

    def test_a_page_too_large_for_1_dpi_is_refused(self):
        # 72 * sqrt(16e6 / 1e12) = 0.29: even 1 DPI would be 1.9e8 pixels.
        message = "a 1000000.0 x 1000000.0 pt page renders over the 16000000-pixel bound at 1 DPI (bound: 0.29 DPI)"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, 1e6, 1e6)

    def test_an_area_beyond_float_range_is_refused(self):
        # 1e200 * 1e200 overflows to inf: the bound is 0 DPI.
        message = "a 1e+200 x 1e+200 pt page renders over the 16000000-pixel bound at 1 DPI (bound: 0.00 DPI)"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, 1e200, 1e200)

    def test_an_area_that_underflows_to_zero_is_refused_not_divided_by(self):
        # 1e-200 * 1e-200 underflows to 0.0: the page fits at the target DPI, and renders 0 px wide there.
        with pytest.raises(RenderRefused, match=re.escape("a 1e-200 x 1e-200 pt page renders 0 px wide at 300 DPI")):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, 1e-200, 1e-200)

    @pytest.mark.parametrize(
        "size,message",
        [
            pytest.param((1e8, 1.0), "a 100000000.0 x 1.0 pt page renders 0 px tall at 28 DPI", id="1e8-by-1"),
            pytest.param((0.01, 100.0), "a 0.01 x 100.0 pt page renders 0 px wide at 300 DPI", id="sliver"),
        ],
    )
    def test_a_page_under_one_pixel_across_is_refused(self, size, message):
        # Rounding a side up to 1 px would break the bound: 1e8 x 1 pt at 28 DPI is 38.9 M x 1 px.
        with pytest.raises(RenderRefused, match=re.escape(message)):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, *size)

    def test_render_refused_is_a_datagrunt_pdf_error(self):
        assert issubclass(RenderRefused, DatagruntPDFError)

    @pytest.mark.parametrize(
        "args,error,message",
        [
            pytest.param((35, MAX_RENDER_PIXELS, *LETTER), ValueError, "target_dpi must be >= 36, got 35", id="dpi"),
            pytest.param((True, MAX_RENDER_PIXELS, *LETTER), TypeError, "target_dpi must be an int", id="dpi-bool"),
            pytest.param((300.0, MAX_RENDER_PIXELS, *LETTER), TypeError, "target_dpi must be an int", id="dpi-float"),
            pytest.param(
                (300, 262_143, *LETTER), ValueError, "max_render_pixels must be >= 262144, got 262143", id="pixels"
            ),
            pytest.param((300, 16e6, *LETTER), TypeError, "max_render_pixels must be an int", id="pixels-float"),
            pytest.param((300, MAX_RENDER_PIXELS, 0.0, 792.0), ValueError, "width_pt must be > 0", id="width-zero"),
            pytest.param((300, MAX_RENDER_PIXELS, 612.0, -1.0), ValueError, "height_pt must be > 0", id="height"),
            pytest.param(
                (300, MAX_RENDER_PIXELS, math.nan, 792.0), ValueError, "width_pt must be finite", id="width-nan"
            ),
            pytest.param(
                (300, MAX_RENDER_PIXELS, 612.0, math.inf), ValueError, "height_pt must be finite", id="height-inf"
            ),
            pytest.param((300, MAX_RENDER_PIXELS, "612", 792.0), TypeError, "width_pt must be an int or float", id="w"),
        ],
    )
    def test_invalid_arguments_raise(self, args, error, message):
        with pytest.raises(error, match=re.escape(message)):
            effective_dpi(*args)


class TestRenderSize:
    """render_size is the pixel size a page renders to: each displayed side at ``dpi``, rounded down."""

    @pytest.mark.parametrize(
        "size,dpi,expected",
        [
            pytest.param(LETTER, 300, (2550, 3300), id="letter"),
            pytest.param((611.9, 791.9), 300, (2549, 3299), id="rounds-down"),
            pytest.param((0.2, 100.0), 300, (0, 416), id="under-one-pixel"),
        ],
    )
    def test_each_side_is_rounded_down(self, size, dpi, expected):
        assert render_size(*size, dpi) == expected

    @pytest.mark.parametrize(
        "args,error,message",
        [
            pytest.param((*LETTER, 0), ValueError, "dpi must be >= 1, got 0", id="dpi-zero"),
            pytest.param((*LETTER, 1.5), TypeError, "dpi must be an int", id="dpi-float"),
            pytest.param((0.0, 792.0, 300), ValueError, "width_pt must be > 0", id="width"),
            pytest.param((612.0, math.inf, 300), ValueError, "height_pt must be finite", id="height"),
        ],
    )
    def test_invalid_arguments_raise(self, args, error, message):
        with pytest.raises(error, match=re.escape(message)):
            render_size(*args)


def refusal_of(path: Path, *, max_objects: int = 1_000_000, password: str | None = None) -> str | None:
    """The image guard's verdict on page 1 of ``path``: why it must not render, or None."""
    with open_document(path, password=password) as handle, loaded_page(handle, 0) as (page, geometry):
        return image_refusal(page, geometry, max_objects=max_objects)


@pytest.fixture
def decodes(monkeypatch):
    """Record every pdfium call that decodes an image's pixels or renders; none of them reaches pdfium."""
    calls = []
    names = (
        "FPDFImageObj_GetBitmap",
        "FPDFImageObj_GetRenderedBitmap",
        "FPDF_RenderPageBitmap",
        "FPDF_RenderPageBitmapWithMatrix",
        "FPDF_RenderPageBitmap_Start",
    )
    for name in names:
        monkeypatch.setattr(pdfium_c, name, lambda *args, _name=name: calls.append(_name))
    return calls


# A JPEG 2000 codestream whose SIZ claims 16000 x 16000 (256 M pixels): A2's bomb.
JPX_BOMB = j2k((16, 16), siz=(16000, 16000), tile=(16000, 16000))
JPX_BOMB_REFUSAL = "object {index} decodes to 16000 x 16000 = 256000000 pixels, over the 89478485-pixel bound"


class TestImageGuard:
    """Before a page renders, every image object pdfium exposes passes A2's header bounds (#369 decision)."""

    def test_a_page_of_decodable_images_passes(self, tmp_path):
        form = FormXObject(x=100.0, y=50.0, w=150.0, h=60.0, items=(RasterImage(10.0, 5.0, 40.0, 20.0, PNG),))
        path = one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG), form)

        assert len(page_images(open_document(path), 0).images) == 2
        assert refusal_of(path) is None

    def test_a_page_without_images_passes(self, tmp_path):
        assert refusal_of(content_page(tmp_path, b"10 10 m 100 100 l S")) is None

    def test_an_inline_image_is_checked_like_an_xobject(self, tmp_path):
        good = content_page(tmp_path, b"q 80 0 0 40 10 20 cm BI /W 4 /H 2 /CS /RGB /BPC 8 ID " + SAMPLES + b" EI Q")
        assert refusal_of(good) is None
        # An inline image's size is its /W and /H (no codestream header): 10000 x 10000 is over the bound.
        bomb = b"q 80 0 0 40 10 20 cm BI /W 10000 /H 10000 /CS /G /BPC 8 /F /Fl ID " + FLATE + b" EI Q"
        expected = "object 0 decodes to 10000 x 10000 = 100000000 pixels, over the 89478485-pixel bound"
        assert refusal_of(content_page(tmp_path, bomb)) == expected

    @pytest.mark.parametrize(
        "data,entries,expected",
        [
            pytest.param(JPX_BOMB, JPX_16, JPX_BOMB_REFUSAL.format(index=0), id="jpx-pixels"),
            pytest.param(
                jpeg_with_frame(26000, 26000),
                DCT_16,
                "object 0 decodes to 26000 x 26000 = 676000000 pixels, over the 89478485-pixel bound",
                id="jpeg-frame",
            ),
            pytest.param(
                j2k((16, 8), siz=(2000, 2000)),
                gray(2000, 2000, b"/JPXDecode"),
                "object 0 decodes in 31250 tiles, over the 4096-tile bound",
                id="jpx-tiles",
            ),
            pytest.param(
                j2k((16, 8), components=5),
                gray(16, 8, b"/JPXDecode"),
                "object 0 has 5 components, over the 4-component bound",
                id="jpx-components",
            ),
            pytest.param(
                FLATE,
                gray(10000, 10000, b"/FlateDecode"),
                "object 0 decodes to 10000 x 10000 = 100000000 pixels, over the 89478485-pixel bound",
                id="flate-declared",
            ),
            pytest.param(
                zlib.compress(jpeg_bytes((16, 8))),
                b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter [/FlateDecode /DCTDecode]",
                "object 0 is a DCTDecode stream behind FlateDecode: its size cannot be read without decoding",
                id="dct-behind-flate",
            ),
            pytest.param(
                b"not a codestream",
                DCT_16,
                "object 0 has a DCTDecode header that cannot be read",
                id="unreadable-header",
            ),
            pytest.param(
                b"\x00" * 16,
                gray(16, 16, b"/JBIG2Decode"),
                "object 0 uses filter 'JBIG2Decode': its decode size cannot be read without decoding",
                id="jbig2",
            ),
            pytest.param(
                FLATE,
                b"/Width 0 /Height 2 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /FlateDecode",
                "object 0 has no pixel size pdfium can decode (0 x 2)",
                id="no-size",
            ),
        ],
    )
    def test_an_image_a2_refuses_refuses_the_render(self, tmp_path, decodes, data, entries, expected):
        assert refusal_of(xobject_page(tmp_path, data, entries)) == expected
        assert decodes == []

    def test_a_raw_stream_over_the_asset_limit_refuses_the_render(self, tmp_path, monkeypatch):
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        monkeypatch.setattr(_pdfium_images, "_MAX_ASSET_BYTES", len(FLATE) - 1)

        expected = f"the raw stream of object 0 is {len(FLATE)} bytes, over the {len(FLATE) - 1}-byte asset limit"
        assert refusal_of(path) == expected

    def test_an_image_past_the_page_stream_budget_refuses_the_render(self, tmp_path, monkeypatch):
        twice = b"q 80 0 0 40 10 20 cm /Im Do Q q 80 0 0 40 100 20 cm /Im Do Q"
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode", content=twice)
        monkeypatch.setattr(_pdfium_images, "_MAX_PAGE_STREAM_BYTES", len(FLATE))

        expected = f"object 1 was not read: the page's {len(FLATE)}-byte image stream budget is spent"
        assert refusal_of(path) == expected

    def test_an_image_drawn_under_an_overflowing_cm_refuses_the_render(self, tmp_path):
        # Its decode size is never read (A2 skips it), so it cannot pass the bounds.
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode", content=b"q " + OVERFLOW + b"/Im Do Q")

        assert refusal_of(path).startswith("object 0: its matrix to page space is not finite (")

    def test_a_refused_image_inside_a_form_is_named_by_its_walk_index(self, tmp_path):
        form_content = b"q 10 0 0 5 1 2 cm /Im Do Q"
        form = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Resources << /XObject << /Im 6 0 R >> >> "
        form += b"/Length %d >>\nstream\n%s\nendstream" % (len(form_content), form_content)
        path = content_page(
            tmp_path, b"/X Do", extra=(form, image_xobject(JPX_BOMB, JPX_16)), resources=b"/XObject << /X 5 0 R >>"
        )

        assert refusal_of(path) == JPX_BOMB_REFUSAL.format(index=1)

    def test_the_first_refused_image_in_walk_order_is_named(self, tmp_path):
        content = b"q 80 0 0 40 10 20 cm /Ok Do Q q 80 0 0 40 10 80 cm /Im Do Q q 80 0 0 40 10 140 cm /Im Do Q"
        ok = image_xobject(FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        resources = b"/XObject << /Im 5 0 R /Ok 6 0 R >>"
        path = content_page(tmp_path, content, extra=(image_xobject(JPX_BOMB, JPX_16), ok), resources=resources)

        assert refusal_of(path) == JPX_BOMB_REFUSAL.format(index=1)

    def test_a_pdfium_failure_while_sizing_refuses_the_render(self, tmp_path, monkeypatch):
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImagePixelSize", lambda *args: 0)

        assert refusal_of(path) == "pdfium cannot read the pixel size of object 0"

    def test_a_refusal_is_a_render_refused_naming_the_page(self, tmp_path):
        path = xobject_page(tmp_path, JPX_BOMB, JPX_16)

        message = f"page 1: {JPX_BOMB_REFUSAL.format(index=0)}; the page is not rendered"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            with render._guarded_page(path, 0, None):
                pytest.fail("the guard let a refused page through")

    def test_a_page_that_passes_is_yielded_loaded_with_its_geometry(self, tmp_path):
        path = one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG), rotate=90)

        with render._guarded_page(path, 0, None) as (page, geometry):
            assert (geometry.page_number, geometry.width_pt, geometry.height_pt) == (1, 300.0, 400.0)
            assert pdfium_c.FPDFPage_CountObjects(page.raw) == 1

    def test_an_unloadable_page_is_a_corrupt_page_not_a_refusal(self, tmp_path):
        path = break_page(build_pdf(tmp_path / "broken.pdf", [PageSpec()]), 1)

        with pytest.raises(PDFCorruptError, match="page 1 is unreadable"):
            with render._guarded_page(path, 0, None):
                pytest.fail("an unloadable page was yielded")

    @pytest.mark.parametrize(
        "replace,error,message",
        [
            pytest.param({"page": "page"}, TypeError, "page must be a pypdfium2 PdfPage, got str", id="page"),
            pytest.param({"geometry": None}, TypeError, "geometry must be a PageGeometry, got NoneType", id="geometry"),
            pytest.param({"max_objects": 0}, ValueError, "max_objects must be >= 1, got 0", id="bound"),
            pytest.param({"max_objects": True}, TypeError, "max_objects must be an int, got True", id="bound-bool"),
        ],
    )
    def test_invalid_arguments_raise(self, tmp_path, replace, error, message):
        path = content_page(tmp_path, FOUR_PATHS)
        with open_document(path) as handle, loaded_page(handle, 0) as (page, geometry):
            arguments = {"page": page, "geometry": geometry, "max_objects": 10, **replace}
            with pytest.raises(error, match=re.escape(message)):
                image_refusal(arguments["page"], arguments["geometry"], max_objects=arguments["max_objects"])

    def test_the_guard_walks_under_its_bound(self, tmp_path, monkeypatch):
        path = content_page(tmp_path, FOUR_PATHS)
        monkeypatch.setattr(render, "_MAX_GUARD_OBJECTS", 4)
        with render._guarded_page(path, 0, None) as (page, _geometry):
            assert pdfium_c.FPDFPage_CountObjects(page.raw) == 4

        monkeypatch.setattr(render, "_MAX_GUARD_OBJECTS", 3)
        message = "page 1: the page holds 4 objects, over the guard's 3-object bound; the page is not rendered"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            with render._guarded_page(path, 0, None):
                pytest.fail("a page over the guard's bound was yielded")

    def test_the_guard_walk_bound(self):
        # ~250 times the corpus's busiest page (3,977 objects, forms' included).
        assert render._MAX_GUARD_OBJECTS == 1_000_000


def chained_forms(tmp_path: Path, depth: int, last: bytes) -> Path:
    """A page drawing form 1, which draws form 2, and so on ``depth`` deep; the last form draws ``last``."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R "
        b"/Resources << /XObject << /X 5 0 R >> >> >>",
        b"<< /Length 5 >>\nstream\n/X Do\nendstream",
    ]
    for level in range(depth):
        body, resources = (
            (b"/X Do", b"/XObject << /X %d 0 R >>" % (len(objects) + 2)) if level < depth - 1 else (last, b"")
        )
        form = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Resources << %s >> /Length %d >>"
        objects.append(form % (resources, len(body)) + b"\nstream\n%s\nendstream" % body)
    return hand_written_pdf(tmp_path / "chained.pdf", objects)


FOUR_PATHS = b"10 10 m 20 20 l S 30 30 m 40 40 l S 50 50 m 60 60 l S 70 70 m 80 80 l S"


class TestBoundedWalk:
    """The guard walks A2's object walk with an explicit object-count and depth bound, and refuses at either."""

    def test_a_page_at_the_object_bound_passes(self, tmp_path):
        assert refusal_of(content_page(tmp_path, FOUR_PATHS), max_objects=4) is None

    def test_a_page_over_the_object_bound_is_refused_before_its_objects_are_read(self, tmp_path, monkeypatch):
        path = content_page(tmp_path, FOUR_PATHS)
        monkeypatch.setattr(pdfium_c, "FPDFPage_GetObject", lambda *args: pytest.fail("an object was read"))

        assert refusal_of(path, max_objects=3) == "the page holds 4 objects, over the guard's 3-object bound"

    def test_form_contents_count_toward_the_bound(self, tmp_path):
        path = chained_forms(tmp_path, 1, FOUR_PATHS)

        assert refusal_of(path, max_objects=5) is None
        expected = "form object 0 holds 4 objects, which take the page over the guard's 4-object bound"
        assert refusal_of(path, max_objects=4) == expected

    def test_a_form_with_objects_past_the_depth_bound_is_refused(self, tmp_path, monkeypatch):
        # pdfium parses forms 40 deep (#365 T2), under the 64-form bound: lower the bound to reach it.
        monkeypatch.setattr(pdfium_engine, "_MAX_FORM_DEPTH", 3)
        path = chained_forms(tmp_path, 4, b"10 10 m 20 20 l S")

        assert refusal_of(path) == "form object 3 holds 1 object(s) past the walk's 3-form depth bound"

    def test_an_empty_form_past_the_depth_bound_passes(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pdfium_engine, "_MAX_FORM_DEPTH", 3)

        assert refusal_of(chained_forms(tmp_path, 4, b"")) is None

    def test_the_extraction_walk_keeps_no_bound(self, tmp_path, monkeypatch):
        # extract_page reads every object pdfium parsed (100 % extraction): only the guard's walk refuses.
        monkeypatch.setattr(pdfium_engine, "_MAX_FORM_DEPTH", 3)
        page = open_document(chained_forms(tmp_path, 4, b"10 10 m 20 20 l S"))._pdf[0]

        assert len(pdfium_engine._page_objects(page.raw)) == 4
        with pytest.raises(pdfium_engine.WalkBoundHit, match="form object 3 holds 1 object"):
            pdfium_engine._page_objects(page.raw, max_objects=1_000_000)
