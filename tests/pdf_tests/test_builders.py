"""Tests for the ReportLab test-PDF builder toolkit (#363).

Every assertion reads the built PDF back with pypdfium2, the v5 engine.
pdfium reports bottom-left page coordinates; the builders take top-left
ones, so each position check converts pdfium's numbers before comparing.
"""

import ctypes
import io
import math
import re

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest
from PIL import Image

from pdf_tests.builders import (
    FormXObject,
    Line,
    PageSpec,
    RasterImage,
    Rect,
    TextRun,
    break_page,
    build_pdf,
    first_unencodable,
    hand_written_pdf,
    solid_png,
)

# Position tolerance in page points.
TOL = 1.0


def stroke_tol(width: float) -> float:
    """Tolerance for a stroked path's bounds.

    pdfium pads an axis-aligned stroke's bounds by exactly 1x the width
    (measured 0.5/2.0/5.0 at widths 0.5/2/5); a diagonal pads by width x
    sqrt(2) (1.06 at 0.75), still inside TOL + width.
    """
    return TOL + width


def rect_corners(obj: pdfium.PdfObject, page_height: float) -> list[tuple[float, float]]:
    """Return a path's distinct points, sorted, in top-left page points."""
    return sorted(set(path_points(obj, page_height)))


def assert_points(actual: list[tuple[float, float]], expected: list[tuple[float, float]]) -> None:
    """Assert two point lists match pairwise within TOL."""
    assert len(actual) == len(expected)
    for got, want in zip(actual, expected, strict=True):
        assert got == pytest.approx(want, abs=TOL)


def page_objects(page: pdfium.PdfPage, kind: int) -> list[pdfium.PdfObject]:
    """Return the page's objects of one pdfium type (FPDF_PAGEOBJ_*)."""
    return list(page.get_objects(filter=(kind,)))


def _rgba(getter, obj: pdfium.PdfObject) -> tuple[int, int, int, int]:
    channels = [ctypes.c_uint() for _ in range(4)]
    assert getter(obj, *channels)
    red, green, blue, alpha = (channel.value for channel in channels)
    return red, green, blue, alpha


def fill_rgba(obj: pdfium.PdfObject) -> tuple[int, int, int, int]:
    """Return the object's fill colour as 0-255 (R, G, B, A)."""
    return _rgba(pdfium_c.FPDFPageObj_GetFillColor, obj)


def stroke_rgba(obj: pdfium.PdfObject) -> tuple[int, int, int, int]:
    """Return the object's stroke colour as 0-255 (R, G, B, A)."""
    return _rgba(pdfium_c.FPDFPageObj_GetStrokeColor, obj)


def top_left_bounds(obj: pdfium.PdfObject, page_height: float) -> tuple[float, float, float, float]:
    """Return the object's bounds as top-left (left, top, right, bottom) points."""
    left, bottom, right, top = obj.get_bounds()
    return left, page_height - top, right, page_height - bottom


def path_points(obj: pdfium.PdfObject, page_height: float) -> list[tuple[float, float]]:
    """Return a path object's segment points as top-left page points."""
    matrix = obj.get_matrix()
    points = []
    for index in range(pdfium_c.FPDFPath_CountSegments(obj)):
        x, y = ctypes.c_float(), ctypes.c_float()
        assert pdfium_c.FPDFPathSegment_GetPoint(pdfium_c.FPDFPath_GetPathSegment(obj, index), x, y)
        page_x, page_y = matrix.on_point(x.value, y.value)
        points.append((page_x, page_height - page_y))
    return points


def draw_mode(obj: pdfium.PdfObject) -> tuple[int, bool]:
    """Return a path object's (FPDF_FILLMODE_*, stroked)."""
    fill_mode, stroke = ctypes.c_int(), ctypes.c_int()
    assert pdfium_c.FPDFPath_GetDrawMode(obj, fill_mode, stroke)
    return fill_mode.value, bool(stroke.value)


def stroke_width(obj: pdfium.PdfObject) -> float:
    """Return the object's stroke width in points."""
    width = ctypes.c_float()
    assert pdfium_c.FPDFPageObj_GetStrokeWidth(obj, width)
    return width.value


def first_char_top_left(page: pdfium.PdfPage) -> tuple[float, float]:
    """Return the first character's (left, baseline) in top-left page points.

    Reads the tight char box, whose bottom is the baseline for a glyph with
    no descender; callers start their text with one ("T", "P").
    """
    left, bottom, _right, _top = page.get_textpage().get_charbox(0)
    return left, page.get_height() - bottom


def on_page(*items):
    """Return a one-page spec list holding ``items``."""
    return [PageSpec(items=items)]


class TestTextRunPosition:
    """A TextRun's (x, y) is its top-left baseline origin."""

    @pytest.mark.parametrize("width,height", [(612.0, 792.0), (400.0, 300.0)])
    def test_baseline_lands_at_top_left_position(self, tmp_path, width, height):
        target = tmp_path / "text.pdf"
        spec = PageSpec(width=width, height=height, items=(TextRun("Total", x=72.0, y=100.0),))

        result = build_pdf(target, [spec])

        assert result == target
        page = pdfium.PdfDocument(result)[0]
        left, baseline = first_char_top_left(page)
        assert left == pytest.approx(72.0, abs=TOL)
        assert baseline == pytest.approx(100.0, abs=TOL)

    @pytest.mark.parametrize(
        "x,y",
        [
            pytest.param(700.0, 100.0, id="right"),
            pytest.param(-200.0, 100.0, id="left"),
            pytest.param(72.0, 900.0, id="below"),
            pytest.param(72.0, -40.0, id="above"),
        ],
    )
    def test_off_page_text_is_kept_outside_the_page_box(self, tmp_path, x, y):
        path = build_pdf(tmp_path / "off.pdf", [PageSpec(items=(TextRun("Off", x=x, y=y),))])

        page = pdfium.PdfDocument(path)[0]
        textpage = page.get_textpage()
        left, bottom, right, top = textpage.get_charbox(0)
        assert textpage.get_text_range() == "Off"
        assert right <= 0 or left >= page.get_width() or top <= 0 or bottom >= page.get_height()


class TestTextRunStyle:
    """Font, size, colour, alpha and render mode reach the text object."""

    @pytest.mark.parametrize(
        "run,font,size",
        [
            pytest.param(TextRun("Total", x=72.0, y=100.0), "Helvetica", 11.0, id="defaults"),
            pytest.param(
                TextRun("Total", x=72.0, y=100.0, font="Courier-Bold", size=17.0), "Courier-Bold", 17.0, id="explicit"
            ),
        ],
    )
    def test_font_and_size(self, tmp_path, run, font, size):
        path = build_pdf(tmp_path / "font.pdf", [PageSpec(items=(run,))])

        (obj,) = page_objects(pdfium.PdfDocument(path)[0], pdfium_c.FPDF_PAGEOBJ_TEXT)
        matrix = obj.get_matrix()
        assert obj.get_font().get_base_name() == font
        assert obj.get_font_size() * math.hypot(matrix.c, matrix.d) == pytest.approx(size)

    @pytest.mark.parametrize(
        "run,rgba",
        [
            pytest.param(TextRun("Total", x=72.0, y=100.0), (0, 0, 0, 255), id="defaults"),
            pytest.param(
                TextRun("Total", x=72.0, y=100.0, rgb=(1.0, 0.0, 0.0), alpha=0.0), (255, 0, 0, 0), id="red-a0"
            ),
        ],
    )
    def test_fill_colour_and_alpha(self, tmp_path, run, rgba):
        path = build_pdf(tmp_path / "fill.pdf", [PageSpec(items=(run,))])

        (obj,) = page_objects(pdfium.PdfDocument(path)[0], pdfium_c.FPDF_PAGEOBJ_TEXT)
        assert fill_rgba(obj) == rgba

    @pytest.mark.parametrize("mode", [1, 2])
    def test_stroke_render_modes_use_rgb_and_alpha(self, tmp_path, mode):
        run = TextRun("Total", x=72.0, y=100.0, rgb=(1.0, 0.0, 0.0), alpha=0.0, render_mode=mode)
        path = build_pdf(tmp_path / "stroke.pdf", on_page(run))

        (obj,) = page_objects(pdfium.PdfDocument(path)[0], pdfium_c.FPDF_PAGEOBJ_TEXT)
        assert stroke_rgba(obj) == (255, 0, 0, 0)
        assert fill_rgba(obj) == (255, 0, 0, 0)

    @pytest.mark.parametrize(
        "kwargs,mode",
        [pytest.param({}, 0, id="default")] + [pytest.param({"render_mode": m}, m, id=f"mode{m}") for m in range(8)],
    )
    def test_render_mode(self, tmp_path, kwargs, mode):
        run = TextRun("Total", x=72.0, y=100.0, **kwargs)
        path = build_pdf(tmp_path / "mode.pdf", [PageSpec(items=(run,))])

        (obj,) = page_objects(pdfium.PdfDocument(path)[0], pdfium_c.FPDF_PAGEOBJ_TEXT)
        assert pdfium_c.FPDFTextObj_GetTextRenderMode(obj) == mode


class TestItemIsolation:
    """One item's graphics state never leaks into the next."""

    def test_rect_after_transparent_red_text_keeps_its_own_colour_and_alpha(self, tmp_path):
        run = TextRun("Total", x=72.0, y=100.0, rgb=(1.0, 0.0, 0.0), alpha=0.0)
        rect = Rect(100.0, 400.0, 200.0, 50.0, fill_rgb=(0.0, 0.0, 1.0))
        path = build_pdf(tmp_path / "two.pdf", on_page(run, rect))

        (obj,) = page_objects(pdfium.PdfDocument(path)[0], pdfium_c.FPDF_PAGEOBJ_PATH)
        assert fill_rgba(obj) == (0, 0, 255, 255)
        assert stroke_rgba(obj) == (0, 0, 0, 255)


STANDARD_14_FONTS = [
    "Courier",
    "Courier-Bold",
    "Courier-Oblique",
    "Courier-BoldOblique",
    "Helvetica",
    "Helvetica-Bold",
    "Helvetica-Oblique",
    "Helvetica-BoldOblique",
    "Times-Roman",
    "Times-Bold",
    "Times-Italic",
    "Times-BoldItalic",
    "Symbol",
    "ZapfDingbats",
]


class TestTextRunEncoding:
    """A run draws every character as itself: no glyph substitution."""

    @pytest.mark.parametrize("font", STANDARD_14_FONTS)
    def test_every_accepted_bmp_character_round_trips(self, tmp_path, font):
        accepted = "".join(char for char in map(chr, range(0x10000)) if first_unencodable(font, char) is None)
        run = TextRun(accepted, x=10.0, y=100.0, font=font, size=6.0)
        path = build_pdf(tmp_path / "chars.pdf", on_page(run))

        assert len(accepted) > 150
        assert pdfium.PdfDocument(path)[0].get_textpage().get_text_range() == accepted

    @pytest.mark.parametrize(
        "text,font,code",
        [
            pytest.param("\ufb01x", "Helvetica", "U+FB01", id="ligature"),
            pytest.param("日本語", "Helvetica", "U+65E5", id="cjk"),
            pytest.param("a\u200db\u200ec", "Helvetica", "U+200D", id="zwj"),
            pytest.param("αβγ", "Helvetica", "U+03B1", id="greek-needs-symbol"),
            pytest.param("1\u00a02", "Helvetica", "U+00A0", id="nbsp-reads-as-space"),
            pytest.param("co\u00adop", "Times-Roman", "U+00AD", id="soft-hyphen-reads-as-hyphen"),
            pytest.param("Δ", "Symbol", "U+0394", id="delta-reads-as-increment"),
            pytest.param("A", "ZapfDingbats", "U+0041", id="latin-in-dingbats"),
        ],
    )
    def test_unencodable_character_raises(self, tmp_path, text, font, code):
        target = tmp_path / "bad.pdf"
        message = f"TextRun.text has {code}, which {font} cannot draw without glyph substitution"

        with pytest.raises(ValueError, match=re.escape(message)):
            build_pdf(target, on_page(TextRun(text, x=72.0, y=100.0, font=font)))
        assert not target.exists()
        assert first_unencodable(font, text) == chr(int(code[2:], 16))

    @pytest.mark.parametrize("text,code", [("A\nB", "U+000A"), ("A\rB", "U+000D")])
    def test_line_break_raises(self, tmp_path, text, code):
        target = tmp_path / "bad.pdf"
        message = f"TextRun.text must be one line (one run = one line), got {code}"

        with pytest.raises(ValueError, match=re.escape(message)):
            build_pdf(target, on_page(TextRun(text, x=72.0, y=100.0)))
        assert not target.exists()


class TestLine:
    """A Line strokes one segment between two top-left points."""

    def test_line_endpoints_bounds_and_width(self, tmp_path):
        line = Line(50.0, 100.0, 250.0, 300.0, width=0.75)
        path = build_pdf(tmp_path / "line.pdf", [PageSpec(items=(line,))])

        page = pdfium.PdfDocument(path)[0]
        (obj,) = page_objects(page, pdfium_c.FPDF_PAGEOBJ_PATH)
        start, end = path_points(obj, page.get_height())
        assert start == pytest.approx((50.0, 100.0), abs=TOL)
        assert end == pytest.approx((250.0, 300.0), abs=TOL)
        assert top_left_bounds(obj, page.get_height()) == pytest.approx(
            (50.0, 100.0, 250.0, 300.0), abs=stroke_tol(0.75)
        )
        assert draw_mode(obj) == (pdfium_c.FPDF_FILLMODE_NONE, True)
        assert stroke_width(obj) == pytest.approx(0.75)


class TestRect:
    """A Rect's (x, y) is its top-left corner; it strokes, fills, or both."""

    def test_stroked_rect_bounds_and_line_width(self, tmp_path):
        rect = Rect(100.0, 400.0, 200.0, 50.0, line_width=2.0)
        path = build_pdf(tmp_path / "rect.pdf", [PageSpec(items=(rect,))])

        page = pdfium.PdfDocument(path)[0]
        (obj,) = page_objects(page, pdfium_c.FPDF_PAGEOBJ_PATH)
        assert top_left_bounds(obj, page.get_height()) == pytest.approx(
            (100.0, 400.0, 300.0, 450.0), abs=stroke_tol(2.0)
        )
        corners = [(100.0, 400.0), (100.0, 450.0), (300.0, 400.0), (300.0, 450.0)]
        assert_points(rect_corners(obj, page.get_height()), corners)
        assert draw_mode(obj) == (pdfium_c.FPDF_FILLMODE_NONE, True)
        assert stroke_width(obj) == pytest.approx(2.0)

    def test_filled_rect_without_stroke(self, tmp_path):
        rect = Rect(100.0, 600.0, 200.0, 50.0, fill_rgb=(0.0, 0.0, 1.0), stroke=False)
        path = build_pdf(tmp_path / "fill.pdf", [PageSpec(items=(rect,))])

        page = pdfium.PdfDocument(path)[0]
        (obj,) = page_objects(page, pdfium_c.FPDF_PAGEOBJ_PATH)
        fill_mode, stroked = draw_mode(obj)
        assert top_left_bounds(obj, page.get_height()) == pytest.approx((100.0, 600.0, 300.0, 650.0), abs=TOL)
        corners = [(100.0, 600.0), (100.0, 650.0), (300.0, 600.0), (300.0, 650.0)]
        assert_points(rect_corners(obj, page.get_height()), corners)
        assert fill_mode != pdfium_c.FPDF_FILLMODE_NONE
        assert not stroked
        assert fill_rgba(obj) == (0, 0, 255, 255)


class TestRasterImage:
    """solid_png makes pixels; RasterImage places them by top-left corner."""

    def test_solid_png_is_a_uniform_png(self):
        image = Image.open(io.BytesIO(solid_png(3, 2, (1.0, 0.0, 0.0))))

        assert image.format == "PNG"
        assert image.size == (3, 2)
        assert image.getcolors() == [(6, (255, 0, 0))]

    def test_raster_image_bounds_and_pixel_size(self, tmp_path):
        image = RasterImage(300.0, 500.0, 120.0, 80.0, png=solid_png(30, 20, (0.0, 1.0, 0.0)))
        path = build_pdf(tmp_path / "image.pdf", [PageSpec(items=(image,))])

        page = pdfium.PdfDocument(path)[0]
        (obj,) = page_objects(page, pdfium_c.FPDF_PAGEOBJ_IMAGE)
        assert top_left_bounds(obj, page.get_height()) == pytest.approx((300.0, 500.0, 420.0, 580.0), abs=TOL)
        assert obj.get_px_size() == (30, 20)

    @pytest.mark.parametrize(
        "alpha,rendered",
        [
            pytest.param(0, (255, 255, 255), id="transparent"),
            pytest.param(128, (255, 127, 127), id="half"),
            pytest.param(255, (255, 0, 0), id="opaque"),
        ],
    )
    def test_png_alpha_is_kept(self, tmp_path, alpha, rendered):
        buffer = io.BytesIO()
        Image.new("RGBA", (20, 20), (255, 0, 0, alpha)).save(buffer, format="PNG")
        image = RasterImage(0.0, 0.0, 100.0, 100.0, png=buffer.getvalue())
        path = build_pdf(tmp_path / "rgba.pdf", [PageSpec(width=100.0, height=100.0, items=(image,))])

        # Red over pdfium's white page background: alpha shows as lightness.
        pixel = pdfium.PdfDocument(path)[0].render(scale=1).to_pil().getpixel((50, 50))
        assert pixel[:3] == pytest.approx(rendered, abs=3)


SECRET_PAGE = PageSpec(items=(TextRun("Secret", x=72.0, y=100.0),))


class TestEncryption:
    """Passwords encrypt the file; pdfium refuses it without the right one."""

    @pytest.mark.parametrize(
        "strength,revision",
        [
            pytest.param(40, 2, id="rc4-40"),
            pytest.param(128, 3, id="rc4-128"),
        ],
    )
    def test_user_password_is_required_to_open(self, tmp_path, strength, revision):
        path = build_pdf(
            tmp_path / "enc.pdf", [SECRET_PAGE], user_password="user-pw", owner_password="owner-pw", strength=strength
        )

        with pytest.raises(pdfium.PdfiumError, match="password"):
            pdfium.PdfDocument(path)
        pdf = pdfium.PdfDocument(path, password="user-pw")
        assert pdfium_c.FPDF_GetSecurityHandlerRevision(pdf) == revision
        assert pdf[0].get_textpage().get_text_range() == "Secret"

    def test_owner_password_also_opens(self, tmp_path):
        path = build_pdf(
            tmp_path / "enc.pdf", [SECRET_PAGE], user_password="user-pw", owner_password="owner-pw", strength=128
        )

        with pytest.raises(pdfium.PdfiumError, match="password"):
            pdfium.PdfDocument(path)
        pdf = pdfium.PdfDocument(path, password="owner-pw")
        assert pdf[0].get_textpage().get_text_range() == "Secret"

    def test_owner_password_alone_encrypts_at_default_128_without_an_open_password(self, tmp_path):
        path = build_pdf(tmp_path / "enc.pdf", [SECRET_PAGE], owner_password="owner-pw")

        pdf = pdfium.PdfDocument(path)
        assert pdfium_c.FPDF_GetSecurityHandlerRevision(pdf) == 3  # RC4-128, the default strength
        assert pdf[0].get_textpage().get_text_range() == "Secret"

    def test_no_password_means_no_encryption(self, tmp_path):
        path = build_pdf(tmp_path / "plain.pdf", [SECRET_PAGE])

        assert pdfium_c.FPDF_GetSecurityHandlerRevision(pdfium.PdfDocument(path)) == -1


class TestDocumentInfo:
    """``title`` and ``author`` set the document's /Info entries; None keeps ReportLab's own."""

    def test_title_and_author_are_written(self, tmp_path):
        path = build_pdf(
            tmp_path / "info.pdf", [SECRET_PAGE], title="Quarterly \u2014 R\u00e9sum\u00e9 \U0001f600", author="Ada"
        )

        info = pdfium.PdfDocument(path).get_metadata_dict()
        assert (info["Title"], info["Author"]) == ("Quarterly \u2014 R\u00e9sum\u00e9 \U0001f600", "Ada")

    def test_none_keeps_reportlabs_defaults(self, tmp_path):
        info = pdfium.PdfDocument(build_pdf(tmp_path / "info.pdf", [SECRET_PAGE])).get_metadata_dict()

        assert (info["Title"], info["Author"]) == ("untitled", "anonymous")


class TestDeterminism:
    """The same spec always builds the same bytes."""

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({}, id="plain"),
            pytest.param({"user_password": "user-pw", "owner_password": "owner-pw"}, id="encrypted"),
        ],
    )
    def test_same_spec_builds_identical_bytes(self, tmp_path, kwargs):
        first = build_pdf(tmp_path / "first.pdf", [SECRET_PAGE], **kwargs).read_bytes()
        second = build_pdf(tmp_path / "second.pdf", [SECRET_PAGE], **kwargs).read_bytes()

        assert first == second


class TestPages:
    """Each PageSpec becomes one page with its own size and rotation."""

    @pytest.mark.parametrize("rotate", [0, 90, 180, 270])
    def test_page_rotation(self, tmp_path, rotate):
        path = build_pdf(tmp_path / "rot.pdf", [PageSpec(rotate=rotate)])

        assert pdfium.PdfDocument(path)[0].get_rotation() == rotate

    @pytest.mark.parametrize(
        "rotate,displayed",
        [(0, (400.0, 300.0)), (90, (300.0, 400.0)), (180, (400.0, 300.0)), (270, (300.0, 400.0))],
    )
    def test_spec_size_is_the_unrotated_media_box(self, tmp_path, rotate, displayed):
        # pdfium behaviour later phases rely on: get_bbox() is the unrotated
        # box, get_size() the displayed one (swapped by /Rotate 90 and 270).
        path = build_pdf(tmp_path / "rot.pdf", [PageSpec(width=400.0, height=300.0, rotate=rotate)])

        page = pdfium.PdfDocument(path)[0]
        assert page.get_rotation() == rotate
        assert page.get_bbox() == pytest.approx((0.0, 0.0, 400.0, 300.0))
        assert page.get_size() == pytest.approx(displayed)

    @pytest.mark.parametrize("rotate", [0, 90, 180, 270])
    def test_rotated_page_items_stay_in_unrotated_space(self, tmp_path, rotate):
        spec = PageSpec(width=400.0, height=300.0, items=(TextRun("Total", x=72.0, y=100.0),), rotate=rotate)
        path = build_pdf(tmp_path / "rot.pdf", [spec])

        page = pdfium.PdfDocument(path)[0]
        left, bottom, _right, _top = page.get_textpage().get_charbox(0)
        unrotated_height = page.get_bbox()[3]
        assert (left, unrotated_height - bottom) == pytest.approx((72.0, 100.0), abs=TOL)

    @pytest.mark.parametrize("rotate,displayed", [(0, (300.0, 240.0)), (90, (240.0, 300.0))])
    def test_crop_box_is_the_visible_box(self, tmp_path, rotate, displayed):
        # Top-left (50, 20)-(350, 260) on a 400 x 300 page is PDF (50, 40)-(350, 280).
        spec = PageSpec(width=400.0, height=300.0, rotate=rotate, crop_box=(50.0, 20.0, 350.0, 260.0))
        path = build_pdf(tmp_path / "crop.pdf", [spec])

        page = pdfium.PdfDocument(path)[0]
        assert page.get_mediabox() == pytest.approx((0.0, 0.0, 400.0, 300.0))
        assert page.get_cropbox() == pytest.approx((50.0, 40.0, 350.0, 280.0))
        assert page.get_bbox() == pytest.approx((50.0, 40.0, 350.0, 280.0))
        assert page.get_size() == pytest.approx(displayed)

    def test_crop_box_applies_to_its_own_page_only(self, tmp_path):
        pages = [PageSpec(crop_box=(10.0, 10.0, 100.0, 100.0)), PageSpec()]
        path = build_pdf(tmp_path / "crop.pdf", pages)

        second = pdfium.PdfDocument(path)[1]
        assert second.get_cropbox(fallback_ok=False) is None
        assert second.get_bbox() == pytest.approx((0.0, 0.0, 612.0, 792.0))

    def test_multi_page_count_sizes_and_content(self, tmp_path):
        sizes = [(612.0, 792.0), (400.0, 300.0), (842.0, 595.0)]
        pages = [
            PageSpec(width=w, height=h, items=(TextRun(f"Page {n}", x=72.0, y=100.0),))
            for n, (w, h) in enumerate(sizes, 1)
        ]
        path = build_pdf(tmp_path / "multi.pdf", pages)

        pdf = pdfium.PdfDocument(path)
        assert len(pdf) == 3
        for n, (page, size) in enumerate(zip(pdf, sizes, strict=True), 1):
            assert page.get_size() == pytest.approx(size)
            assert page.get_textpage().get_text_range() == f"Page {n}"
            assert first_char_top_left(page)[1] == pytest.approx(100.0, abs=TOL)


class TestBreakPage:
    """break_page leaves one page that pdfium counts but cannot load."""

    @pytest.mark.parametrize("kwargs", [pytest.param({}, id="plain"), pytest.param({"user_password": "pw"}, id="enc")])
    def test_only_the_broken_page_fails_to_load(self, tmp_path, kwargs):
        pages = [PageSpec(items=(TextRun(f"Page {n}", x=72.0, y=100.0),)) for n in (1, 2, 3)]
        path = build_pdf(tmp_path / "three.pdf", pages, **kwargs)
        size = path.stat().st_size

        assert break_page(path, 2) == path
        assert path.stat().st_size == size
        pdf = pdfium.PdfDocument(path, password=kwargs.get("user_password"))
        assert len(pdf) == 3
        with pytest.raises(pdfium.PdfiumError, match="Failed to load page"):
            pdf[1]
        assert [pdf[i].get_textpage().get_text_range() for i in (0, 2)] == ["Page 1", "Page 3"]

    @pytest.mark.parametrize("page_number,error", [(0, ValueError), (4, ValueError), (True, TypeError)])
    def test_page_number_outside_the_file_raises(self, tmp_path, page_number, error):
        path = build_pdf(tmp_path / "three.pdf", [PageSpec(), PageSpec(), PageSpec()])
        original = path.read_bytes()

        with pytest.raises(error, match="page_number"):
            break_page(path, page_number)
        assert path.read_bytes() == original


class TestFormXObject:
    """A FormXObject draws its items inside a Form XObject placed on the page."""

    def test_text_is_nested_in_a_form_at_its_scaled_page_position(self, tmp_path):
        run = TextRun("Tx", x=10.0, y=40.0, size=10.0)
        form = FormXObject(x=100.0, y=50.0, w=150.0, h=60.0, items=(run,), scale=2.0)
        page = pdfium.PdfDocument(build_pdf(tmp_path / "form.pdf", on_page(form)))[0]

        objects = list(page.get_objects())
        assert [(obj.type, obj.level) for obj in objects] == [
            (pdfium_c.FPDF_PAGEOBJ_FORM, 0),
            (pdfium_c.FPDF_PAGEOBJ_TEXT, 1),
        ]
        # Form (10, 40) at scale 2 from the form's top-left corner (100, 50).
        assert first_char_top_left(page) == pytest.approx((120.0, 130.0), abs=TOL)
        matrix = pdfium_c.FS_MATRIX()
        assert pdfium_c.FPDFText_GetMatrix(page.get_textpage(), 0, matrix)
        assert (matrix.a, matrix.b, matrix.c, matrix.d) == pytest.approx((2.0, 0.0, 0.0, 2.0))

    def test_rect_in_a_form_renders_at_its_scaled_page_box(self, tmp_path):
        rect = Rect(20.0, 10.0, 30.0, 15.0, fill_rgb=(0.0, 0.0, 0.0), stroke=False)
        form = FormXObject(x=100.0, y=50.0, w=150.0, h=60.0, items=(rect,), scale=2.0)
        page = pdfium.PdfDocument(build_pdf(tmp_path / "form.pdf", on_page(form)))[0]

        image = page.render(scale=1).to_pil().convert("L")

        ink = image.point(lambda p: 255 if p < 128 else 0).getbbox()
        assert ink == pytest.approx((140.0, 70.0, 200.0, 100.0), abs=TOL)

    def test_several_forms_on_several_pages_each_draw_their_own_items(self, tmp_path):
        def form(text: str, y: float) -> FormXObject:
            return FormXObject(x=72.0, y=y, w=200.0, h=40.0, items=(TextRun(text, x=0.0, y=20.0),))

        # Apart: pdfium drops a char drawn over the same char (A1 on A2 reads "A12").
        pages = [PageSpec(items=(form("A1", 72.0), form("A2", 172.0))), PageSpec(items=(form("B1", 72.0),))]
        pdf = pdfium.PdfDocument(build_pdf(tmp_path / "forms.pdf", pages))

        texts = [page.get_textpage().get_text_range() for page in pdf]
        # pdfium places its generated line breaks by form-space position, and
        # both lines sit at form (0, 20): it inserts no break between them.
        assert texts == ["A1A2", "B1"]


class TestHandWrittenPdf:
    """hand_written_pdf writes raw objects behind an xref that points at each one."""

    def test_xref_offsets_point_at_each_object_and_pdfium_reads_it(self, tmp_path):
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 100] >>",
        ]

        path = hand_written_pdf(tmp_path / "hand.pdf", objects)

        data = path.read_bytes()
        xref = int(data.rsplit(b"startxref\n", 1)[1].split(b"\n", 1)[0])
        entries = data[xref:].split(b"\n")[3 : 3 + len(objects)]
        offsets = [int(entry[:10]) for entry in entries]
        assert [data[offset:].split(b"\n", 1)[0] for offset in offsets] == [b"1 0 obj", b"2 0 obj", b"3 0 obj"]
        assert pdfium.PdfDocument(path)[0].get_size() == (200.0, 100.0)


class TestValidation:
    """build_pdf rejects a bad spec before it writes anything."""

    @pytest.mark.parametrize(
        "pages,kwargs,message",
        [
            pytest.param([], {}, "pages must not be empty", id="no-pages"),
            pytest.param([PageSpec(width=0.0)], {}, "PageSpec.width must be > 0, got 0.0", id="page-width"),
            pytest.param([PageSpec(height=-1.0)], {}, "PageSpec.height must be > 0, got -1.0", id="page-height"),
            pytest.param(
                [PageSpec(rotate=45)], {}, "PageSpec.rotate must be one of (0, 90, 180, 270), got 45", id="rotate"
            ),
            pytest.param(on_page(TextRun("", 72.0, 100.0)), {}, "TextRun.text must not be empty", id="text-empty"),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, size=0.0)), {}, "TextRun.size must be > 0, got 0.0", id="text-size"
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, font="Comic-Sans")),
                {},
                "TextRun.font must be one of the standard 14 PDF fonts, got 'Comic-Sans'",
                id="font-unknown",
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, alpha=-0.1)),
                {},
                "TextRun.alpha must be in [0, 1], got -0.1",
                id="alpha-low",
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, alpha=1.5)),
                {},
                "TextRun.alpha must be in [0, 1], got 1.5",
                id="alpha-high",
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, render_mode=-1)),
                {},
                "TextRun.render_mode must be in 0..7, got -1",
                id="mode-low",
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, render_mode=8)),
                {},
                "TextRun.render_mode must be in 0..7, got 8",
                id="mode-high",
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, rgb=(255, 0, 0))),
                {},
                "TextRun.rgb must be 3 channels in [0, 1], got (255, 0, 0)",
                id="text-rgb",
            ),
            pytest.param(
                on_page(Line(0.0, 0.0, 10.0, 10.0, width=0.0)), {}, "Line.width must be > 0, got 0.0", id="line-width"
            ),
            pytest.param(on_page(Rect(0.0, 0.0, 0.0, 10.0)), {}, "Rect.w must be > 0, got 0.0", id="rect-w"),
            pytest.param(on_page(Rect(0.0, 0.0, 10.0, -5.0)), {}, "Rect.h must be > 0, got -5.0", id="rect-h"),
            pytest.param(
                on_page(Rect(0.0, 0.0, 10.0, 10.0, line_width=0.0)),
                {},
                "Rect.line_width must be > 0, got 0.0",
                id="rect-line-width",
            ),
            pytest.param(
                on_page(Rect(0.0, 0.0, 10.0, 10.0, fill_rgb=(0.0, 0.0))),
                {},
                "Rect.fill_rgb must be 3 channels in [0, 1], got (0.0, 0.0)",
                id="rect-fill-rgb",
            ),
            pytest.param(
                on_page(Rect(0.0, 0.0, 10.0, 10.0, stroke=False)),
                {},
                "Rect draws nothing: stroke=False and fill_rgb=None",
                id="rect-invisible",
            ),
            pytest.param(
                on_page(RasterImage(0.0, 0.0, 0.0, 10.0, png=b"")), {}, "RasterImage.w must be > 0, got 0.0", id="img-w"
            ),
            pytest.param(
                on_page(RasterImage(0.0, 0.0, 10.0, 0.0, png=b"")), {}, "RasterImage.h must be > 0, got 0.0", id="img-h"
            ),
            pytest.param([PageSpec(width=math.inf)], {}, "PageSpec.width must be finite, got inf", id="page-width-inf"),
            pytest.param(
                on_page(TextRun("a", math.nan, 100.0)), {}, "TextRun.x must be finite, got nan", id="text-x-nan"
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, size=math.inf)),
                {},
                "TextRun.size must be finite, got inf",
                id="text-size-inf",
            ),
            pytest.param(
                on_page(Line(0.0, 0.0, 10.0, math.inf)), {}, "Line.y1 must be finite, got inf", id="line-y1-inf"
            ),
            pytest.param(
                on_page(Rect(0.0, math.nan, 10.0, 10.0)), {}, "Rect.y must be finite, got nan", id="rect-y-nan"
            ),
            pytest.param(
                on_page(Rect(0.0, 0.0, 10.0, math.inf)), {}, "Rect.h must be finite, got inf", id="rect-h-inf"
            ),
            pytest.param(
                on_page(RasterImage(-math.inf, 0.0, 10.0, 10.0, png=b"")),
                {},
                "RasterImage.x must be finite, got -inf",
                id="img-x-ninf",
            ),
            pytest.param(
                on_page(RasterImage(0.0, 0.0, 10.0, 10.0, png=b"GIF89a")),
                {},
                "RasterImage.png must be PNG data (it lacks the PNG signature)",
                id="img-not-png",
            ),
            pytest.param(
                [PageSpec(crop_box=(0.0, 0.0, 10.0))],
                {},
                "PageSpec.crop_box must have 4 values (x0, y0, x1, y1), got 3",
                id="crop-length",
            ),
            pytest.param(
                [PageSpec(crop_box=(0.0, 0.0, math.inf, 10.0))],
                {},
                "PageSpec.crop_box[2] must be finite, got inf",
                id="crop-inf",
            ),
            pytest.param(
                [PageSpec(crop_box=(50.0, 0.0, 50.0, 10.0))],
                {},
                "PageSpec.crop_box must have x0 < x1 and y0 < y1, got (50.0, 0.0, 50.0, 10.0)",
                id="crop-empty-x",
            ),
            pytest.param(
                [PageSpec(crop_box=(0.0, 20.0, 10.0, 10.0))],
                {},
                "PageSpec.crop_box must have x0 < x1 and y0 < y1, got (0.0, 20.0, 10.0, 10.0)",
                id="crop-reversed-y",
            ),
            pytest.param(
                [PageSpec(crop_box=(-1.0, 0.0, 10.0, 10.0))],
                {},
                "PageSpec.crop_box must lie within the 612.0 x 792.0 page, got (-1.0, 0.0, 10.0, 10.0)",
                id="crop-off-page-left",
            ),
            pytest.param(
                [PageSpec(crop_box=(0.0, 0.0, 10.0, 800.0))],
                {},
                "PageSpec.crop_box must lie within the 612.0 x 792.0 page, got (0.0, 0.0, 10.0, 800.0)",
                id="crop-off-page-bottom",
            ),
            pytest.param(
                on_page(FormXObject(0.0, 0.0, 0.0, 10.0)), {}, "FormXObject.w must be > 0, got 0.0", id="form-w"
            ),
            pytest.param(
                on_page(FormXObject(0.0, 0.0, 10.0, -1.0)), {}, "FormXObject.h must be > 0, got -1.0", id="form-h"
            ),
            pytest.param(
                on_page(FormXObject(0.0, 0.0, 10.0, 10.0, scale=0.0)),
                {},
                "FormXObject.scale must be > 0, got 0.0",
                id="form-scale",
            ),
            pytest.param(
                on_page(FormXObject(float("nan"), 0.0, 10.0, 10.0)),
                {},
                "FormXObject.x must be finite, got nan",
                id="form-x",
            ),
            pytest.param(
                on_page(FormXObject(0.0, 0.0, 10.0, 10.0, items=(TextRun("", 1.0, 1.0),))),
                {},
                "TextRun.text must not be empty",
                id="form-item-checked",
            ),
            pytest.param([PageSpec()], {"strength": 64}, "strength must be one of (40, 128), got 64", id="strength"),
            pytest.param(
                [PageSpec()],
                {"strength": 256},
                "strength 256 (AES-256) is not supported: ReportLab needs pyaes for it, and this toolkit does not "
                "install it; use 40 or 128",
                id="strength-aes-256",
            ),
        ],
    )
    def test_invalid_spec_raises_value_error(self, tmp_path, pages, kwargs, message):
        target = tmp_path / "bad.pdf"

        with pytest.raises(ValueError, match=re.escape(message)):
            build_pdf(target, pages, **kwargs)
        assert not target.exists()

    @pytest.mark.parametrize(
        "pages,kwargs,message",
        [
            pytest.param(["page"], {}, "pages must hold PageSpec, got str", id="page"),
            pytest.param(on_page("item"), {}, "unsupported page item: str", id="item"),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, render_mode=3.0)),
                {},
                "TextRun.render_mode must be an int, got 3.0",
                id="mode-float",
            ),
            pytest.param(
                on_page(TextRun("a", 72.0, 100.0, render_mode=True)),
                {},
                "TextRun.render_mode must be an int, got True",
                id="mode-bool",
            ),
            pytest.param([PageSpec(rotate=90.0)], {}, "PageSpec.rotate must be an int, got 90.0", id="rotate-float"),
            pytest.param([PageSpec(rotate=False)], {}, "PageSpec.rotate must be an int, got False", id="rotate-bool"),
            pytest.param(
                [PageSpec(crop_box=[0.0, 0.0, 10.0, 10.0])],
                {},
                "PageSpec.crop_box must be a tuple or None, got list",
                id="crop-list",
            ),
            pytest.param(
                on_page(FormXObject(0.0, 0.0, 10.0, 10.0, items=(FormXObject(0.0, 0.0, 5.0, 5.0),))),
                {},
                "FormXObject.items cannot hold a FormXObject: forms do not nest",
                id="form-nested",
            ),
            pytest.param(
                on_page(FormXObject(0.0, 0.0, 10.0, 10.0, items=("item",))),
                {},
                "unsupported page item: str",
                id="form-item",
            ),
            pytest.param([PageSpec()], {"strength": 128.0}, "strength must be an int, got 128.0", id="strength-float"),
            pytest.param([PageSpec()], {"strength": True}, "strength must be an int, got True", id="strength-bool"),
            pytest.param([PageSpec()], {"title": 1}, "title must be a str or None, got 1", id="title"),
            pytest.param([PageSpec()], {"author": b"Ada"}, "author must be a str or None, got b'Ada'", id="author"),
        ],
    )
    def test_wrong_type_raises_type_error(self, tmp_path, pages, kwargs, message):
        target = tmp_path / "bad.pdf"

        with pytest.raises(TypeError, match=re.escape(message)):
            build_pdf(target, pages, **kwargs)
        assert not target.exists()

    @pytest.mark.parametrize(
        "args,message",
        [
            pytest.param((0, 2, (0.0, 0.0, 0.0)), "solid_png width_px must be > 0, got 0", id="width"),
            pytest.param((2, -1, (0.0, 0.0, 0.0)), "solid_png height_px must be > 0, got -1", id="height"),
            pytest.param(
                (2, 2, (1.5, 0.0, 0.0)), "solid_png rgb must be 3 channels in [0, 1], got (1.5, 0.0, 0.0)", id="rgb"
            ),
        ],
    )
    def test_solid_png_rejects_bad_input(self, args, message):
        with pytest.raises(ValueError, match=re.escape(message)):
            solid_png(*args)
