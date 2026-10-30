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
    Line,
    PageSpec,
    RasterImage,
    Rect,
    TextRun,
    build_pdf,
    first_unencodable,
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

    def test_rotated_page_size_is_unrotated(self, tmp_path):
        # pdfium behaviour later phases rely on: get_size() ignores /Rotate.
        path = build_pdf(tmp_path / "rot.pdf", [PageSpec(width=400.0, height=300.0, rotate=90)])

        page = pdfium.PdfDocument(path)[0]
        assert page.get_rotation() == 90
        assert page.get_size() == pytest.approx((400.0, 300.0))

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
            pytest.param([PageSpec()], {"strength": 128.0}, "strength must be an int, got 128.0", id="strength-float"),
            pytest.param([PageSpec()], {"strength": True}, "strength must be an int, got True", id="strength-bool"),
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
