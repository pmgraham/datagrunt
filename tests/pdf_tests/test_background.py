"""Tests for the v5 background check (#400): text whose paint matches the rendered background is flagged.

Unit tests build a :class:`~datagrunt.pdf.quality.Backdrop` by hand (24-bit
BGR, one pixel a point unless a test says otherwise) under spans built
directly on a 400 x 300 pt page. Fixture tests build PDFs with the #363
toolkit (``pdf_tests.builders``), read them with ``extract_page``, and draw
their backdrop as the worker does (BGR, text removed, at 72 DPI): in this
process, through ``render._render_loaded``, or through ``render_pages``.
"""

import dataclasses
import io
import random
import re
from pathlib import Path

import pytest
from PIL import Image

from datagrunt.pdf import quality, render
from datagrunt.pdf.pdfium_engine import RawPage, extract_page, open_document
from datagrunt.pdf.quality import MATCH_DISTANCE, Backdrop, SpanPiece, classify_spans, flag_background
from datagrunt.pdf.render import RenderedPage, render_pages
from pdf_tests.builders import PageSpec, RasterImage, Rect, TextRun, build_pdf, solid_png
from pdf_tests.test_quality import raw_span, row

W, H = 400.0, 300.0
WHITE, BLACK = (255, 255, 255), (0, 0, 0)
FLAGGED = ("matches_background",)
# A white page holding one black box, (100, 50)-(200, 100) pt, drawn 1 px a point.
BOX = (100, 50, 200, 100)


def backdrop(fill=WHITE, *boxes, scale: int = 1) -> Backdrop:
    """A 400 x 300 pt page drawn at ``scale`` px a point: ``fill`` (RGB), then each ``(box, rgb)`` painted over it.

    A box is ``(x0, y0, x1, y1)`` in points, ends excluded.
    """
    width, height = int(W) * scale, int(H) * scale
    pixels = bytearray(bytes((fill[2], fill[1], fill[0])) * (width * height))
    for (x0, y0, x1, y1), (red, green, blue) in boxes:
        for y in range(y0 * scale, y1 * scale):
            for x in range(x0 * scale, x1 * scale):
                at = (y * width + x) * 3
                pixels[at : at + 3] = bytes((blue, green, red))
    return Backdrop(pixels=bytes(pixels), px_width=width, px_height=height)


def painted(rgb, *, mode: int = 0, stroke=None, text: str = "Abcd", boxes=None) -> SpanPiece:
    """A visible piece, the whole of a span painted ``rgb`` (fill; and stroke unless ``stroke`` is given)."""
    span = raw_span(
        text,
        boxes,
        render_mode=mode,
        fill_rgba=(*rgb, 255),
        stroke_rgba=(*(rgb if stroke is None else stroke), 255),
    )
    return SpanPiece(span=span, start=0, end=len(text), hidden_reasons=())


def flags(*pieces: SpanPiece, ground: Backdrop | None = None) -> list[tuple[str, ...]]:
    """The hidden reasons of each piece once flag_background has checked it against ``ground`` (white by default)."""
    checked = flag_background(pieces, ground or backdrop(), width_pt=W, height_pt=H)
    return [piece.hidden_reasons for piece in checked]


class TestBackdrop:
    """A Backdrop is a validated, frozen 24-bit BGR picture of a page drawn without its text."""

    def test_valid(self):
        ground = Backdrop(pixels=bytes(18), px_width=3, px_height=2)

        assert (ground.px_width, ground.px_height, len(ground.pixels)) == (3, 2, 18)
        assert "pixels" not in repr(ground)
        with pytest.raises(dataclasses.FrozenInstanceError):
            ground.px_width = 4

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"pixels": bytearray(18)}, TypeError, "pixels must be bytes, got bytearray", id="bytearray"),
            pytest.param({"pixels": bytes(6)}, ValueError, "pixels must hold 3 x 2 x 3 = 18 bytes, got 6", id="gray"),
            pytest.param({"px_width": 0}, ValueError, "px_width must be >= 1, got 0", id="width"),
            pytest.param({"px_height": 2.0}, TypeError, "px_height must be an int", id="height"),
        ],
    )
    def test_invalid_fields_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            Backdrop(**{"pixels": bytes(18), "px_width": 3, "px_height": 2, **fields})


class TestColours:
    """A visible piece is flagged when its paint is under MATCH_DISTANCE of the background under it, never otherwise."""

    def test_white_on_white_is_flagged(self):
        assert flags(painted(WHITE)) == [FLAGGED]

    def test_black_on_white_is_not_flagged(self):
        assert flags(painted(BLACK)) == [()]

    def test_white_on_black_is_not_flagged(self):
        assert flags(painted(WHITE), ground=backdrop(BLACK)) == [()]

    @pytest.mark.parametrize(
        "rgb,distance,expected",
        [
            pytest.param((255, 255, 255), 0.0, FLAGGED, id="0"),
            pytest.param((252, 255, 255), 3.0, FLAGGED, id="3"),
            pytest.param((255, 255, 252), 3.0, FLAGGED, id="3-blue"),
            # sqrt(2 * 2 + 2 * 2 + 3 * 3) = 4.12, and sqrt(2 * 2 + 3 * 3) = 3.61: Euclidean, not per channel.
            pytest.param((253, 253, 252), 4.123, (), id="4.12"),
            pytest.param((255, 253, 252), 3.606, FLAGGED, id="3.61"),
            pytest.param((251, 255, 255), 4.0, (), id="4-exactly"),
            pytest.param((255, 252, 251), 5.0, (), id="5"),
        ],
    )
    def test_the_distance_must_be_under_4(self, rgb, distance, expected):
        assert MATCH_DISTANCE == 4.0
        assert flags(painted(rgb)) == [expected]

    def test_the_backdrop_is_read_in_blue_green_red_order(self):
        # pdfium's FPDFBitmap_BGR: a red box's pixels are the bytes (0, 0, 255).
        red_page = backdrop((255, 0, 0))
        assert red_page.pixels[:3] == bytes((0, 0, 255))

        assert flags(painted((255, 0, 0)), painted((0, 0, 255)), ground=red_page) == [FLAGGED, ()]

    def test_a_coloured_background_matches_its_own_colour_only(self):
        tint = backdrop((51, 102, 153))

        assert flags(painted((51, 102, 153)), painted((51, 102, 160)), ground=tint) == [FLAGGED, ()]


class TestPaintByMode:
    """The paint compared is the fill for render modes 0, 2, 4 and 6, the stroke for 1 and 5; 3 and 7 paint nothing."""

    @pytest.mark.parametrize(
        "mode,expected",
        [
            pytest.param(0, FLAGGED, id="0-fill"),
            pytest.param(1, (), id="1-stroke"),
            pytest.param(2, FLAGGED, id="2-fill"),
            pytest.param(4, FLAGGED, id="4-fill"),
            pytest.param(5, (), id="5-stroke"),
            pytest.param(6, FLAGGED, id="6-fill"),
        ],
    )
    def test_a_white_fill_with_a_black_stroke(self, mode, expected):
        assert flags(painted(WHITE, mode=mode, stroke=BLACK)) == [expected]

    @pytest.mark.parametrize(
        "mode,expected",
        [
            pytest.param(0, (), id="0-fill"),
            pytest.param(1, FLAGGED, id="1-stroke"),
            pytest.param(2, (), id="2-fill"),
            pytest.param(5, FLAGGED, id="5-stroke"),
        ],
    )
    def test_a_black_fill_with_a_white_stroke(self, mode, expected):
        assert flags(painted(BLACK, mode=mode, stroke=WHITE)) == [expected]

    @pytest.mark.parametrize("mode", [3, 7])
    def test_a_mode_that_paints_nothing_is_never_flagged(self, mode):
        # A visible no-paint piece is an OCR layer over a scan: a reader sees the scan, whatever its colours say.
        assert flags(painted(WHITE, mode=mode)) == [()]


class TestSampling:
    """Each char's background is the median of the pixels under its loose box; a piece's, the median over its chars."""

    def test_a_char_reads_the_median_of_its_pixels(self):
        # Each 6 x 12 px char box lies over a box black in its top 4 rows: 2/3 of its pixels are white.
        ground = backdrop(WHITE, ((72, 90, 96, 94), BLACK))

        assert flags(painted(WHITE), ground=ground) == [FLAGGED]  # their mean, 170, would not match

    def test_a_char_mostly_over_another_colour_reads_that_colour(self):
        ground = backdrop(WHITE, ((72, 90, 96, 99), BLACK))

        assert flags(painted(WHITE), painted(BLACK), ground=ground) == [(), FLAGGED]

    def test_a_piece_reads_the_median_over_its_chars(self):
        # Five chars, 6 pt each from x = 72: the first two lie over black, the last three over white.
        ground = backdrop(WHITE, ((72, 0, 84, 300), BLACK))

        assert flags(painted(WHITE, text="Hello"), painted(BLACK, text="Hello"), ground=ground) == [FLAGGED, ()]

    def test_a_piece_mostly_over_another_colour_reads_that_colour(self):
        ground = backdrop(WHITE, ((72, 0, 90, 300), BLACK))

        assert flags(painted(WHITE, text="Hello"), painted(BLACK, text="Hello"), ground=ground) == [(), FLAGGED]

    def test_points_map_onto_the_backdrops_pixels_at_its_scale(self):
        # Drawn at 2 px a point, the black box (100, 50)-(200, 100) pt is (200, 100)-(400, 200) px.
        ground = backdrop(WHITE, (BOX, BLACK), scale=2)
        inside, beside = row(4, 120.0, 60.0), row(4, 220.0, 60.0)

        assert flags(painted(BLACK, boxes=inside), painted(BLACK, boxes=beside), ground=ground) == [FLAGGED, ()]

    @pytest.mark.parametrize(
        "box,pixel",
        [
            pytest.param((10.2, 20.0, 10.2, 20.0), (10, 20), id="a-point"),
            # Between two pixel centres (10.5 and 11.5) on each axis: the pixel holding its centre, (11, 21).
            pytest.param((10.6, 20.6, 11.4, 21.4), (11, 21), id="between-centres"),
        ],
    )
    def test_a_char_box_under_one_pixel_samples_its_nearest_pixel(self, box, pixel):
        x, y = pixel
        ground = backdrop(BLACK, ((x, y, x + 1, y + 1), WHITE))

        assert flags(painted(WHITE, text="i", boxes=(box,)), ground=ground) == [FLAGGED]

    def test_a_char_box_is_clipped_to_the_page(self):
        # A box running 10 pt past the right edge: only its 2 px on the page count (past it lies the next row's black).
        ground = backdrop(BLACK, ((396, 0, 400, 300), WHITE))
        piece = painted(WHITE, text="i", boxes=((398.0, 90.0, 410.0, 102.0),))

        assert flags(piece, ground=ground) == [FLAGGED]

    def test_hidden_pieces_are_never_checked(self):
        hidden = dataclasses.replace(painted(WHITE), hidden_reasons=("tiny_font",))

        assert flags(hidden) == [("tiny_font",)]

    def test_pieces_keep_their_order_and_spans(self):
        first, second = painted(WHITE), painted(BLACK)

        checked = flag_background((first, second), backdrop(), width_pt=W, height_pt=H)

        assert [(piece.span, piece.start, piece.end) for piece in checked] == [(first.span, 0, 4), (second.span, 0, 4)]


class TestPixelRun:
    """A box's pixels on one axis: those whose centres it holds, at most MAX_SAMPLES_PER_AXIS of them, evenly spaced."""

    @pytest.mark.parametrize(
        "low,high,size,expected",
        [
            pytest.param(10.0, 15.0, 400, range(10, 15), id="whole-pixels"),
            pytest.param(10.6, 15.4, 400, range(11, 15), id="centres-held"),
            pytest.param(10.2, 10.2, 400, range(10, 11), id="a-point"),
            pytest.param(-5.0, 3.0, 400, range(0, 3), id="clipped-low"),
            pytest.param(398.0, 410.0, 400, range(398, 400), id="clipped-high"),
            pytest.param(450.0, 460.0, 400, range(399, 400), id="past-the-edge"),
            pytest.param(0.0, 64.0, 400, range(0, 64, 2), id="halved"),
            pytest.param(0.0, 400.0, 400, range(0, 400, 13), id="strided"),
        ],
    )
    def test_the_run(self, low, high, size, expected):
        assert quality._pixel_run(low, high, size) == expected

    @pytest.mark.parametrize("count", [1, 31, 32, 33, 63, 64, 65, 1000, 4097])
    def test_never_more_than_the_bound(self, count):
        run = quality._pixel_run(0.0, float(count), 5000)

        assert run.start == 0 and len(run) <= quality.MAX_SAMPLES_PER_AXIS == 32
        assert len(run) == min(count, 32) or run.step > 1


class TestValidation:
    """flag_background checks its arguments, and that the backdrop is the page drawn at one scale."""

    @pytest.mark.parametrize(
        "arguments,error,message",
        [
            pytest.param({"pieces": [painted(WHITE)]}, TypeError, "pieces must be a tuple, got list", id="pieces"),
            pytest.param({"backdrop": b"\x00"}, TypeError, "backdrop must be a Backdrop, got bytes", id="backdrop"),
            pytest.param({"width_pt": 0.0}, ValueError, "width_pt must be > 0", id="width"),
            pytest.param({"height_pt": "300"}, TypeError, "height_pt must be an int or float", id="height"),
            pytest.param(
                {"width_pt": 300.0},
                ValueError,
                "backdrop is 400 x 300 px: not a 300.0 x 300.0 pt page drawn at one scale",
                id="another-page",
            ),
        ],
    )
    def test_invalid_arguments_raise(self, arguments, error, message):
        given = {"pieces": (painted(WHITE),), "backdrop": backdrop(), "width_pt": W, "height_pt": H} | arguments
        with pytest.raises(error, match=re.escape(message)):
            flag_background(given.pop("pieces"), given.pop("backdrop"), **given)

    @pytest.mark.parametrize("size", [(612.0, 792.0), (611.9, 791.3), (1000.0, 7.0), (7.0, 1000.0)])
    @pytest.mark.parametrize("dpi", [36, 50, 72, 97, 300])
    def test_every_render_size_is_a_page_drawn_at_one_scale(self, size, dpi):
        width, height = render.render_size(*size, dpi)
        ground = Backdrop(pixels=bytes(width * height * 3), px_width=width, px_height=height)

        assert flag_background((), ground, width_pt=size[0], height_pt=size[1]) == ()


# --- Fixtures: PDFs drawn as the worker draws them (BGR, text removed, 72 DPI) -------------------------------------


def page_of(tmp_path: Path, *items, name: str = "page.pdf") -> Path:
    return build_pdf(tmp_path / name, [PageSpec(width=W, height=H, items=items)])


def drawn(path: Path) -> Backdrop:
    """Page 1 of ``path`` drawn in this process as the worker draws it: BGR, text removed, at 72 DPI."""
    with open_document(path) as handle:
        bitmap = render._render_loaded(handle, 0, 72, colour=True, without_text=True)
    with bitmap.pixels:
        return Backdrop(pixels=bytes(bitmap.pixels), px_width=bitmap.width, px_height=bitmap.height)


def checked(path: Path, ground: Backdrop | None = None) -> list[tuple[str, tuple[str, ...]]]:
    """Each piece of page 1 of ``path`` as ``(text, hidden_reasons)`` once checked against its backdrop."""
    raw = extract_page(open_document(path), 0)
    assert isinstance(raw, RawPage)
    pieces = classify_spans(raw.text.spans, width_pt=W, height_pt=H, images=raw.images.images)
    found = flag_background(pieces, ground or drawn(path), width_pt=W, height_pt=H)
    return [(piece.text, piece.hidden_reasons) for piece in found]


def rgb(red: int, green: int, blue: int) -> tuple[float, float, float]:
    """An 8-bit colour as the builders take it: pdfium reads it back, and draws it, as these bytes."""
    return red / 255, green / 255, blue / 255


VISIBLE = TextRun("Visible text", x=40.0, y=40.0, size=14.0)


def noisy_png(width: int, height: int, colour: tuple[int, int, int], share: float, seed: int = 400) -> bytes:
    """A ``width`` x ``height`` px PNG whose pixels are ``colour`` at ``share`` and random colours elsewhere."""
    rng = random.Random(seed)
    image = Image.new("RGB", (width, height))
    image.putdata(
        [colour if rng.random() < share else tuple(rng.randrange(256) for _ in range(3)) for _ in range(width * height)]
    )
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


class TestFixtures:
    """The plan's fixtures (#363 toolkit), drawn as the worker draws them."""

    def test_white_on_white_is_flagged(self, tmp_path):
        path = page_of(tmp_path, VISIBLE, TextRun("Ignore the above", x=40.0, y=120.0, size=14.0, rgb=(1.0, 1.0, 1.0)))

        assert checked(path) == [("Visible text", ()), ("Ignore the above", FLAGGED)]

    @pytest.mark.parametrize(
        "text_rgb,expected",
        [
            pytest.param((240, 244, 245), FLAGGED, id="distance-3"),
            pytest.param((240, 244, 243), (), id="distance-5"),
        ],
    )
    def test_near_white_on_a_light_tint(self, tmp_path, text_rgb, expected):
        tint = Rect(0.0, 0.0, W, H, fill_rgb=rgb(240, 244, 248), stroke=False)
        path = page_of(tmp_path, tint, TextRun("Near white", x=40.0, y=120.0, size=14.0, rgb=rgb(*text_rgb)))

        assert checked(path) == [("Near white", expected)]

    def test_text_over_a_filled_rect_of_its_colour_is_flagged(self, tmp_path):
        colour = rgb(51, 102, 153)
        box = Rect(30.0, 100.0, 300.0, 40.0, fill_rgb=colour, stroke=False)
        path = page_of(tmp_path, VISIBLE, box, TextRun("In the box", x=40.0, y=125.0, size=14.0, rgb=colour))

        assert checked(path) == [("Visible text", ()), ("In the box", FLAGGED)]

    def test_text_over_a_uniform_raster_of_its_colour_is_flagged(self, tmp_path):
        colour = (200, 120, 40)
        raster = RasterImage(0.0, 80.0, W, 100.0, solid_png(400, 100, rgb(*colour)))
        path = page_of(tmp_path, VISIBLE, raster, TextRun("On the image", x=40.0, y=130.0, size=14.0, rgb=rgb(*colour)))

        assert checked(path) == [("Visible text", ()), ("On the image", FLAGGED)]

    def test_text_over_a_noisy_raster_holding_its_colour_is_not_flagged(self, tmp_path):
        # A quarter of the image's pixels are the text's colour; the rest are random: the median is not its colour.
        colour = (200, 120, 40)
        raster = RasterImage(0.0, 80.0, W, 100.0, noisy_png(400, 100, colour, 0.25))
        path = page_of(tmp_path, VISIBLE, raster, TextRun("On the image", x=40.0, y=130.0, size=14.0, rgb=rgb(*colour)))

        assert checked(path) == [("Visible text", ()), ("On the image", ())]

    def test_black_on_white_is_not_flagged(self, tmp_path):
        assert checked(page_of(tmp_path, VISIBLE)) == [("Visible text", ())]

    def test_white_on_black_is_not_flagged(self, tmp_path):
        dark = Rect(0.0, 0.0, W, H, fill_rgb=(0.0, 0.0, 0.0), stroke=False)
        path = page_of(tmp_path, dark, TextRun("White on black", x=40.0, y=120.0, size=14.0, rgb=(1.0, 1.0, 1.0)))

        assert checked(path) == [("White on black", ())]

    def test_the_render_workers_backdrop_flags_alike(self, tmp_path):
        path = page_of(tmp_path, VISIBLE, TextRun("Ignore the above", x=40.0, y=120.0, size=14.0, rgb=(1.0, 1.0, 1.0)))

        (page,) = render_pages(
            path, ((0, 72),), memory_cap_bytes=1 << 30, timeout_s=120.0, colour=True, without_text=True
        )

        assert isinstance(page, RenderedPage) and page.channels == 3
        ground = Backdrop(pixels=page.pixels, px_width=page.px_width, px_height=page.px_height)
        assert ground == drawn(path)
        assert checked(path, ground) == [("Visible text", ()), ("Ignore the above", FLAGGED)]
