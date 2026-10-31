"""Tests for the v5 hidden-span flags and raster cover (#366, A3 part 1).

Unit tests build RawSpan and RawImage values directly on a 400 x 300 pt
page: boxes are displayed, top-left ``(x0, y0, x1, y1)`` points. Integration
tests build PDFs with the ReportLab toolkit (``pdf_tests.builders``) and read
them with the engine's ``extract_page``.
"""

import ast
import dataclasses
import itertools
import pickle
import random
import re
import time
from pathlib import Path

import pytest

import datagrunt
import datagrunt.pdf
from datagrunt.pdf import _pdfium_recover, elements, quality
from datagrunt.pdf._pdfium_recover import em_reasons
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.pdfium_engine import RawImage, RawPage, RawSpan, extract_page, open_document
from datagrunt.pdf.quality import SpanPiece, assess_page, classify_spans, raster_cover_fraction
from pdf_tests.builders import PageSpec, RasterImage, TextRun, build_pdf, solid_png
from pdf_tests.test_pdfium_engine import ENGINE_MODULES, imported_modules
from pdf_tests.test_pdfium_spans import SQUEEZED, TURNED, content_page, nested_text_page, two_forms_page

W, H = 400.0, 300.0
SHA = "0" * 64


def union(boxes):
    """The smallest box holding every one of ``boxes``."""
    return (
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    )


def row(count: int, x: float, y: float = 90.0, *, width: float = 6.0, height: float = 12.0):
    """``count`` adjacent char boxes running right from ``(x, y)``."""
    return tuple((x + i * width, y, x + (i + 1) * width, y + height) for i in range(count))


def column(count: int, y: float, x: float = 100.0, *, width: float = 12.0, height: float = 6.0):
    """``count`` adjacent char boxes running down from ``(x, y)`` (text on a quarter-turned page)."""
    return tuple((x, y + i * height, x + width, y + (i + 1) * height) for i in range(count))


def raw_span(text: str = "Abcd", boxes=None, **overrides) -> RawSpan:
    """A valid RawSpan of ``text``, one box per char (a row at x=72 by default)."""
    boxes = row(len(text), 72.0) if boxes is None else boxes
    fields = {
        "text": text,
        "bbox": union(boxes),
        "char_boxes": boxes,
        "font_size_pt": 11.0,
        "em_width_pt": 11.0,
        "base_font_name": "Helvetica",
        "is_bold": False,
        "is_italic": False,
        "render_mode": 0,
        "fill_rgba": (0, 0, 0, 255),
        "stroke_rgba": (0, 0, 0, 255),
        "object_index": 0,
        "generated_breaks": (),
        "map_errors": (),
    }
    return RawSpan(**(fields | overrides))


def image(bbox, **overrides) -> RawImage:
    """A valid RawImage drawn into ``bbox``."""
    fields = {
        "bbox": bbox,
        "px_width": 8,
        "px_height": 8,
        "declared_px": (8, 8),
        "object_index": 1,
        "raw_sha256": SHA,
        "filters": ("FlateDecode",),
        "bits_per_pixel": 24,
        "colorspace": "DeviceRGB",
    }
    return RawImage(**(fields | overrides))


FULL_BLEED = image((0.0, 0.0, W, H))
# A scan in four horizontal strips: the union covers the page, no strip passes 25 %.
STRIPS = tuple(image((0.0, i * H / 4, W, (i + 1) * H / 4), object_index=i + 1) for i in range(4))


def piece(**overrides) -> SpanPiece:
    """A valid SpanPiece of chars 1..3 of a four-char span."""
    fields = {"span": raw_span(), "start": 1, "end": 3, "hidden_reasons": ()}
    return SpanPiece(**(fields | overrides))


def classify(*spans: RawSpan, images=()):
    """classify_spans on the 400 x 300 pt page."""
    return classify_spans(spans, width_pt=W, height_pt=H, images=images)


def layout(pieces):
    """Each piece as ``(start, end, text, hidden_reasons)``."""
    return [(item.start, item.end, item.text, item.hidden_reasons) for item in pieces]


def reasons_of(*spans: RawSpan, images=()):
    """The hidden reasons of each piece classify_spans makes."""
    return [item.hidden_reasons for item in classify(*spans, images=images)]


def meets_page_rule(item: SpanPiece, width: float, height: float) -> bool:
    """A1's ``Page`` placement rule: flagged ``off_page`` exactly when the bbox is not within the page."""
    x0, y0, x1, y1 = item.bbox
    within = BBox(x0=x0, y0=y0, x1=x1, y1=y1).within(width, height)
    return within == ("off_page" not in item.hidden_reasons)


class TestSpanPiece:
    """SpanPiece is a validated, immutable, picklable slice of one span."""

    def test_pickle_round_trip(self):
        original = piece(hidden_reasons=("off_page", "tiny_font"))

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = piece()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.start = 0
        assert not hasattr(value, "__dict__")

    def test_stores_only_the_span_offsets_and_reasons(self):
        # text and bbox are derived, so there is no stored copy to drift.
        assert [item.name for item in dataclasses.fields(SpanPiece)] == ["span", "start", "end", "hidden_reasons"]

    def test_text_and_bbox_are_the_slice_of_the_span(self):
        boxes = ((10.0, 20.0, 16.0, 30.0), (16.0, 18.0, 22.0, 31.0), (22.0, 21.0, 28.0, 33.0), (28.0, 19.0, 34.0, 30.0))
        value = piece(span=raw_span("Abcd", boxes))

        assert value.text == "bc"
        assert value.bbox == (16.0, 18.0, 28.0, 33.0)

    def test_whole_span_piece_has_the_span_bbox(self):
        source = raw_span("Abcd")

        assert piece(span=source, start=0, end=4).bbox == source.bbox

    def test_replace_revalidates(self):
        with pytest.raises(ValueError, match="start < end"):
            dataclasses.replace(piece(), end=9)

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"span": "Abcd"}, TypeError, "span must be a RawSpan, got str", id="span-str"),
            pytest.param({"start": True}, TypeError, "start must be an int, got True (bool)", id="start-bool"),
            pytest.param({"start": -1}, ValueError, "start must be >= 0, got -1", id="start-negative"),
            pytest.param({"end": 3.0}, TypeError, "end must be an int, got 3.0 (float)", id="end-float"),
            pytest.param(
                {"start": 2, "end": 2},
                ValueError,
                "SpanPiece must have start < end <= 4 (the span's char count), got start=2, end=2",
                id="empty",
            ),
            pytest.param(
                {"start": 3, "end": 2},
                ValueError,
                "SpanPiece must have start < end <= 4 (the span's char count), got start=3, end=2",
                id="reversed",
            ),
            pytest.param(
                {"end": 5},
                ValueError,
                "SpanPiece must have start < end <= 4 (the span's char count), got start=1, end=5",
                id="past-the-end",
            ),
            pytest.param(
                {"hidden_reasons": ["off_page"]},
                TypeError,
                "hidden_reasons must be a tuple of str, got list",
                id="reasons-list",
            ),
            pytest.param(
                {"hidden_reasons": ("hidden",)}, ValueError, "hidden_reasons has unknown reason(s)", id="unknown"
            ),
            pytest.param(
                # zero_scale (recovered Tz 0 text) is last in the canonical order.
                {"hidden_reasons": ("zero_scale", "tiny_font")},
                ValueError,
                "hidden_reasons must be in canonical order ('tiny_font', 'zero_scale')",
                id="zero-scale-order",
            ),
            pytest.param(
                {"hidden_reasons": ("invisible", "invisible")},
                ValueError,
                "hidden_reasons has duplicate entries",
                id="duplicate",
            ),
            pytest.param(
                {"hidden_reasons": ("tiny_font", "off_page")},
                ValueError,
                "hidden_reasons must be in canonical order ('off_page', 'tiny_font')",
                id="order",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            piece(**overrides)


class TestVisibleSpans:
    """A span with no reason and no off-page char is one piece."""

    def test_visible_span_is_one_whole_piece(self):
        source = raw_span("Abcd")

        (got,) = classify(source)

        assert got.span is source
        assert layout([got]) == [(0, 4, "Abcd", ())]

    def test_no_spans_make_no_pieces(self):
        assert classify() == ()

    def test_pieces_follow_span_order(self):
        first, second = raw_span("One", row(3, 20.0)), raw_span("Two", row(3, 200.0), object_index=1)

        assert [item.span for item in classify(first, second)] == [first, second]

    def test_generated_breaks_do_not_split(self):
        source = raw_span("NameValue", generated_breaks=(4,))

        assert layout(classify(source)) == [(0, 9, "NameValue", ())]


class TestOffPage:
    """A char whose box is not within the page (A1's rule and tolerance) is off_page."""

    @pytest.mark.parametrize(
        "boxes,expected",
        [
            pytest.param(row(4, -12.0), [(0, 2, "Ab", ("off_page",)), (2, 4, "cd", ())], id="left"),
            pytest.param(row(4, 388.0), [(0, 2, "Ab", ()), (2, 4, "cd", ("off_page",))], id="right"),
            pytest.param(column(4, -12.0), [(0, 2, "Ab", ("off_page",)), (2, 4, "cd", ())], id="top"),
            pytest.param(column(4, 288.0), [(0, 2, "Ab", ()), (2, 4, "cd", ("off_page",))], id="bottom"),
        ],
    )
    def test_span_splits_at_each_page_edge(self, boxes, expected):
        assert layout(classify(raw_span("Abcd", boxes))) == expected

    def test_char_partly_past_the_edge_is_off_page(self):
        # Most of the char is on the page, but its box ends 1 pt past the right edge.
        assert reasons_of(raw_span("A", ((395.0, 90.0, 401.0, 102.0),))) == [("off_page",)]

    @pytest.mark.parametrize(
        "box",
        [
            pytest.param((394.5, 90.0, 400.5, 102.0), id="right"),
            pytest.param((-0.5, 90.0, 5.5, 102.0), id="left"),
            pytest.param((100.0, -0.5, 106.0, 11.5), id="top"),
            pytest.param((100.0, 288.5, 106.0, 300.5), id="bottom"),
        ],
    )
    def test_char_within_the_tolerance_is_on_the_page(self, box):
        assert reasons_of(raw_span("A", (box,))) == [()]

    def test_char_wholly_outside_is_off_page(self):
        assert reasons_of(raw_span("A", ((-500.0, -20.0, -494.0, -8.0),))) == [("off_page",)]

    def test_pieces_are_maximal_runs(self):
        boxes = (*row(2, 20.0), *row(2, 500.0), *row(2, 40.0))

        assert layout(classify(raw_span("AbCdEf", boxes))) == [
            (0, 2, "Ab", ()),
            (2, 4, "Cd", ("off_page",)),
            (4, 6, "Ef", ()),
        ]

    def test_every_piece_meets_the_page_rule_and_the_pieces_tile_each_span(self):
        rng = random.Random(366)
        for _ in range(200):
            count = rng.randint(1, 12)
            boxes = row(count, rng.uniform(-40.0, W + 10.0), rng.uniform(-20.0, H + 5.0), width=rng.uniform(0.0, 9.0))
            source = raw_span("x" * count, boxes)

            got = classify(source)

            assert all(meets_page_rule(item, W, H) for item in got)
            ends = [item.end for item in got]
            assert [item.start for item in got] == [0, *ends[:-1]]
            assert ends[-1] == count
            assert "".join(item.text for item in got) == source.text
            assert all(earlier.hidden_reasons != later.hidden_reasons for earlier, later in itertools.pairwise(got))


class TestInvisible:
    """Render modes 3 and 7 paint nothing: invisible, except an OCR layer over raster cover."""

    @pytest.mark.parametrize("mode", [3, 7])
    def test_no_paint_modes_are_invisible(self, mode):
        assert reasons_of(raw_span(render_mode=mode)) == [("invisible",)]

    @pytest.mark.parametrize("mode", [0, 1, 2, 4, 5, 6])
    def test_painting_modes_are_visible(self, mode):
        assert reasons_of(raw_span(render_mode=mode)) == [()]

    @pytest.mark.parametrize("mode", [3, 7])
    def test_ocr_layer_over_a_full_bleed_scan_is_visible(self, mode):
        assert reasons_of(raw_span(render_mode=mode), images=(FULL_BLEED,)) == [()]

    def test_ocr_layer_over_a_scan_in_strips_is_visible(self):
        # No single strip covers over half the page; their union covers all of it.
        assert all(raster_cover_fraction((strip,), W, H) == 0.25 for strip in STRIPS)

        assert reasons_of(raw_span(render_mode=3), images=STRIPS) == [()]

    def test_text_over_a_small_image_stays_invisible(self):
        small = image((60.0, 80.0, 140.0, 120.0))

        assert reasons_of(raw_span(render_mode=3), images=(small,)) == [("invisible",)]

    def test_cover_of_exactly_half_the_page_is_not_raster_cover(self):
        half = image((0.0, 0.0, W / 2, H))

        assert reasons_of(raw_span(render_mode=3, boxes=row(4, 20.0)), images=(half,)) == [("invisible",)]

    def test_char_beside_the_raster_cover_stays_invisible(self):
        # The image covers the left 60 % of the page; the text is on the right.
        left = image((0.0, 0.0, 240.0, H))

        assert reasons_of(raw_span(render_mode=3, boxes=row(4, 300.0)), images=(left,)) == [("invisible",)]

    def test_exemption_is_per_char_by_its_centre(self):
        left = image((0.0, 0.0, 240.0, H))
        # Char centres at 231, 237 (over the image), 243 and 249 (beside it).
        source = raw_span("Abcd", row(4, 228.0), render_mode=3)

        assert layout(classify(source, images=(left,))) == [(0, 2, "Ab", ()), (2, 4, "cd", ("invisible",))]

    def test_exemption_lifts_only_invisible(self):
        transparent = raw_span(fill_rgba=(0, 0, 0, 0))
        tiny_ocr = raw_span(render_mode=3, font_size_pt=0.5)

        assert reasons_of(transparent, tiny_ocr, images=(FULL_BLEED,)) == [("transparent",), ("tiny_font",)]


class TestTinyFont:
    """A font size under 1 pt in magnitude is tiny_font."""

    @pytest.mark.parametrize(
        "size,expected",
        [
            pytest.param(0.5, ("tiny_font",), id="half-point"),
            pytest.param(0.999, ("tiny_font",), id="just-under"),
            pytest.param(0.0, ("tiny_font",), id="zero"),
            pytest.param(1.0, (), id="one-point"),
            pytest.param(11.0, (), id="body"),
            # A negative Tf draws mirrored glyphs at its magnitude.
            pytest.param(-0.5, ("tiny_font",), id="mirrored-tiny"),
            pytest.param(-12.0, (), id="mirrored-body"),
        ],
    )
    def test_threshold(self, size, expected):
        assert reasons_of(raw_span(font_size_pt=size)) == [expected]


class TestZeroScale:
    """An em under 1 pt wide and narrower than tall is zero_scale: text squeezed flat (Tz), recovery's rule."""

    @pytest.mark.parametrize(
        "width,expected",
        [
            pytest.param(0.0, ("zero_scale",), id="flat"),
            pytest.param(0.006, ("zero_scale",), id="tz-0.05"),
            pytest.param(0.5, ("zero_scale",), id="half-point"),
            pytest.param(0.999, ("zero_scale",), id="just-under"),
            pytest.param(1.0, (), id="one-point"),
            pytest.param(11.0, (), id="body"),
            pytest.param(30.0, (), id="wide"),
        ],
    )
    def test_threshold(self, width, expected):
        assert reasons_of(raw_span(font_size_pt=11.0, em_width_pt=width)) == [expected]

    @pytest.mark.parametrize(
        "size,width,expected",
        [
            # A negative Tf draws mirrored glyphs at its magnitude: 12 pt tall.
            pytest.param(-12.0, 0.12, ("zero_scale",), id="mirrored-squeezed"),
            pytest.param(-12.0, 12.0, (), id="mirrored-body"),
            # Tiny but not squeezed (no narrower than tall): the size hides it, not the scale.
            pytest.param(0.5, 0.5, ("tiny_font",), id="tiny-square"),
            pytest.param(0.0, 0.0, ("tiny_font",), id="zero-matrix"),
            pytest.param(0.5, 0.1, ("tiny_font", "zero_scale"), id="tiny-and-squeezed"),
        ],
    )
    def test_the_em_must_be_narrower_than_tall(self, size, width, expected):
        assert reasons_of(raw_span(font_size_pt=size, em_width_pt=width)) == [expected]

    def test_the_rule_is_recoverys(self):
        cases = [(width, height) for width in (0.0, 0.006, 0.5, 0.999, 1.0, 12.0) for height in (0.0, 0.5, 1.0, 12.0)]

        for width, height in cases:
            (got,) = reasons_of(raw_span(font_size_pt=height, em_width_pt=width))
            assert got == em_reasons(width, height), (width, height)

    def test_off_page_invisible_tiny_and_zero_scale(self):
        source = raw_span("A", ((-40.0, 90.0, -39.5, 90.5),), render_mode=3, font_size_pt=0.5, em_width_pt=0.1)

        assert reasons_of(source) == [("off_page", "invisible", "tiny_font", "zero_scale")]

    def test_off_page_tiny_transparent_and_zero_scale(self):
        source = raw_span("A", ((-40.0, 90.0, -39.5, 90.5),), font_size_pt=0.5, em_width_pt=0.1, fill_rgba=(0, 0, 0, 0))

        assert reasons_of(source) == [("off_page", "tiny_font", "transparent", "zero_scale")]

    def test_transparent_and_zero_scale(self):
        assert reasons_of(raw_span(em_width_pt=0.12, fill_rgba=(0, 0, 0, 0))) == [("transparent", "zero_scale")]

    def test_zero_scale_stays_on_both_sides_of_an_edge_split(self):
        source = raw_span("Abcd", row(4, 388.0), em_width_pt=0.12)

        assert layout(classify(source)) == [(0, 2, "Ab", ("zero_scale",)), (2, 4, "cd", ("off_page", "zero_scale"))]

    def test_an_ocr_layer_squeezed_flat_is_still_zero_scale(self):
        # Raster cover lifts invisible only: the squeeze still hides the text.
        source = raw_span(render_mode=3, em_width_pt=0.12)

        assert reasons_of(source, images=(FULL_BLEED,)) == [("zero_scale",)]


class TestTransparent:
    """Transparent: alpha 0 on every channel the render mode paints."""

    @pytest.mark.parametrize(
        "mode,fill_alpha,stroke_alpha,expected",
        [
            pytest.param(0, 0, 255, ("transparent",), id="fill-clear"),
            pytest.param(0, 255, 0, (), id="fill-opaque-stroke-clear"),
            pytest.param(0, 1, 255, (), id="fill-alpha-1"),
            pytest.param(4, 0, 255, ("transparent",), id="fill-clip-clear"),
            pytest.param(1, 255, 0, ("transparent",), id="stroke-clear"),
            pytest.param(1, 0, 255, (), id="stroke-opaque-fill-clear"),
            pytest.param(5, 255, 0, ("transparent",), id="stroke-clip-clear"),
            pytest.param(2, 0, 0, ("transparent",), id="fill-stroke-clear"),
            pytest.param(2, 0, 255, (), id="clear-fill-opaque-stroke"),
            pytest.param(2, 255, 0, (), id="opaque-fill-clear-stroke"),
            pytest.param(6, 0, 0, ("transparent",), id="fill-stroke-clip-clear"),
            pytest.param(6, 0, 255, (), id="fill-stroke-clip-opaque-stroke"),
            # Modes 3 and 7 paint nothing: invisible, never transparent.
            pytest.param(3, 0, 0, ("invisible",), id="no-paint"),
            pytest.param(7, 0, 0, ("invisible",), id="clip-only"),
        ],
    )
    def test_painted_channels(self, mode, fill_alpha, stroke_alpha, expected):
        source = raw_span(render_mode=mode, fill_rgba=(255, 255, 255, fill_alpha), stroke_rgba=(0, 0, 0, stroke_alpha))

        assert reasons_of(source) == [expected]


class TestCombinedReasons:
    """Reasons combine in A1's canonical order."""

    def test_off_page_invisible_and_tiny(self):
        source = raw_span("A", ((-40.0, 90.0, -39.5, 90.5),), render_mode=3, font_size_pt=0.5)

        assert reasons_of(source) == [("off_page", "invisible", "tiny_font")]

    def test_off_page_tiny_and_transparent(self):
        source = raw_span("A", ((-40.0, 90.0, -39.5, 90.5),), font_size_pt=0.5, fill_rgba=(0, 0, 0, 0))

        assert reasons_of(source) == [("off_page", "tiny_font", "transparent")]

    def test_span_reasons_stay_on_both_sides_of_an_edge_split(self):
        source = raw_span("Abcd", row(4, 388.0), font_size_pt=0.5, fill_rgba=(0, 0, 0, 0))

        assert layout(classify(source)) == [
            (0, 2, "Ab", ("tiny_font", "transparent")),
            (2, 4, "cd", ("off_page", "tiny_font", "transparent")),
        ]

    def test_raster_edge_splits_invisible_and_keeps_the_span_reasons(self):
        right = image((W / 4, 0.0, W, H))  # 75 % cover, starting at x = 100
        source = raw_span("Abcdef", row(6, 88.0), render_mode=3, font_size_pt=0.5)

        assert layout(classify(source, images=(right,))) == [
            (0, 2, "Ab", ("invisible", "tiny_font")),
            (2, 6, "cdef", ("tiny_font",)),
        ]


class TestClassifyValidation:
    """classify_spans rejects bad input at its boundary."""

    @pytest.mark.parametrize(
        "kwargs,error,message",
        [
            pytest.param({"spans": [raw_span()]}, TypeError, "spans must be a tuple, got list", id="spans-list"),
            pytest.param({"spans": ("Abcd",)}, TypeError, "spans[0] must be a RawSpan, got str", id="spans-item"),
            pytest.param({"images": [FULL_BLEED]}, TypeError, "images must be a tuple, got list", id="images-list"),
            pytest.param(
                {"images": (FULL_BLEED, (0.0, 0.0, 1.0, 1.0))},
                TypeError,
                "images[1] must be a RawImage, got tuple",
                id="images-item",
            ),
            pytest.param({"width_pt": 0.0}, ValueError, "width_pt must be > 0, got 0.0", id="width-zero"),
            pytest.param({"height_pt": -1.0}, ValueError, "height_pt must be > 0, got -1.0", id="height-negative"),
            pytest.param({"width_pt": float("nan")}, ValueError, "width_pt must be finite", id="width-nan"),
            pytest.param({"height_pt": True}, TypeError, "height_pt must be an int or float", id="height-bool"),
        ],
    )
    def test_invalid_argument_raises(self, kwargs, error, message):
        arguments = {"spans": (raw_span(),), "width_pt": W, "height_pt": H, "images": ()} | kwargs

        with pytest.raises(error, match=re.escape(message)):
            classify_spans(**arguments)


def brute_force_cover(boxes, width: float, height: float) -> float:
    """Union cover by enumerating every cell of the boxes' coordinate grid (an independent oracle)."""
    clipped = [(max(x0, 0.0), max(y0, 0.0), min(x1, width), min(y1, height)) for x0, y0, x1, y1 in boxes]
    clipped = [box for box in clipped if box[0] < box[2] and box[1] < box[3]]
    xs = sorted({value for box in clipped for value in (box[0], box[2])})
    ys = sorted({value for box in clipped for value in (box[1], box[3])})
    area = 0.0
    for (left, right), (top, bottom) in itertools.product(itertools.pairwise(xs), itertools.pairwise(ys)):
        x, y = (left + right) / 2, (top + bottom) / 2
        if any(box[0] <= x <= box[2] and box[1] <= y <= box[3] for box in clipped):
            area += (right - left) * (bottom - top)
    return area / (width * height)


def random_box(rng: random.Random):
    """A box around the page: float or whole-point edges (shared edges), sometimes degenerate."""
    draw = rng.uniform if rng.random() < 0.5 else rng.randint
    x0, x1 = sorted((draw(-50, int(W) + 50), draw(-50, int(W) + 50)))
    y0, y1 = sorted((draw(-50, int(H) + 50), draw(-50, int(H) + 50)))
    if rng.random() < 0.1:
        x1 = x0
    return float(x0), float(y0), float(x1), float(y1)


class TestRasterCoverFraction:
    """The exact union of image boxes, clipped to the page, as a fraction of it."""

    @pytest.mark.parametrize(
        "boxes,expected",
        [
            pytest.param((), 0.0, id="no-images"),
            pytest.param(((0.0, 0.0, W, H),), 1.0, id="full-page"),
            pytest.param(((-20.0, -20.0, W + 20.0, H + 20.0),), 1.0, id="bleed"),
            pytest.param(((0.0, 0.0, W / 2, H),), 0.5, id="half"),
            pytest.param(((0.0, 0.0, W / 2, H), (0.0, 0.0, W / 2, H)), 0.5, id="identical-overlap"),
            pytest.param(((0.0, 0.0, 200.0, 150.0), (100.0, 75.0, 300.0, 225.0)), 0.4375, id="partial-overlap"),
            pytest.param(((0.0, 0.0, 40.0, 30.0), (200.0, 150.0, 240.0, 180.0)), 0.02, id="disjoint"),
            pytest.param(((0.0, 0.0, 40.0, 30.0), (40.0, 0.0, 80.0, 30.0)), 0.02, id="touching"),
            pytest.param(((-100.0, -100.0, -10.0, -10.0),), 0.0, id="wholly-off-page"),
            pytest.param(((-200.0, -150.0, 200.0, 150.0),), 0.25, id="partly-off-page"),
            pytest.param(((100.0, 100.0, 100.0, 200.0),), 0.0, id="zero-width"),
            pytest.param(tuple(strip.bbox for strip in STRIPS), 1.0, id="strips"),
        ],
    )
    def test_known_cover(self, boxes, expected):
        images = tuple(image(box, object_index=index) for index, box in enumerate(boxes))

        got = raster_cover_fraction(images, W, H)

        assert type(got) is float
        assert got == pytest.approx(expected, rel=1e-12, abs=1e-12)

    def test_every_image_counts_unsized_and_unread_included(self):
        # Oversized, unsized and over-budget images are recorded without pixel facts: still raster.
        fields = {"px_width": 0, "px_height": 0, "declared_px": (0, 0), "raw_sha256": None}
        unread = image((0.0, 0.0, W, H), **fields, bits_per_pixel=None, colorspace=None)

        assert raster_cover_fraction((unread,), W, H) == 1.0

    @pytest.mark.parametrize("seed", range(60))
    def test_agrees_with_brute_force_on_random_sets(self, seed):
        rng = random.Random(seed)
        boxes = [random_box(rng) for _ in range(rng.randint(1, 20))]
        images = tuple(image(box, object_index=index) for index, box in enumerate(boxes))

        assert raster_cover_fraction(images, W, H) == pytest.approx(brute_force_cover(boxes, W, H), abs=1e-9)

    def test_twenty_thousand_images_in_under_two_seconds(self):
        rng = random.Random(20_000)
        images = tuple(image(random_box(rng), object_index=index) for index in range(20_000))

        started = time.perf_counter()
        got = raster_cover_fraction(images, W, H)
        elapsed = time.perf_counter() - started

        assert 0.0 < got <= 1.0
        assert elapsed < 2.0, f"raster_cover_fraction took {elapsed:.2f}s for {len(images)} images"

    def test_twenty_thousand_tiles_cover_the_page(self):
        tiles = itertools.product(range(200), range(100))
        images = tuple(image((x * 2.0, y * 3.0, x * 2.0 + 2.0, y * 3.0 + 3.0), object_index=0) for x, y in tiles)

        assert raster_cover_fraction(images, W, H) == pytest.approx(1.0, rel=1e-12)

    @pytest.mark.parametrize(
        "args,error,message",
        [
            pytest.param(([FULL_BLEED], W, H), TypeError, "images must be a tuple, got list", id="list"),
            pytest.param(((FULL_BLEED, "img"), W, H), TypeError, "images[1] must be a RawImage, got str", id="item"),
            pytest.param(((), 0, H), ValueError, "width_pt must be > 0, got 0.0", id="width-zero"),
            pytest.param(((), W, -3.0), ValueError, "height_pt must be > 0, got -3.0", id="height-negative"),
            pytest.param(((), float("inf"), H), ValueError, "width_pt must be finite", id="width-inf"),
            pytest.param(((), W, False), TypeError, "height_pt must be an int or float", id="height-bool"),
        ],
    )
    def test_invalid_argument_raises(self, args, error, message):
        with pytest.raises(error, match=re.escape(message)):
            raster_cover_fraction(*args)


PNG = solid_png(8, 8, (1.0, 1.0, 1.0))


def classified(tmp_path: Path, *items, rotate: int = 0) -> tuple[tuple[SpanPiece, ...], RawPage]:
    """Build a 400 x 300 pt page of ``items``, read it with extract_page and classify its spans."""
    spec = PageSpec(width=W, height=H, items=items, rotate=rotate)
    raw = extract_page(open_document(build_pdf(tmp_path / "page.pdf", [spec])), 0)
    assert isinstance(raw, RawPage)
    geometry = raw.geometry
    pieces = classify_spans(
        raw.text.spans, width_pt=geometry.width_pt, height_pt=geometry.height_pt, images=raw.images.images
    )
    return pieces, raw


def texts(pieces):
    """Each piece as ``(text, hidden_reasons)``."""
    return [(item.text, item.hidden_reasons) for item in pieces]


class TestThroughExtractPage:
    """classify_spans over the engine's raw facts for builder PDFs."""

    def test_ocr_layer_over_a_full_page_scan_is_visible(self, tmp_path):
        pieces, _ = classified(
            tmp_path, RasterImage(0.0, 0.0, W, H, PNG), TextRun("Scan", x=72.0, y=100.0, render_mode=3)
        )

        assert texts(pieces) == [("Scan", ())]

    def test_ocr_layer_over_a_scan_in_strips_is_visible(self, tmp_path):
        strips = tuple(RasterImage(0.0, i * H / 4, W, H / 4, PNG) for i in range(4))

        pieces, raw = classified(tmp_path, *strips, TextRun("Scan", x=72.0, y=100.0, render_mode=3))

        assert raw.images.stats.max_cover_fraction == pytest.approx(0.25)  # no single image passes 50 %
        assert texts(pieces) == [("Scan", ())]

    def test_render_mode_3_without_a_scan_is_invisible(self, tmp_path):
        pieces, _ = classified(tmp_path, TextRun("Scan", x=72.0, y=100.0, render_mode=3))

        assert texts(pieces) == [("Scan", ("invisible",))]

    def test_render_mode_3_over_a_small_image_is_invisible(self, tmp_path):
        pieces, _ = classified(
            tmp_path, RasterImage(60.0, 80.0, 80.0, 40.0, PNG), TextRun("Scan", x=72.0, y=100.0, render_mode=3)
        )

        assert texts(pieces) == [("Scan", ("invisible",))]

    def test_alpha_zero_text_is_transparent(self, tmp_path):
        pieces, _ = classified(tmp_path, TextRun("Ghost", x=72.0, y=100.0, alpha=0.0))

        assert texts(pieces) == [("Ghost", ("transparent",))]

    def test_half_point_text_is_tiny(self, tmp_path):
        pieces, _ = classified(tmp_path, TextRun("Tiny", x=72.0, y=100.0, size=0.5))

        assert texts(pieces) == [("Tiny", ("tiny_font",))]

    @pytest.mark.parametrize(
        "run,rotate,expected",
        [
            pytest.param(TextRun("ABCDEF", x=-21.0, y=100.0), 0, [("ABC", ("off_page",)), ("DEF", ())], id="left"),
            pytest.param(TextRun("ABCDEF", x=372.0, y=100.0), 0, [("ABC", ()), ("DEF", ("off_page",))], id="right"),
            pytest.param(TextRun("ABCDEF", x=72.0, y=3.0), 0, [("ABCDEF", ("off_page",))], id="top"),
            pytest.param(TextRun("ABCDEF", x=72.0, y=301.0), 0, [("ABCDEF", ("off_page",))], id="bottom"),
            # Rotate 90: the unrotated x axis runs down the displayed page.
            pytest.param(
                TextRun("ABCDEF", x=-21.0, y=100.0), 90, [("ABC", ("off_page",)), ("DEF", ())], id="rot90-top"
            ),
            pytest.param(
                TextRun("ABCDEF", x=372.0, y=100.0), 90, [("ABC", ()), ("DEF", ("off_page",))], id="rot90-bottom"
            ),
        ],
    )
    def test_text_past_each_page_edge(self, tmp_path, run, rotate, expected):
        pieces, raw = classified(tmp_path, run, rotate=rotate)
        geometry = raw.geometry

        assert texts(pieces) == expected
        assert all(meets_page_rule(item, geometry.width_pt, geometry.height_pt) for item in pieces)


# A visible control line above the text under test, on hand-written pages (ReportLab cannot write Tz).
CONTROL = b"BT /F1 12 Tf 20 270 Td (Visible) Tj ET "


def classified_page(path: Path) -> tuple[SpanPiece, ...]:
    """extract_page of page 1 of ``path``, its spans classified."""
    raw = extract_page(open_document(path), 0)
    assert isinstance(raw, RawPage)
    geometry = raw.geometry
    return classify_spans(
        raw.text.spans, width_pt=geometry.width_pt, height_pt=geometry.height_pt, images=raw.images.images
    )


class TestZeroScaleThroughExtractPage:
    """Text pdfium's textpage keeps though it is squeezed under 1 pt per em is flagged zero_scale, never dropped."""

    @pytest.mark.parametrize(
        "content,expected",
        [
            pytest.param(SQUEEZED % b"0.05", ("zero_scale",), id="tz-0.05"),
            pytest.param(SQUEEZED % b"0.5", ("zero_scale",), id="tz-0.5"),
            pytest.param(SQUEEZED % b"1", ("zero_scale",), id="tz-1"),
            pytest.param(SQUEEZED % b"100", (), id="tz-100"),
            # 25 % of 4 pt is an em exactly 1 pt wide: not under the threshold.
            pytest.param(b"BT /F1 4 Tf 25 Tz 72 100 Td (HIDDEN) Tj ET", (), id="exactly-1-pt"),
            pytest.param(SQUEEZED % b"-1", ("zero_scale",), id="mirrored-tz-1"),
            pytest.param(SQUEEZED % b"-100", (), id="mirrored-tz-100"),
            pytest.param(TURNED % (b"1", b"0 1 -1 0"), ("zero_scale",), id="turned-tz-1"),
            pytest.param(TURNED % (b"100", b"0 1 -1 0"), (), id="turned-tz-100"),
            # Tiny and squeezed: 0.5 pt tall, 0.005 pt wide.
            pytest.param(b"q BT /F1 0.5 Tf 1 Tz 72 100 Td (HIDDEN) Tj ET Q", ("tiny_font", "zero_scale"), id="tiny"),
        ],
    )
    def test_on_a_plain_page(self, tmp_path, content, expected):
        pieces = classified_page(content_page(tmp_path, CONTROL + content))

        assert texts(pieces) == [("Visible", ()), ("HIDDEN", expected)]

    @pytest.mark.parametrize(
        "make_page,expected",
        [
            pytest.param(
                lambda tmp_path: nested_text_page(tmp_path, b"q BT /F1 12 Tf 1 Tz 10 10 Td (HIDDEN) Tj ET Q"),
                ("zero_scale",),
                id="nested-tz-1",
            ),
            # 5 % of 12 pt is 0.6 pt, but the forms scale it 3 times: 1.8 pt.
            pytest.param(
                lambda tmp_path: nested_text_page(tmp_path, b"q BT /F1 12 Tf 5 Tz 10 10 Td (HIDDEN) Tj ET Q"),
                (),
                id="nested-tz-5",
            ),
            pytest.param(
                lambda tmp_path: nested_text_page(tmp_path, b"BT /F1 12 Tf 10 10 Td (HIDDEN) Tj ET"), (), id="nested"
            ),
            pytest.param(
                lambda tmp_path: two_forms_page(
                    tmp_path,
                    b"BT /F1 12 Tf 10 10 Td (HIDDEN) Tj ET",
                    outer=b"[0.05 0 0 1 20 30]",
                    inner=b"[1 0 0 1 0 0]",
                ),
                ("zero_scale",),
                id="outer-form-squeezes",
            ),
            pytest.param(
                lambda tmp_path: two_forms_page(
                    tmp_path,
                    b"q BT /F1 12 Tf 1 Tz 10 10 Td (HIDDEN) Tj ET Q",
                    outer=b"[10 0 0 1 0 30]",
                    inner=b"[1 0 0 1 0 0]",
                ),
                (),
                id="outer-form-widens",
            ),
        ],
    )
    def test_inside_forms(self, tmp_path, make_page, expected):
        assert texts(classified_page(make_page(tmp_path))) == [("HIDDEN", expected)]

    @pytest.mark.parametrize(
        "matrix,expected",
        [
            # Both em sides about a unit long, sheared onto almost one line: 0.0012 pt tall, and a hair
            # narrower than tall (its slanted side is 1.000000005 units long), so squeezed too.
            pytest.param(b"1 0 1 0.0001", ("tiny_font", "zero_scale"), id="sheared-flat"),
            # Both sides on one line: the glyphs collapse onto it.
            pytest.param(b"1 0 1 0", ("tiny_font",), id="rank-1"),
            # Controls: an oblique (italic) em and a turned one stay unflagged.
            pytest.param(b"1 0 0.2 1", (), id="oblique"),
            pytest.param(b"0 1 -1 0", (), id="quarter-turn"),
        ],
    )
    def test_a_collapsed_em_is_flagged(self, tmp_path, matrix, expected):
        content = CONTROL + b"BT /F1 12 Tf %s 200 100 Tm (HIDDEN) Tj ET" % matrix

        assert texts(classified_page(content_page(tmp_path, content))) == [("Visible", ()), ("HIDDEN", expected)]

    @pytest.mark.parametrize("scale,route", [(b"1", "vision"), (b"100", "digital")], ids=["tz-1", "tz-100"])
    def test_squeezed_text_is_kept_but_never_routes_the_page(self, tmp_path, scale, route):
        content = b"q BT /F1 12 Tf %s Tz 20 150 Td (Thirty characters of body text.) Tj ET Q" % scale

        result = assess_page(extract_page(open_document(content_page(tmp_path, content)), 0))

        assert [item.text for item in result.pieces] == ["Thirty characters of body text."]
        assert result.route == route


class TestModuleImports:
    """quality is pure and internal: no pdfium, no v4 stack, nothing exported."""

    def test_imports_no_pdfium_and_no_v4_stack(self):
        banned = ("pypdfium2", "fitz", "pymupdf", "pdfplumber", "datagrunt.core.pdf_io", "datagrunt.pdf_api")

        assert not [name for name in imported_modules(quality) if name.lower().startswith(banned)]

    def test_imports_only_the_raw_fact_types_and_a1(self):
        allowed = {
            "datagrunt.pdf._pdfium_common",
            "datagrunt.pdf._pdfium_images",
            "datagrunt.pdf._pdfium_recover",
            "datagrunt.pdf._pdfium_text",
            "datagrunt.pdf._validate",
            "datagrunt.pdf.elements",
            "datagrunt.pdf.pdfium_engine",  # for RawPage only
        }

        assert {name for name in imported_modules(quality) if name.startswith("datagrunt")} == allowed

    @pytest.mark.parametrize("module", ENGINE_MODULES, ids=lambda module: module.__name__)
    def test_the_engine_never_imports_quality(self, module):
        assert "datagrunt.pdf.quality" not in imported_modules(module)

    def test_imports_no_private_name(self):
        tree = ast.parse(Path(quality.__file__).read_text(encoding="utf-8"))
        imports = [node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]

        assert [alias.name for node in imports for alias in node.names if alias.name.startswith("_")] == []

    def test_validates_reasons_with_a1s_public_rule(self):
        assert quality.check_hidden_reasons is elements.check_hidden_reasons
        assert not hasattr(elements, "_check_hidden_reasons")

    def test_flags_tiny_and_squeezed_text_with_recoverys_one_rule(self):
        # One em rule and one threshold for textpage spans and recovered text alike.
        assert quality.em_reasons is _pdfium_recover.em_reasons
        assert not hasattr(quality, "TINY_FONT_PT")

    def test_validates_routes_with_a1s_public_table(self):
        assert quality.PAGE_ROUTES is elements.PAGE_ROUTES
        assert elements.PAGE_ROUTES == ("digital", "hybrid", "vision")
        assert not hasattr(elements, "_PAGE_ROUTES")

    def test_module_level_tables_are_immutable(self):
        mutable = (dict, list, set, bytearray)
        names = {name: value for name, value in vars(quality).items() if not name.startswith("__")}

        assert {name for name, value in names.items() if isinstance(value, mutable)} == set()

    @pytest.mark.parametrize(
        "name", ["SpanPiece", "classify_spans", "raster_cover_fraction", "OcrMode", "PageAssessment", "assess_page"]
    )
    def test_not_exported(self, name):
        assert name not in datagrunt.pdf.__all__
        assert not hasattr(datagrunt.pdf, name)
        assert not hasattr(datagrunt, name)
