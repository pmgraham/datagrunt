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
import sys
import zlib
from pathlib import Path

import pytest

import datagrunt
import datagrunt.pdf
from datagrunt.pdf import _hidden_rules, _pdfium_recover, elements, quality
from datagrunt.pdf._hidden_geometry import ClipPathPoly, ClipRegion, Occluder
from datagrunt.pdf._hidden_rules import em_reasons
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.pdfium_engine import (
    ImageStats,
    ObjectBoxes,
    PageGeometry,
    PageImages,
    PageSegments,
    PageText,
    RawImage,
    RawPage,
    RawSpan,
    SegmentStats,
    TextStats,
    extract_page,
    open_document,
)
from datagrunt.pdf.quality import (
    Backdrop,
    SpanPiece,
    apply_visibility_diff,
    assess_page,
    classify_spans,
    needs_background,
    needs_visibility_diff,
    raster_cover_fraction,
)
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
    """A valid RawSpan of ``text``, one box per char (a row at x=72 by default), its tight boxes the same."""
    boxes = row(len(text), 72.0) if boxes is None else boxes
    fields = {
        "text": text,
        "bbox": union(boxes),
        "char_boxes": boxes,
        "tight_boxes": boxes,
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
    """A valid RawImage drawn into ``bbox``: 1000 x 750 px, a scan's resolution (180 ppi over the page)."""
    fields = {
        "bbox": bbox,
        "px_width": 1000,
        "px_height": 750,
        "declared_px": (1000, 750),
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


class TestScanLikeCover:
    """Only images drawn at 50 ppi or more on each side are raster cover: the scan an OCR layer lies on."""

    @pytest.mark.parametrize(
        "px,counts",
        [
            pytest.param((1, 1), False, id="1x1-stretched"),
            pytest.param((8, 8), False, id="8x8"),
            pytest.param((0, 0), False, id="unknown-size"),
            pytest.param((250, 0), False, id="unknown-height"),
            # A gradient drawn as one row of pixels stretched over the page.
            pytest.param((1000, 1), False, id="gradient-row"),
            pytest.param((250, 200), True, id="exactly-50-ppi"),
            pytest.param((249, 200), False, id="just-under-across"),
            pytest.param((250, 199), False, id="just-under-down"),
            pytest.param((2500, 2000), True, id="500-ppi"),
        ],
    )
    def test_the_ocr_layer_exemption_needs_a_scan_like_image(self, px, counts):
        # 360 x 288 pt is 5 x 4 in, 86 % of the page: 250 x 200 px is 50 ppi each way.
        drawn = image((0.0, 0.0, 360.0, 288.0), px_width=px[0], px_height=px[1], declared_px=px)

        assert reasons_of(raw_span(render_mode=3), images=(drawn,)) == [() if counts else ("invisible",)]

    def test_a_quarter_turned_scan_pairs_its_pixel_sides_with_the_other_box_sides(self):
        # 200 x 250 px shown 5 x 4 in, as a quarter-turned page or image shows a 4 x 5 in scan: 50 ppi.
        turned = image((0.0, 0.0, 360.0, 288.0), px_width=200, px_height=250, declared_px=(200, 250))

        assert reasons_of(raw_span(render_mode=3), images=(turned,)) == [()]

    def test_low_resolution_images_add_no_cover_to_a_scan(self):
        # A scan of the left 40 % of the page and a 1 x 1 px image over the rest: 40 % scan cover.
        scan = image((0.0, 0.0, 160.0, H))
        stretched = image((160.0, 0.0, W, H), px_width=1, px_height=1, declared_px=(1, 1))

        assert reasons_of(raw_span(render_mode=3, boxes=row(4, 20.0)), images=(scan, stretched)) == [("invisible",)]

    def test_raster_cover_fraction_still_counts_every_image(self):
        stretched = image((0.0, 0.0, W, H), px_width=1, px_height=1, declared_px=(1, 1))

        assert raster_cover_fraction((stretched,), W, H) == 1.0


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
            pytest.param(0, 1, 255, ("transparent",), id="fill-alpha-1"),
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

    @pytest.mark.parametrize(
        "alpha,expected",
        [
            pytest.param(0, ("transparent",), id="0"),
            pytest.param(12, ("transparent",), id="12"),
            pytest.param(13, (), id="13"),
            pytest.param(255, (), id="255"),
        ],
    )
    @pytest.mark.parametrize("mode", [0, 1, 2, 4, 5, 6])
    def test_an_alpha_of_at_most_12_is_transparent_on_each_channel(self, mode, alpha, expected):
        # 12 of 255 is under 5 %: one threshold for fill and stroke alike.
        source = raw_span(render_mode=mode, fill_rgba=(0, 0, 0, alpha), stroke_rgba=(0, 0, 0, alpha))

        assert reasons_of(source) == [expected]

    @pytest.mark.parametrize("fill_alpha,stroke_alpha", [(12, 13), (13, 12)])
    def test_both_painted_channels_must_be_faint(self, fill_alpha, stroke_alpha):
        source = raw_span(render_mode=2, fill_rgba=(0, 0, 0, fill_alpha), stroke_rgba=(0, 0, 0, stroke_alpha))

        assert reasons_of(source) == [()]


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


def lines_run(count: int) -> int:
    """How many lines of quality.py run (sys.settrace line events) while raster_cover_fraction reads ``count`` images.

    The images are random_box's, from one seed. The trace function in place before is restored.
    """
    rng = random.Random(20_000)
    images = tuple(image(random_box(rng), object_index=index) for index in range(count))
    counted = [0]

    def local(frame, event, arg):
        counted[0] += event == "line"
        return local

    def tracer(frame, event, arg):
        return local if frame.f_code.co_filename == quality.__file__ else None

    previous = sys.gettrace()
    sys.settrace(tracer)
    try:
        got = raster_cover_fraction(images, W, H)
    finally:
        sys.settrace(previous)
    assert 0.0 < got <= 1.0
    return counted[0]


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

    @pytest.mark.no_cover  # this test sets its own trace function: coverage's must not be running
    def test_its_work_grows_as_n_log_n_not_as_n_squared(self):
        # Lines of quality.py run for n and 4n images: n log n grows ~4.7x (measured 4.70 from 2,000 to 8,000),
        # n squared 16x. A count, not a time: the same on any machine, under any load.
        small, large = lines_run(2_000), lines_run(8_000)

        assert large / small < 5.5, f"{small} lines for 2,000 images, {large} for 8,000"

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


# A blank scan: 400 x 300 px is 72 ppi over the 400 x 300 pt page (scan-like: 50 ppi or more each way).
PNG = solid_png(400, 300, (1.0, 1.0, 1.0))


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

    def test_render_mode_3_over_a_stretched_1_px_image_is_invisible(self, tmp_path):
        pieces, raw = classified(
            tmp_path,
            RasterImage(0.0, 0.0, W, H, solid_png(1, 1, (1.0, 1.0, 1.0))),
            TextRun("Scan", x=72.0, y=100.0, render_mode=3),
        )

        assert (raw.images.images[0].px_width, raw.images.stats.max_cover_fraction) == (1, 1.0)
        assert texts(pieces) == [("Scan", ("invisible",))]

    def test_ocr_layer_over_a_scan_on_a_quarter_turned_page_is_visible(self, tmp_path):
        # The page shows a 300 x 225 px scan of 400 x 300 pt (54 ppi) as 300 x 400 pt: unpaired with the turned
        # box, its 225 px would be 40 ppi over 400 pt.
        scan = RasterImage(0.0, 0.0, W, H, solid_png(300, 225, (1.0, 1.0, 1.0)))

        pieces, raw = classified(tmp_path, scan, TextRun("Scan", x=72.0, y=100.0, render_mode=3), rotate=90)

        assert raw.images.images[0].bbox == pytest.approx((0.0, 0.0, H, W))
        assert texts(pieces) == [("Scan", ())]

    def test_a_clip_path_is_not_applied_to_a_scan(self, tmp_path):
        # Known limitation: a scan clipped away entirely still lies under the OCR layer.
        pixels = zlib.compress(bytes(400 * 300))
        scan = b"<< /Type /XObject /Subtype /Image /Width 400 /Height 300 /ColorSpace /DeviceGray"
        scan += b" /BitsPerComponent 8 /Filter /FlateDecode /Length %d >>\nstream\n%s\nendstream" % (
            len(pixels),
            pixels,
        )
        content = b"q 0 0 0 0 re W n 400 0 0 300 0 0 cm /Im Do Q BT /F1 12 Tf 3 Tr 72 100 Td (Scan) Tj ET"

        pieces = classified_page(content_page(tmp_path, content, extra=(scan,), resources=b"/XObject << /Im 6 0 R >>"))

        assert texts(pieces) == [("Scan", ())]

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


class TestTransparentThroughExtractPage:
    """pdfium rounds an ExtGState alpha to 0-255; at most 12 (under 5 %) is transparent."""

    @pytest.mark.parametrize(
        "alpha,read,expected",
        [
            pytest.param(b"0", 0, ("transparent",), id="ca-0"),
            pytest.param(b"0.002", 1, ("transparent",), id="ca-0.002"),
            pytest.param(b"0.01", 3, ("transparent",), id="ca-0.01"),
            pytest.param(b"0.048", 12, ("transparent",), id="ca-0.048"),
            # pdfium rounds 0.05 x 255 = 12.75 up: 13, over the threshold.
            pytest.param(b"0.05", 13, (), id="ca-0.05"),
            pytest.param(b"0.06", 15, (), id="ca-0.06"),
        ],
    )
    @pytest.mark.parametrize("mode", [b"0", b"1"], ids=["fill", "stroke"])
    def test_a_faint_alpha_is_transparent(self, tmp_path, alpha, read, expected, mode):
        state = b"<< /Type /ExtGState /ca %s /CA %s >>" % (alpha, alpha)
        content = CONTROL + b"q /Faint gs BT /F1 12 Tf %s Tr 72 100 Td (HIDDEN) Tj ET Q" % mode
        path = content_page(tmp_path, content, resources=b"/ExtGState << /Faint 6 0 R >>", extra=(state,))

        pieces = classified_page(path)

        assert (pieces[1].span.fill_rgba[3], pieces[1].span.stroke_rgba[3]) == (read, read)
        assert texts(pieces) == [("Visible", ()), ("HIDDEN", expected)]


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
        # A rule under the text is paint OCR could read, so the count rule applies (P2-C): squeezed text counts for
        # nothing, and the page has no other text.
        rule = b"10 120 m 390 120 l S "
        content = rule + b"q BT /F1 12 Tf %s Tz 20 150 Td (Thirty characters of body text.) Tj ET Q" % scale

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
            "datagrunt.pdf._hidden_geometry",
            "datagrunt.pdf._hidden_rules",
            "datagrunt.pdf._options",  # OcrMode, which the caller checks without pdfium
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

    def test_validates_image_only_reasons_with_a1s_public_rule(self):
        # One rule for every image_only_reasons tuple (#470): A3's assessment allows its routing shortfalls only.
        assert quality.check_image_only_reasons is elements.check_image_only_reasons

    def test_painted_checks_bounds_without_union_sweep(self, tmp_path, monkeypatch):
        raw = extract_page(open_document(content_page(tmp_path, b"")), 0)
        assert isinstance(raw, RawPage)

        def forbid_union(_boxes):
            raise AssertionError("_painted must not run O(N log N) _union_area")

        monkeypatch.setattr(quality, "_union_area", forbid_union)
        off_boxes = tuple((-20.0, -20.0, -5.0, -5.0) for _ in range(100))
        zero_area = ((10.0, 10.0, 10.0, 50.0),)
        on_boxes = (*off_boxes, (10.0, 10.0, 20.0, 20.0))

        def boxes_of(boxes):
            return dataclasses.replace(raw, object_boxes=dataclasses.replace(raw.object_boxes, boxes=boxes))

        assert quality._painted(boxes_of(off_boxes)) is False
        assert quality._painted(boxes_of(zero_area)) is False
        assert quality._painted(boxes_of(on_boxes)) is True

    def test_flags_tiny_and_squeezed_text_with_recoverys_one_rule(self):
        # One em rule and one threshold for textpage spans and recovered text alike, in a pure module.
        assert quality.em_reasons is _pdfium_recover.em_reasons is _hidden_rules.em_reasons
        assert not hasattr(quality, "TINY_FONT_PT")
        # Recovery reads the threshold from the pure module; it defines none of its own.
        assert "TINY_FONT_PT = " not in Path(_pdfium_recover.__file__).read_text(encoding="utf-8")
        assert _pdfium_recover.TINY_FONT_PT == _hidden_rules.TINY_FONT_PT == 1.0

    def test_flags_off_page_text_with_recoverys_one_rule(self):
        assert quality.off_page is _pdfium_recover.off_page is _hidden_rules.off_page

    def test_flags_drawn_duplicates_with_recoverys_one_window(self):
        # A3's duplicate window is recovery's overprint window: its share, floor and cap are defined once (#406).
        assert quality.duplicate_window is _pdfium_recover.duplicate_window
        assert not {"OVERPRINT_TOLERANCE_PT", "DUPLICATE_REACH_CAP_PT", "_PDFIUM_SHARE"} & set(vars(quality))
        assert _pdfium_recover.duplicate_window((0.0, 0.0, 10.0, 0.25)) == (9.0, 0.5)
        assert _pdfium_recover.duplicate_window((0.0, 0.0, 30.0, 20.0)) == (12.0, 12.0)

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


def _synthetic_raw_page(
    spans: tuple[RawSpan, ...],
    *,
    width_pt: float = W,
    height_pt: float = H,
    boxes: tuple[tuple[float, float, float, float], ...] = (),
    glyph_boxes: tuple[tuple[float, float, float, float], ...] = (),
    occluders: tuple[Occluder, ...] = (),
    image_occluders: tuple[Occluder, ...] = (),
) -> RawPage:
    """Build an in-memory RawPage for deterministic quality and gate scaling tests."""
    char_count = sum(len(s.text) for s in spans)
    map_err_count = sum(len(s.map_errors) for s in spans)
    return RawPage(
        geometry=PageGeometry(
            page_number=1,
            width_pt=width_pt,
            height_pt=height_pt,
            rotation_deg=0,
            crop_offset=(0.0, 0.0),
        ),
        text=PageText(
            spans=spans,
            stats=TextStats(
                char_count=char_count,
                generated_count=0,
                ownerless_count=0,
                unicode_map_error_count=map_err_count,
                skipped_nonfinite_objects=0,
                dropped_text_objects=0,
                empty_text_objects=0,
            ),
        ),
        segments=PageSegments(segments=(), stats=SegmentStats(skipped_nonfinite_paths=0)),
        images=PageImages(
            images=(),
            stats=ImageStats(
                skipped_nonfinite_images=0,
                skipped_oversized_images=0,
                skipped_unsized_images=0,
                skipped_over_budget_images=0,
                skipped_overlong_images=0,
                max_cover_fraction=0.0,
            ),
        ),
        object_boxes=ObjectBoxes(boxes=boxes, unboxed=0, glyph_stroke_boxes=glyph_boxes),
        occluders=occluders,
        image_occluders=image_occluders,
    )


class TestHotPathScalingAndAsciiFastPath:
    """Wave-2 hot-path optimizations: split x0 bands, gap-bridge guard, sorted gate bands, and ASCII fast-path."""

    def test_wide_cad_border_and_compound_strokes_do_not_poison_narrow_glyph_bands(self):
        cad_w, cad_h = 842.0, 595.0
        strokes: list[tuple[float, float, float, float]] = []
        for band in range(32):
            y = 8.0 + band * (cad_h / 32.0)
            strokes.append((10.0, y, 820.0, y + 12.0))  # >600pt border rule
            strokes.append((10.0, y, 460.0, y + 12.0))  # 450pt compound path
        spans: list[RawSpan] = []
        for idx in range(1_500):
            r_idx, c_idx = idx % 50, idx // 50
            bx = 500.0 + (c_idx * 10.5) % 300.0
            by = 15.0 + r_idx * 11.0
            word = f"K{idx % 900 + 100}A"
            cboxes = tuple((bx + k * 4.2, by, bx + k * 4.2 + 3.8, by + 6.0) for k in range(len(word)))
            for cb in cboxes:
                strokes.append((cb[0] + 0.3, cb[1] + 0.4, cb[2] - 0.3, cb[3] - 0.4))
            spans.append(raw_span(word, cboxes, font_size_pt=6.0, em_width_pt=6.0, render_mode=3, object_index=idx))
        rng = random.Random(99)
        while len(strokes) < 12_000:
            sx = rng.uniform(20.0, 480.0)
            sy = rng.uniform(10.0, 580.0)
            strokes.append((sx, sy, sx + 2.5, sy + 4.0))

        pieces = classify_spans(tuple(spans), width_pt=cad_w, height_pt=cad_h, images=(), glyph_boxes=tuple(strokes))
        assert all(not p.hidden_reasons for p in pieces)

        # Compound word outline (200 x 10 pt) still backs normal letters while rejecting rule-like chars.
        comp_box = ((60.0, 91.0, 260.0, 101.0),)
        word_span = raw_span("WORD", row(4, 72.0), render_mode=3)
        rule_span = raw_span("____", row(4, 72.0), render_mode=3)
        assert [
            p.hidden_reasons
            for p in classify_spans((word_span,), width_pt=W, height_pt=H, images=(), glyph_boxes=comp_box)
        ] == [()]
        assert [
            p.hidden_reasons
            for p in classify_spans((rule_span,), width_pt=W, height_pt=H, images=(), glyph_boxes=comp_box)
        ] == [("invisible",)]

    def test_bridge_short_drift_gaps_skips_token_glyph_intervals_when_all_chars_backed(self, monkeypatch):
        cboxes = row(8, 72.0)
        span = raw_span("PN-20450", cboxes, render_mode=3)
        strokes = tuple((b[0] + 0.5, b[1] + 1.0, b[2] - 0.5, b[3] - 1.0) for b in cboxes)

        def forbid_intervals(*_args, **_kwargs):
            raise AssertionError("_token_glyph_intervals must not run when every char is already backed")

        monkeypatch.setattr(quality, "_token_glyph_intervals", forbid_intervals)
        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), glyph_boxes=strokes)
        assert [(p.text, p.hidden_reasons) for p in pieces] == [("PN-20450", ())]

    def test_gate_selection_x0_sorted_bands_avoids_linear_scan_budget_exhaustion(self, monkeypatch):
        header_rule = (10.0, 2.0, 390.0, 4.0)
        left_ticks = tuple(
            (5.0 + (i % 8) * 3.0, 15.0 + (i * 0.018) % 275.0, 7.0 + (i % 8) * 3.0, 17.0 + (i * 0.018) % 275.0)
            for i in range(15_000)
        )
        spans = tuple(
            raw_span(
                f"ITEM-{i:04d}",
                row(9, 70.0 + (i // 50) * 5.0, 20.0 + (i % 50) * 5.2, width=4.0, height=4.5),
                font_size_pt=6.0,
                em_width_pt=6.0,
                object_index=i,
            )
            for i in range(2_500)
        )
        raw = _synthetic_raw_page(spans, boxes=(header_rule, *left_ticks))
        assessed = assess_page(raw)

        assert needs_background(raw, assessed) is False

        monkeypatch.setattr(quality, "MAX_GATE_CHECKS", 0)
        assert needs_background(raw, assessed) is True

    def test_pure_ascii_map_error_free_spans_bypass_per_char_valid_with_exact_parity(self, monkeypatch):
        clean_ascii = raw_span("Clean ASCII\ttext\n123!\x07", row(22, 20.0, 30.0), object_index=0)
        partial_offpage = raw_span("EDGE_VISIBLE", row(12, -18.0, 40.0), object_index=1)
        mapped_err = raw_span("BadMap", row(6, 20.0, 50.0), map_errors=(2, 4), object_index=2)
        unicode_leader = raw_span("Café \ufffd\ufffd\ufffd \ue000!", row(11, 20.0, 70.0), object_index=3)
        hidden_ascii = raw_span("TinyHidden", row(10, 20.0, 90.0), font_size_pt=0.5, object_index=4)
        page = _synthetic_raw_page((clean_ascii, partial_offpage, mapped_err, unicode_leader, hidden_ascii))

        real_fast = quality._is_clean_ascii_span
        monkeypatch.setattr(quality, "_is_clean_ascii_span", lambda _span: False)
        slow_oracle = assess_page(page)
        monkeypatch.setattr(quality, "_is_clean_ascii_span", real_fast)

        real_valid = quality._valid

        def guarded_valid(span: RawSpan, offset: int) -> bool:
            if not span.map_errors and span.text.isascii():
                raise AssertionError(f"_valid called on pure-ASCII map-error-free span {span.text!r}")
            return real_valid(span, offset)

        monkeypatch.setattr(quality, "_valid", guarded_valid)
        got = assess_page(page)
        assert got == slow_oracle


class TestHiddenReasons395:
    """Issue #395: occluded, clipped, and blank_glyph quality flagging, space bridging, and gate skipping."""

    def test_classify_spans_validates_occluders_boundary(self):
        with pytest.raises(TypeError, match=re.escape("occluders must be a tuple, got list")):
            classify_spans((raw_span(),), width_pt=W, height_pt=H, images=(), occluders=[])
        with pytest.raises(TypeError, match=re.escape("occluders[0] must be a Occluder, got str")):
            classify_spans((raw_span(),), width_pt=W, height_pt=H, images=(), occluders=("bad",))

    def test_later_opaque_rect_occludes_multi_word_span_as_one_piece(self):
        boxes = row(14, 72.0, 90.0)
        span = raw_span("SECRET PAYLOAD", boxes, object_index=0)
        occ = Occluder(object_index=1, bbox=(70.0, 88.0, 170.0, 106.0))

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(occ,))
        assert layout(pieces) == [(0, 14, "SECRET PAYLOAD", ("occluded",))]

    def test_earlier_opaque_rect_leaves_text_visible(self):
        boxes = row(6, 72.0, 90.0)
        span = raw_span("SECRET", boxes, object_index=2)
        occ = Occluder(object_index=1, bbox=(70.0, 88.0, 140.0, 106.0))

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(occ,))
        assert layout(pieces) == [(0, 6, "SECRET", ())]

    def test_partial_occlusion_splits_without_bridging_boundary_space(self):
        boxes = row(9, 72.0, 90.0)  # "OK HIDDEN": chars 0-2 at x=72..90, chars 3-8 at x=90..126
        span = raw_span("OK HIDDEN", boxes, object_index=0)
        occ = Occluder(object_index=1, bbox=(89.5, 88.0, 130.0, 106.0))

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(occ,))
        assert layout(pieces) == [(0, 3, "OK ", ()), (3, 9, "HIDDEN", ("occluded",))]

    def test_blank_glyph_tight_box_flagged_and_split_cleanly(self):
        cboxes = row(4, 72.0, 90.0)
        standin = (84.0, 95.0, 90.0, 95.01)  # 0.01 pt height <= 0.0025 * 11 pt
        tboxes = (cboxes[0], cboxes[1], standin, (90.0, 95.0, 96.0, 95.01))
        span = raw_span("OKXY", cboxes, tight_boxes=tboxes)

        pieces = classify(span)
        assert layout(pieces) == [(0, 2, "OK", ()), (2, 4, "XY", ("blank_glyph",))]

    @pytest.mark.parametrize("mode", [3, 7])
    def test_no_paint_modes_strictly_exempt_from_blank_glyph(self, mode):
        cboxes = row(4, 72.0, 90.0)
        zero_tight = tuple((b[0], b[1], b[0], b[1]) for b in cboxes)
        span = raw_span("CAD1", cboxes, tight_boxes=zero_tight, render_mode=mode)

        pieces = classify(span, images=(FULL_BLEED,))
        assert layout(pieces) == [(0, 4, "CAD1", ())]

    def test_collapsed_em_zero_scale_not_double_flagged_as_blank_glyph(self):
        cboxes = row(4, 72.0, 90.0)
        squeezed_tight = tuple((b[0], b[1], b[0] + 0.004, b[3]) for b in cboxes)
        span = raw_span("FLAT", cboxes, tight_boxes=squeezed_tight, font_size_pt=12.0, em_width_pt=0.006)

        assert reasons_of(span) == [("zero_scale",)]

    def test_gate_selection_skips_occluded_clipped_and_blank_glyph_pieces(self):
        cboxes = row(4, 72.0, 90.0)
        occ_span = raw_span("OCCL", cboxes, fill_rgba=(255, 255, 255, 255), object_index=0)
        clip = ClipRegion(bbox=(250.0, 0.0, 300.0, 50.0), paths=(ClipPathPoly(bbox=(250.0, 0.0, 300.0, 50.0)),))
        clip_span = raw_span("CLIP", cboxes, fill_rgba=(255, 255, 255, 255), object_index=1, clip_region=clip)
        blank_span = raw_span(
            "BLNK",
            cboxes,
            fill_rgba=(255, 255, 255, 255),
            object_index=2,
            blank_glyph_offsets=(0, 1, 2, 3),
        )
        occ = Occluder(object_index=5, bbox=(60.0, 80.0, 140.0, 120.0))
        page = _synthetic_raw_page(
            (occ_span, clip_span, blank_span),
            boxes=((60.0, 80.0, 140.0, 120.0),),
            occluders=(occ,),
        )
        assessed = assess_page(page)

        assert [p.hidden_reasons for p in assessed.pieces] == [
            ("occluded",),
            ("occluded", "clipped"),
            ("occluded", "blank_glyph"),
        ]
        assert needs_background(page, assessed) is False

    def test_replacement_and_control_chars_exempt_from_blank_glyph_but_printable_map_errors_checked(self):
        cboxes = row(4, 72.0, 90.0)
        standin = tuple((b[0], b[1], b[2], b[1] + 0.01) for b in cboxes)
        fffd_span = raw_span("\ufffd\ufffd\ufffd\ufffd", cboxes, tight_boxes=standin, object_index=0)
        ctrl_span = raw_span("\x03\x04\x05\x06", cboxes, tight_boxes=standin, map_errors=(0, 1, 2, 3), object_index=1)
        ascii_map_err = raw_span("ABCD", cboxes, tight_boxes=standin, map_errors=(0, 1, 2, 3), object_index=2)

        assert reasons_of(fffd_span, ctrl_span, ascii_map_err) == [(), (), ("blank_glyph",)]

    def test_recovered_span_with_point_tight_box_not_flagged_blank_glyph(self):
        point_box = (72.0, 100.0, 72.0, 100.0)
        rec_raw = raw_span("COPY", (point_box,) * 4, tight_boxes=(point_box,) * 4, font_size_pt=12.0, object_index=9)
        rec_item = _pdfium_recover.RecoveredSpan(span=rec_raw, hidden_reasons=())
        rec_stats = _pdfium_recover.RecoveryStats(
            overprint_duplicates=0,
            whitespace_objects=0,
            empty_text_objects=0,
            unrecovered_text_objects=0,
        )
        recovered = _pdfium_recover.RecoveredText(page_number=1, spans=(rec_item,), stats=rec_stats)
        base = _synthetic_raw_page((raw_span("Visible body text here!!", row(24, 20.0)),))
        page = dataclasses.replace(
            base,
            text=dataclasses.replace(
                base.text,
                stats=dataclasses.replace(base.text.stats, dropped_text_objects=1),
            ),
        )

        assessed = assess_page(page, recovered=recovered)
        assert [p.hidden_reasons for p in assessed.recovered_pieces] == [()]


def _solid_backdrop(
    bgr: tuple[int, int, int], *, rects: tuple[tuple[tuple[int, int, int, int], tuple[int, int, int]], ...] = ()
) -> Backdrop:
    """Build a 1 px/pt 400 x 300 BGR Backdrop filled with ``bgr`` and optional ``(box, bgr)`` rects."""
    w, h = int(W), int(H)
    buf = bytearray(bytes(bgr) * (w * h))
    for (x0, y0, x1, y1), color in rects:
        row_bytes = bytes(color) * (x1 - x0)
        for y in range(y0, y1):
            start = (y * w + x0) * 3
            buf[start : start + len(row_bytes)] = row_bytes
    return Backdrop(pixels=bytes(buf), px_width=w, px_height=h)


class TestVisibilityDiff395:
    """#369-compliant rendered BGR diff for Type3 lying-d1 blank glyphs and image occluders."""

    def test_needs_visibility_diff_false_for_plain_helvetica_page(self):
        page = _synthetic_raw_page((raw_span("Hello World", row(11, 72.0, 90.0)),))
        assessed = assess_page(page)

        assert needs_visibility_diff(assessed, page) is False

    def test_needs_visibility_diff_true_for_visible_type3_and_exempts_tr3_and_replacement(self):
        t3_vis = raw_span("OK", row(2, 72.0, 90.0), base_font_name="")
        t3_enc_only = raw_span("AB", row(2, 72.0, 90.0), base_font_name="", map_errors=(0, 1))
        t3_tr3 = raw_span("OK", row(2, 72.0, 90.0), base_font_name="", render_mode=3)
        t3_fffd = raw_span("\ufffd\ufffd", row(2, 72.0, 90.0), base_font_name="", map_errors=(0, 1))
        page_vis = _synthetic_raw_page((t3_vis,))
        page_enc_only = _synthetic_raw_page((t3_enc_only,))
        page_tr3 = _synthetic_raw_page((t3_tr3,))
        page_fffd = _synthetic_raw_page((t3_fffd,))

        assert needs_visibility_diff(assess_page(page_vis), page_vis) is True
        assert needs_visibility_diff(assess_page(page_enc_only), page_enc_only) is True
        assert needs_visibility_diff(assess_page(page_tr3), page_tr3) is False
        assert needs_visibility_diff(assess_page(page_fffd), page_fffd) is False

    def test_apply_visibility_diff_flags_zero_paint_type3_and_splits_mixed_span(self):
        cboxes = row(3, 72.0, 90.0, width=8.0, height=12.0)  # 'O':[72..80], 'K':[80..88], 'X':[88..96]
        span = raw_span("OKX", cboxes, base_font_name="")
        page = _synthetic_raw_page((span,))
        assessed = assess_page(page)
        without_bg = _solid_backdrop((255, 255, 255))
        # Paint black ink over 'O' and 'K' only; 'X' leaves pixels unchanged.
        with_bg = _solid_backdrop((255, 255, 255), rects=(((73, 91, 87, 101), (0, 0, 0)),))

        updated = apply_visibility_diff(assessed, page, with_text=with_bg, without_text=without_bg)
        assert layout(updated.pieces) == [(0, 2, "OK", ()), (2, 3, "X", ("blank_glyph",))]
        assert updated.quality.hidden_spans_flagged == 1

    def test_apply_visibility_diff_distinguishes_opaque_vs_translucent_image_occluders(self):
        cboxes = row(6, 72.0, 90.0, width=8.0, height=12.0)
        span = raw_span("SECRET", cboxes, object_index=0)
        img_occ = Occluder(object_index=1, bbox=(65.0, 85.0, 130.0, 110.0))
        page = _synthetic_raw_page((span,), image_occluders=(img_occ,))
        assessed = assess_page(page)
        assert needs_visibility_diff(assessed, page) is True

        # Opaque image covers text: with_text and without_text are identical -> occluded.
        same_bg = _solid_backdrop((255, 255, 255))
        occluded = apply_visibility_diff(assessed, page, with_text=same_bg, without_text=same_bg)
        assert layout(occluded.pieces) == [(0, 6, "SECRET", ("occluded",))]

        # Translucent /SMask image lets text show through -> pixels differ -> stays visible.
        blended_with = _solid_backdrop((255, 255, 255), rects=(((73, 91, 119, 101), (64, 64, 64)),))
        visible = apply_visibility_diff(assessed, page, with_text=blended_with, without_text=same_bg)
        assert layout(visible.pieces) == [(0, 6, "SECRET", ())]

    def test_apply_visibility_diff_validates_inputs(self):
        page = _synthetic_raw_page((raw_span("OK", row(2, 72.0, 90.0), base_font_name=""),))
        assessed = assess_page(page)
        bg = _solid_backdrop((255, 255, 255))
        small = Backdrop(pixels=bytes(200 * 150 * 3), px_width=200, px_height=150)

        with pytest.raises(TypeError, match="assessment must be a PageAssessment"):
            apply_visibility_diff("bad", page, with_text=bg, without_text=bg)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="same pixel size"):
            apply_visibility_diff(assessed, page, with_text=bg, without_text=small)
