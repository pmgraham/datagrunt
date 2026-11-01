"""Tests for the v5 page quality and routing (#366, A3 part 4).

Unit tests build RawPage values directly (on test_quality's 400 x 300 pt
page) for exact control of each decision: char categories, the 20-char
threshold, the 50 % raster cover. The A0 regressions run through
``extract_page`` on PDFs built portably, with no system font: the ReportLab
toolkit (``pdf_tests.builders``) for Latin text, scans and blank pages, and
hand-written pages in a non-embedded CID font with a ToUnicode map for
CJK, right-to-left, math and dot-leader text.
"""

import dataclasses
import pickle
import re
import unicodedata
from pathlib import Path
from typing import get_args

import pytest

from datagrunt.pdf import quality
from datagrunt.pdf._pdfium_common import union_box
from datagrunt.pdf.elements import PageQuality
from datagrunt.pdf.pdfium_engine import (
    ImageStats,
    PageFailure,
    PageGeometry,
    PageImages,
    PageSegments,
    PageText,
    RawPage,
    RawSegment,
    RawSpan,
    RecoveredSpan,
    RecoveredText,
    RecoveryStats,
    SegmentStats,
    TextStats,
    extract_page,
    open_document,
    recover_dropped_text,
)
from datagrunt.pdf.quality import OcrMode, PageAssessment, assess_page, classify_spans, raster_cover_fraction
from pdf_tests.builders import Line, PageSpec, RasterImage, TextRun, build_pdf
from pdf_tests.test_pdfium_spans import OVERFLOW, bfchar_cmap, cid_font_page, content_page
from pdf_tests.test_quality import FULL_BLEED, PNG, STRIPS, H, W, image, raw_span, row

NEVER_WARNING = (
    "ocr='never': the page has {count} valid, visible, non-whitespace chars (under 20); "
    "it stays digital instead of going to OCR"
)


def text_span(text: str, x: float = 20.0, y: float = 90.0, **overrides):
    """A RawSpan of ``text`` in a row of 6 pt chars from ``(x, y)``."""
    return raw_span(text, row(len(text), x, y), **overrides)


def letters(count: int, y: float = 90.0, **overrides):
    """A span of ``count`` lowercase letters on the line at ``y``."""
    text = "".join(chr(ord("a") + index % 26) for index in range(count))
    return text_span(text, y=y, **overrides)


def textpage_chars(text: str) -> int:
    """The textpage chars a span's text takes: two for a non-BMP char (a surrogate pair)."""
    return sum(2 if ord(char) > 0xFFFF else 1 for char in text)


def segment(index: int) -> RawSegment:
    """A horizontal stroke on its own line."""
    return RawSegment(
        orientation="h",
        position=20.0 + index,
        start=10.0,
        end=110.0,
        width=0.5,
        kind="stroke",
        object_index=index,
        dashed=False,
        paint_rgba=(0, 0, 0, 255),
    )


def raw_page(*spans, images=(), segments: int = 0, rotation: int = 0, dropped: int = 0) -> RawPage:
    """A RawPage of the 400 x 300 pt page: ``spans``, ``images``, ``segments`` strokes, ``dropped`` text objects."""
    geometry = PageGeometry(page_number=1, width_pt=W, height_pt=H, rotation_deg=rotation, crop_offset=(0.0, 0.0))
    stats = TextStats(
        char_count=sum(textpage_chars(span.text) for span in spans),
        generated_count=0,
        ownerless_count=0,
        unicode_map_error_count=sum(len(span.map_errors) for span in spans),
        skipped_nonfinite_objects=0,
        dropped_text_objects=dropped,
        empty_text_objects=0,
    )
    largest = max((raster_cover_fraction((item,), W, H) for item in images), default=0.0)
    image_stats = ImageStats(
        skipped_nonfinite_images=0,
        skipped_oversized_images=0,
        skipped_unsized_images=0,
        skipped_over_budget_images=0,
        max_cover_fraction=largest,
    )
    return RawPage(
        geometry=geometry,
        text=PageText(spans=spans, stats=stats),
        segments=PageSegments(
            segments=tuple(segment(index) for index in range(segments)),
            stats=SegmentStats(skipped_nonfinite_paths=0),
        ),
        images=PageImages(images=images, stats=image_stats),
    )


def assess(*spans, images=(), ocr: OcrMode = "auto", **page) -> PageAssessment:
    """assess_page of a raw_page holding ``spans`` and ``images``."""
    return assess_page(raw_page(*spans, images=images, **page), ocr=ocr)


def assessment(**overrides) -> PageAssessment:
    """A valid PageAssessment of a one-span page with ``overrides`` applied."""
    good = assess(letters(24))
    fields = {item.name: getattr(good, item.name) for item in dataclasses.fields(PageAssessment)}
    return PageAssessment(**(fields | overrides))


class TestOcrMode:
    """The reader's OCR option."""

    def test_modes(self):
        assert get_args(OcrMode) == ("auto", "always", "never")


class TestPageAssessment:
    """PageAssessment is a frozen, slotted, picklable, validated record."""

    def test_pickle_round_trip(self):
        original = assess(letters(24), text_span("\ufffd\ufffd", y=120.0), letters(8, y=-60.0), images=(FULL_BLEED,))

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = assessment()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.route = "vision"
        assert not hasattr(value, "__dict__")

    def test_fields(self):
        names = [item.name for item in dataclasses.fields(PageAssessment)]

        assert names == ["pieces", "recovered_pieces", "quality", "route", "unmapped_regions", "warnings"]

    def test_region_numbers_are_stored_as_floats(self):
        value = assessment(unmapped_regions=((1, 2, 3, 4),))

        assert value.unmapped_regions == ((1.0, 2.0, 3.0, 4.0),)
        assert all(type(number) is float for number in value.unmapped_regions[0])

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"pieces": []}, TypeError, "pieces must be a tuple, got list", id="pieces-list"),
            pytest.param({"pieces": ("Abcd",)}, TypeError, "pieces[0] must be a SpanPiece, got str", id="pieces-item"),
            pytest.param(
                {"recovered_pieces": []}, TypeError, "recovered_pieces must be a tuple, got list", id="recovered-list"
            ),
            pytest.param(
                {"recovered_pieces": (None,)},
                TypeError,
                "recovered_pieces[0] must be a SpanPiece, got NoneType",
                id="recovered-item",
            ),
            pytest.param({"quality": None}, TypeError, "quality must be a PageQuality, got NoneType", id="quality"),
            pytest.param({"route": 1}, TypeError, "route must be a str, got 1 (int)", id="route-int"),
            pytest.param(
                {"route": "scan"}, ValueError, "route must be 'digital' or 'hybrid' or 'vision', got 'scan'", id="route"
            ),
            pytest.param(
                {"unmapped_regions": [(0.0, 0.0, 1.0, 1.0)]},
                TypeError,
                "unmapped_regions must be a tuple, got list",
                id="regions-list",
            ),
            pytest.param(
                {"unmapped_regions": ([0.0, 0.0, 1.0, 1.0],)},
                TypeError,
                "unmapped_regions[0] must be a tuple, got list",
                id="region-list",
            ),
            pytest.param(
                {"unmapped_regions": ((0.0, 0.0, 1.0),)},
                ValueError,
                "unmapped_regions[0] must have 4 values, got 3",
                id="region-short",
            ),
            pytest.param(
                {"unmapped_regions": ((5.0, 0.0, 1.0, 1.0),)},
                ValueError,
                "unmapped_regions[0] must have x0 <= x1 and y0 <= y1",
                id="region-reversed",
            ),
            pytest.param(
                {"unmapped_regions": ((0.0, float("nan"), 1.0, 1.0),)},
                ValueError,
                "unmapped_regions[0][1] must be finite",
                id="region-nan",
            ),
            pytest.param({"warnings": ["x"]}, TypeError, "warnings must be a tuple of str, got list", id="warnings"),
            pytest.param({"warnings": (" ",)}, ValueError, "warnings[0] must be a non-blank str", id="warning-blank"),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            assessment(**overrides)


class TestAssessPage:
    """assess_page classifies the page's spans and rejects bad input."""

    def test_pieces_are_classify_spans_of_the_page(self):
        spans = (letters(24), letters(6, y=-60.0), text_span("Scan", y=150.0, render_mode=3))
        page = raw_page(*spans, images=(STRIPS[0],))

        expected = classify_spans(spans, width_pt=W, height_pt=H, images=(STRIPS[0],))

        assert assess_page(page).pieces == expected

    def test_auto_is_the_default(self):
        page = raw_page(letters(5))

        assert assess_page(page) == assess_page(page, ocr="auto")

    def test_ocr_is_keyword_only(self):
        with pytest.raises(TypeError):
            assess_page(raw_page(), "always")

    @pytest.mark.parametrize(
        "page,ocr,error,message",
        [
            pytest.param("page", "auto", TypeError, "page must be a RawPage, got str", id="page-str"),
            pytest.param(
                PageFailure(page_number=1, stage="text", message="page 1: bad"),
                "auto",
                TypeError,
                "page must be a RawPage, got PageFailure",
                id="page-failure",
            ),
            pytest.param(
                None, "sometimes", ValueError, "ocr must be 'auto' or 'always' or 'never', got 'sometimes'", id="mode"
            ),
            pytest.param(None, True, TypeError, "ocr must be a str, got True (bool)", id="mode-bool"),
        ],
    )
    def test_invalid_argument_raises(self, page, ocr, error, message):
        with pytest.raises(error, match=re.escape(message)):
            assess_page(raw_page() if page is None else page, ocr=ocr)


class TestCharValidity:
    """U+FFFD, Co, Cn and Cs are invalid; every other char is valid text (no stripping)."""

    @pytest.mark.parametrize(
        "char,category,valid",
        [
            pytest.param("A", "Lu", True, id="Lu"),
            pytest.param("a", "Ll", True, id="Ll"),
            pytest.param("\u01c5", "Lt", True, id="Lt"),
            pytest.param("\u02b0", "Lm", True, id="Lm"),
            pytest.param("\u6587", "Lo", True, id="Lo-cjk"),
            pytest.param("\U0001d465", "Ll", True, id="Ll-non-bmp-math"),
            pytest.param("7", "Nd", True, id="Nd"),
            pytest.param("\u216b", "Nl", True, id="Nl"),
            pytest.param("\u00bd", "No", True, id="No"),
            pytest.param(".", "Po", True, id="Po-leader-dot"),
            pytest.param("-", "Pd", True, id="Pd"),
            pytest.param("(", "Ps", True, id="Ps"),
            pytest.param("$", "Sc", True, id="Sc"),
            pytest.param("+", "Sm", True, id="Sm"),
            pytest.param("^", "Sk", True, id="Sk"),
            pytest.param("\u00a9", "So", True, id="So"),
            pytest.param(" ", "Zs", True, id="Zs"),
            pytest.param("\u2028", "Zl", True, id="Zl"),
            pytest.param("\u2029", "Zp", True, id="Zp"),
            # Marks carry accented, Indic and RTL text: valid.
            pytest.param("\u0301", "Mn", True, id="Mn"),
            pytest.param("\u093e", "Mc", True, id="Mc"),
            pytest.param("\u20dd", "Me", True, id="Me"),
            # ZWJ / ZWNJ / LRM are legitimate text; an unmapped code reads as a control char.
            pytest.param("\u200d", "Cf", True, id="Cf-zwj"),
            pytest.param("\u200e", "Cf", True, id="Cf-lrm"),
            pytest.param("\x04", "Cc", True, id="Cc"),
            pytest.param("\t", "Cc", True, id="Cc-tab"),
            # U+FFFD is So, but it is the replacement char: a code with no mapping.
            pytest.param("\ufffd", "So", False, id="replacement"),
            pytest.param("\ue000", "Co", False, id="Co-bmp"),
            pytest.param("\U000f0000", "Co", False, id="Co-plane-15"),
            pytest.param("\u0378", "Cn", False, id="Cn-unassigned"),
            pytest.param("\uffff", "Cn", False, id="Cn-noncharacter"),
            pytest.param("\U0010ffff", "Cn", False, id="Cn-last"),
            pytest.param("\ud835", "Cs", False, id="Cs-high"),
            pytest.param("\udc00", "Cs", False, id="Cs-low"),
        ],
    )
    def test_category(self, char, category, valid):
        assert unicodedata.category(char) == category

        quality = assess(text_span(char)).quality

        assert quality.valid_char_ratio == (1.0 if valid else 0.0)

    def test_ratio_is_valid_over_every_char(self):
        quality = assess(text_span("Ab\ufffd\ue000c\u0378d\ud835")).quality

        assert quality.valid_char_ratio == pytest.approx(4 / 8)

    def test_a_map_error_is_invalid_whatever_it_reads_as(self):
        # pdfium reads a code it cannot map as the raw code: here "\x03" and "B" (U+0042).
        quality = assess(text_span("A\x03Bc", map_errors=(1, 2))).quality

        assert quality.valid_char_ratio == pytest.approx(2 / 4)


class TestPageQuality:
    """The PageQuality assembled from the page's raw facts and its pieces."""

    def test_signals(self):
        spans = (
            # Off the page on the left, then visible, then off on the right: two flagged pieces.
            raw_span("ABCDEFGH", (*row(2, -20.0), *row(4, 100.0), *row(2, 450.0))),
            letters(10, y=120.0, font_size_pt=0.5),
            text_span("\ufffd\ufffd", y=150.0),
        )

        got = assess(*spans, segments=3, rotation=90).quality

        assert got == PageQuality(
            valid_char_ratio=18 / 20,
            char_count=20,
            vector_line_count=3,
            hidden_spans_flagged=3,
            rotation_deg=90,
            deskew_angle_deg=None,
            ocr_mean_confidence=None,
        )

    def test_hidden_chars_count_toward_char_count_and_ratio(self):
        got = assess(letters(4), text_span("\ufffd" * 4, y=-60.0)).quality

        assert (got.char_count, got.valid_char_ratio, got.hidden_spans_flagged) == (8, 0.5, 1)

    def test_a_non_bmp_char_counts_once(self):
        page = raw_page(text_span("A\U0001d465"))

        assert page.text.stats.char_count == 3  # a surrogate pair in the textpage
        assert assess_page(page).quality.char_count == 2

    def test_no_chars_no_ratio(self):
        got = assess().quality

        assert (got.char_count, got.valid_char_ratio, got.hidden_spans_flagged) == (0, None, 0)

    def test_visible_pieces_are_not_flagged(self):
        assert assess(letters(30), letters(5, y=120.0)).quality.hidden_spans_flagged == 0


class TestRoutingCount:
    """Routing counts valid, visible, non-whitespace chars; the threshold is 20."""

    @pytest.mark.parametrize("count,route", [(0, "vision"), (19, "vision"), (20, "digital"), (400, "digital")])
    def test_threshold(self, count, route):
        spans = tuple(letters(min(40, count - start), y=20.0 + start // 2) for start in range(0, count, 40))

        assert assess(*spans).route == route

    def test_the_count_spans_every_span(self):
        assert assess(letters(10), letters(10, y=120.0)).route == "digital"

    @pytest.mark.parametrize("space", [" ", "\u00a0", "\u3000", "\t", "\u2028"])
    def test_whitespace_does_not_count(self, space):
        text = "abcdefghij" + space * 12 + "klmnopqrs"  # 19 letters

        assert assess(text_span(text)).route == "vision"

    @pytest.mark.parametrize(
        "overrides,reasons",
        [
            pytest.param({"y": -60.0}, ("off_page",), id="off-page"),
            pytest.param({"render_mode": 3}, ("invisible",), id="invisible"),
            pytest.param({"font_size_pt": 0.5}, ("tiny_font",), id="tiny"),
            pytest.param({"fill_rgba": (0, 0, 0, 0)}, ("transparent",), id="transparent"),
        ],
    )
    def test_hidden_chars_do_not_count(self, overrides, reasons):
        position = {"y": overrides.get("y", 120.0)}
        hidden = text_span("x" * 40, **(overrides | position))

        got = assess(letters(19), hidden)

        assert [item.hidden_reasons for item in got.pieces] == [(), reasons]
        assert got.route == "vision"

    @pytest.mark.parametrize("invalid", ["\ufffd", "\ue000", "\u0378", "\ud835", "\udc00"])
    def test_invalid_chars_do_not_count(self, invalid):
        assert assess(letters(19), text_span(invalid * 30, y=120.0)).route == "vision"

    def test_map_errors_do_not_count(self):
        unmapped = letters(30, y=120.0, map_errors=tuple(range(30)))

        assert assess(letters(19), unmapped).route == "vision"
        assert assess(letters(19), letters(30, y=120.0, map_errors=tuple(range(29)))).route == "digital"

    def test_the_ratio_is_recorded_but_never_routed_on(self):
        got = assess(letters(20), text_span("\ufffd" * 60, y=120.0), text_span("\ufffd" * 60, y=150.0))

        assert got.quality.valid_char_ratio == pytest.approx(20 / 140)
        assert got.route == "digital"

    def test_an_ocr_layer_over_a_scan_counts(self):
        # Render mode 3 over a full-bleed raster is visible text: the scan's OCR layer.
        got = assess(letters(20, render_mode=3), images=(FULL_BLEED,))

        assert [item.hidden_reasons for item in got.pieces] == [()]
        assert got.route == "hybrid"


class TestHybrid:
    """A page with enough text over raster cover above half the page is hybrid."""

    def test_text_over_a_full_bleed_raster_is_hybrid(self):
        assert assess(letters(24), images=(FULL_BLEED,)).route == "hybrid"

    def test_cover_of_exactly_half_the_page_is_digital(self):
        half = image((0.0, 0.0, W / 2, H))

        assert assess(letters(24), images=(half,)).route == "digital"

    def test_cover_just_over_half_the_page_is_hybrid(self):
        over = image((0.0, 0.0, W / 2 + 1.0, H))

        assert assess(letters(24), images=(over,)).route == "hybrid"

    def test_cover_is_the_union_of_the_images(self):
        # No strip covers over a quarter of the page; together they cover all of it.
        got = assess(letters(24), images=STRIPS)

        assert raw_page(images=STRIPS).images.stats.max_cover_fraction == 0.25
        assert got.route == "hybrid"

    def test_cover_is_of_the_visible_page(self):
        # Two thirds of the image lie left of the page: it covers half of it.
        mostly_off = image((-W, 0.0, W / 2, H))

        assert assess(letters(24), images=(mostly_off,)).route == "digital"

    @pytest.mark.parametrize("px", [(1, 1), (8, 8), (0, 0)], ids=["1x1", "8x8", "unknown"])
    def test_a_low_resolution_image_is_not_a_scan(self, px):
        # A 1 x 1 px image stretched over the page is no scan: the page is digital, not hybrid.
        stretched = image((0.0, 0.0, W, H), px_width=px[0], px_height=px[1], declared_px=px)

        assert assess(letters(24), images=(stretched,)).route == "digital"

    def test_too_little_text_over_a_scan_is_vision(self):
        assert assess(letters(19), images=(FULL_BLEED,)).route == "vision"


class TestOcrModes:
    """ocr="always" sends every page to vision; ocr="never" keeps a page without enough text digital."""

    @pytest.mark.parametrize(
        "spans,images",
        [
            pytest.param((letters(30),), (), id="digital-page"),
            pytest.param((letters(30),), (FULL_BLEED,), id="hybrid-page"),
            pytest.param((), (), id="blank-page"),
        ],
    )
    def test_always_is_vision(self, spans, images):
        got = assess(*spans, images=images, ocr="always")

        assert (got.route, got.warnings) == ("vision", ())

    @pytest.mark.parametrize("count", [0, 5, 19])
    def test_never_keeps_a_page_without_enough_text_digital_with_a_warning(self, count):
        spans = (letters(count),) if count else ()

        got = assess(*spans, ocr="never")

        assert (got.route, got.warnings) == ("digital", (NEVER_WARNING.format(count=count),))

    def test_never_counts_like_auto(self):
        # 19 letters and 12 spaces: 19 routing chars.
        got = assess(text_span("abcdefghij" + " " * 12 + "klmnopqrs"), ocr="never")

        assert got.warnings == (NEVER_WARNING.format(count=19),)

    def test_never_with_enough_text_is_digital_without_a_warning(self):
        got = assess(letters(20), ocr="never")

        assert (got.route, got.warnings) == ("digital", ())

    def test_never_with_enough_text_over_a_scan_is_digital(self):
        # hybrid OCRs the unmapped regions, which "never" rules out: the page
        # keeps its digital text and is not OCR'd.
        got = assess(letters(20), images=(FULL_BLEED,), ocr="never")

        assert (got.route, got.warnings) == ("digital", ())

    def test_auto_with_enough_text_over_a_scan_is_hybrid(self):
        got = assess(letters(20), images=(FULL_BLEED,))

        assert (got.route, got.warnings) == ("hybrid", ())

    def test_auto_without_enough_text_is_vision_without_a_warning(self):
        got = assess(letters(19))

        assert (got.route, got.warnings) == ("vision", ())


class TestUnmappedRegions:
    """Maximal runs of invalid chars in visible pieces keep their geometry for OCR (A5)."""

    def test_no_invalid_chars_no_regions(self):
        assert assess(letters(24)).unmapped_regions == ()

    def test_a_run_is_the_union_of_its_char_boxes(self):
        source = text_span("Ab\ufffd\ufffd\ufffdcd")

        assert assess(source).unmapped_regions == (union_box(source.char_boxes[2:5]),)

    def test_runs_are_maximal_and_mix_invalid_kinds(self):
        source = text_span("\ufffd\ue000A\u0378\ud835\U000f0000bc\ufffd")
        boxes = source.char_boxes

        assert assess(source).unmapped_regions == (
            union_box(boxes[0:2]),
            union_box(boxes[3:6]),
            union_box(boxes[8:9]),
        )

    def test_a_run_of_map_errors_is_a_region(self):
        source = text_span("Ab\x03\x04cd", map_errors=(2, 3))

        assert assess(source).unmapped_regions == (union_box(source.char_boxes[2:4]),)

    def test_map_errors_and_invalid_chars_make_one_run(self):
        source = text_span("A\x03\ufffd\x05B", map_errors=(1, 3))

        assert assess(source).unmapped_regions == (union_box(source.char_boxes[1:4]),)

    def test_regions_follow_piece_order_across_spans(self):
        first, second = text_span("A\ufffdB"), text_span("\ufffdC", y=150.0)

        regions = assess(first, second).unmapped_regions

        assert regions == (first.char_boxes[1], second.char_boxes[0])

    def test_hidden_pieces_have_no_regions(self):
        tiny = text_span("\ufffd" * 6, font_size_pt=0.5)

        assert assess(tiny).unmapped_regions == ()

    def test_a_run_stops_at_the_page_edge(self):
        # Chars 2 and 3 run past the right edge: off_page, so only chars 0 and 1 are a region.
        source = raw_span("\ufffd" * 4, row(4, 388.0))

        got = assess(source)

        assert [item.hidden_reasons for item in got.pieces] == [(), ("off_page",)]
        assert got.unmapped_regions == (union_box(source.char_boxes[0:2]),)

    def test_an_ocr_layer_keeps_its_unmapped_regions(self):
        source = text_span("Scanned\ufffd\ufffdtext", render_mode=3)

        assert assess(source, images=(FULL_BLEED,)).unmapped_regions == (union_box(source.char_boxes[7:9]),)


# Integration: the A0 regressions through extract_page.


def utf16_hex(char: str) -> bytes:
    """``char`` as the UTF-16BE hex a ToUnicode CMap maps a code to (a lone surrogate as itself)."""
    return char.encode("utf-16-be", "surrogatepass").hex().upper().encode()


def cid_text_page(tmp_path: Path, lines, *, size: float = 10.0) -> Path:
    """A 400 x 300 pt page drawing each of ``lines`` in one ``Tj``, 20 pt apart from the top.

    The font is test_pdfium_spans' non-embedded CID font (every code 0.6 em
    wide) with a ToUnicode map giving each distinct char its own code, so
    pdfium reads the text without any system font.
    """
    chars = sorted({char for line in lines for char in line})
    codes = {char: b"%04X" % (index + 1) for index, char in enumerate(chars)}
    cmap = bfchar_cmap({codes[char]: utf16_hex(char) for char in chars})
    shows = b" ".join(
        b"BT /F1 %g Tf 20 %g Td <%s> Tj ET" % (size, 280.0 - 20.0 * index, b"".join(codes[char] for char in line))
        for index, line in enumerate(lines)
    )
    return cid_font_page(tmp_path, shows, cmap)


def built(tmp_path: Path, *items, rotate: int = 0) -> Path:
    """A one-page 400 x 300 pt PDF of ``items``, written with the ReportLab toolkit."""
    return build_pdf(tmp_path / "page.pdf", [PageSpec(width=W, height=H, items=items, rotate=rotate)])


def assessed(path: Path, *, ocr: OcrMode = "auto") -> tuple[PageAssessment, RawPage]:
    """assess_page of page 1 of ``path``, with the RawPage it read."""
    with open_document(path) as handle:
        raw = extract_page(handle, 0)
    assert isinstance(raw, RawPage)
    return assess_page(raw, ocr=ocr), raw


def page_text(result: PageAssessment) -> str:
    """The text of every piece, in order."""
    return "".join(item.text for item in result.pieces)


# A rulebook contents page (A0, #359): 234 valid chars and leader dots its
# font maps to U+FFFD, every line padded to 52 columns (442 leaders, ratio 0.346).
TOC_ENTRIES = (
    ("Introduction", "1"),
    ("Getting started", "3"),
    ("Installing the tools", "7"),
    ("Reading a CSV file", "12"),
    ("Writing a CSV file", "18"),
    ("Working with tables", "25"),
    ("Hidden spans", "31"),
    ("Page routing", "38"),
    ("Error handling", "44"),
    ("Appendix A: formats", "52"),
    ("Appendix B: limits", "57"),
    ("Glossary of terms", "60"),
    ("Index of examples", "63"),
)
TOC_COLUMNS = 52
TOC_LINES = tuple(title + "\ufffd" * (TOC_COLUMNS - len(title) - len(page)) + page for title, page in TOC_ENTRIES)

RTL_TEXT = {
    "hebrew": "\u05e9\u05dc\u05d5\u05dd \u05e2\u05d5\u05dc\u05dd \u05d6\u05d4\u05d5 \u05d8\u05e7\u05e1\u05d8 "
    "\u05d1\u05e2\u05d1\u05e8\u05d9\u05ea",
    "arabic": "\u0645\u0631\u062d\u0628\u0627 \u0628\u0627\u0644\u0639\u0627\u0644\u0645 \u0647\u0630\u0627 "
    "\u0646\u0635 \u0639\u0631\u0628\u064a",
    # A zero-width non-joiner (Cf) inside the first word.
    "persian": "\u0645\u06cc\u200c\u062e\u0648\u0627\u0647\u0645 \u0627\u06cc\u0646 \u0631\u0627 "
    "\u0628\u0628\u06cc\u0646\u0645 \u0645\u062a\u0646",
}

LTR_TEXT = {
    "cjk": "\u6587\u5b57\u5316\u3051\u306e\u306a\u3044\u65e5\u672c\u8a9e\u306e\u6587\u7ae0\u3067\u3059\u3002"
    "\u4e2d\u6587\u4e5f\u53ef\u4ee5\u3002\ud55c\uad6d\uc5b4",
    "math": "\u2211\u1d62 \u03b1\u1d62x\u1d62 \u2264 \u221e, \u222b\u2080\u00b9 f(x) dx = \u03c0\u00b2/6 "
    "\U0001d465\u2208\u211d",
    # Combining accents (Mn), as NFD text reads.
    "decomposed-accents": "Cafe\u0301 cre\u0300me bru\u0302le\u0301e nai\u0308ve",
    "devanagari": "\u0928\u092e\u0938\u094d\u0924\u0947 \u0926\u0941\u0928\u093f\u092f\u093e "
    "\u0939\u093f\u0928\u094d\u0926\u0940 \u092a\u093e\u0920",
}


class TestRegressions:
    """The A0 regressions, through extract_page."""

    def test_dot_leader_contents_page_is_digital(self, tmp_path):
        result, raw = assessed(cid_text_page(tmp_path, TOC_LINES, size=8.0))
        quality = result.quality

        assert [span.text for span in raw.text.spans] == list(TOC_LINES)
        assert quality.char_count == 676
        assert round(quality.valid_char_ratio * quality.char_count) == 234
        assert quality.valid_char_ratio == pytest.approx(0.346, abs=5e-4)
        assert result.route == "digital"

    def test_dot_leaders_are_unmapped_regions(self, tmp_path):
        result, raw = assessed(cid_text_page(tmp_path, TOC_LINES, size=8.0))
        leader_runs = [(len(title), TOC_COLUMNS - len(page)) for title, page in TOC_ENTRIES]

        expected = tuple(
            union_box(span.char_boxes[start:end]) for span, (start, end) in zip(raw.text.spans, leader_runs)
        )

        assert result.unmapped_regions == expected

    def test_a_page_of_codes_with_no_unicode_mapping_is_vision(self, tmp_path):
        # A subset CID font whose ToUnicode maps none of its codes: pdfium reads each as itself (U+0003 ...).
        codes = b"".join(b"%04X" % code for code in range(0x03, 0x1C))
        path = cid_font_page(tmp_path, b"BT /F1 10 Tf 20 150 Td <%s> Tj ET" % codes, bfchar_cmap({b"FFF0": b"0041"}))

        result, raw = assessed(path)

        (span,) = raw.text.spans
        assert span.text == "".join(chr(code) for code in range(0x03, 0x1C))
        assert span.map_errors == tuple(range(25))
        assert (result.route, result.quality.valid_char_ratio) == ("vision", 0.0)
        assert result.unmapped_regions == (span.bbox,)

    def test_searchable_scan_is_hybrid(self, tmp_path):
        text = "Scanned page with its OCR layer"
        path = built(tmp_path, RasterImage(0.0, 0.0, W, H, PNG), TextRun(text, x=40.0, y=100.0, render_mode=3))

        result, _ = assessed(path)

        assert [(item.text, item.hidden_reasons) for item in result.pieces] == [(text, ())]
        assert (result.route, result.quality.hidden_spans_flagged, result.warnings) == ("hybrid", 0, ())

    def test_searchable_scan_in_strips_is_hybrid(self, tmp_path):
        strips = tuple(RasterImage(0.0, index * H / 4, W, H / 4, PNG) for index in range(4))
        path = built(tmp_path, *strips, TextRun("Scanned page with its OCR layer", x=40.0, y=100.0, render_mode=3))

        result, raw = assessed(path)

        assert raw.images.stats.max_cover_fraction == pytest.approx(0.25)
        assert result.route == "hybrid"

    def test_hidden_text_alone_does_not_make_a_scan_digital(self, tmp_path):
        hidden = (
            TextRun("Tiny text that no reader sees", x=40.0, y=100.0, size=0.5),
            TextRun("Transparent text no reader sees", x=40.0, y=150.0, alpha=0.0),
            TextRun("Text drawn above the top edge", x=40.0, y=-60.0),
        )
        path = built(tmp_path, RasterImage(0.0, 0.0, W, H, PNG), *hidden)

        result, _ = assessed(path)

        reasons = [item.hidden_reasons for item in result.pieces]
        assert reasons == [("tiny_font",), ("transparent",), ("off_page",)]
        assert (result.route, result.quality.hidden_spans_flagged) == ("vision", 3)

    def test_invisible_text_without_a_scan_is_vision(self, tmp_path):
        path = built(tmp_path, TextRun("Invisible text with no scan under it", x=40.0, y=100.0, render_mode=3))

        result, _ = assessed(path)

        assert [item.hidden_reasons for item in result.pieces] == [("invisible",)]
        assert result.route == "vision"

    def test_blank_page_is_vision(self, tmp_path):
        result, _ = assessed(built(tmp_path))

        assert result.quality == PageQuality(
            valid_char_ratio=None,
            char_count=0,
            vector_line_count=0,
            hidden_spans_flagged=0,
            rotation_deg=0,
            deskew_angle_deg=None,
            ocr_mean_confidence=None,
        )
        assert (result.pieces, result.route, result.unmapped_regions, result.warnings) == ((), "vision", (), ())

    def test_blank_page_with_ocr_never_is_digital_with_a_warning(self, tmp_path):
        result, _ = assessed(built(tmp_path), ocr="never")

        assert (result.route, result.warnings) == ("digital", (NEVER_WARNING.format(count=0),))

    def test_accented_latin_text_is_digital_byte_for_byte(self, tmp_path):
        # Precomposed Latin-1 letters, which Helvetica's WinAnsi encoding draws as themselves.
        text = (
            "Cr\u00e8me br\u00fbl\u00e9e \u00e0 la fa\u00e7on de Zo\u00eb, "
            "na\u00efvet\u00e9 \u00fcber \u00c5ngstr\u00f6m"
        )

        result, _ = assessed(built(tmp_path, TextRun(text, x=20.0, y=100.0)))

        assert page_text(result).encode("utf-8") == text.encode("utf-8")
        assert (result.route, result.quality.valid_char_ratio, result.unmapped_regions) == ("digital", 1.0, ())

    @pytest.mark.parametrize("name", list(LTR_TEXT))
    def test_left_to_right_text_is_digital_byte_for_byte(self, tmp_path, name):
        text = LTR_TEXT[name]

        result, _ = assessed(cid_text_page(tmp_path, (text,)))

        assert page_text(result).encode("utf-8") == text.encode("utf-8")
        assert (result.route, result.quality.valid_char_ratio, result.unmapped_regions) == ("digital", 1.0, ())

    @pytest.mark.parametrize("name", list(RTL_TEXT))
    def test_right_to_left_text_is_digital_byte_for_byte(self, tmp_path, name):
        # A producer draws right-to-left text in visual order; pdfium reads it in logical order.
        text = RTL_TEXT[name]

        result, _ = assessed(cid_text_page(tmp_path, (text[::-1],)))

        assert page_text(result).encode("utf-8") == text.encode("utf-8")
        assert (result.route, result.quality.valid_char_ratio, result.unmapped_regions) == ("digital", 1.0, ())

    def test_vector_lines_and_rotation_are_recorded(self, tmp_path):
        rules = (Line(20.0, 50.0, 300.0, 50.0), Line(20.0, 80.0, 300.0, 80.0))

        result, raw = assessed(
            built(tmp_path, *rules, TextRun("A rotated page of digital text", x=40.0, y=100.0), rotate=90)
        )

        assert len(raw.segments.segments) == 2
        assert (result.quality.vector_line_count, result.quality.rotation_deg, result.route) == (2, 90, "digital")


UNRECOVERED_WARNING = (
    "{count} text object(s) that pdfium's textpage drops could not be recovered; their text is missing from this page"
)
NOT_RECOVERED_WARNING = (
    "{count} text object(s) that pdfium's textpage drops were not recovered (no recovery was passed); "
    "their text is missing from this page"
)


def recovered_span(text: str, *, reasons=("tiny_font",), y: float = 200.0, **overrides) -> RecoveredSpan:
    """A RecoveredSpan of ``text`` whose every char box is the point (72, ``y``): Tf 0 text's box."""
    box = (72.0, y, 72.0, y)
    span = raw_span(text, (box,) * len(text), **({"font_size_pt": 0.0, "object_index": 9} | overrides))
    return RecoveredSpan(span=span, hidden_reasons=reasons)


def recovery(
    *spans: RecoveredSpan,
    overprint: int = 0,
    whitespace: int = 0,
    empty: int = 0,
    unrecovered: int = 0,
    page_number: int = 1,
) -> RecoveredText:
    counts = RecoveryStats(
        overprint_duplicates=overprint,
        whitespace_objects=whitespace,
        empty_text_objects=empty,
        unrecovered_text_objects=unrecovered,
    )
    return RecoveredText(page_number=page_number, spans=spans, stats=counts)


def accounted(recovered: RecoveredText) -> int:
    """The dropped text objects ``recovered`` accounts for: one per object with spans, plus its counts."""
    objects = {item.span.object_index for item in recovered.spans}
    return len(objects) + sum(dataclasses.astuple(recovered.stats))


def assess_recovered(*spans, recovered: RecoveredText, ocr: OcrMode = "auto", images=()) -> PageAssessment:
    """assess_page of a raw_page of ``spans`` whose dropped count ``recovered`` accounts for."""
    page = raw_page(*spans, images=images, dropped=accounted(recovered))
    return assess_page(page, ocr=ocr, recovered=recovered)


class TestRecoveredPieces:
    """assess_page gives recovered text as pieces apart from the page's; it never routes the page."""

    def test_no_recovery_is_the_default(self):
        page = raw_page(letters(24), dropped=1)

        assert assess_page(page) == assess_page(page, recovered=None)

    @pytest.mark.parametrize("count", [1, 3])
    def test_dropped_objects_with_no_recovery_are_a_warning(self, count):
        result = assess_page(raw_page(letters(24), dropped=count))

        assert result.warnings == (NOT_RECOVERED_WARNING.format(count=count),)

    def test_a_page_with_nothing_dropped_needs_no_recovery(self):
        assert assess_page(raw_page(letters(24))).warnings == ()

    def test_the_not_recovered_warning_follows_the_routing_warning(self):
        result = assess_page(raw_page(letters(5), dropped=2), ocr="never")

        assert result.warnings == (NEVER_WARNING.format(count=5), NOT_RECOVERED_WARNING.format(count=2))

    def test_recovered_is_keyword_only(self):
        with pytest.raises(TypeError):
            assess_page(raw_page(), "auto", recovery())

    def test_recovered_pieces_are_apart_from_the_page_pieces(self):
        # A4 lays out the page's pieces: a recovered copy must never read as page text.
        spans = (letters(24), text_span("Gone", y=-60.0))
        result = assess_recovered(*spans, recovered=recovery(recovered_span("HIDDEN")))

        assert result.pieces == classify_spans(spans, width_pt=W, height_pt=H, images=())
        assert [(item.text, item.hidden_reasons) for item in result.recovered_pieces] == [("HIDDEN", ("tiny_font",))]

    def test_no_recovery_has_no_recovered_pieces(self):
        assert assess_page(raw_page(letters(24))).recovered_pieces == ()

    def test_reasons_are_the_recovered_ones_and_the_computed_ones_in_canonical_order(self):
        # A Tz 0 object (zero_scale, 12 pt) drawn above the page and in render mode 3.
        squeezed = recovered_span("UP", reasons=("zero_scale",), y=-40.0, font_size_pt=12.0, render_mode=3)

        result = assess_recovered(letters(24), recovered=recovery(squeezed))

        assert result.recovered_pieces[-1].hidden_reasons == ("off_page", "invisible", "zero_scale")

    def test_a_tiny_recovered_span_is_flagged_once(self):
        result = assess_recovered(letters(24), recovered=recovery(recovered_span("HIDDEN")))

        assert result.recovered_pieces[-1].hidden_reasons == ("tiny_font",)

    def test_recovered_pieces_count_as_flagged(self):
        spans = (letters(24), text_span("Gone", y=-60.0))
        recovered = recovery(recovered_span("ONE"), recovered_span("TWO", reasons=("zero_scale",), object_index=10))

        result = assess_recovered(*spans, recovered=recovered)

        assert result.quality.hidden_spans_flagged == 3

    def test_recovered_text_never_routes_the_page(self):
        # 30 recovered chars with no hidden reason (a drawn copy of invisible text) still leave 5 visible chars.
        copy = recovered_span("abcdefghijklmnopqrstuvwxyzabcd", reasons=(), font_size_pt=12.0)

        result = assess_recovered(letters(5), recovered=recovery(copy))

        assert (result.route, result.recovered_pieces[-1].hidden_reasons) == ("vision", ())

    def test_recovered_text_is_not_in_the_char_counts_or_unmapped_regions(self):
        recovered = recovery(recovered_span("���", reasons=(), font_size_pt=12.0))
        page = raw_page(letters(24), dropped=1)

        result = assess_page(page, recovered=recovered)

        alone = assess_page(page)
        assert (result.quality.char_count, result.quality.valid_char_ratio) == (24, 1.0)
        assert result.quality == alone.quality
        assert result.unmapped_regions == alone.unmapped_regions == ()

    @pytest.mark.parametrize("count", [1, 3])
    def test_unrecovered_objects_are_a_warning(self, count):
        result = assess_recovered(letters(24), recovered=recovery(unrecovered=count))

        assert result.warnings == (UNRECOVERED_WARNING.format(count=count),)

    def test_counted_objects_alone_are_no_warning(self):
        result = assess_recovered(letters(24), recovered=recovery(overprint=2, whitespace=1, empty=3))

        assert (len(result.pieces), result.recovered_pieces, result.warnings) == (1, (), ())

    def test_the_unrecovered_warning_follows_the_routing_warning(self):
        result = assess_recovered(letters(5), recovered=recovery(unrecovered=1), ocr="never")

        assert result.warnings == (NEVER_WARNING.format(count=5), UNRECOVERED_WARNING.format(count=1))

    @pytest.mark.parametrize(
        "recovered,dropped,error,message",
        [
            pytest.param("text", 0, TypeError, "recovered must be a RecoveredText or None, got str", id="type"),
            pytest.param(
                recovery(unrecovered=1),
                2,
                ValueError,
                "recovered accounts for 1 dropped text object(s), but the page has 2: it is not this page's recovery",
                id="too-few",
            ),
            pytest.param(
                recovery(recovered_span("A"), recovered_span("B", object_index=10)),
                1,
                ValueError,
                "recovered accounts for 2 dropped text object(s), but the page has 1",
                id="too-many",
            ),
        ],
    )
    def test_invalid_recovery_raises(self, recovered, dropped, error, message):
        with pytest.raises(error, match=re.escape(message)):
            assess_page(raw_page(letters(24), dropped=dropped), recovered=recovered)

    def test_another_pages_recovery_raises(self):
        with pytest.raises(ValueError, match=re.escape("recovered is the recovery of page 2, not of page 1")):
            assess_page(raw_page(letters(24), dropped=1), recovered=recovery(unrecovered=1, page_number=2))

    def test_two_spans_of_one_object_account_for_it_once(self):
        recovered = recovery(recovered_span("A"), recovered_span("B"))

        assert assess_page(raw_page(letters(24), dropped=1), recovered=recovered).quality.hidden_spans_flagged == 2


DUPLICATE_CAP_WARNING = (
    "{count} recovered piece(s) were not checked for a drawn duplicate: each has over 64 visible pieces "
    "of the same text on the page; they stay visible"
)
# Ink boxes of a visible twin, and the duplicate window (0.9 of a side, 0.5 to 12 pt) a copy of the same size has.
SQUARE = (20.0, 90.0, 30.0, 100.0)  # "Title" in 2 x 10 pt chars: 9 pt each way.
WIDE = (20.0, 90.0, 50.0, 100.0)  # 6 x 10 pt chars: 12 pt (the cap) across, 9 pt down.
TALL = (20.0, 90.0, 50.0, 110.0)  # 6 x 20 pt chars: 12 pt (the cap) each way.
DOT = (20.0, 90.0, 20.25, 90.25)  # "." in a 0.25 pt box: 0.9 of it is 0.225 pt, so the 0.5 pt floor.


def twin(text: str, box, **overrides) -> RawSpan:
    """A span of ``text`` whose chars fill ``box`` in a row (its tight boxes the same): visible unless overridden."""
    x0, y0, x1, y1 = box
    return raw_span(text, row(len(text), x0, y0, width=(x1 - x0) / len(text), height=y1 - y0), **overrides)


def drawn_copy(text: str, box, *, reasons=(), **overrides) -> RecoveredSpan:
    """A recovered span of ``text`` at 11 pt, every char box its object's ink box ``box``, as recovery places it."""
    span = raw_span(text, (box,) * len(text), **({"object_index": 9} | overrides))
    return RecoveredSpan(span=span, hidden_reasons=reasons)


def shifted(box, dx: float, dy: float):
    """``box`` moved ``dx`` right and ``dy`` down."""
    return (box[0] + dx, box[1] + dy, box[2] + dx, box[3] + dy)


def copies_read(*spans, copies, unrecovered: int = 0) -> tuple[list, tuple[str, ...]]:
    """Each recovered piece as ``(text, hidden_reasons)`` on a page of ``spans`` and ``copies``; and the warnings."""
    result = assess_recovered(*spans, recovered=recovery(*copies, unrecovered=unrecovered))
    return [(item.text, item.hidden_reasons) for item in result.recovered_pieces], result.warnings


def vees(count: int) -> tuple[RawSpan, ...]:
    """``count`` visible "V" spans, one 6 x 12 pt char each, 25 to a row."""
    return tuple(raw_span("V", row(1, 10.0 + (at % 25) * 15.0, 10.0 + (at // 25) * 20.0)) for at in range(count))


class TestRecoveredDuplicates:
    """A recovered drawn copy of visible text, drawn on it, is flagged ``duplicate``: a reader reads it once (#406)."""

    @pytest.mark.parametrize(
        "text,box,dx,dy,flagged",
        [
            pytest.param("Title", SQUARE, 0.0, 0.0, True, id="on-it"),
            pytest.param("Title", SQUARE, 9.0, 0.0, True, id="nine-tenths-right"),
            pytest.param("Title", SQUARE, -9.0, 0.0, True, id="nine-tenths-left"),
            pytest.param("Title", SQUARE, 0.0, 9.0, True, id="nine-tenths-down"),
            pytest.param("Title", SQUARE, 0.0, -9.0, True, id="nine-tenths-up"),
            pytest.param("Title", SQUARE, 9.0, 9.0, True, id="nine-tenths-each-way"),
            pytest.param("Title", SQUARE, 9.125, 0.0, False, id="past-nine-tenths-across"),
            pytest.param("Title", SQUARE, 0.0, -9.125, False, id="past-nine-tenths-down"),
            pytest.param("Title", WIDE, 12.0, 0.0, True, id="the-cap-across"),
            pytest.param("Title", WIDE, -12.125, 0.0, False, id="past-the-cap-across"),
            pytest.param("Title", WIDE, 0.0, 9.0, True, id="down-has-its-own-window"),
            pytest.param("Title", WIDE, 0.0, 12.0, False, id="the-across-window-is-not-down"),
            pytest.param("Title", TALL, 0.0, 12.0, True, id="the-cap-down"),
            pytest.param("Title", TALL, 0.0, 12.125, False, id="past-the-cap-down"),
            pytest.param(".", DOT, 0.5, -0.5, True, id="the-floor"),
            pytest.param(".", DOT, 0.515625, 0.0, False, id="past-the-floor-across"),
            pytest.param(".", DOT, 0.0, 0.515625, False, id="past-the-floor-down"),
            pytest.param("Title", WIDE, 20.0, 0.0, False, id="20-pt-away"),
        ],
    )
    def test_a_copy_within_its_duplicate_window_of_a_visible_twin_is_a_duplicate(self, text, box, dx, dy, flagged):
        # The window is recovery's overprint window of the copy's last char box: 0.9 of each side, 0.5 to 12 pt.
        read, warnings = copies_read(letters(24), twin(text, box), copies=(drawn_copy(text, shifted(box, dx, dy)),))

        assert (read, warnings) == ([(text, ("duplicate",) if flagged else ())], ())

    def test_the_window_is_the_copys(self):
        # A 20 pt wide copy has a 12 pt window; its 10 pt wide twin's would be 9 pt. Its right edge is 10 pt past.
        read, _ = copies_read(twin("Title", SQUARE), copies=(drawn_copy("Title", (20.0, 90.0, 40.0, 100.0)),))

        assert read == [("Title", ("duplicate",))]

    def test_every_edge_must_lie_within_the_window(self):
        # Its left, top and bottom edges are on the twin's; its right edge is 15 pt past it (a 12 pt window).
        read, _ = copies_read(twin("Title", SQUARE), copies=(drawn_copy("Title", (20.0, 90.0, 45.0, 100.0)),))

        assert read == [("Title", ())]

    def test_the_twins_ink_box_is_compared_not_its_loose_box(self):
        # A recovered char box is its object's ink (its tight boxes): the twin's ink is (20, 90)-(30, 100), its
        # loose boxes (advance by the font's height) reach 10 pt above and below it, past the 9 pt window.
        loose = row(5, 20.0, 80.0, width=2.0, height=30.0)
        visible = raw_span("Title", loose, tight_boxes=row(5, 20.0, 90.0, width=2.0, height=10.0))

        read, _ = copies_read(visible, copies=(drawn_copy("Title", SQUARE),))

        assert read == [("Title", ("duplicate",))]

    @pytest.mark.parametrize(
        "reasons,overrides,expected",
        [
            pytest.param(("tiny_font",), {}, ("tiny_font",), id="recovered-reason"),
            pytest.param(("zero_scale",), {}, ("zero_scale",), id="squeezed"),
            pytest.param((), {"render_mode": 3}, ("invisible",), id="computed-reason"),
        ],
    )
    def test_a_copy_hidden_for_another_reason_is_not_also_a_duplicate(self, reasons, overrides, expected):
        copy = drawn_copy("Title", SQUARE, reasons=reasons, **overrides)

        read, _ = copies_read(twin("Title", SQUARE), copies=(copy,))

        assert read == [("Title", expected)]

    @pytest.mark.parametrize("hidden", [{"render_mode": 3}, {"font_size_pt": 0.5}], ids=["invisible", "tiny_font"])
    def test_a_copy_of_hidden_text_stays_visible(self, hidden):
        # pdfium kept the hidden copy and dropped the drawn one: the drawn copy is the text a reader sees.
        read, _ = copies_read(letters(24), twin("Title", SQUARE, **hidden), copies=(drawn_copy("Title", SQUARE),))

        assert read == [("Title", ())]

    @pytest.mark.parametrize("text", ["Title ", "title", "TITLE", "Titl", "Title page"])
    def test_a_copy_of_other_text_stays_visible(self, text):
        # The texts must be equal: a copy of other text is text a reader reads (recovered unique text included).
        read, _ = copies_read(twin("Title", SQUARE), copies=(drawn_copy(text, SQUARE),))

        assert read == [(text, ())]

    def test_a_copy_of_part_of_a_visible_piece_stays_visible(self):
        read, _ = copies_read(twin("Title page", WIDE), copies=(drawn_copy("Title", SQUARE),))

        assert read == [("Title", ())]

    def test_any_visible_twin_within_the_window_counts(self):
        far = twin("Title", shifted(SQUARE, 200.0, 100.0))

        read, _ = copies_read(far, twin("Title", SQUARE), copies=(drawn_copy("Title", SQUARE),))

        assert read == [("Title", ("duplicate",))]

    def test_each_copy_is_flagged_on_its_own(self):
        copies = (
            drawn_copy("Title", SQUARE),
            drawn_copy("Title", shifted(SQUARE, 0.0, 50.0), object_index=10),
            drawn_copy("Words", SQUARE, object_index=11),
        )

        read, _ = copies_read(twin("Title", SQUARE), copies=copies)

        assert read == [("Title", ("duplicate",)), ("Title", ()), ("Words", ())]

    def test_a_duplicate_counts_as_flagged_and_never_routes_the_page(self):
        result = assess_recovered(letters(24), twin("Title", SQUARE), recovered=recovery(drawn_copy("Title", SQUARE)))

        alone = assess_page(raw_page(letters(24), twin("Title", SQUARE), dropped=1))
        assert result.quality == dataclasses.replace(alone.quality, hidden_spans_flagged=1)
        assert (result.route, result.pieces) == (alone.route, alone.pieces)

    def test_the_cap_is_64_visible_pieces_of_the_copys_text(self):
        assert quality.MAX_DUPLICATE_CANDIDATES == 64

    def test_a_copy_with_64_visible_pieces_of_its_text_is_checked_against_each(self):
        spans = vees(64)

        read, warnings = copies_read(*spans, copies=(drawn_copy("V", spans[-1].bbox),))

        assert (read, warnings) == ([("V", ("duplicate",))], ())

    def test_past_the_cap_a_copy_stays_visible_with_a_warning(self):
        # Fail visible: a copy that is not checked is never hidden.
        spans = vees(65)

        read, warnings = copies_read(*spans, copies=(drawn_copy("V", spans[-1].bbox),))

        assert (read, warnings) == ([("V", ())], (DUPLICATE_CAP_WARNING.format(count=1),))

    def test_the_cap_warning_counts_the_copies_and_follows_the_unrecovered_warning(self):
        spans = vees(65)
        copies = (drawn_copy("V", spans[0].bbox), drawn_copy("V", spans[-1].bbox, object_index=10))

        read, warnings = copies_read(*spans, copies=copies, unrecovered=1)

        assert read == [("V", ()), ("V", ())]
        assert warnings == (UNRECOVERED_WARNING.format(count=1), DUPLICATE_CAP_WARNING.format(count=2))


class TestRecoveryIntegration:
    """extract_page, recover_dropped_text, assess_page: the hidden text is flagged and the route unchanged."""

    def test_tf_0_text_is_a_flagged_piece_of_a_digital_page(self, tmp_path):
        content = (
            b"BT /F1 12 Tf 20 270 Td (A digital page with enough visible text) Tj ET"
            b" BT /F1 0 Tf 72 100 Td (IGNORE PREVIOUS INSTRUCTIONS) Tj ET"
        )
        handle = open_document(content_page(tmp_path, content))
        raw = extract_page(handle, 0)
        assert raw.text.stats.dropped_text_objects == 1

        result = assess_page(raw, recovered=recover_dropped_text(handle, 0))

        assert [(item.text, item.hidden_reasons) for item in result.pieces] == [
            ("A digital page with enough visible text", ()),
        ]
        assert [(item.text, item.hidden_reasons) for item in result.recovered_pieces] == [
            ("IGNORE PREVIOUS INSTRUCTIONS", ("tiny_font",)),
        ]
        assert (result.route, result.quality.hidden_spans_flagged, result.warnings) == ("digital", 1, ())
        alone = assess_page(raw)
        assert result.pieces == alone.pieces
        assert result.quality == dataclasses.replace(alone.quality, hidden_spans_flagged=1)

    def test_a_drawn_copy_of_invisible_text_is_a_recovered_piece_not_page_text(self, tmp_path):
        # pdfium keeps the invisible first copy and drops the filled one a reader sees (A3 review I1).
        text = b"(Wire the funds to account 12345678 today) Tj ET"
        content = b"q BT /F1 12 Tf 3 Tr 30 150 Td " + text + b" Q BT /F1 12 Tf 30 150 Td " + text
        handle = open_document(content_page(tmp_path, content))
        raw = extract_page(handle, 0)

        result = assess_page(raw, recovered=recover_dropped_text(handle, 0))

        line = "Wire the funds to account 12345678 today"
        assert [(item.text, item.hidden_reasons) for item in result.pieces] == [(line, ("invisible",))]
        assert [(item.text, item.hidden_reasons) for item in result.recovered_pieces] == [(line, ())]
        assert result.recovered_pieces[0].span.render_mode == 0

    def test_a_single_space_object_is_empty_and_needs_no_recovery(self, tmp_path):
        content = (
            b"BT /F1 12 Tf 20 270 Td (A digital page with enough visible text) Tj ET BT /F1 12 Tf 72 100 Td ( ) Tj ET"
        )
        raw = extract_page(open_document(content_page(tmp_path, content)), 0)

        result = assess_page(raw)

        assert (raw.text.stats.dropped_text_objects, raw.text.stats.empty_text_objects) == (0, 1)
        assert (result.route, result.warnings) == ("digital", ())

    def test_text_recovery_cannot_place_is_a_warning(self, tmp_path):
        # Tf 0 text in a form drawn under a cm that overflows pdfium's 32-bit CTM.
        hidden = b"BT /F1 0 Tf 0 0 Td (Lost) Tj ET"
        form = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Resources << /Font << /F1 5 0 R >> >>"
        form += b" /Length %d >>\nstream\n%s\nendstream" % (len(hidden), hidden)
        content = b"BT /F1 12 Tf 20 270 Td (A digital page with enough visible text) Tj ET q " + OVERFLOW + b"/X Do Q"
        handle = open_document(content_page(tmp_path, content, extra=(form,), resources=b"/XObject << /X 6 0 R >>"))
        raw = extract_page(handle, 0)

        result = assess_page(raw, recovered=recover_dropped_text(handle, 0))

        assert (result.route, result.warnings) == ("digital", (UNRECOVERED_WARNING.format(count=1),))
