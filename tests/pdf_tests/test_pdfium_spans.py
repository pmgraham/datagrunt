"""Tests for the v5 pypdfium2 engine's text spans (#365 T2).

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``), whose
coordinates are top-left points in the unrotated page space, or written by
hand (``hand_written_pdf``) for what ReportLab cannot write.
"""

import dataclasses
import pickle
import re
from collections import Counter
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest
from reportlab.pdfbase.pdfmetrics import stringWidth

from datagrunt.pdf import PDFCorruptError
from datagrunt.pdf.pdfium_engine import PageText, RawSpan, TextStats, open_document, page_text
from pdf_tests.builders import FormXObject, PageSpec, TextRun, break_page, build_pdf, hand_written_pdf

# Position tolerance in page points.
TOL = 1.0

BOX_A = (10.0, 20.0, 16.0, 32.0)
BOX_B = (16.0, 21.0, 22.0, 33.0)

ROTATIONS = [0, 90, 180, 270]
# Top-left (50, 20)-(350, 260) on the 400 x 300 page: PDF CropBox (50, 40)-(350, 280).
CROPS = [pytest.param(None, id="no-crop"), pytest.param((50.0, 20.0, 350.0, 260.0), id="offset-crop")]

HELVETICA = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"


def one_page(tmp_path: Path, *items, rotate: int = 0, crop_box=None) -> Path:
    """A one-page 400 x 300 pt PDF holding ``items``."""
    spec = PageSpec(width=400.0, height=300.0, items=items, rotate=rotate, crop_box=crop_box)
    return build_pdf(tmp_path / "page.pdf", [spec])


def content_page(
    tmp_path: Path,
    content: bytes,
    *,
    font: bytes = HELVETICA,
    extra: tuple[bytes, ...] = (),
    resources: bytes = b"",
) -> Path:
    """A hand-written 400 x 300 pt page drawing ``content`` with font /F1 (object 5).

    ``extra`` objects are numbered from 6, for a font's descriptor or CMap;
    ``resources`` adds page resource entries beside /Font.
    """
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R"
        b" /Resources << /Font << /F1 5 0 R >> %s >> >>" % resources,
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        font,
        *extra,
    ]
    return hand_written_pdf(tmp_path / "hand.pdf", objects)


def spans_of(path: Path) -> tuple[RawSpan, ...]:
    """The spans of page 1 of ``path``."""
    return page_text(open_document(path), 0).spans


def display_point(u: float, v: float, rotation: int, width: float, height: float) -> tuple[float, float]:
    """Turn top-left point (u, v) of an unrotated ``width`` x ``height`` page clockwise."""
    return {0: (u, v), 90: (height - v, u), 180: (width - u, height - v), 270: (v, width - u)}[rotation]


def span(**overrides) -> RawSpan:
    """A valid two-char RawSpan with ``overrides`` applied."""
    fields = {
        "text": "Ab",
        "bbox": (10.0, 20.0, 22.0, 33.0),
        "char_boxes": (BOX_A, BOX_B),
        "font_size_pt": 12.0,
        "base_font_name": "Helvetica-Bold",
        "is_bold": True,
        "is_italic": False,
        "render_mode": 0,
        "fill_rgba": (0, 0, 0, 255),
        "stroke_rgba": (255, 0, 0, 128),
        "object_index": 3,
    }
    return RawSpan(**(fields | overrides))


def stats(**overrides) -> TextStats:
    """A valid TextStats with ``overrides`` applied."""
    return TextStats(**({"char_count": 2, "generated_count": 1, "unicode_map_error_count": 0} | overrides))


class TestRawSpan:
    """RawSpan is a validated, immutable, picklable value."""

    def test_pickle_round_trip(self):
        original = span()

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = span()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.text = "x"
        assert not hasattr(value, "__dict__")

    def test_int_coordinates_and_size_are_stored_as_floats(self):
        value = span(text="A", bbox=(10, 20, 16, 32), char_boxes=((10, 20, 16, 32),), font_size_pt=12, object_index=0)

        assert [type(part) for part in value.bbox] == [float] * 4
        assert [type(part) for part in value.char_boxes[0]] == [float] * 4
        assert type(value.font_size_pt) is float

    def test_negative_font_size_and_empty_font_name_are_preserved(self):
        # Tf -12 draws mirrored glyphs; a Type3 font has no /BaseFont.
        value = span(font_size_pt=-12.0, base_font_name="")

        assert (value.font_size_pt, value.base_font_name) == (-12.0, "")

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"text": b"Ab"}, TypeError, "text must be a str, got bytes", id="text-bytes"),
            pytest.param({"text": ""}, ValueError, "text must not be empty", id="text-empty"),
            pytest.param({"char_boxes": [BOX_A, BOX_B]}, TypeError, "char_boxes must be a tuple, got list", id="list"),
            pytest.param(
                {"char_boxes": (BOX_A,)},
                ValueError,
                "char_boxes must hold one box per char of text: 1 boxes for 2 chars",
                id="box-count",
            ),
            pytest.param(
                {"char_boxes": (BOX_A, (22.0, 21.0, 16.0, 33.0))},
                ValueError,
                "char_boxes[1] must have x0 <= x1 and y0 <= y1",
                id="box-reversed",
            ),
            pytest.param(
                {"char_boxes": (BOX_A, (16.0, 21.0, float("nan"), 33.0))},
                ValueError,
                "char_boxes[1][2] must be finite",
                id="box-nan",
            ),
            pytest.param(
                {"bbox": (10.0, 20.0, 22.0, 34.0)},
                ValueError,
                "bbox must be the union of char_boxes, (10.0, 20.0, 22.0, 33.0), got (10.0, 20.0, 22.0, 34.0)",
                id="bbox-not-union",
            ),
            pytest.param({"bbox": (10.0, 20.0, 22.0)}, ValueError, "bbox must have 4 values, got 3", id="bbox-len"),
            pytest.param({"font_size_pt": float("inf")}, ValueError, "font_size_pt must be finite", id="size-inf"),
            pytest.param({"base_font_name": None}, TypeError, "base_font_name must be a str, got None", id="font"),
            pytest.param({"is_bold": 1}, TypeError, "is_bold must be a bool, got 1 (int)", id="bold-int"),
            pytest.param({"is_italic": None}, TypeError, "is_italic must be a bool, got None", id="italic"),
            pytest.param({"render_mode": 8}, ValueError, "render_mode must be in 0..7, got 8", id="mode-8"),
            pytest.param({"render_mode": -1}, ValueError, "render_mode must be in 0..7, got -1", id="mode-neg"),
            pytest.param({"render_mode": True}, TypeError, "render_mode must be an int", id="mode-bool"),
            pytest.param(
                {"fill_rgba": (0, 0, 0)}, ValueError, "fill_rgba must have 4 channels, got 3", id="fill-channels"
            ),
            pytest.param(
                {"fill_rgba": (0, 0, 256, 255)}, ValueError, "fill_rgba[2] must be in 0..255, got 256", id="fill-256"
            ),
            pytest.param(
                {"stroke_rgba": (0, 0, 0, 1.0)}, TypeError, "stroke_rgba[3] must be an int, got 1.0", id="stroke"
            ),
            pytest.param({"stroke_rgba": [0, 0, 0, 0]}, TypeError, "stroke_rgba must be a tuple", id="stroke-list"),
            pytest.param({"object_index": -1}, ValueError, "object_index must be >= 0, got -1", id="index-neg"),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            span(**overrides)


class TestTextStats:
    """TextStats counts a page's textpage chars; the counts must be consistent."""

    def test_pickle_round_trip_and_frozen(self):
        original = stats(unicode_map_error_count=1)

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.char_count = 0

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"char_count": -1}, ValueError, "char_count must be >= 0, got -1", id="chars"),
            pytest.param({"generated_count": 1.0}, TypeError, "generated_count must be an int", id="generated"),
            pytest.param(
                {"unicode_map_error_count": 3},
                ValueError,
                "unicode_map_error_count (3) cannot exceed char_count (2)",
                id="errors-over-chars",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            stats(**overrides)


class TestPageText:
    """PageText pairs a page's spans with its counts; they must agree."""

    def test_pickle_round_trip(self):
        original = PageText(spans=(span(),), stats=stats())

        assert pickle.loads(pickle.dumps(original)) == original

    @pytest.mark.parametrize("char_count", [2, 3, 4], ids=["one-each", "one-pair", "all-pairs"])
    def test_span_chars_take_one_or_two_textpage_chars(self, char_count):
        # A non-BMP char is two textpage chars (a UTF-16 surrogate pair).
        value = PageText(spans=(span(),), stats=stats(char_count=char_count))

        assert value.stats.char_count == char_count

    @pytest.mark.parametrize(
        "spans,page_stats,error,message",
        [
            pytest.param([span()], stats(), TypeError, "spans must be a tuple, got list", id="list"),
            pytest.param(("span",), stats(), TypeError, "spans[0] must be a RawSpan, got str", id="item"),
            pytest.param((span(),), "stats", TypeError, "stats must be a TextStats, got str", id="stats"),
            pytest.param(
                (span(),),
                stats(char_count=1),
                ValueError,
                "spans hold 2 chars, which 1 textpage chars cannot make",
                id="too-few",
            ),
            pytest.param(
                (span(),),
                stats(char_count=5),
                ValueError,
                "spans hold 2 chars, which 5 textpage chars cannot make",
                id="too-many",
            ),
        ],
    )
    def test_invalid_field_raises(self, spans, page_stats, error, message):
        with pytest.raises(error, match=re.escape(message)):
            PageText(spans=spans, stats=page_stats)


class TestSpanGrouping:
    """A span is a maximal run of consecutive textpage chars owned by one text object."""

    def test_one_run_is_one_span(self, tmp_path):
        (got,) = spans_of(one_page(tmp_path, TextRun("Hello World", x=72.0, y=100.0)))

        assert (got.text, len(got.char_boxes), got.object_index) == ("Hello World", 11, 0)

    def test_two_runs_on_one_line_are_two_spans(self, tmp_path):
        runs = (TextRun("Left", x=72.0, y=100.0), TextRun("Right", x=200.0, y=100.0))

        got = spans_of(one_page(tmp_path, *runs))

        assert [(item.text, item.object_index) for item in got] == [("Left", 0), ("Right", 1)]

    def test_touching_runs_are_two_spans(self, tmp_path):
        # The second run starts where the first ends: pdfium generates no
        # space between them, so only the owning object tells them apart.
        second_x = 72.0 + stringWidth("AB", "Helvetica", 11.0)
        path = one_page(tmp_path, TextRun("AB", x=72.0, y=100.0), TextRun("CD", x=second_x, y=100.0))

        got = spans_of(path)

        assert [item.text for item in got] == ["AB", "CD"]
        assert page_text(open_document(path), 0).stats.generated_count == 0

    def test_one_text_block_across_two_baselines_is_two_spans(self, tmp_path):
        # pdfium makes one text object per text-showing operator, so one
        # BT/ET block drawing two lines owns two objects.
        content = b"BT /F1 12 Tf 72 250 Td (Line one) Tj 0 -14 Td (Line two) Tj ET"

        got = spans_of(content_page(tmp_path, content))

        assert [(item.text, item.object_index) for item in got] == [("Line one", 0), ("Line two", 1)]
        assert got[0].bbox[1] < got[1].bbox[1]

    def test_kerned_gap_in_one_tj_stays_one_span_without_the_generated_space(self, tmp_path):
        # pdfium generates a space inside the wide gap; the content stream draws none.
        path = content_page(tmp_path, b"BT /F1 12 Tf 72 250 Td [(A) -2000 (B)] TJ ET")

        result = page_text(open_document(path), 0)

        assert [item.text for item in result.spans] == ["AB"]
        assert result.stats == TextStats(char_count=2, generated_count=1, unicode_map_error_count=0)
        assert result.spans[0].char_boxes[1][0] - result.spans[0].char_boxes[0][2] > 20.0

    def test_generated_line_breaks_are_counted_not_kept(self, tmp_path):
        runs = (TextRun("Up", x=72.0, y=100.0), TextRun("Down", x=72.0, y=200.0))

        result = page_text(open_document(one_page(tmp_path, *runs)), 0)

        assert [item.text for item in result.spans] == ["Up", "Down"]
        assert result.stats == TextStats(char_count=6, generated_count=2, unicode_map_error_count=0)

    def test_a_run_drawn_twice_in_place_reads_once(self, tmp_path):
        # Observed pdfium behaviour (fake-bold removal): a char drawn on an
        # identical char at the same place is not in the textpage.
        runs = (TextRun("Twice", x=72.0, y=100.0), TextRun("Twice", x=72.0, y=100.0))

        result = page_text(open_document(one_page(tmp_path, *runs)), 0)

        assert [(item.text, item.object_index) for item in result.spans] == [("Twice", 0)]
        assert result.stats.char_count == 5


class TestSpanPositions:
    """Char boxes are pdfium's loose boxes, in displayed top-left points."""

    @pytest.mark.parametrize("crop_box", CROPS)
    @pytest.mark.parametrize("rotate", ROTATIONS)
    def test_char_boxes_span_each_advance_across_the_baseline(self, tmp_path, rotate, crop_box):
        run = TextRun("Hix", x=120.0, y=150.0, size=20.0)
        (got,) = spans_of(one_page(tmp_path, run, rotate=rotate, crop_box=crop_box))

        crop_x0, crop_y0, crop_x1, crop_y1 = crop_box or (0.0, 0.0, 400.0, 300.0)
        along = 0 if rotate in (0, 180) else 1  # the displayed axis the text runs along
        cross = 1 - along
        for index, box in enumerate(got.char_boxes):
            start = run.x + stringWidth(run.text[:index], run.font, run.size)
            end = run.x + stringWidth(run.text[: index + 1], run.font, run.size)
            first, last = (
                display_point(u - crop_x0, run.y - crop_y0, rotate, crop_x1 - crop_x0, crop_y1 - crop_y0)
                for u in (start, end)
            )
            assert (box[along], box[along + 2]) == pytest.approx(sorted((first[along], last[along])), abs=TOL)
            assert box[cross] < first[cross] < box[cross + 2]
        assert got.bbox == (
            min(box[0] for box in got.char_boxes),
            min(box[1] for box in got.char_boxes),
            max(box[2] for box in got.char_boxes),
            max(box[3] for box in got.char_boxes),
        )

    def test_off_page_text_keeps_negative_coordinates(self, tmp_path):
        # Preserve, not clip: A3 flags off-page text from these coordinates.
        (got,) = spans_of(one_page(tmp_path, TextRun("Off", x=-60.0, y=-20.0)))

        assert got.bbox[0] == pytest.approx(-60.0, abs=TOL)
        assert got.bbox[3] < 0.0


class TestSpanStyle:
    """Each span carries its text object's size, render mode and colours."""

    def test_font_size_is_the_run_size(self, tmp_path):
        (got,) = spans_of(one_page(tmp_path, TextRun("Size", x=72.0, y=100.0, size=17.0)))

        assert got.font_size_pt == pytest.approx(17.0)

    @pytest.mark.parametrize(
        "content,size",
        [
            pytest.param(b"q 2 0 0 2 0 0 cm BT /F1 6 Tf 36 50 Td (Big) Tj ET Q", 12.0, id="cm-2x"),
            pytest.param(b"BT /F1 10 Tf 3 0 0 2 72 100 Tm (Wide) Tj ET", 20.0, id="anisotropic-tm"),
            pytest.param(b"BT /F1 10 Tf 0 2 -2 0 200 100 Tm (Turned) Tj ET", 20.0, id="rotated-tm"),
            pytest.param(b"BT /F1 -12 Tf 200 150 Td (Neg) Tj ET", -12.0, id="negative-tf"),
        ],
    )
    def test_font_size_is_tf_times_the_vertical_scale(self, tmp_path, content, size):
        (got,) = spans_of(content_page(tmp_path, content))

        assert got.font_size_pt == pytest.approx(size)

    def test_zero_font_size_text_is_not_in_the_textpage(self, tmp_path):
        # Observed pdfium behaviour: Tf 0 text yields no textpage chars at all.
        content = b"BT /F1 0 Tf 72 100 Td (Zero) Tj ET BT /F1 12 Tf 72 150 Td (Seen) Tj ET"

        result = page_text(open_document(content_page(tmp_path, content)), 0)

        assert [item.text for item in result.spans] == ["Seen"]
        assert result.stats.char_count == 4

    @pytest.mark.parametrize("mode", range(8))
    def test_render_mode(self, tmp_path, mode):
        (got,) = spans_of(one_page(tmp_path, TextRun("Mode", x=72.0, y=100.0, render_mode=mode)))

        assert got.render_mode == mode

    def test_fill_and_stroke_colour_with_alpha(self, tmp_path):
        run = TextRun("Ink", x=72.0, y=100.0, rgb=(1.0, 0.5, 0.0), alpha=0.5, render_mode=2)

        (got,) = spans_of(one_page(tmp_path, run))

        assert (got.fill_rgba, got.stroke_rgba) == ((255, 128, 0, 128), (255, 128, 0, 128))

    def test_fill_and_stroke_are_read_separately_in_any_colour_space(self, tmp_path):
        content = b"BT 0.5 g 1 0 0 0 K 2 Tr /F1 12 Tf 72 100 Td (Two) Tj ET"

        (got,) = spans_of(content_page(tmp_path, content))

        assert got.fill_rgba == (128, 128, 128, 255)
        assert got.stroke_rgba[3] == 255 and got.stroke_rgba[0] < 64 < got.stroke_rgba[1]

    def test_pattern_fill_reads_as_the_colour_pdfium_picks(self, tmp_path):
        # Observed pdfium behaviour: no API tells a pattern fill apart; a
        # tiling pattern of blue squares reads as opaque 75% grey.
        tile = b"0 0 1 rg 0 0 5 5 re f"
        pattern = (
            b"<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 10 10] /XStep 10 /YStep 10"
            b" /Resources << >> /Length %d >>\nstream\n%s\nendstream" % (len(tile), tile)
        )
        content = b"BT /Pattern cs /P0 scn /F1 20 Tf 72 150 Td (Pat) Tj ET"
        path = content_page(tmp_path, content, extra=(pattern,), resources=b"/Pattern << /P0 6 0 R >>")

        (got,) = spans_of(path)

        assert got.fill_rgba == (191, 191, 191, 255)


def font_page(tmp_path: Path, base_font: bytes, descriptor: bytes | None = None) -> Path:
    """A page drawing "Abc" in a non-embedded Type1 font named ``base_font``.

    ``descriptor`` holds extra FontDescriptor entries (/Flags, /StemV,
    /FontWeight); None writes no descriptor.
    """
    widths = b" ".join([b"500"] * 95)
    font = b"<< /Type /Font /Subtype /Type1 /BaseFont /%s /FirstChar 32 /LastChar 126 /Widths [%s]" % (
        base_font,
        widths,
    )
    extra: tuple[bytes, ...] = ()
    if descriptor is not None:
        font += b" /FontDescriptor 6 0 R"
        metrics = b"/FontBBox [0 -200 1000 900] /ItalicAngle 0 /Ascent 900 /Descent -200 /CapHeight 700"
        extra = (b"<< /Type /FontDescriptor /FontName /%s %s %s >>" % (base_font, descriptor, metrics),)
    return content_page(tmp_path, b"BT /F1 12 Tf 72 100 Td (Abc) Tj ET", font=font + b" >>", extra=extra)


class TestBoldItalic:
    """Bold and italic come from the font name, the font flags and the weight."""

    @pytest.mark.parametrize(
        "font,text,bold,italic",
        [
            ("Helvetica", "Abc", False, False),
            ("Helvetica-Bold", "Abc", True, False),
            ("Helvetica-Oblique", "Abc", False, True),
            ("Helvetica-BoldOblique", "Abc", True, True),
            ("Times-Roman", "Abc", False, False),
            ("Times-Bold", "Abc", True, False),
            ("Times-Italic", "Abc", False, True),
            ("Times-BoldItalic", "Abc", True, True),
            ("Courier", "Abc", False, False),
            ("Courier-Bold", "Abc", True, False),
            ("Courier-Oblique", "Abc", False, True),
            ("Courier-BoldOblique", "Abc", True, True),
            ("Symbol", "αβ", False, False),
            ("ZapfDingbats", "✓❤", False, False),
        ],
    )
    def test_standard_14_style_comes_from_the_name(self, tmp_path, font, text, bold, italic):
        (got,) = spans_of(one_page(tmp_path, TextRun(text, x=72.0, y=100.0, font=font)))

        assert (got.base_font_name, got.is_bold, got.is_italic) == (font, bold, italic)

    @pytest.mark.parametrize("font", ["Helvetica-Bold", "Times-BoldItalic"])
    def test_v4_family_and_weight_miss_standard_14_bold(self, tmp_path, font):
        path = one_page(tmp_path, TextRun("Bold", x=72.0, y=100.0, font=font))
        (text_object,) = pdfium.PdfDocument(path)[0].get_objects()
        v4_font = text_object.get_font()

        # v4 (core/pdf_io/extraction/pdfium_document.py) read the family name
        # (Arial, Times New Roman) and weight (0 -> 400): never bold here.
        assert "bold" not in v4_font.get_family_name().lower()
        assert (v4_font.get_weight() or 400) < 600
        (got,) = spans_of(path)
        assert got.is_bold

    @pytest.mark.parametrize(
        "base_font,descriptor,name,bold,italic",
        [
            pytest.param(b"Plain", b"/Flags 32 /StemV 80", "Plain", False, False, id="plain"),
            pytest.param(b"Plain", b"/Flags 262176 /StemV 80", "Plain", True, False, id="force-bold-flag"),
            pytest.param(b"Plain", b"/Flags 96 /StemV 80", "Plain", False, True, id="italic-flag"),
            pytest.param(b"Plain", b"/Flags 32 /FontWeight 600", "Plain", True, False, id="weight-600"),
            pytest.param(b"Plain", b"/Flags 32 /FontWeight 500", "Plain", False, False, id="weight-500"),
            pytest.param(b"Plain", b"/Flags 32 /StemV 140", "Plain", True, False, id="weight-from-stem-v"),
            pytest.param(b"ABCDEF+Arial-BoldMT", None, "Arial-BoldMT", True, False, id="subset-tag-stripped"),
            pytest.param(b"Abcdef+Plain", None, "Abcdef+Plain", False, False, id="lowercase-tag-kept"),
            pytest.param(b"ABCDE+Plain", None, "ABCDE+Plain", False, False, id="five-letter-tag-kept"),
            pytest.param(b"MinionPro-Semibold", None, "MinionPro-Semibold", True, False, id="semibold"),
            pytest.param(b"Font-DemiBold", None, "Font-DemiBold", True, False, id="demibold"),
            pytest.param(b"Roboto-Black", None, "Roboto-Black", True, False, id="black"),
            pytest.param(b"Roboto-Heavy", None, "Roboto-Heavy", True, False, id="heavy"),
            pytest.param(b"Font-Oblique", None, "Font-Oblique", False, True, id="oblique"),
            pytest.param(b"Font,BoldItalic", None, "Font,BoldItalic", True, True, id="comma-style"),
            pytest.param(b"ArialBlack", None, "ArialBlack", True, False, id="no-separator-whole-name"),
            pytest.param(b"BlackadderITC-Regular", None, "BlackadderITC-Regular", False, False, id="family-ignored"),
            # pdfium reports the standard-14 font it substitutes for an alias.
            pytest.param(b"Arial,BoldItalic", None, "Helvetica-BoldOblique", True, True, id="pdfium-alias"),
            pytest.param(b"Caf#c3#a9-Bold", None, "Café-Bold", True, False, id="utf-8-name"),
            pytest.param(b"Bad#ff#feName", None, "Bad\\xff\\xfeName", False, False, id="invalid-utf-8-escaped"),
        ],
    )
    def test_name_flags_and_weight_each_set_the_style(self, tmp_path, base_font, descriptor, name, bold, italic):
        (got,) = spans_of(font_page(tmp_path, base_font, descriptor))

        assert (got.base_font_name, got.is_bold, got.is_italic) == (name, bold, italic)


# ToUnicode for the CID font below: 0001 is U+1D400 and 0003 U+1F600 (each a
# surrogate pair), 0002 is "A", 0005 a lone high and 0006 a lone low
# surrogate; 0004 is unmapped.
TO_UNICODE = b"""/CIDInit /ProcSet findresource begin
12 dict begin
begincmap
/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def
/CMapName /Adobe-Identity-UCS def
/CMapType 2 def
1 begincodespacerange
<0000> <FFFF>
endcodespacerange
5 beginbfchar
<0001> <D835DC00>
<0002> <0041>
<0003> <D83DDE00>
<0005> <D835>
<0006> <DC00>
endbfchar
endcmap
CMapName currentdict /CMap defineresource pop
end
end"""


def cid_page(tmp_path: Path, codes: bytes) -> Path:
    """A page drawing 2-byte ``codes`` (hex) at 20 pt, 12 pt apart, in a non-embedded CID font."""
    font = b"<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H /DescendantFonts [6 0 R]"
    descendant = (
        b"<< /Type /Font /Subtype /CIDFontType2 /BaseFont /TestCID /FontDescriptor 8 0 R /DW 600"
        b" /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> >>"
    )
    descriptor = (
        b"<< /Type /FontDescriptor /FontName /TestCID /Flags 32 /FontBBox [0 -200 1000 900]"
        b" /ItalicAngle 0 /Ascent 900 /Descent -200 /CapHeight 700 /StemV 80 >>"
    )
    cmap = b"<< /Length %d >>\nstream\n%s\nendstream" % (len(TO_UNICODE), TO_UNICODE)
    content = b"BT /F1 20 Tf 72 150 Td <%s> Tj ET" % codes
    return content_page(tmp_path, content, font=font + b" /ToUnicode 7 0 R >>", extra=(descendant, cmap, descriptor))


class TestUnicode:
    """Every char pdfium reports is kept; a surrogate pair is one char."""

    def test_non_bmp_chars_round_trip_as_one_char_each(self, tmp_path):
        # pdfium reports U+1D400 as two textpage chars, U+D835 then U+DC00.
        result = page_text(open_document(cid_page(tmp_path, b"0002000100030002")), 0)

        (got,) = result.spans
        assert got.text == "A\U0001d400\U0001f600A"
        assert [box[0] for box in got.char_boxes] == pytest.approx([72.0, 84.0, 96.0, 108.0], abs=TOL)
        assert got.char_boxes[1][2] == pytest.approx(96.0, abs=TOL)
        assert result.stats == TextStats(char_count=6, generated_count=0, unicode_map_error_count=0)

    def test_lone_surrogates_and_unmapped_codes_are_kept(self, tmp_path):
        # Preserve, not transform: A3 classifies these chars.
        result = page_text(open_document(cid_page(tmp_path, b"00020005000400050002")), 0)

        assert [item.text for item in result.spans] == ["A\ud835\x04\ud835A"]
        assert result.stats == TextStats(char_count=5, generated_count=0, unicode_map_error_count=1)

    def test_a_lone_low_surrogate_is_kept(self, tmp_path):
        result = page_text(open_document(cid_page(tmp_path, b"000200060002")), 0)

        assert [item.text for item in result.spans] == ["A\udc00A"]
        assert result.stats.char_count == 3


def nested_forms_page(tmp_path: Path, depth: int) -> Path:
    """A page whose content draws form 1, which draws its text and form 2, and so on ``depth`` deep.

    Form k's text "Lk" sits at its own place (4 columns of 20 rows), so no
    two texts overlap (pdfium drops a char drawn on an identical one).
    """
    page = b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R /Resources %s >>"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        page % b"<< /XObject << /X 6 0 R >> >>",
        b"<< /Length 4 >>\nstream\n/X Do\nendstream",
        HELVETICA,
    ]
    for level in range(depth):
        x, y = 10 + 90 * (level % 4), 10 + 14 * (level // 4)
        body = b"BT /F1 8 Tf %d %d Td (L%d) Tj ET" % (x, y, level)
        resources = b"/Font << /F1 5 0 R >>"
        if level < depth - 1:
            body += b" /X Do"
            resources += b" /XObject << /X %d 0 R >>" % (len(objects) + 2)
        form = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Resources << %s >> /Length %d >>"
        objects.append(form % (resources, len(body)) + b"\nstream\n%s\nendstream" % body)
    return hand_written_pdf(tmp_path / "nested.pdf", objects)


class TestNestedText:
    """Text inside Form XObjects gets page-space boxes and a paint-order index."""

    def test_text_in_a_scaled_form_gets_page_space_boxes_and_size(self, tmp_path):
        inner = TextRun("Hix", x=10.0, y=40.0, size=10.0)
        form = FormXObject(x=100.0, y=50.0, w=150.0, h=60.0, items=(inner,), scale=2.0)
        path = one_page(tmp_path, form, TextRun("Top", x=72.0, y=250.0))

        nested, top = spans_of(path)

        # Index 0 is the form object itself; its text follows it.
        assert [(nested.text, nested.object_index), (top.text, top.object_index)] == [("Hix", 1), ("Top", 2)]
        assert nested.font_size_pt == pytest.approx(20.0)
        # Form (10, 40) at scale 2 from the form's top-left (100, 50): baseline origin (120, 130).
        for index, box in enumerate(nested.char_boxes):
            start = 120.0 + 2.0 * stringWidth(inner.text[:index], inner.font, inner.size)
            end = 120.0 + 2.0 * stringWidth(inner.text[: index + 1], inner.font, inner.size)
            assert (box[0], box[2]) == pytest.approx((start, end), abs=TOL)
            assert box[1] < 130.0 < box[3]

    def test_nested_object_bounds_are_form_space_so_spans_use_textpage_boxes(self, tmp_path):
        # Observed pdfium behaviour: get_bounds() of an object inside a
        # transformed form is in the form's space, unlike textpage char boxes.
        form = FormXObject(x=100.0, y=50.0, w=150.0, h=60.0, items=(TextRun("T", x=10.0, y=40.0),), scale=2.0)
        page = pdfium.PdfDocument(one_page(tmp_path, form))[0]
        (_form, text_object) = page.get_objects()

        left, _bottom, _right, _top = text_object.get_bounds()
        assert left == pytest.approx(10.0, abs=TOL)
        assert page.get_textpage().get_charbox(0)[0] == pytest.approx(120.0, abs=TOL)

    def test_every_form_level_pdfium_parses_is_indexed(self, tmp_path):
        # Observed: pdfium parses forms 40 levels deep and ignores deeper ones.
        got = spans_of(nested_forms_page(tmp_path, 80))

        assert [item.text for item in got] == [f"L{level}" for level in range(40)]
        assert [item.object_index for item in got] == [2 * level + 1 for level in range(40)]


def textpage_chars(path: Path) -> tuple[str, int, int]:
    """Page 1's non-generated textpage chars, read straight from pdfium.

    Returns the chars (surrogate pairs joined by Python's UTF-16 codec, an
    oracle independent of the engine's pairing), their textpage count, and
    the generated count.
    """
    textpage = pdfium.PdfDocument(path)[0].get_textpage()
    units, generated = [], 0
    for index in range(textpage.count_chars()):
        if pdfium_c.FPDFText_IsGenerated(textpage, index):
            generated += 1
            continue
        units.append(chr(pdfium_c.FPDFText_GetUnicode(textpage, index)))
    chars = "".join(units).encode("utf-16-le", "surrogatepass").decode("utf-16-le", "surrogatepass")
    return chars, len(units), generated


def busy_page(tmp_path: Path) -> Path:
    """A rotated page mixing fonts, sizes, render modes, colours, a form, touching and off-page runs."""
    runs = [TextRun(f"Mode{mode}", x=20.0 + 45.0 * mode, y=40.0, render_mode=mode) for mode in range(8)]
    runs += [
        TextRun("Bold body", x=20.0, y=80.0, font="Times-Bold", size=14.0, rgb=(1.0, 0.0, 0.0), alpha=0.3),
        TextRun("ab", x=20.0, y=120.0),
        TextRun("cd", x=20.0 + stringWidth("ab", "Helvetica", 11.0), y=120.0),
        TextRun("αβγ", x=200.0, y=120.0, font="Symbol"),
        TextRun("Gone", x=-80.0, y=160.0),
        TextRun("Same", x=20.0, y=200.0),
        TextRun("Same", x=20.0, y=200.0),  # drawn twice in place: pdfium keeps one copy
    ]
    form = FormXObject(x=150.0, y=180.0, w=200.0, h=60.0, items=(TextRun("In form", x=5.0, y=30.0),), scale=1.5)
    return one_page(tmp_path, *runs, form, rotate=90)


CONSERVATION_PAGES = [
    pytest.param(busy_page, id="busy"),
    pytest.param(lambda tmp_path: cid_page(tmp_path, b"000200010003000500040002"), id="cid"),
    pytest.param(lambda tmp_path: nested_forms_page(tmp_path, 45), id="nested-forms"),
    pytest.param(lambda tmp_path: content_page(tmp_path, b"BT /F1 12 Tf 72 250 Td [(A) -2000 (B)] TJ ET"), id="kerned"),
]


class TestConservation:
    """Spans hold exactly the textpage's non-generated chars, in textpage order."""

    @pytest.mark.parametrize("make_page", CONSERVATION_PAGES)
    def test_span_chars_are_the_textpage_chars(self, tmp_path, make_page):
        path = make_page(tmp_path)
        chars, char_count, generated = textpage_chars(path)

        result = page_text(open_document(path), 0)

        span_chars = "".join(item.text for item in result.spans)
        assert Counter(c for c in span_chars if not c.isspace()) == Counter(c for c in chars if not c.isspace())
        assert span_chars == chars
        assert (result.stats.char_count, result.stats.generated_count) == (char_count, generated)

    def test_an_empty_page_has_no_spans(self, tmp_path):
        result = page_text(open_document(one_page(tmp_path)), 0)

        assert result == PageText(spans=(), stats=TextStats(char_count=0, generated_count=0, unicode_map_error_count=0))


@pytest.fixture
def handle_closes(monkeypatch):
    """Record each explicit PdfPage / PdfTextPage close() call (not a parent's cascade).

    Refcounting alone would also free a dropped handle, so counting open
    handles cannot show that the engine closes them; counting close() calls can.
    """
    calls = []
    for cls in (pdfium.PdfPage, pdfium.PdfTextPage):

        def counting_close(self, *args, _original=cls.close, **kwargs):
            if not kwargs.get("_by_parent", False):
                calls.append(type(self).__name__)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(cls, "close", counting_close)
    return calls


def failing(value):
    """A pdfium stand-in that reports failure by returning ``value``."""
    return lambda *args: value


def writes_inf_box(textpage, index, rect):
    rect.left = float("inf")
    return 1


def writes_inf_size(text_object, size):
    size.value = float("inf")
    return 1


def name_size_then(second):
    """FPDFFont_GetBaseFontName reporting 10 bytes, then ``second`` when asked to copy them."""
    calls = iter([10, second])
    return lambda *args: next(calls)


# (pdfium function, stand-in, message): each makes page_text raise PDFCorruptError.
PDFIUM_FAILURES = [
    ("FPDFText_CountChars", failing(-1), "page 1: pdfium cannot count the text page's chars"),
    ("FPDFText_IsGenerated", failing(-1), "page 1: pdfium cannot read whether char 0 is generated"),
    ("FPDFText_HasUnicodeMapError", failing(-1), "page 1: pdfium cannot read whether char 0 maps to Unicode"),
    ("FPDFText_GetTextObject", failing(None), "page 1: pdfium cannot read the text object of char 0"),
    ("FPDFText_GetUnicode", failing(0x110000), "page 1: pdfium cannot read char 0: code 0x110000 is beyond Unicode"),
    ("FPDFText_GetLooseCharBox", failing(0), "page 1: pdfium cannot read the box of char 0"),
    ("FPDFText_GetLooseCharBox", writes_inf_box, "page 1: pdfium cannot read the box of char 0: it is not finite"),
    ("FPDFText_GetMatrix", failing(0), "page 1: pdfium cannot read the matrix of char 0"),
    ("FPDFTextObj_GetFont", failing(None), "page 1: pdfium cannot read the font of object 0"),
    ("FPDFFont_GetBaseFontName", failing(0), "page 1: pdfium cannot read the font name of object 0"),
    (
        "FPDFFont_GetBaseFontName",
        failing(4097),
        "page 1: pdfium cannot read the font name of object 0: 4097 bytes is over the 4096-byte bound",
    ),
    ("FPDFFont_GetBaseFontName", name_size_then(0), "page 1: pdfium cannot read the font name of object 0"),
    ("FPDFTextObj_GetFontSize", failing(0), "page 1: pdfium cannot read the font size of object 0"),
    ("FPDFTextObj_GetFontSize", writes_inf_size, "page 1: pdfium cannot read the font size of object 0: it is not"),
    ("FPDFTextObj_GetTextRenderMode", failing(-1), "page 1: pdfium cannot read the render mode of object 0"),
    ("FPDFPageObj_GetFillColor", failing(0), "page 1: pdfium cannot read the fill colour of object 0"),
    ("FPDFPageObj_GetStrokeColor", failing(0), "page 1: pdfium cannot read the stroke colour of object 0"),
    ("FPDFPage_CountObjects", failing(-1), "page 1: pdfium cannot count the objects of the page"),
    ("FPDFPage_GetObject", failing(None), "page 1: pdfium cannot read object 0 of the page"),
    (
        "FPDFPage_CountObjects",
        failing(0),
        "page 1: pdfium cannot read the text object of char 0: it is not in the page's object tree",
    ),
]

FORM_FAILURES = [
    ("FPDFFormObj_CountObjects", failing(-1), "page 1: pdfium cannot count the objects of form object 0"),
    ("FPDFFormObj_GetObject", failing(None), "page 1: pdfium cannot read object 0 of form object 0"),
]


class TestPageTextErrors:
    """page_text raises PDFCorruptError for the page when pdfium fails, and closes its handles."""

    def test_closes_the_textpage_then_the_page(self, tmp_path, handle_closes):
        page_text(open_document(one_page(tmp_path, TextRun("Hi", x=72.0, y=100.0))), 0)

        assert handle_closes == ["PdfTextPage", "PdfPage"]

    @pytest.mark.parametrize("function,stand_in,message", PDFIUM_FAILURES)
    def test_pdfium_failure_raises_for_the_page_and_closes_handles(
        self, tmp_path, monkeypatch, handle_closes, function, stand_in, message
    ):
        handle = open_document(one_page(tmp_path, TextRun("Hi", x=72.0, y=100.0)))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        with pytest.raises(PDFCorruptError, match=re.escape(message)) as caught:
            page_text(handle, 0)

        assert caught.value.page_number == 1
        assert handle_closes == ["PdfTextPage", "PdfPage"]

    @pytest.mark.parametrize("function,stand_in,message", FORM_FAILURES)
    def test_form_walk_failure_raises_for_the_page(self, tmp_path, monkeypatch, function, stand_in, message):
        form = FormXObject(x=72.0, y=72.0, w=100.0, h=40.0, items=(TextRun("In", x=5.0, y=20.0),))
        handle = open_document(one_page(tmp_path, form))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        with pytest.raises(PDFCorruptError, match=re.escape(message)):
            page_text(handle, 0)

    def test_textpage_load_failure_raises_and_closes_the_page(self, tmp_path, monkeypatch, handle_closes):
        def failing_textpage(self):
            raise pdfium.PdfiumError("Failed to load text page.")

        monkeypatch.setattr(pdfium.PdfPage, "get_textpage", failing_textpage)
        handle = open_document(one_page(tmp_path, TextRun("Hi", x=72.0, y=100.0)))

        with pytest.raises(PDFCorruptError, match="page 1: pdfium cannot load the text page") as caught:
            page_text(handle, 0)

        assert caught.value.page_number == 1
        assert handle_closes == ["PdfPage"]

    def test_unloadable_page_raises_for_that_page_only(self, tmp_path):
        pages = [PageSpec(items=(TextRun(f"Page {n}", x=72.0, y=100.0),)) for n in (1, 2, 3)]
        handle = open_document(break_page(build_pdf(tmp_path / "three.pdf", pages), 2))

        with pytest.raises(PDFCorruptError, match="Failed to load page") as caught:
            page_text(handle, 1)

        assert caught.value.page_number == 2
        assert [page_text(handle, index).spans[0].text for index in (0, 2)] == ["Page 1", "Page 3"]

    @pytest.mark.parametrize(
        "index,error,message",
        [
            pytest.param(-1, IndexError, "index must be in 0..0, got -1", id="negative"),
            pytest.param(1, IndexError, "index must be in 0..0, got 1", id="past-end"),
            pytest.param(True, TypeError, "index must be an int, got True (bool)", id="bool"),
        ],
    )
    def test_invalid_index_raises(self, tmp_path, index, error, message):
        handle = open_document(one_page(tmp_path))

        with pytest.raises(error, match=re.escape(message)):
            page_text(handle, index)

    def test_non_handle_and_closed_handle_raise(self, tmp_path):
        handle = open_document(one_page(tmp_path))
        handle.close()

        with pytest.raises(TypeError, match=re.escape("handle must be a DocumentHandle, got str")):
            page_text("doc.pdf", 0)
        with pytest.raises(ValueError, match="document handle is closed"):
            page_text(handle, 0)
