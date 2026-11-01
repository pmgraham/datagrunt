"""Tests for the v5 vision route (#369, A5b): ``datagrunt.pdf.vision``.

Unit tests build OCR words (``OCRWord``) directly: boxes are displayed,
top-left ``(x0, y0, x1, y1)`` points, laid out so each expected element is
derived from the fixture (a line's words 0.3 em apart, cells over 1.8 em
apart, a heading twice the body's height). End-to-end tests build PDFs with
the ReportLab toolkit (``pdf_tests.builders``), render them through the
guarded worker and read them with tesseract; those skip when tesseract is
absent (CI installs ``tesseract-ocr``).
"""

import dataclasses
import io
import pickle
import time
from pathlib import Path

import pypdfium2 as pdfium
import pytest
from PIL import Image

from datagrunt.pdf import vision
from datagrunt.pdf.assembly import FigureDraft, assemble_page, conservation
from datagrunt.pdf.elements import BBox, Figure, Page, PageQuality, Paragraph, Table
from datagrunt.pdf.layout import find_lattice_tables
from datagrunt.pdf.ocr import OCRResult, OCRWord, Orientation, bands, normalized, rotated
from datagrunt.pdf.pdfium_engine import (
    ImageLoader,
    PageFailure,
    PageGeometry,
    extract_page,
    open_document,
    recover_dropped_text,
)
from datagrunt.pdf.quality import SpanPiece, assess_page
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_size
from datagrunt.pdf.vision import (
    PageRender,
    VisionElements,
    VisionOptions,
    fallback_page,
    ocr_pieces,
    superseded,
    vision_elements,
    vision_page,
)
from pdf_tests.builders import PageSpec, RasterImage, TextRun, break_page, build_pdf
from pdf_tests.test_ocr import fake_tesseract, needs_tesseract, posix_only, tsv
from pdf_tests.test_ocr import row as tsv_row
from pdf_tests.test_ocr_scans import LINES, SENTENCES, TOKENS, png_of, rendered, turned, word_boxes
from pdf_tests.test_pdfium_engine import imported_modules
from pdf_tests.test_pdfium_spans import content_page
from pdf_tests.test_quality import SHA, image, raw_span, row

LETTER = (612.0, 792.0)
GEOMETRY = PageGeometry(page_number=3, width_pt=LETTER[0], height_pt=LETTER[1], rotation_deg=0, crop_offset=(0.0, 0.0))
BODY = 10.0


def word(
    text: str, x0: float, y0: float, x1: float, y1: float, confidence: float = 90.0, line: int | None = None
) -> OCRWord:
    """An OCR word; on the line keyed by its top (to 0.01 pt) unless ``line`` is given."""
    number = round(y0 * 100) if line is None else line
    return OCRWord(text=text, bbox=BBox(x0=x0, y0=y0, x1=x1, y1=y1), confidence=confidence, line=number)


def line_words(text: str, x: float, y: float, size: float = BODY) -> list[OCRWord]:
    """``text``'s words on a line from ``(x, y)``: each char 0.5 em wide, words 0.3 em apart, boxes 1 em tall."""
    found = []
    for token in text.split():
        width = 0.5 * size * len(token)
        found.append(word(token, x, y, x + width, y + size))
        x += width + 0.3 * size
    return found


def inked(parts: tuple[tuple[str, str], ...], x: float, baseline: float, size: float, line: int) -> list[OCRWord]:
    """Words on one line boxed as OCR boxes them, from their ink, for a font of ``size``: each part's reach is "x"
    (x-height letters only: 0.7 x 2/3 em over the baseline), "above" (to the cap height or ascender: 0.7 em over
    it), or "both" (that, and 0.7 x 1/3 em under it). Each char 0.5 em wide, words 0.3 em apart."""
    found = []
    for text, reach in parts:
        top = baseline - 0.7 * size * (2 / 3 if reach == "x" else 1.0)
        bottom = baseline + (0.7 * size / 3 if reach == "both" else 0.0)
        width = 0.5 * size * len(text)
        found.append(word(text, x, top, x + width, bottom, line=line))
        x += width + 0.3 * size
    return found


def row_words(cells: tuple[str, ...], xs: tuple[float, ...], y: float) -> list[OCRWord]:
    """One table row: each cell's words from its column's x."""
    return [found for text, x in zip(cells, xs) for found in line_words(text, x, y)]


PARTS = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"), ("Washer", "100", "0.01"))
XS = (72.0, 220.0, 360.0)


def mixed_words() -> tuple[OCRWord, ...]:
    """A heading, a paragraph, a key-value, a list and a borderless table, top to bottom."""
    return (
        *line_words("Results", 72.0, 60.0, size=2 * BODY),
        *line_words("The woodland is vast and its creatures many,", 72.0, 100.0),
        *line_words("each with its own purpose.", 72.0, 112.0),
        *line_words("Invoice: 10442", 72.0, 150.0),
        *line_words("• first item", 72.0, 180.0),
        *line_words("• second item", 72.0, 194.0),
        *(found for index, cells in enumerate(PARTS) for found in row_words(cells, XS, 240.0 + 14.0 * index)),
    )


class TestModule:
    """The vision module is v5 only, runs tesseract only through ocr.py, and keeps no mutable state."""

    def test_imports_no_v4_and_no_subprocess_of_its_own(self):
        banned = ("fitz", "pymupdf", "pdfplumber", "pytesseract", "datagrunt.core.pdf_io", "datagrunt.pdf_api")
        names = imported_modules(vision)

        assert not [name for name in names if name.lower().startswith(banned)]
        assert "subprocess" not in names

    def test_module_level_tables_are_immutable(self):
        mutable = (dict, list, set, bytearray)
        names = {name: value for name, value in vars(vision).items() if not name.startswith("__")}

        assert {name for name, value in names.items() if isinstance(value, mutable)} == set()


class TestOcrPieces:
    """Each OCR word is one visible piece of a span of its own: its text, its box, its chars split evenly."""

    def test_a_word_is_one_visible_piece_of_its_whole_span(self):
        (found,) = ocr_pieces((word("Fox", 10.0, 20.0, 40.0, 32.0),))

        assert isinstance(found, SpanPiece)
        assert (found.text, found.start, found.end, found.hidden_reasons) == ("Fox", 0, 3, ())
        assert found.span.text == "Fox"
        assert found.span.bbox == (10.0, 20.0, 40.0, 32.0)

    def test_its_chars_split_the_box_evenly_and_loose_is_tight(self):
        (found,) = ocr_pieces((word("abcd", 10.0, 20.0, 30.0, 32.0),))

        expected = tuple((x, 20.0, x + 5.0, 32.0) for x in (10.0, 15.0, 20.0, 25.0))
        assert found.span.char_boxes == expected
        assert found.span.tight_boxes == expected

    def test_the_last_char_ends_at_the_words_edge_whatever_the_float_division(self):
        # 56.88 pt over 7 chars does not divide exactly: the chars still tile the box, its union exactly it.
        (found,) = ocr_pieces((word("Faction", 60.96, 87.6, 117.84, 97.92),))

        boxes = found.span.char_boxes
        assert boxes[0][0] == 60.96 and boxes[-1][2] == 117.84
        assert all(left[2] == right[0] for left, right in zip(boxes, boxes[1:]))
        assert [right - left for left, _, right, _ in boxes] == pytest.approx([56.88 / 7] * 7)
        # Here x0 + (x1 - x0) * 10 / 10 is not x1 in floats: the last edge must be x1 itself.
        assert 44.93 + (111.55 - 44.93) * 10 / 10 != 111.55
        (found,) = ocr_pieces((word("Conspiracy", 44.93, 87.6, 111.55, 97.92),))
        assert found.span.char_boxes[-1][2] == found.span.bbox[2] == 111.55

    def test_its_font_is_the_box_height_upright_plain_and_painted(self):
        (found,) = ocr_pieces((word("Fox", 10.0, 20.0, 40.0, 32.5),))
        span = found.span

        assert (span.font_size_pt, span.em_width_pt) == (12.5, 12.5)
        assert (span.is_bold, span.is_italic, span.render_mode, span.base_font_name) == (False, False, 0, "")
        assert (span.fill_rgba, span.stroke_rgba) == ((0, 0, 0, 255), (0, 0, 0, 255))
        assert (span.generated_breaks, span.map_errors) == ((), ())

    def test_a_lines_words_share_its_size_the_median_of_their_heights_scaled_by_what_they_reach(self):
        # Heights from the ink: "over" x-height only (x 3/2), "The" and "gun" one side of it (x 1), "jumps" and
        # "dog" both (x 3/4). Scaled: 9, 10, 9, 9, 8.25; their median is 9.
        heights = (("over", 6.0), ("The", 10.0), ("gun", 9.0), ("jumps", 12.0), ("dog", 11.0))
        words = tuple(
            word(text, 10.0 + 40.0 * at, 20.0, 40.0 + 40.0 * at, 20.0 + height, line=4)
            for at, (text, height) in enumerate(heights)
        )

        found = ocr_pieces(words)

        assert [piece.span.font_size_pt for piece in found] == [9.0] * 5
        assert [piece.span.em_width_pt for piece in found] == [9.0] * 5
        assert [piece.span.bbox for piece in found] == [
            (item.bbox.x0, item.bbox.y0, item.bbox.x1, item.bbox.y1) for item in words
        ]

    @pytest.mark.parametrize(
        "text,height,size",
        [
            pytest.param("over", 6.0, 9.0, id="x-height"),
            pytest.param("a,", 6.0, 9.0, id="x-height-and-punctuation"),
            pytest.param("the", 9.0, 9.0, id="ascender"),
            pytest.param("ARE", 9.0, 9.0, id="capitals"),
            pytest.param("17", 9.0, 9.0, id="digits"),
            pytest.param("gun", 9.0, 9.0, id="descender"),
            pytest.param("jumps", 12.0, 9.0, id="both"),
            pytest.param("(on)", 12.0, 9.0, id="brackets-reach-both"),
            pytest.param("-", 1.0, 1.0, id="no-letter"),
            pytest.param("日本", 10.0, 10.0, id="another-script"),
        ],
    )
    def test_each_words_height_is_scaled_by_what_its_glyphs_reach(self, text, height, size):
        (found,) = ocr_pieces((word(text, 10.0, 20.0, 40.0, 20.0 + height),))

        assert found.span.font_size_pt == size

    def test_each_line_has_its_own_size(self):
        words = (
            word("big", 10.0, 20.0, 40.0, 40.0, line=1),
            word("small", 10.0, 60.0, 40.0, 70.0, line=2),
            word("fox", 50.0, 20.0, 80.0, 38.0, line=1),
        )

        found = ocr_pieces(words)

        # Line 1: "big" reaches both sides (20 x 3/4 = 15), "fox" one (18); line 2: "small" one (10).
        assert [piece.span.font_size_pt for piece in found] == [16.5, 10.0, 16.5]

    @pytest.mark.parametrize("gap,sizes", [(16.0, [7.7] * 7), (16.1, [7.5] * 4 + [10.1] * 3)])
    def test_a_line_read_across_a_gutter_is_sized_by_each_side(self, gap, sizes):
        # tesseract read a body line and a heading across the gutter as one line (Root's Law p. 5's "6.2"). Words
        # over 1.8 ems apart (the taller's scaled height: "6.2"'s 8.9, so 16.02) part it. Scaled heights: on the
        # left 7.7, 7.5, 7.4, 7.5 (median 7.5); on the right 8.9, 10.1, 10.1 (median 10.1); all seven's median 7.7.
        left = (("Place", 0.0, 7.7), ("a", 25.0, 5.0), ("ruin", 50.0, 7.4), ("on", 75.0, 5.0))
        right = (("6.2", 95.0 + gap, 8.9), ("FACTION", 120.0 + gap, 10.1), ("RULES", 145.0 + gap, 10.1))
        words = tuple(word(text, x, 0.0, x + 20.0, height, line=7) for text, x, height in (*left, *right))

        found = ocr_pieces(words)

        assert [piece.span.font_size_pt for piece in found] == sizes

    def test_a_zero_confidence_word_is_sized_apart_from_its_lines_other_words(self):
        # tesseract put art read as letters (confidence 0) on a word's line (Root's Law p. 20: "wx Wible,").
        words = (
            word("wx", 10.0, 20.0, 30.0, 27.0, line=1),
            word("Wible,", 40.0, 0.0, 50.0, 51.0, confidence=0.0, line=1),
        )

        found = ocr_pieces(words)

        # "wx" alone: x-height letters, 7 x 3/2; with "Wible," (51, one-sided) their median would be 30.75.
        assert [piece.span.font_size_pt for piece in found] == [10.5, 51.0]

    def test_a_word_with_no_height_has_the_least_size_a_line_reads(self):
        (found,) = ocr_pieces((word("rule", 10.0, 20.0, 40.0, 20.0),))

        assert found.span.font_size_pt == vision.LEAST_SIZE_PT == 0.1
        assert found.span.char_boxes[0] == (10.0, 20.0, 17.5, 20.0)

    def test_each_word_is_its_own_span_in_order(self):
        words = tuple(word(text, x, 20.0, x + 15.0, 30.0) for text, x in (("one", 10.0), ("one", 30.0), ("two", 50.0)))

        found = ocr_pieces(words)

        assert [piece.text for piece in found] == ["one", "one", "two"]
        assert [piece.span.object_index for piece in found] == [0, 1, 2]
        assert len({id(piece.span) for piece in found}) == 3

    def test_no_words_no_pieces(self):
        assert ocr_pieces(()) == ()

    @pytest.mark.parametrize("bad", [[word("a", 0.0, 0.0, 1.0, 1.0)], ("a",), (None,)])
    def test_rejects_anything_but_a_tuple_of_ocr_words(self, bad):
        with pytest.raises(TypeError, match="words"):
            ocr_pieces(bad)


def displayed_box(box, rotation: int, width: float, height: float):
    """A box on the righted page, on the ``width`` x ``height`` page displayed (ocr.py's mapping, in points)."""
    x0, y0, x1, y1 = box
    if rotation == 90:
        return (y0, height - x1, y1, height - x0)
    if rotation == 180:
        return (width - x1, height - y1, width - x0, height - y0)
    if rotation == 270:
        return (width - y1, x0, width - y0, x1)
    return box


def plain(bbox: BBox) -> tuple[float, float, float, float]:
    return (bbox.x0, bbox.y0, bbox.x1, bbox.y1)


def as_upright(element, upright):
    """A turned page's ``element`` with ``upright``'s box, and column centres for a table (a turned one has none)."""
    centres = {"column_centres": upright.column_centres} if isinstance(element, Table) else {}
    return dataclasses.replace(element, bbox=upright.bbox, **centres)


def kinds(found: VisionElements) -> list[str]:
    return [type(element).__name__ for element in found.elements]


class TestVisionElements:
    """OCR words go through A4b's page_layout, with no lattice, into A1 elements with canonical ids; Pass C runs."""

    def test_a_scan_yields_headings_paragraphs_key_values_lists_and_stream_tables(self):
        found = vision_elements(mixed_words(), geometry=GEOMETRY)

        assert kinds(found) == ["Heading", "Paragraph", "KeyValue", "BulletList", "Table"]
        heading, paragraph, pair, listed, table = found.elements
        assert (heading.text, heading.level, heading.scope_path) == ("Results", 1, ("Results",))
        assert paragraph.text == "The woodland is vast and its creatures many, each with its own purpose."
        assert (pair.key, pair.value) == ("Invoice", "10442")
        assert (listed.items, listed.markers, listed.ordered) == (("first item", "second item"), ("•", "•"), False)
        assert (table.headers, table.rows, table.extraction_method) == (PARTS[0], PARTS[1:], "stream")
        assert found.warnings == ()

    def test_elements_carry_the_pages_number_and_canonical_ids_in_order(self):
        found = vision_elements(mixed_words(), geometry=GEOMETRY)

        assert [element.element_id for element in found.elements] == [f"p0003_e00{index}" for index in range(5)]
        assert [element.reading_order for element in found.elements] == list(range(5))
        assert {element.page_number for element in found.elements} == {3}

    def test_every_word_char_is_in_an_element_once(self):
        words = mixed_words()
        found = vision_elements(words, geometry=GEOMETRY)

        assert conservation(ocr_pieces(words), found.elements) == (True, ())

    def test_no_words_no_elements(self):
        assert vision_elements((), geometry=GEOMETRY) == VisionElements(elements=(), warnings=())

    def test_a_zero_confidence_word_is_kept_visible_apart_and_joins_no_line(self):
        # Art read as one tall word of confidence 0 (Root's Law p. 1: a column of digits read as "PPWWNHD") beside a
        # paragraph's two lines: laid out with them, it would make one line of both. It reads apart, before the
        # paragraph (its top is higher), as visible text: kept, never dropped, and every char in an element once.
        words = (
            *line_words("The woodland is vast", 72.0, 100.0),
            *line_words("and its creatures many", 72.0, 112.0),
            word("PPWWNHD", 200.0, 95.0, 207.0, 140.0, confidence=0.0),
        )

        found = vision_elements(words, geometry=GEOMETRY)

        assert [(type(element).__name__, element.text, element.hidden_reasons) for element in found.elements] == [
            ("Paragraph", "PPWWNHD", ()),
            ("Paragraph", "The woodland is vast and its creatures many", ()),
        ]
        assert found.warnings == ()
        assert conservation(ocr_pieces(words), found.elements) == (True, ())

    def test_lines_of_tall_or_short_words_are_body_text_and_a_larger_line_of_capitals_its_heading(self):
        # Ink boxes as OCR draws them (#416): on a 10 pt body, words of x-height letters only are 4.67 pt tall,
        # those reaching the ascender or the descender 7 pt, those reaching both 9.33 pt; the 14 pt heading's
        # capitals reach 9.8 pt. Sized by its own box, a line of tall words read as a heading and lifted the body
        # past the capitals; sized by its line, every body line is 7 pt and the capitals a heading.
        lines = (
            (("FACTION", "above"), ("SETUP", "above")),
            (("The", "above"), ("bold", "above"), ("fox", "above"), ("hid", "above")),
            (("jumpy", "both"), ("yogi", "both"), ("quips", "both")),
            (("over", "x"), ("a", "x"), ("rose", "x"), ("vase", "x")),
            (("Light", "both"), ("fog", "both"), ("lifts", "above")),
        )
        words = tuple(
            found
            for number, parts in enumerate(lines)
            for found in inked(parts, 72.0, 100.0 + 40.0 * number, 14.0 if number == 0 else BODY, number)
        )

        found = vision_elements(words, geometry=GEOMETRY)

        assert [(type(element).__name__, element.text) for element in found.elements] == [
            ("Heading", "FACTION SETUP"),
            ("Paragraph", "The bold fox hid"),
            ("Paragraph", "jumpy yogi quips"),
            ("Paragraph", "over a rose vase"),
            ("Paragraph", "Light fog lifts"),
        ]

    def test_a_layout_losing_a_word_warns_with_pass_c(self, monkeypatch):
        real = vision.page_layout

        def lose_the_first(*args, **kwargs):
            found = real(*args, **kwargs)
            return dataclasses.replace(found, items=found.items[1:])

        monkeypatch.setattr(vision, "page_layout", lose_the_first)

        words = mixed_words()
        found = vision_elements(words, geometry=GEOMETRY)

        total = sum(len(item.text) for item in words)
        lost = len("Results")
        held = f"the page's text holds {total} non-whitespace chars and its elements {total - lost}"
        assert found.warnings == (f"Pass C: {held}: {lost} missing, 0 extra",)

    def test_a_figure_is_a_slot_in_reading_order_and_never_loaded(self, tmp_path, monkeypatch):
        raw = image((72.0, 400.0, 272.0, 550.0))
        loader = ImageLoader(source=tmp_path / "doc.pdf", page_index=2, image=raw)
        draft = FigureDraft(
            bbox=BBox(x0=72.0, y0=400.0, x1=272.0, y1=550.0),
            hidden_reasons=(),
            sha256=SHA,
            width_px=1000,
            height_px=750,
            loader=loader,
        )
        monkeypatch.setattr(ImageLoader, "png_bytes", lambda self: pytest.fail("a figure was loaded"))

        found = vision_elements(mixed_words(), geometry=GEOMETRY, figures=(draft,))

        assert kinds(found)[-1] == "Figure"
        figure = found.elements[-1]
        assert isinstance(figure, Figure)
        assert (figure.element_id, figure.sha256, figure.bbox) == ("p0003_e005", SHA, draft.bbox)

    def test_hidden_digital_pieces_read_apart_with_their_reasons(self):
        hidden = SpanPiece(span=raw_span("SECRET", row(6, 72.0, 600.0)), start=0, end=6, hidden_reasons=("invisible",))
        recovered = SpanPiece(span=raw_span("Echo", row(4, 72.0, 700.0)), start=0, end=4, hidden_reasons=("duplicate",))

        found = vision_elements(mixed_words(), geometry=GEOMETRY, apart=(hidden,), recovered=(recovered,))

        apart = [(element.text, element.hidden_reasons) for element in found.elements if element.is_hidden]
        assert apart == [("SECRET", ("invisible",)), ("Echo", ("duplicate",))]
        assert all(isinstance(element, Paragraph) for element in found.elements if element.is_hidden)
        assert found.warnings == ()

    @pytest.mark.parametrize("name", ["apart", "recovered"])
    def test_a_visible_digital_piece_is_refused(self, name):
        visible = SpanPiece(span=raw_span("Shown", row(5, 72.0, 600.0)), start=0, end=5, hidden_reasons=())

        with pytest.raises(ValueError, match=f"{name}\\[0\\] is visible"):
            vision_elements(mixed_words(), geometry=GEOMETRY, **{name: (visible,)})

    @pytest.mark.parametrize("rotation", [90, 180, 270])
    def test_a_turned_pages_words_lay_out_righted_and_box_on_the_displayed_page(self, rotation):
        # mixed_words() as OCR reads them on the righted (upright) page, placed on the page as displayed.
        size = (LETTER[1], LETTER[0]) if rotation in (90, 270) else LETTER
        on_display = tuple(
            word(item.text, *displayed_box(plain(item.bbox), rotation, *size), item.confidence, item.line)
            for item in mixed_words()
        )
        upright = vision_elements(mixed_words(), geometry=GEOMETRY)
        geometry = dataclasses.replace(GEOMETRY, width_pt=size[0], height_pt=size[1])

        found = vision_elements(on_display, geometry=geometry, rotation=rotation)

        assert kinds(found) == kinds(upright)
        for element, expected in zip(found.elements, upright.elements):
            assert plain(element.bbox) == pytest.approx(displayed_box(plain(expected.bbox), rotation, *size))
            assert as_upright(element, expected) == expected
        (table,) = [element for element in found.elements if isinstance(element, Table)]
        assert table.column_centres == ()

    def test_digital_text_kept_apart_reads_on_a_turned_page_where_it_lies(self):
        size = (LETTER[1], LETTER[0])
        geometry = dataclasses.replace(GEOMETRY, width_pt=size[0], height_pt=size[1])
        hidden = flagged("SECRET", 600.0, "invisible")

        found = vision_elements((), geometry=geometry, rotation=90, apart=(hidden,))

        (element,) = found.elements
        assert (element.text, element.hidden_reasons) == ("SECRET", ("invisible",))
        assert plain(element.bbox) == pytest.approx(hidden.span.bbox)

    def test_rejects_wrong_types(self):
        with pytest.raises(TypeError, match="words"):
            vision_elements([], geometry=GEOMETRY)
        with pytest.raises(ValueError, match="rotation"):
            vision_elements((), geometry=GEOMETRY, rotation=45)
        with pytest.raises(TypeError, match="geometry"):
            vision_elements((), geometry=LETTER)
        with pytest.raises(TypeError, match="figures"):
            vision_elements((), geometry=GEOMETRY, figures=(None,))
        with pytest.raises(TypeError, match="apart"):
            vision_elements((), geometry=GEOMETRY, apart=[None])


# The vision page (task 3): the digital text superseded, then render, orientation, OCR, elements and the page image.


def flagged(text: str, y: float, *reasons) -> SpanPiece:
    """A digital piece of ``text`` on the line at ``y``, with ``reasons``."""
    return SpanPiece(span=raw_span(text, row(len(text), 72.0, y)), start=0, end=len(text), hidden_reasons=reasons)


class TestSuperseded:
    """superseded: each visible piece is flagged "superseded"; a hidden piece keeps its own reasons, never more."""

    def test_visible_pieces_are_superseded_and_hidden_ones_keep_their_reasons(self):
        pieces = (flagged("Shown", 100.0), flagged("Ghost", 120.0, "invisible"), flagged("Faint", 140.0, "transparent"))

        found = superseded(pieces)

        assert [(piece.text, piece.hidden_reasons) for piece in found] == [
            ("Shown", ("superseded",)),
            ("Ghost", ("invisible",)),
            ("Faint", ("transparent",)),
        ]
        assert [(piece.span, piece.start, piece.end) for piece in found] == [
            (piece.span, piece.start, piece.end) for piece in pieces
        ]
        assert found[1:] == pieces[1:]

    def test_nothing_is_superseded_twice(self):
        pieces = (flagged("Shown", 100.0), flagged("Echo", 120.0, "duplicate"))

        assert superseded(superseded(pieces)) == superseded(pieces)

    def test_no_pieces_none_superseded(self):
        assert superseded(()) == ()

    def test_rejects_anything_but_a_tuple_of_pieces(self):
        with pytest.raises(TypeError, match="pieces"):
            superseded([flagged("Shown", 100.0)])


CAPTION = TextRun("Page 7", 500.0, 760.0, size=10.0)


@pytest.fixture(scope="module")
def scans(tmp_path_factory) -> dict[str, Path]:
    """A scan of four sentences (no text layer), and the same scan with a short digital caption drawn on it."""
    directory = tmp_path_factory.mktemp("vision")
    digital = build_pdf(directory / "digital.pdf", [PageSpec(items=LINES)])
    scan = RasterImage(0.0, 0.0, *LETTER, png_of(rendered(digital)))
    return {
        "scan": build_pdf(directory / "scan.pdf", [PageSpec(items=(scan,))]),
        "captioned": build_pdf(directory / "captioned.pdf", [PageSpec(items=(scan, CAPTION))]),
    }


def inputs(path: Path, ocr: str = "auto"):
    """Page 1's raw facts and its assessment (with its recovery)."""
    with open_document(path) as handle:
        raw = extract_page(handle, 0)
        recovered = recover_dropped_text(handle, 0) if raw.text.stats.dropped_text_objects else None
    return raw, assess_page(raw, ocr=ocr, recovered=recovered)


def visioned(path: Path, ocr: str = "auto", **options) -> Page:
    """vision_page of page 1 of ``path``."""
    raw, assessment = inputs(path, ocr)
    return vision_page(raw, assessment, source=path, **options)


@pytest.fixture(scope="module")
def read(scans):
    """read(name): vision_page of that scan (once a scan)."""
    pages: dict[str, Page] = {}

    def reading(name: str) -> Page:
        if name not in pages:
            pages[name] = visioned(scans[name])
        return pages[name]

    return reading


def texts(page: Page, *, hidden: bool = False) -> list[str]:
    """The texts of the page's text elements, visible ones (or hidden ones)."""
    return [element.text for element in page.elements if element.is_hidden == hidden and hasattr(element, "text")]


def decoded(png: bytes) -> tuple[bytes, int, int]:
    """A PNG's 8-bit grey pixels and size."""
    with Image.open(io.BytesIO(png)) as picture:
        assert picture.mode == "L"
        return picture.tobytes(), picture.width, picture.height


@needs_tesseract
class TestVisionPage:
    """A vision page OCR'd: status ok, route vision, never verified; OCR's elements; its page image lazy."""

    def test_a_scan_reads_as_ocr_elements(self, read):
        page = read("scan")

        assert (page.status, page.route, page.integrity_verified, page.error) == ("ok", "vision", False, None)
        read_tokens = " ".join(texts(page)).split()
        assert len(set(TOKENS) & set(read_tokens)) >= 0.9 * len(set(TOKENS))
        assert texts(page, hidden=True) == []
        assert not [warning for warning in page.warnings if warning.startswith("Pass C")]

    def test_its_quality_is_a3s_with_the_mean_ocr_confidence(self, scans, read):
        page = read("scan")
        _, assessment = inputs(scans["scan"])

        mean = page.quality.ocr_mean_confidence
        assert 0.0 < mean <= 100.0
        assert page.quality == dataclasses.replace(assessment.quality, ocr_mean_confidence=mean)

    def test_a_captioned_scan_supersedes_its_digital_caption(self, read):
        page = read("captioned")

        hidden = [(element.text, element.hidden_reasons) for element in page.elements if element.is_hidden]
        assert hidden == [("Page 7", ("superseded",))]
        assert "Page" in " ".join(texts(page)).split()
        assert page.status == "ok"

    def test_its_page_image_is_the_normalized_render_as_displayed(self, scans, read):
        image_of = read("scan").page_image
        expected = normalized(rendered(scans["scan"]))

        assert (image_of.page_number, image_of.dpi, image_of.contrast_normalized) == (1, 300.0, True)
        assert (image_of.width_px, image_of.height_px) == (expected.px_width, expected.px_height) == (2550, 3300)
        assert decoded(image_of.read_bytes()) == (expected.pixels, 2550, 3300)

    def test_its_bands_are_the_bands_ocr_read_as_png(self, scans, read):
        expected = bands(normalized(rendered(scans["scan"])))

        found = [decoded(band) for band in read("scan").page_image.read_bands()]

        assert found == [(band.pixels, band.px_width, band.px_height) for band in expected]
        assert [height for _, _, height in found] == [900, 900, 900, 3300 - 2520]

    def test_the_page_pickles_with_its_lazy_image(self, read):
        page = read("captioned")

        again = pickle.loads(pickle.dumps(page))

        assert again == page
        assert again.page_image.read_bytes() == page.page_image.read_bytes()


def no_tesseract(monkeypatch, tmp_path) -> None:
    """PATH holds no tesseract."""
    empty = tmp_path / "empty-path"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))


def unread_warning(reason: str) -> str:
    return f"OCR could not read the page: {reason}"


class TestOcrUnavailable:
    """A vision page OCR cannot read is 'ocr_unavailable': assemble_page's digital elements, and why."""

    def test_no_tesseract(self, scans, monkeypatch, tmp_path):
        raw, assessment = inputs(scans["captioned"])
        expected = assemble_page(
            raw, assessment, find_lattice_tables(raw.segments.segments, assessment.pieces), source=scans["captioned"]
        )
        no_tesseract(monkeypatch, tmp_path)

        page = vision_page(raw, assessment, source=scans["captioned"])

        assert (page.status, page.route, page.integrity_verified) == ("ocr_unavailable", "vision", False)
        assert page.elements == expected.elements
        assert texts(page) == ["Page 7"]
        assert page.quality == expected.quality
        assert page.warnings == (
            *expected.warnings,
            unread_warning("tesseract is unavailable: no tesseract in an absolute PATH directory"),
        )
        assert page.page_image is not None and page.page_image.width_px == 2550

    @pytest.mark.parametrize("error", [RenderFailed, RenderRefused])
    @pytest.mark.parametrize("step", ["effective_dpi", "render_page"])
    def test_a_page_not_rendered(self, scans, monkeypatch, step, error):
        def fail(*args, **kwargs):
            raise error("page 1: it cannot be drawn")

        monkeypatch.setattr(vision, step, fail)
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: pytest.fail("OCR ran"))

        page = visioned(scans["captioned"])

        assert (page.status, page.page_image) == ("ocr_unavailable", None)
        assert texts(page) == ["Page 7"]
        assert page.warnings[-1] == unread_warning("the page was not rendered: page 1: it cannot be drawn")

    @pytest.mark.parametrize("status", ["failed", "timeout"])
    def test_an_ocr_failure_leaves_its_partial_words_unused(self, scans, monkeypatch, status):
        reason = "band 2 of 4 (rows 840 to 1739): tesseract did not finish"
        partial = OCRResult(words=tuple(line_words("The quick", 72.0, 100.0)), status=status, reason=reason)
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: UPRIGHT)
        monkeypatch.setattr(vision, "ocr_words", lambda *args, **kwargs: partial)

        page = visioned(scans["captioned"])

        assert page.status == "ocr_unavailable"
        assert texts(page) == ["Page 7"]
        unused = "the 2 word(s) it read before that are not used"
        assert page.warnings[-1] == unread_warning(f"tesseract {status}: {reason}; {unused}")
        assert page.page_image is not None

    def test_ocr_unavailable_at_orientation_runs_no_ocr(self, scans, monkeypatch):
        gone = Orientation(degrees=0, confidence=None, status="unavailable", reason="cannot start tesseract")
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: gone)
        monkeypatch.setattr(vision, "ocr_words", lambda *args, **kwargs: pytest.fail("OCR ran"))

        page = visioned(scans["captioned"])

        assert page.status == "ocr_unavailable"
        assert page.warnings[-1] == unread_warning("tesseract is unavailable: cannot start tesseract")


UPRIGHT = Orientation(degrees=0, confidence=30.0, status="ok", reason="")


def spy_ocr(monkeypatch, words=()) -> list[dict]:
    """Record each ocr_words call's options (its result: ``words``, ok)."""
    calls: list[dict] = []

    def recorded(page_bands, **options):
        calls.append({"bands": page_bands, **options})
        return OCRResult(words=tuple(words), status="ok", reason="")

    monkeypatch.setattr(vision, "ocr_words", recorded)
    return calls


class TestOrientation:
    """OSD's turn is made only at a confidence of ROTATION_CONFIDENCE or more; otherwise the page reads as displayed."""

    def test_the_threshold_is_set_from_the_evidence(self):
        # Corpus (95 pages x 4 turns): right turns at 3.59 and up, the one wrong turn at 0.02 (A5b evidence).
        assert vision.ROTATION_CONFIDENCE == 2.0

    @pytest.mark.parametrize(
        ("found", "turn", "warning"),
        [
            pytest.param(
                Orientation(degrees=90, confidence=2.0, status="ok", reason=""),
                90,
                "the page reads turned: OCR righted it by 90 degrees clockwise (orientation confidence 2)",
                id="at-the-threshold",
            ),
            pytest.param(
                Orientation(degrees=270, confidence=35.9, status="ok", reason=""),
                270,
                "the page reads turned: OCR righted it by 270 degrees clockwise (orientation confidence 35.9)",
                id="confident",
            ),
            pytest.param(
                Orientation(degrees=90, confidence=1.99, status="ok", reason=""),
                0,
                "orientation detection suggests turning the page 90 degrees clockwise at a confidence of 1.99, "
                "under 2; OCR read it as displayed",
                id="under-the-threshold",
            ),
            pytest.param(UPRIGHT, 0, None, id="upright"),
            pytest.param(
                Orientation(degrees=0, confidence=None, status="undetermined", reason="too few characters"),
                0,
                "orientation detection undetermined: too few characters; OCR read the page as displayed",
                id="undetermined",
            ),
            pytest.param(
                Orientation(degrees=0, confidence=None, status="failed", reason="no osd.traineddata"),
                0,
                "orientation detection failed: no osd.traineddata; OCR read the page as displayed",
                id="failed",
            ),
            pytest.param(
                Orientation(degrees=0, confidence=None, status="timeout", reason="killed after 120.0 s"),
                0,
                "orientation detection timeout: killed after 120.0 s; OCR read the page as displayed",
                id="timeout",
            ),
        ],
    )
    def test_the_turn_ocr_reads_at(self, scans, monkeypatch, found, turn, warning):
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: found)
        calls = spy_ocr(monkeypatch)

        page = visioned(scans["captioned"])

        (call,) = calls
        assert call["rotation"] == turn
        assert call["dpi"] == 300 and call["page_size_pt"] == LETTER
        assert call["bands"][0].px_width == (3300 if turn in (90, 270) else 2550)
        assert [item for item in page.warnings if "orient" in item or "turned" in item] == (
            [warning] if warning else []
        )

    def test_the_bands_are_of_the_righted_page(self, scans, monkeypatch):
        monkeypatch.setattr(
            vision,
            "orientation",
            lambda *args, **kwargs: Orientation(degrees=90, confidence=9.0, status="ok", reason=""),
        )
        spy_ocr(monkeypatch)

        page = visioned(scans["captioned"])

        displayed = normalized(rendered(scans["captioned"]))
        found = [decoded(band) for band in page.page_image.read_bands()]
        assert found == [(band.pixels, band.px_width, band.px_height) for band in bands(rotated(displayed, 90))]
        # The image itself is the page as displayed, not as OCR turned it.
        assert (page.page_image.width_px, page.page_image.height_px) == (2550, 3300)
        assert decoded(page.page_image.read_bytes()) == (displayed.pixels, 2550, 3300)


class TestVisionPageArguments:
    """vision_page checks its arguments at its boundary."""

    def test_rejects_wrong_types(self, scans):
        raw, assessment = inputs(scans["captioned"])

        with pytest.raises(TypeError, match="raw_page"):
            vision_page(None, assessment, source=scans["captioned"])
        with pytest.raises(TypeError, match="assessment"):
            vision_page(raw, None, source=scans["captioned"])

    def test_rejects_a_page_not_routed_to_vision(self, tmp_path):
        path = build_pdf(tmp_path / "digital.pdf", [PageSpec(items=LINES)])
        raw, assessment = inputs(path)

        with pytest.raises(ValueError, match="routes the page 'digital', not 'vision'"):
            vision_page(raw, assessment, source=path)

    def test_rejects_another_pages_assessment(self, scans):
        raw, _ = inputs(scans["scan"])
        _, other = inputs(scans["captioned"])

        with pytest.raises(ValueError, match="another page"):
            vision_page(raw, other, source=scans["scan"])


class TestVisionOptions:
    """VisionOptions holds the spec's defaults and checks each option at construction."""

    def test_defaults(self):
        options = VisionOptions()

        assert (options.vision_dpi, options.max_render_pixels, options.language) == (300, 16_000_000, "eng")
        assert (options.memory_cap_bytes, options.render_timeout_s, options.ocr_timeout_s) == (2 << 30, 120.0, 120.0)
        assert options.page_budget_s == 600.0
        assert pickle.loads(pickle.dumps(options)) == options

    @pytest.mark.parametrize(
        ("fields", "error", "message"),
        [
            ({"vision_dpi": 35}, ValueError, "vision_dpi"),
            ({"vision_dpi": 300.0}, TypeError, "vision_dpi"),
            ({"max_render_pixels": 262_143}, ValueError, "max_render_pixels"),
            ({"language": " "}, ValueError, "language"),
            ({"memory_cap_bytes": 0}, ValueError, "memory_cap_bytes"),
            ({"render_timeout_s": 0.0}, ValueError, "render_timeout_s"),
            ({"ocr_timeout_s": float("nan")}, ValueError, "ocr_timeout_s"),
            ({"page_budget_s": 0.0}, ValueError, "page_budget_s must be > 0"),
            ({"page_budget_s": float("inf")}, ValueError, "page_budget_s must be finite"),
            ({"page_budget_s": "600"}, TypeError, "page_budget_s must be an int or float"),
        ],
    )
    def test_rejects_bad_options(self, fields, error, message):
        with pytest.raises(error, match=message):
            VisionOptions(**fields)

    def test_vision_page_takes_only_vision_options(self, scans):
        raw, assessment = inputs(scans["captioned"])

        with pytest.raises(TypeError, match="options must be a VisionOptions"):
            vision_page(raw, assessment, source=scans["captioned"], options={"vision_dpi": 300})


def budget_warning(reason: str, budget: str = "600") -> str:
    """The warning of a vision page whose OCR ran out of its page budget (#418)."""
    return unread_warning(f"{reason}: the page's {budget} s budget is spent")


def spy_render(monkeypatch, page: RenderedPage | None = None, *, sleep_s: float = 0.0) -> list[dict]:
    """Record each render_page call's options; render really, or answer ``page`` after ``sleep_s``."""
    calls: list[dict] = []
    real = vision.render_page

    def recorded(*args, **options):
        calls.append(options)
        if page is None:
            return real(*args, **options)
        time.sleep(sleep_s)
        return page

    monkeypatch.setattr(vision, "render_page", recorded)
    return calls


def spy_orientation(monkeypatch) -> list[float]:
    """Record each OSD call's timeout; the page is upright."""
    timeouts: list[float] = []

    def recorded(page, *, timeout_s):
        timeouts.append(timeout_s)
        return UPRIGHT

    monkeypatch.setattr(vision, "orientation", recorded)
    return timeouts


class TestPageBudget:
    """A vision page's render and OCR stop when its page budget is spent (#418), whatever the page's size."""

    @pytest.mark.parametrize("route", ["vision_page", "fallback_page"])
    def test_every_call_is_given_at_most_the_time_left(self, scans, monkeypatch, route):
        failure = failed_at_text(scans["scan"], monkeypatch) if route == "fallback_page" else None
        renders, timeouts, reads = spy_render(monkeypatch), spy_orientation(monkeypatch), spy_ocr(monkeypatch)
        options = VisionOptions(page_budget_s=30.0)

        began = time.monotonic()
        if failure is None:
            visioned(scans["scan"], options=options)
        else:
            fell_back(scans["scan"], failure, options=options)
        ended = time.monotonic()

        # The render's 120 s and OSD's 120 s are cut to the 30 s left; each band's call is cut by the deadline.
        ((render,), (osd,), (read,)) = (renders, timeouts, reads)
        assert 0.0 < render["timeout_s"] <= 30.0 and 0.0 < osd <= render["timeout_s"]
        assert began + 30.0 <= read["deadline"] <= ended + 30.0
        assert read["timeout_s"] == 120.0

    def test_a_budget_spent_before_the_render_draws_nothing(self, scans, monkeypatch):
        monkeypatch.setattr(vision, "render_page", lambda *args, **kwargs: pytest.fail("rendered"))

        page = visioned(scans["captioned"], options=VisionOptions(page_budget_s=1e-9))

        assert (page.status, page.page_image, texts(page)) == ("ocr_unavailable", None, ["Page 7"])
        assert page.warnings[-1] == unread_warning("the page was not rendered: the page's 1e-09 s budget is spent")

    def test_a_render_that_spends_the_budget_leaves_no_time_for_ocr(self, scans, monkeypatch):
        spy_render(monkeypatch, rendered(scans["scan"]), sleep_s=0.3)
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: pytest.fail("OSD ran"))

        page = visioned(scans["captioned"], options=VisionOptions(page_budget_s=0.2))

        assert (page.status, texts(page)) == ("ocr_unavailable", ["Page 7"])
        assert page.warnings[-1] == budget_warning("OCR stopped before orientation detection", "0.2")
        assert page.page_image is not None

    def test_a_spent_reading_is_ocr_unavailable_and_its_words_unused(self, scans, monkeypatch):
        partial = OCRResult(
            words=tuple(line_words("The quick", 72.0, 100.0)), status="spent", reason="OCR stopped after 2 of 4 bands"
        )
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: UPRIGHT)
        monkeypatch.setattr(vision, "ocr_words", lambda *args, **kwargs: partial)

        page = visioned(scans["captioned"])

        assert (page.status, texts(page)) == ("ocr_unavailable", ["Page 7"])
        unused = "; the 2 word(s) it read before that are not used"
        assert page.warnings[-1] == budget_warning("OCR stopped after 2 of 4 bands") + unused

    def test_a_failed_page_whose_reading_is_spent_is_an_error(self, scans, monkeypatch):
        failure = failed_at_text(scans["scan"], monkeypatch)
        spent = OCRResult(words=(), status="spent", reason="OCR stopped after 1 of 4 bands")
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: UPRIGHT)
        monkeypatch.setattr(vision, "ocr_words", lambda *args, **kwargs: spent)

        page = fell_back(scans["scan"], failure)

        assert (page.status, page.elements, page.quality) == ("error", (), None)
        reason = "OCR stopped after 1 of 4 bands: the page's 600 s budget is spent"
        assert page.error == f"{FAILED}{failure.message}; OCR could not read the page: {reason}"

    @posix_only
    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_page_whose_bands_outrun_its_budget_stops_with_the_warning(self, scans, tmp_path, monkeypatch):
        # Each tesseract call takes 2 s, within its 30 s timeout; the page's 3 s budget is 12 times its render (~0.25 s
        # here). OSD alone leaves under 1 s, so no band is read, however slow the runner: 0 of the page's 4 bands.
        body = (
            "cat > /dev/null\nsleep 2\n"
            'if [ "$4" = "0" ]; then printf "Rotate: 0\\nOrientation confidence: 9.00\\n"; else cat "{d}/stdout"; fi'
        )
        fake_tesseract(tmp_path, monkeypatch, body, stdout=tsv(tsv_row(5, (10, 20, 30, 15), "90", "fox")))

        began = time.monotonic()
        page = visioned(scans["scan"], options=VisionOptions(ocr_timeout_s=30.0, page_budget_s=3.0))
        elapsed = time.monotonic() - began

        assert (page.status, texts(page)) == ("ocr_unavailable", [])
        assert page.warnings[-1] == budget_warning("OCR stopped after 0 of 4 bands", "3")
        assert elapsed < 20.0  # ~3 s here: the budget, then one call's reap at most; unbounded, its 5 calls take 10 s


class TestPageRender:
    """A PageImage's loaders: picklable, holding no pixels, checked at construction."""

    def fields(self, tmp_path, **overrides) -> dict:
        values = {
            "source": tmp_path / "doc.pdf",
            "page_index": 0,
            "dpi": 72,
            "rotation": 0,
            "password": "secret",
            "memory_cap_bytes": 1 << 30,
            "timeout_s": 60.0,
        }
        return values | overrides

    def test_holds_an_absolute_path_and_hides_the_password(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        loader = PageRender(**self.fields(tmp_path, source=Path("doc.pdf")))

        assert loader.source == tmp_path / "doc.pdf"
        assert "secret" not in repr(loader)
        assert pickle.loads(pickle.dumps(loader)) == loader

    @pytest.mark.parametrize(
        ("overrides", "error", "message"),
        [
            ({"source": b"doc.pdf"}, TypeError, "source"),
            ({"page_index": -1}, ValueError, "page_index"),
            ({"dpi": 0}, ValueError, "dpi"),
            ({"rotation": 45}, ValueError, "rotation"),
            ({"rotation": 90.0}, ValueError, "rotation"),
            ({"rotation": False}, ValueError, "rotation"),
            ({"password": "a\x00b"}, ValueError, "password"),
            ({"memory_cap_bytes": 0}, ValueError, "memory_cap_bytes"),
            ({"timeout_s": -1.0}, ValueError, "timeout_s"),
        ],
    )
    def test_rejects_bad_fields(self, tmp_path, overrides, error, message):
        with pytest.raises(error, match=message):
            PageRender(**self.fields(tmp_path, **overrides))

    def test_each_load_renders_the_page_again(self, scans, monkeypatch):
        calls = []
        real = vision.render_page

        def counted(*args, **kwargs):
            calls.append(kwargs["dpi"])
            return real(*args, **kwargs)

        monkeypatch.setattr(vision, "render_page", counted)
        loader = PageRender(
            source=scans["scan"], page_index=0, dpi=72, rotation=0, memory_cap_bytes=1 << 30, timeout_s=60.0
        )

        first, second = loader.png_bytes(), loader.png_bytes()

        assert first == second
        assert decoded(first) == (normalized(rendered(scans["scan"], dpi=72)).pixels, 612, 792)
        assert calls == [72, 72]


# The page whose extraction failed (task 4): its geometry, then render + OCR; else an error page.


def failing_text(monkeypatch) -> None:
    """Make every textpage load in this process fail, as a corrupt page's does; the render worker is unaffected."""

    def broken(self):
        raise pdfium.PdfiumError("Failed to load text page.")

    monkeypatch.setattr(pdfium.PdfPage, "get_textpage", broken)


def failed_at_text(path: Path, monkeypatch) -> PageFailure:
    """Page 1's PageFailure, its text stage failing (A2's extract_page)."""
    failing_text(monkeypatch)
    with open_document(path) as handle:
        failure = extract_page(handle, 0)
    monkeypatch.undo()
    assert isinstance(failure, PageFailure) and failure.stage == "text"
    return failure


def fell_back(path: Path, failure: PageFailure, **options) -> Page:
    """fallback_page of ``failure`` on its own load of ``path``."""
    with open_document(path) as handle:
        return fallback_page(handle, failure, source=path, **options)


FAILED = "extraction failed at stage 'text': "
READ_BY_OCR = "; the page was read by OCR of its render: its text layer, vector rules and images were not read"


@needs_tesseract
class TestFallbackPage:
    """A page whose extraction raised is read by OCR of its render: 'ocr_fallback', route vision, never verified."""

    def test_a_page_whose_extraction_raises_is_ocr_fallback(self, scans, monkeypatch):
        failure = failed_at_text(scans["captioned"], monkeypatch)

        page = fell_back(scans["captioned"], failure)

        assert (page.status, page.route, page.integrity_verified, page.error) == ("ocr_fallback", "vision", False, None)
        read_tokens = " ".join(texts(page)).split()
        assert len(set(TOKENS) & set(read_tokens)) >= 0.9 * len(set(TOKENS))
        assert "Page" in read_tokens
        assert texts(page, hidden=True) == []
        assert page.warnings[0] == f"{FAILED}{failure.message}{READ_BY_OCR}"
        assert (page.page_image.width_px, page.page_image.height_px) == (2550, 3300)

    def test_its_quality_is_the_ocrs(self, scans, monkeypatch):
        failure = failed_at_text(scans["scan"], monkeypatch)

        page = fell_back(scans["scan"], failure)

        words = " ".join(texts(page)).replace(" ", "")
        assert page.quality.char_count == len(words)
        assert page.quality.valid_char_ratio == 1.0
        assert 0.0 < page.quality.ocr_mean_confidence <= 100.0
        assert (page.quality.vector_line_count, page.quality.hidden_spans_flagged) == (0, 0)
        assert (page.quality.rotation_deg, page.quality.deskew_angle_deg) == (0, None)


def ok_reading(monkeypatch, words) -> None:
    """OCR stands in: the page upright, ``words`` read."""
    monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: UPRIGHT)
    spy_ocr(monkeypatch, words)


class TestFallbackQuality:
    """An OCR'd fallback page's quality counts the words' chars by A3's rule."""

    def test_the_counts_and_the_mean_confidence(self, scans, monkeypatch):
        failure = failed_at_text(scans["scan"], monkeypatch)
        words = (word("Bolt", 72.0, 100.0, 92.0, 110.0, 80.0), word("4�", 100.0, 100.0, 115.0, 110.0, 50.0))
        ok_reading(monkeypatch, words)

        page = fell_back(scans["scan"], failure)

        assert page.quality == PageQuality(
            valid_char_ratio=5 / 7,
            char_count=7,
            vector_line_count=0,
            hidden_spans_flagged=0,
            rotation_deg=0,
            deskew_angle_deg=None,
            ocr_mean_confidence=65.0,
        )
        assert page.status == "ocr_fallback"

    def test_no_words_no_ratio_and_no_mean(self, scans, monkeypatch):
        failure = failed_at_text(scans["scan"], monkeypatch)
        ok_reading(monkeypatch, ())

        page = fell_back(scans["scan"], failure)

        assert (page.quality.char_count, page.quality.valid_char_ratio, page.quality.ocr_mean_confidence) == (
            0,
            None,
            None,
        )
        assert (page.status, page.elements) == ("ocr_fallback", ())


class TestFallbackErrors:
    """A failed page whose geometry, render or OCR fails too is an error page: why, and no elements (A1)."""

    def test_a_page_that_cannot_load_is_an_error_without_rendering(self, tmp_path, monkeypatch):
        path = break_page(build_pdf(tmp_path / "broken.pdf", [PageSpec(items=LINES)]), 1)
        with open_document(path) as handle:
            failure = extract_page(handle, 0)
        assert isinstance(failure, PageFailure) and failure.stage == "page"
        monkeypatch.setattr(vision, "render_page", lambda *args, **kwargs: pytest.fail("rendered"))

        page = fell_back(path, failure)

        assert (page.status, page.route, page.quality, page.elements, page.page_image) == (
            "error",
            "vision",
            None,
            (),
            None,
        )
        assert page.error.startswith(f"extraction failed at stage 'page': {failure.message}; ")
        assert "its geometry cannot be read either: " in page.error
        assert (page.width_pt, page.height_pt) == (612.0, 792.0)
        geometry_error = page.error.split("its geometry cannot be read either: ", 1)[1]
        default = "612 x 792 pt is pdfium's default page size, not this page's"
        assert page.warnings == (f"the page's size could not be read (PDFCorruptError: {geometry_error}); {default}",)

    @pytest.mark.parametrize("error", [RenderFailed, RenderRefused])
    def test_a_page_not_rendered_either_is_an_error(self, scans, monkeypatch, error):
        failure = failed_at_text(scans["scan"], monkeypatch)

        def fail(*args, **kwargs):
            raise error("page 1: it cannot be drawn")

        monkeypatch.setattr(vision, "render_page", fail)

        page = fell_back(scans["scan"], failure)

        assert (page.status, page.elements, page.quality, page.page_image) == ("error", (), None, None)
        assert page.error == f"{FAILED}{failure.message}; the page was not rendered: page 1: it cannot be drawn"
        assert (page.width_pt, page.height_pt) == LETTER

    def test_no_tesseract_is_an_error_with_the_render_kept(self, scans, monkeypatch, tmp_path):
        failure = failed_at_text(scans["scan"], monkeypatch)
        no_tesseract(monkeypatch, tmp_path)

        page = fell_back(scans["scan"], failure)

        assert (page.status, page.elements, page.quality) == ("error", (), None)
        reason = "tesseract is unavailable: no tesseract in an absolute PATH directory"
        assert page.error == f"{FAILED}{failure.message}; OCR could not read the page: {reason}"
        assert page.page_image is not None

    def test_an_ocr_timeout_is_an_error_and_its_partial_words_unused(self, scans, monkeypatch):
        failure = failed_at_text(scans["scan"], monkeypatch)
        partial = OCRResult(words=tuple(line_words("The", 72.0, 100.0)), status="timeout", reason="band 3 of 4: killed")
        monkeypatch.setattr(vision, "orientation", lambda *args, **kwargs: UPRIGHT)
        monkeypatch.setattr(vision, "ocr_words", lambda *args, **kwargs: partial)

        page = fell_back(scans["scan"], failure)

        assert (page.status, page.elements) == ("error", ())
        unused = "the 1 word(s) it read before that are not used"
        assert page.error.endswith(f"OCR could not read the page: tesseract timeout: band 3 of 4: killed; {unused}")

    def test_ocr_never_reads_nothing_by_ocr(self, scans, monkeypatch):
        failure = failed_at_text(scans["scan"], monkeypatch)
        monkeypatch.setattr(vision, "render_page", lambda *args, **kwargs: pytest.fail("rendered"))

        page = fell_back(scans["scan"], failure, ocr="never")

        assert (page.status, page.elements, page.page_image) == ("error", (), None)
        assert page.error == f"{FAILED}{failure.message}; ocr='never', so the page is not read by OCR"
        assert (page.width_pt, page.height_pt) == LETTER


class TestFallbackArguments:
    """fallback_page checks its arguments at its boundary."""

    def test_rejects_wrong_arguments(self, scans, monkeypatch):
        failure = failed_at_text(scans["scan"], monkeypatch)
        with open_document(scans["scan"]) as handle:
            with pytest.raises(TypeError, match="handle"):
                fallback_page(None, failure, source=scans["scan"])
            with pytest.raises(TypeError, match="failure"):
                fallback_page(handle, None, source=scans["scan"])
            with pytest.raises(ValueError, match="ocr"):
                fallback_page(handle, failure, source=scans["scan"], ocr="sometimes")
            with pytest.raises(TypeError, match="options"):
                fallback_page(handle, failure, source=scans["scan"], options=None)


# The spec's done-when (§806, task 5): rotated scans, a large-format sheet, a searchable scan, a garbled font.

SHEET = (36 * 72.0, 48 * 72.0)  # 36" x 48", in points


def cropped_top(page, rows: int) -> RenderedPage:
    """The top ``rows`` of a rendered page."""
    return RenderedPage(pixels=page.pixels[: rows * page.px_width], px_width=page.px_width, px_height=rows)


def pua_cmap(text: str) -> bytes:
    """A one-byte ToUnicode CMap reading each char of ``text`` but the space as private use (U+E000 + its code)."""
    chars = sorted(set(text))
    pairs = b"\n".join(b"<%02X> <%04X>" % (ord(char), 0x20 if char == " " else 0xE000 + ord(char)) for char in chars)
    return (
        b"/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n/CMapName /Test-UCS def\n/CMapType 2 def\n"
        b"1 begincodespacerange\n<00> <FF>\nendcodespacerange\n%d beginbfchar\n%s\nendbfchar\nendcmap\n"
        b"CMapName currentdict /CMap defineresource pop\nend\nend" % (len(chars), pairs)
    )


GARBLED = "Quick zebras jump"


@pytest.fixture(scope="module")
def done_when(tmp_path_factory) -> dict[str, Path]:
    """The done-when fixtures: the scan at /Rotate 90, 180, 270; searchable scans; a sheet; a garbled font."""
    directory = tmp_path_factory.mktemp("done-when")
    digital = build_pdf(directory / "digital.pdf", [PageSpec(items=LINES)])
    page = rendered(digital)
    scan = RasterImage(0.0, 0.0, *LETTER, png_of(page))
    paths = {"digital": digital}
    for rotate in (90, 180, 270):
        paths[f"scan-{rotate}"] = build_pdf(directory / f"scan-{rotate}.pdf", [PageSpec(items=(scan,), rotate=rotate)])
    invisible = tuple(TextRun(line.text, line.x, line.y, size=line.size, render_mode=3) for line in LINES)
    paths["searchable"] = build_pdf(directory / "searchable.pdf", [PageSpec(items=(scan, *invisible))])
    # The scan's top half only: raster cover is 50 %, not over it, so its no-paint layer is invisible (A3).
    half = RasterImage(0.0, 0.0, LETTER[0], LETTER[1] / 2, png_of(cropped_top(page, page.px_height // 2)))
    paths["half-searchable"] = build_pdf(directory / "half.pdf", [PageSpec(items=(half, *invisible))])
    sheet = tuple(TextRun(sentence, 144.0, 288.0 + index * 72.0, size=36.0) for index, sentence in enumerate(SENTENCES))
    paths["sheet"] = build_pdf(directory / "sheet.pdf", [PageSpec(width=SHEET[0], height=SHEET[1], items=sheet)])
    content = b"BT /F1 24 Tf 40 200 Td (%s) Tj ET" % GARBLED.encode("ascii")
    font = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /ToUnicode 6 0 R >>"
    cmap = pua_cmap(GARBLED)
    stream = b"<< /Length %d >>\nstream\n%s\nendstream" % (len(cmap), cmap)
    garbled = directory / "garbled"
    garbled.mkdir()
    paths["garbled"] = content_page(garbled, content, font=font, extra=(stream,))
    return paths


def union_of(boxes) -> tuple[float, float, float, float]:
    boxes = list(boxes)
    return (min(b[0] for b in boxes), min(b[1] for b in boxes), max(b[2] for b in boxes), max(b[3] for b in boxes))


@needs_tesseract
class TestRotatedScans:
    """90 / 180 / 270 degree scans orient to 0 degrees: OCR reads them righted, in order, where they were drawn."""

    @pytest.mark.parametrize("rotate", [90, 180, 270])
    def test_a_turned_scan_reads_righted_in_order_where_it_was_drawn(self, done_when, rotate):
        page = visioned(done_when[f"scan-{rotate}"])

        righted = (360 - rotate) % 360
        assert page.status == "ok"
        assert f"the page reads turned: OCR righted it by {righted} degrees clockwise" in " ".join(page.warnings)
        visible = [element for element in page.elements if not element.is_hidden and not isinstance(element, Figure)]
        read_tokens = " ".join(element.text for element in visible).split()
        assert len(set(TOKENS) & set(read_tokens)) >= 0.9 * len(set(TOKENS))
        assert read_tokens[:4] == ["The", "quick", "brown", "fox"]
        drawn = turned(union_of(box for _, box in word_boxes(done_when["digital"])), rotate, *LETTER)
        found = union_of(plain(element.bbox) for element in visible)
        assert found == pytest.approx(drawn, abs=3.0)

    @pytest.mark.parametrize("rotate", [90, 180, 270])
    def test_its_bands_are_900_px_bands_of_the_righted_page(self, done_when, rotate):
        image_of = visioned(done_when[f"scan-{rotate}"]).page_image

        found = [decoded(band) for band in image_of.read_bands()]

        assert (image_of.width_px, image_of.height_px) == ((3300, 2550) if rotate in (90, 270) else (2550, 3300))
        assert {width for _, width, _ in found} == {2550}
        assert [height for _, _, height in found] == [900, 900, 900, 3300 - 2520]


@needs_tesseract
class TestLargeFormatSheet:
    """A 36" x 48" sheet renders, bands and reads within max_render_pixels."""

    @pytest.mark.parametrize("bound", [16_000_000, 4_000_000])
    def test_the_sheet_reads_within_the_pixel_bound(self, done_when, bound):
        page = visioned(done_when["sheet"], ocr="always", options=VisionOptions(max_render_pixels=bound))

        image_of = page.page_image
        dpi = effective_dpi(300, bound, *SHEET)
        assert (image_of.dpi, image_of.width_px, image_of.height_px) == (float(dpi), *render_size(*SHEET, dpi))
        assert image_of.width_px * image_of.height_px <= bound
        found = [decoded(band) for band in image_of.read_bands()]
        assert all(height <= 900 for _, _, height in found)
        assert sum(len(pixels) for pixels, _, _ in found) <= 2 * bound
        read_tokens = " ".join(texts(page)).split()
        assert len(set(TOKENS) & set(read_tokens)) >= 0.9 * len(set(TOKENS))


@needs_tesseract
class TestSearchableScans:
    """A searchable scan's text layer stays hidden, by its own reason or superseded, and is never superseded twice."""

    def test_a3_reads_a_whole_page_scans_layer_as_the_scan_so_it_routes_hybrid(self, done_when):
        _, assessment = inputs(done_when["searchable"])

        assert assessment.route == "hybrid"
        assert {piece.hidden_reasons for piece in assessment.pieces} == {()}

    def test_forced_to_vision_its_layer_is_superseded_once(self, done_when):
        page = visioned(done_when["searchable"], ocr="always")

        hidden = [element for element in page.elements if element.is_hidden]
        assert {element.hidden_reasons for element in hidden} == {("superseded",)}
        assert " ".join(element.text for element in hidden).split() == TOKENS
        read_tokens = " ".join(texts(page)).split()
        assert len(set(TOKENS) & set(read_tokens)) >= 0.9 * len(set(TOKENS))

    def test_an_invisible_layer_keeps_its_reason_and_is_not_superseded(self, done_when):
        _, assessment = inputs(done_when["half-searchable"])
        page = visioned(done_when["half-searchable"])

        assert assessment.route == "vision"
        hidden = [element for element in page.elements if element.is_hidden]
        assert {element.hidden_reasons for element in hidden} == {("invisible",)}
        assert " ".join(element.text for element in hidden).split() == TOKENS
        assert len(set(TOKENS) & set(" ".join(texts(page)).split())) >= 0.9 * len(set(TOKENS))


@needs_tesseract
class TestGarbledFont:
    """A font mapping its glyphs to private use (valid_char_ratio < 0.70): read by OCR, its digital text superseded."""

    def test_its_digital_text_is_superseded_by_what_ocr_reads(self, done_when):
        _, assessment = inputs(done_when["garbled"])
        page = visioned(done_when["garbled"])

        assert (assessment.route, page.status) == ("vision", "ok")
        assert page.quality.valid_char_ratio == pytest.approx(2 / len(GARBLED)) and page.quality.valid_char_ratio < 0.70
        hidden = [(element.text, element.hidden_reasons) for element in page.elements if element.is_hidden]
        garbled = "".join(" " if char == " " else chr(0xE000 + ord(char)) for char in GARBLED)
        assert hidden == [(garbled, ("superseded",))]
        assert texts(page) == [GARBLED]
