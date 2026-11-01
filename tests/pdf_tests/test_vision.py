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

import pytest

from datagrunt.pdf import vision
from datagrunt.pdf.assembly import FigureDraft, conservation
from datagrunt.pdf.elements import BBox, Figure, Paragraph
from datagrunt.pdf.ocr import OCRWord
from datagrunt.pdf.pdfium_engine import ImageLoader, PageGeometry
from datagrunt.pdf.quality import SpanPiece
from datagrunt.pdf.vision import VisionElements, ocr_pieces, vision_elements
from pdf_tests.test_pdfium_engine import imported_modules
from pdf_tests.test_quality import SHA, image, raw_span, row

LETTER = (612.0, 792.0)
GEOMETRY = PageGeometry(page_number=3, width_pt=LETTER[0], height_pt=LETTER[1], rotation_deg=0, crop_offset=(0.0, 0.0))
BODY = 10.0


def word(text: str, x0: float, y0: float, x1: float, y1: float, confidence: float = 90.0) -> OCRWord:
    return OCRWord(text=text, bbox=BBox(x0=x0, y0=y0, x1=x1, y1=y1), confidence=confidence)


def line_words(text: str, x: float, y: float, size: float = BODY) -> list[OCRWord]:
    """``text``'s words on a line from ``(x, y)``: each char 0.5 em wide, words 0.3 em apart, boxes 1 em tall."""
    found = []
    for token in text.split():
        width = 0.5 * size * len(token)
        found.append(word(token, x, y, x + width, y + size))
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

    def test_its_font_is_the_box_height_upright_plain_and_painted(self):
        (found,) = ocr_pieces((word("Fox", 10.0, 20.0, 40.0, 32.5),))
        span = found.span

        assert (span.font_size_pt, span.em_width_pt) == (12.5, 12.5)
        assert (span.is_bold, span.is_italic, span.render_mode, span.base_font_name) == (False, False, 0, "")
        assert (span.fill_rgba, span.stroke_rgba) == ((0, 0, 0, 255), (0, 0, 0, 255))
        assert (span.generated_breaks, span.map_errors) == ((), ())

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

    def test_rejects_wrong_types(self):
        with pytest.raises(TypeError, match="words"):
            vision_elements([], geometry=GEOMETRY)
        with pytest.raises(TypeError, match="geometry"):
            vision_elements((), geometry=LETTER)
        with pytest.raises(TypeError, match="figures"):
            vision_elements((), geometry=GEOMETRY, figures=(None,))
        with pytest.raises(TypeError, match="apart"):
            vision_elements((), geometry=GEOMETRY, apart=[None])
