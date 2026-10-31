"""Tests for the v5 stream layout pass (#368, A4b): lines.

Unit tests build SpanPiece values directly: boxes are displayed, top-left
``(x0, y0, x1, y1)`` points. End-to-end tests build PDFs with the ReportLab
toolkit (``pdf_tests.builders``), whose coordinates are top-left points in the
unrotated page space, and read them through ``extract_page``, ``assess_page``,
``find_lattice_tables`` and the stream functions.
"""

import dataclasses
import pickle
import re

import pytest
from reportlab.pdfbase.pdfmetrics import stringWidth

from datagrunt.pdf.layout import LatticeResult, TextCell, TextLine, find_lattice_tables, text_lines
from datagrunt.pdf.pdfium_engine import extract_page, open_document
from datagrunt.pdf.quality import SpanPiece, assess_page
from pdf_tests.builders import PageSpec, TextRun, build_pdf
from pdf_tests.test_layout_lattice import box, chars, rising, rules, written
from pdf_tests.test_quality import raw_span

NO_TABLES = LatticeResult(tables=(), used_pieces=())


def after(text: str, x: float, *, size: float = 11.0, font: str = "Helvetica") -> float:
    """Where a run of ``text`` drawn from ``x`` ends: its advance width added."""
    return x + stringWidth(text, font, size)


def page_of(tmp_path, *items, rotate: int = 0):
    """Page 1 of a letter-size PDF of ``items``: its raw facts and assessed pieces."""
    path = build_pdf(tmp_path / "page.pdf", [PageSpec(items=items, rotate=rotate)])
    page = extract_page(open_document(path), 0)
    return page, assess_page(page).pieces


def read_lines(tmp_path, *items, rotate: int = 0) -> tuple[TextLine, ...]:
    """text_lines of page 1, through extract_page, assess_page and find_lattice_tables."""
    page, pieces = page_of(tmp_path, *items, rotate=rotate)
    return text_lines(pieces, find_lattice_tables(page.segments.segments, pieces))


def texts(lines) -> list[str]:
    """Each line's text."""
    return [line.text for line in lines]


def cells_of(lines) -> list[list[str]]:
    """Each line's cell texts."""
    return [[cell.text for cell in line.cells] for line in lines]


def piece(text: str = "Name", x: float = 10.0, y: float = 10.0) -> SpanPiece:
    """A visible piece of ``text`` in 6 pt chars from ``(x, y)``, 12 pt tall."""
    return chars(text, x, y)


def cell(text: str = "Name", x: float = 10.0, y: float = 10.0) -> TextCell:
    """A TextCell of one piece, boxed exactly."""
    return TextCell(text=text, bbox=box(x, y, x + 6.0 * len(text), y + 12.0), pieces=(piece(text, x, y),))


def line(*cells: TextCell, **overrides) -> TextLine:
    """A TextLine of ``cells`` (one default cell if none), boxed by their union."""
    cells = cells or (cell(),)
    union = box(
        min(item.bbox.x0 for item in cells),
        min(item.bbox.y0 for item in cells),
        max(item.bbox.x1 for item in cells),
        max(item.bbox.y1 for item in cells),
    )
    fields = {"cells": cells, "bbox": union, "turn": 0, "font_size_pt": 11.0, "is_bold": False}
    return TextLine(**(fields | overrides))


class TestTextCell:
    """TextCell is a frozen, slotted, picklable, validated record."""

    def test_pickle_round_trip(self):
        original = cell()

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = cell()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.text = "x"
        assert not hasattr(value, "__dict__")

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"text": None}, TypeError, "text must be a str, got None (NoneType)", id="text-none"),
            pytest.param({"text": " \t"}, ValueError, "text must be a non-blank str, got ' \\t'", id="text-blank"),
            pytest.param({"bbox": (10.0, 10.0, 34.0, 22.0)}, TypeError, "bbox must be a BBox, got tuple", id="bbox"),
            pytest.param({"pieces": [piece()]}, TypeError, "pieces must be a tuple, got list", id="pieces-list"),
            pytest.param({"pieces": ("Name",)}, TypeError, "pieces[0] must be a SpanPiece, got str", id="pieces-str"),
            pytest.param({"pieces": ()}, ValueError, "pieces must hold at least one piece", id="pieces-empty"),
            pytest.param(
                {"pieces": (piece("Name", 40.0),)},
                ValueError,
                "pieces[0] must lie within the bbox, got (40.0, 10.0, 64.0, 22.0) in "
                "BBox(x0=10.0, y0=10.0, x1=34.0, y1=22.0)",
                id="piece-outside",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        fields = {"text": "Name", "bbox": box(10.0, 10.0, 34.0, 22.0), "pieces": (piece(),)}

        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            TextCell(**(fields | overrides))


class TestTextLine:
    """TextLine is a frozen, slotted, picklable, validated record; its text joins its cells with a space."""

    def test_pickle_round_trip(self):
        original = line(cell("Name"), cell("Value", 80.0))

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = line()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.turn = 90
        assert not hasattr(value, "__dict__")

    def test_text_joins_the_cells_with_one_space(self):
        assert line(cell("Name"), cell("Value", 80.0)).text == "Name Value"

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"cells": [cell()]}, TypeError, "cells must be a tuple, got list", id="cells-list"),
            pytest.param({"cells": ("Name",)}, TypeError, "cells[0] must be a TextCell, got str", id="cells-str"),
            pytest.param({"cells": ()}, ValueError, "cells must hold at least one cell", id="cells-empty"),
            pytest.param({"bbox": None}, TypeError, "bbox must be a BBox, got NoneType", id="bbox-none"),
            pytest.param(
                {"bbox": box(10.0, 10.0, 20.0, 22.0)},
                ValueError,
                "cells[0].bbox must lie within the bbox, got BBox(x0=10.0, y0=10.0, x1=34.0, y1=22.0) in "
                "BBox(x0=10.0, y0=10.0, x1=20.0, y1=22.0)",
                id="cell-outside",
            ),
            pytest.param({"turn": True}, TypeError, "turn must be an int, got True (bool)", id="turn-bool"),
            pytest.param({"turn": 180}, ValueError, "turn must be 0, 90 or 270, got 180", id="turn-180"),
            pytest.param({"font_size_pt": 0.0}, ValueError, "font_size_pt must be > 0, got 0.0", id="size-zero"),
            pytest.param({"is_bold": 1}, TypeError, "is_bold must be a bool, got 1 (int)", id="bold-int"),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            line(**overrides)


class TestTextLines:
    """Visible text outside the lattice tables, end to end: lines, joins and cells (spec §3.3 Pass B)."""

    def test_runs_on_one_baseline_are_one_line_read_left_to_right(self, tmp_path):
        items = (TextRun("world", after("Hello", 72.0) + 4.0, 100.0), TextRun("Hello", 72.0, 100.0))

        assert texts(read_lines(tmp_path, *items)) == ["Hello world"]

    def test_lines_come_top_to_bottom(self, tmp_path):
        items = (TextRun("third", 72.0, 140.0), TextRun("first", 72.0, 100.0), TextRun("second", 72.0, 120.0))

        assert texts(read_lines(tmp_path, *items)) == ["first", "second", "third"]

    @pytest.mark.parametrize(
        "gap,joined",
        [
            # 11 pt text: a gap of at most 0.22 em (2.42 pt) joins with nothing, a wider one with a space.
            pytest.param(2.3, "football", id="tight"),
            pytest.param(2.6, "foot ball", id="spaced"),
        ],
    )
    def test_runs_join_by_the_gap_between_them(self, tmp_path, gap, joined):
        items = (TextRun("foot", 72.0, 100.0), TextRun("ball", after("foot", 72.0) + gap, 100.0))

        assert texts(read_lines(tmp_path, *items)) == [joined]

    @pytest.mark.parametrize(
        "left,right,joined",
        [
            pytest.param("20", "°C", "20°C", id="before-degree"),
            pytest.param("20°", "C", "20°C", id="after-degree"),
            pytest.param("Hello", ",", "Hello,", id="comma"),
            pytest.param("Total", ":", "Total:", id="colon"),
            pytest.param("(", "see", "(see", id="opening-bracket"),
            pytest.param("note", ")", "note)", id="closing-bracket"),
            pytest.param("end.", "Next", "end. Next", id="after-a-full-stop-a-space"),
        ],
    )
    def test_runs_next_to_degree_or_punctuation_join_with_nothing(self, tmp_path, left, right, joined):
        # 4 pt apart: past 0.22 em, so the gap alone would join them with a space.
        items = (TextRun(left, 72.0, 100.0), TextRun(right, after(left, 72.0) + 4.0, 100.0))

        assert texts(read_lines(tmp_path, *items)) == [joined]

    def test_a_raised_degree_sign_stays_on_its_line(self, tmp_path):
        # A 7 pt degree sign raised 6 pt, 3 pt from each neighbour: one line, joined tight.
        degree_x = after("Water boils at 100", 72.0) + 3.0
        items = (
            TextRun("Water boils at 100", 72.0, 100.0),
            TextRun("°", degree_x, 94.0, size=7.0),
            TextRun("C at sea level", after("°", degree_x, size=7.0) + 3.0, 100.0),
            TextRun("Next line", 72.0, 120.0),
        )

        lines = read_lines(tmp_path, *items)

        assert texts(lines) == ["Water boils at 100°C at sea level", "Next line"]
        assert lines[0].font_size_pt == 11.0

    @pytest.mark.parametrize(
        "gap,cells",
        [
            # 11 pt text: a gap over 1.8 em (19.8 pt) splits the line into cells.
            pytest.param(19.5, [["Name Value"]], id="one-cell"),
            pytest.param(20.1, [["Name", "Value"]], id="two-cells"),
        ],
    )
    def test_a_gap_over_1_8_em_splits_the_line_into_cells(self, tmp_path, gap, cells):
        items = (TextRun("Name", 72.0, 100.0), TextRun("Value", after("Name", 72.0) + gap, 100.0))

        assert cells_of(read_lines(tmp_path, *items)) == cells

    def test_cells_keep_their_boxes_and_source_pieces(self, tmp_path):
        items = (TextRun("Name", 72.0, 100.0), TextRun("Value", 200.0, 100.0))

        (only,) = read_lines(tmp_path, *items)

        name, value = only.cells
        assert (round(name.bbox.x0, 2), round(name.bbox.x1, 2)) == (72.0, round(after("Name", 72.0), 2))
        assert (round(value.bbox.x0, 2), round(only.bbox.x1, 2)) == (200.0, round(after("Value", 200.0), 2))
        assert [(item.text, item.start, item.end, item.hidden_reasons) for item in value.pieces] == [
            ("Value", 0, 5, ())
        ]
        assert only.turn == 0

    def test_hidden_text_is_not_laid_out(self, tmp_path):
        items = (TextRun("shown", 72.0, 100.0), TextRun("secret", 72.0, 120.0, render_mode=3))

        assert texts(read_lines(tmp_path, *items)) == ["shown"]

    def test_text_beside_a_lattice_table_is_laid_out_and_the_tables_text_is_not(self, tmp_path):
        xs, ys = (50.0, 150.0, 250.0), (40.0, 60.0, 80.0)
        items = rules(xs, ys) + written((("Part", "Qty"), ("Bolt", "4")), xs, ys)
        items += (TextRun("Beside the table", 300.0, 54.0), TextRun("Below it", 50.0, 120.0))

        assert texts(read_lines(tmp_path, *items)) == ["Beside the table", "Below it"]

    def test_text_running_out_of_a_table_keeps_only_the_chars_outside_it(self, tmp_path):
        # From x 230 the run crosses the table's right rule at 250: chars centred inside stay in the cell.
        xs, ys = (50.0, 150.0, 250.0), (40.0, 60.0, 80.0)
        items = rules(xs, ys) + written((("Part", "Qty"), ("Bolt", "")), xs, ys) + (TextRun("ABCDEFGHIJ", 230.0, 74.0),)
        page, pieces = page_of(tmp_path, *items)
        lattice = find_lattice_tables(page.segments.segments, pieces)

        (only,) = text_lines(pieces, lattice)

        inside = lattice.tables[0].grid()[1][1]
        assert inside + only.text == "ABCDEFGHIJ"
        assert inside and only.text
        assert [(item.start, item.end) for item in only.cells[0].pieces] == [(len(inside), 10)]

    def test_a_page_turned_by_rotate_90_reads_its_lines_as_written(self, tmp_path):
        items = (
            TextRun("I", 72.0, 100.0),
            TextRun("am here", after("I", 72.0) + 3.0, 100.0),
            TextRun("Second line", 72.0, 120.0),
        )

        lines = read_lines(tmp_path, *items, rotate=90)

        assert texts(lines) == ["I am here", "Second line"]
        assert [line.turn for line in lines] == [90, 90]

    def test_a_line_carries_its_dominant_font_size_and_whether_it_is_all_bold(self, tmp_path):
        items = (
            TextRun("Bold heading", 72.0, 100.0, font="Helvetica-Bold"),
            TextRun("Big", 72.0, 130.0, size=14.0),
            TextRun("small text here", after("Big", 72.0, size=14.0) + 4.0, 130.0, size=9.0),
            TextRun("Lead.", 72.0, 160.0, font="Helvetica-Bold"),
            TextRun("Body", after("Lead.", 72.0, font="Helvetica-Bold") + 4.0, 160.0),
        )

        lines = read_lines(tmp_path, *items)

        assert [(line.text, line.font_size_pt, line.is_bold) for line in lines] == [
            ("Bold heading", 11.0, True),
            ("Big small text here", 9.0, False),
            ("Lead. Body", 11.0, False),
        ]


class TestTextLinesUnits:
    """Line rules that need pieces the toolkit cannot draw: right-to-left text, turned text, generated breaks."""

    def test_right_to_left_text_keeps_its_char_order(self):
        # pdfium gives right-to-left text in logical order, its chars running leftward: kept as given.
        boxes = tuple((60.0 - 6.0 * (i + 1), 10.0, 60.0 - 6.0 * i, 22.0) for i in range(4))
        hebrew = SpanPiece(span=raw_span("שלום", boxes), start=0, end=4, hidden_reasons=())

        assert texts(text_lines((hebrew,), NO_TABLES)) == ["שלום"]

    def test_text_turned_against_the_page_reads_along_its_own_lines(self):
        # A label rising up the margin beside two upright lines: its own line, not glue for theirs.
        pieces = (chars("alpha", 50.0, 10.0), chars("beta", 50.0, 30.0), rising("vertical", 20.0, 60.0))

        lines = text_lines(pieces, NO_TABLES)

        assert [(line.text, line.turn) for line in lines] == [("alpha", 0), ("vertical", 270), ("beta", 0)]

    def test_a_generated_break_across_a_gap_over_1_8_em_splits_the_span_into_cells(self):
        boxes = tuple((10.0 + 6.0 * i, 10.0, 16.0 + 6.0 * i, 22.0) for i in range(4))
        boxes += tuple((60.0 + 6.0 * i, 10.0, 66.0 + 6.0 * i, 22.0) for i in range(5))
        span = raw_span("NameValue", boxes, generated_breaks=(4,))

        (only,) = text_lines((SpanPiece(span=span, start=0, end=9, hidden_reasons=()),), NO_TABLES)

        assert [[(item.start, item.end) for item in found.pieces] for found in only.cells] == [[(0, 4)], [(4, 9)]]
        assert [found.text for found in only.cells] == ["Name", "Value"]

    def test_a_generated_break_across_a_smaller_gap_reads_as_one_space(self):
        boxes = tuple((10.0 + 6.0 * i, 10.0, 16.0 + 6.0 * i, 22.0) for i in range(9))
        span = raw_span("NameValue", boxes, generated_breaks=(4,))

        assert cells_of(text_lines((SpanPiece(span=span, start=0, end=9, hidden_reasons=()),), NO_TABLES)) == [
            ["Name Value"]
        ]

    def test_whitespace_only_text_makes_no_line(self):
        assert text_lines((chars("   ", 10.0, 10.0), chars("word", 10.0, 40.0)), NO_TABLES)[0].text == "word"
        assert text_lines((chars(" \t ", 10.0, 10.0),), NO_TABLES) == ()

    def test_a_raised_piece_overlapping_the_line_by_45_percent_or_more_stays_on_it(self):
        # '2' is 6 pt tall and overlaps the 12 pt line by 2.76 pt (0.46 of the smaller height); 2.64 pt is 0.44.
        on = text_lines((chars("m", 10.0, 10.0), chars("2", 16.0, 6.76, height=6.0)), NO_TABLES)
        off = text_lines((chars("m", 10.0, 10.0), chars("2", 16.0, 6.64, height=6.0)), NO_TABLES)

        assert (texts(on), texts(off)) == (["m2"], ["2", "m"])


class TestTextLinesArguments:
    """The pieces and the lattice result are checked at the boundary."""

    @pytest.mark.parametrize(
        "pieces,lattice,error,message",
        [
            pytest.param([piece()], NO_TABLES, TypeError, "pieces must be a tuple, got list", id="pieces-list"),
            pytest.param(("text",), NO_TABLES, TypeError, "pieces[0] must be a SpanPiece, got str", id="pieces-str"),
            pytest.param((), None, TypeError, "lattice must be a LatticeResult, got NoneType", id="lattice-none"),
        ],
    )
    def test_invalid_argument_raises(self, pieces, lattice, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            text_lines(pieces, lattice)

    def test_a_piece_passed_twice_raises(self):
        twice = piece()

        with pytest.raises(ValueError, match=r"^pieces\[1\] is pieces\[0\] again"):
            text_lines((twice, twice), NO_TABLES)

    def test_lattice_text_from_another_page_raises(self):
        elsewhere = SpanPiece(span=raw_span("Other"), start=0, end=5, hidden_reasons=())
        lattice = LatticeResult(tables=(), used_pieces=(elsewhere,))

        message = "lattice.used_pieces[0] is not visible text of these pieces: the lattice result is of another page"
        with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
            text_lines((piece(),), lattice)

    def test_lattice_text_reaching_past_its_piece_raises(self):
        own = piece("Name")
        lattice = LatticeResult(tables=(), used_pieces=(dataclasses.replace(own, start=2, end=4),))
        shorter = dataclasses.replace(own, end=3)

        with pytest.raises(ValueError, match=r"^lattice\.used_pieces\[0\] is not visible text of these pieces"):
            text_lines((shorter,), lattice)
