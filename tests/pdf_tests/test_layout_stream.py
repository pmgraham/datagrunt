"""Tests for the v5 stream layout pass (#368, A4b): lines, blocks and reading order.

Unit tests build SpanPiece values directly: boxes are displayed, top-left
``(x0, y0, x1, y1)`` points. End-to-end tests build PDFs with the ReportLab
toolkit (``pdf_tests.builders``), whose coordinates are top-left points in the
unrotated page space, and read them through ``extract_page``, ``assess_page``,
``find_lattice_tables`` and the stream functions.
"""

import dataclasses
import pickle
import re
import time

import pytest
from reportlab.pdfbase.pdfmetrics import stringWidth

from datagrunt.pdf import layout
from datagrunt.pdf.layout import (
    LatticeResult,
    TextBlock,
    TextCell,
    TextLine,
    find_lattice_tables,
    text_blocks,
    text_lines,
)
from datagrunt.pdf.pdfium_engine import extract_page, open_document
from datagrunt.pdf.quality import SpanPiece, assess_page
from pdf_tests.builders import PageSpec, TextRun, build_pdf
from pdf_tests.test_layout_lattice import box, chars, falling, rising, rules, written
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


def read_blocks(tmp_path, *items, rotate: int = 0) -> tuple[TextBlock, ...]:
    """text_blocks of page 1, through extract_page, assess_page and find_lattice_tables."""
    page, pieces = page_of(tmp_path, *items, rotate=rotate)
    return text_blocks(pieces, find_lattice_tables(page.segments.segments, pieces))


def block_texts(blocks) -> list[str]:
    """Each block's text."""
    return [found.text for found in blocks]


def column(texts, x: float, top: float, *, pitch: float = 14.0, **overrides) -> tuple[TextRun, ...]:
    """One TextRun per text from ``x``, their baselines ``pitch`` apart from ``top``."""
    return tuple(TextRun(text, x, top + index * pitch, **overrides) for index, text in enumerate(texts))


def block(*lines: TextLine, **overrides) -> TextBlock:
    """A TextBlock of ``lines`` (one default line if none), boxed by their union."""
    lines = lines or (line(),)
    union = box(
        min(item.bbox.x0 for item in lines),
        min(item.bbox.y0 for item in lines),
        max(item.bbox.x1 for item in lines),
        max(item.bbox.y1 for item in lines),
    )
    return TextBlock(**({"lines": lines, "bbox": union} | overrides))


class TestTextBlock:
    """TextBlock is a frozen, slotted, picklable, validated record; its text joins its lines with a space."""

    def test_pickle_round_trip(self):
        original = block(line(cell("first")), line(cell("second", 10.0, 30.0)))

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = block()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.lines = ()
        assert not hasattr(value, "__dict__")

    def test_text_joins_the_lines_with_one_space(self):
        assert block(line(cell("first")), line(cell("second", 10.0, 30.0))).text == "first second"

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"lines": [line()]}, TypeError, "lines must be a tuple, got list", id="lines-list"),
            pytest.param({"lines": ("Name",)}, TypeError, "lines[0] must be a TextLine, got str", id="lines-str"),
            pytest.param({"lines": ()}, ValueError, "lines must hold at least one line", id="lines-empty"),
            pytest.param({"bbox": None}, TypeError, "bbox must be a BBox, got NoneType", id="bbox-none"),
            pytest.param(
                {"bbox": box(10.0, 10.0, 20.0, 22.0)},
                ValueError,
                "lines[0].bbox must lie within the bbox, got BBox(x0=10.0, y0=10.0, x1=34.0, y1=22.0) in "
                "BBox(x0=10.0, y0=10.0, x1=20.0, y1=22.0)",
                id="line-outside",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            block(**overrides)


def rows(x0: float, x1: float, count: int = 3, *, top: float = 0.0) -> list[tuple[float, float, float, float]]:
    """``count`` boxes from ``x0`` to ``x1``, 12 pt tall, 20 pt apart from ``top``: a column of lines."""
    return [(x0, top + 20.0 * index, x1, top + 20.0 * index + 12.0) for index in range(count)]


def cut(boxes, weights=None) -> list[list[int]]:
    """The XY-cut's segments of ``boxes`` (each weighing 10 unless given)."""
    return layout._xy_cut(list(boxes), list(weights or [10] * len(boxes)))


def staircase(count: int, *, width: float = 2e9):
    """``count`` boxes, each step's box peeling off the left 0.3 of what is left, a 0.1 gutter after it.

    The widest gutter of every step is its own, so each XY-cut level peels one box.
    """
    boxes, left, span = [], 0.0, width
    for _ in range(count - 1):
        boxes.append((left, 0.0, left + 0.3 * span, 12.0))
        left, span = left + 0.4 * span, 0.6 * span
    boxes.append((left, 0.0, width, 12.0))
    return boxes


LEFT, RIGHT = rows(0.0, 140.0), rows(160.0, 300.0)


class TestXYCut:
    """v4's XY-cut (layout_sorter.PageLayoutSorter), ported: gutters, spanning bands and the depth cap."""

    def test_one_column_is_one_segment(self):
        assert cut(LEFT) == [[0, 1, 2]]

    def test_lines_spanning_a_gutter_with_text_on_one_side_only_are_one_segment(self):
        # v5 fix: v4 read each full-width line as a band of its own (no column text on the right of the gutter).
        short = (0.0, 60.0, 90.0, 72.0)

        assert cut(LEFT + [short]) == [[0, 1, 2, 3]]

    def test_two_columns_read_left_then_right(self):
        assert cut(RIGHT + LEFT) == [[3, 4, 5], [0, 1, 2]]

    @pytest.mark.parametrize(
        "gap,segments",
        [
            pytest.param(11.0, [[0, 1, 2, 3, 4, 5]], id="11-pt"),
            pytest.param(12.0, [[0, 1, 2], [3, 4, 5]], id="12-pt"),
        ],
    )
    def test_a_gutter_is_at_least_12_pt_wide(self, gap, segments):
        assert cut(LEFT + rows(140.0 + gap, 280.0 + gap)) == segments

    def test_a_gutter_outside_the_middle_half_does_not_split(self):
        assert cut(rows(0.0, 40.0) + rows(60.0, 300.0)) == [[0, 1, 2, 3, 4, 5]]

    @pytest.mark.parametrize(
        "weight,segments",
        [
            pytest.param(1, [[0, 1, 2, 6], [3, 4, 5]], id="weight-1-leaves-the-gutter"),
            pytest.param(2, [[0, 1, 2, 3, 4, 5, 6]], id="weight-2-blocks-it"),
        ],
    )
    def test_the_gutter_holds_a_weight_of_at_most_1(self, weight, segments):
        assert cut(LEFT + RIGHT + [(145.0, 0.0, 150.0, 12.0)], [10] * 6 + [weight]) == segments

    def test_the_widest_gutter_splits_first(self):
        # A heading (weight 1, so it blocks nothing) crosses only the wider gutter, between columns 2 and 3:
        # cut there first, it spans and reads first; cut at the narrower gutter first, column 1 would.
        heading = (110.0, -30.0, 290.0, -18.0)
        columns = rows(0.0, 80.0) + rows(100.0, 180.0) + rows(220.0, 300.0)

        assert cut(columns + [heading], [10] * 9 + [1]) == [[9], [0, 1, 2], [3, 4, 5], [6, 7, 8]]

    @pytest.mark.parametrize(
        "width,segments",
        [
            pytest.param(211.0, [[6], [0, 1, 2], [3, 4, 5]], id="over-70-percent-spans"),
            pytest.param(209.0, [[0, 1, 2, 3, 4, 5, 6]], id="under-70-percent-blocks-the-gutter"),
        ],
    )
    def test_only_items_over_70_percent_of_the_width_leave_the_density(self, width, segments):
        # Known limitation (v4): a spanning heading narrower than that blocks the gutter, so its page is one segment.
        assert cut(LEFT + RIGHT + [(45.0, -30.0, 45.0 + width, -18.0)]) == segments

    @pytest.mark.parametrize(
        "right,segments",
        [
            pytest.param(155.0, [[0, 1, 2, 6], [3, 4, 5]], id="5-pt-past-the-gutter-is-a-column-item"),
            pytest.param(156.0, [[6], [0, 1, 2], [3, 4, 5]], id="over-5-pt-past-it-spans"),
        ],
    )
    def test_an_item_spans_when_it_reaches_over_5_pt_past_the_gutter_centre(self, right, segments):
        assert cut(LEFT + RIGHT + [(100.0, -30.0, right, -18.0)], [10] * 6 + [1]) == segments

    def test_an_item_as_tall_as_80_percent_of_the_text_never_spans(self):
        assert cut(LEFT + RIGHT + [(100.0, 0.0, 200.0, 52.0)], [10] * 6 + [1]) == [[0, 1, 2], [3, 4, 5, 6]]

    def test_spanning_items_cut_the_columns_into_bands_read_in_order(self):
        # Both spanning items are over 70 % of the width, so neither adds weight to the gutter.
        heading, rule = (10.0, -30.0, 290.0, -18.0), (10.0, 60.0, 290.0, 72.0)
        boxes = LEFT + RIGHT + rows(0.0, 140.0, top=80.0) + rows(160.0, 300.0, top=80.0) + [rule, heading]

        assert cut(boxes) == [[13], [0, 1, 2], [3, 4, 5], [12], [6, 7, 8], [9, 10, 11]]

    def test_overlapping_spanning_items_are_one_band_read_top_then_left(self):
        # The column item whose top falls inside the band joins it, read between the two by its top.
        lower, upper, inside = (10.0, -24.0, 290.0, -12.0), (20.0, -30.0, 280.0, -18.0), (0.0, -27.0, 60.0, -15.0)

        assert cut(LEFT + RIGHT + [lower, upper, inside]) == [[7, 8, 6], [0, 1, 2], [3, 4, 5]]

    @pytest.mark.parametrize(
        "boxes,count",
        [
            pytest.param([LEFT[0], RIGHT[0]], 1, id="2-items"),
            pytest.param(rows(0.0, 40.0) + rows(55.0, 99.0), 1, id="99-pt-wide"),
            pytest.param(rows(0.0, 42.0) + rows(58.0, 100.0), 2, id="100-pt-wide"),
        ],
    )
    def test_fewer_than_3_items_or_under_100_pt_wide_is_one_segment(self, boxes, count):
        assert len(cut(boxes)) == count

    def test_past_max_partition_depth_the_rest_is_one_segment(self):
        segments = cut(staircase(40))

        assert layout.MAX_PARTITION_DEPTH == 32
        assert segments == [[index] for index in range(32)] + [list(range(32, 40))]

    def test_a_lower_depth_cap_stops_cutting_sooner(self, monkeypatch):
        monkeypatch.setattr(layout, "MAX_PARTITION_DEPTH", 1)
        columns = rows(0.0, 80.0) + rows(100.0, 180.0) + rows(200.0, 300.0)

        assert cut(columns) == [[0, 1, 2], [3, 4, 5, 6, 7, 8]]

    def test_no_items_no_segments(self):
        assert cut([]) == []


def inked(text: str, x: float, y: float, *, height: float = 12.0, bearing: float = 1.0, rise: float = 2.0) -> SpanPiece:
    """A visible piece of ``text`` in 6 pt advances from ``(x, y)``, ``height`` tall; its tight boxes are each glyph's
    ink, ``bearing`` in from both sides of its advance and ``rise`` in from its top and bottom."""
    loose = chars(text, x, y, height=height).span.char_boxes
    tight = tuple((x0 + bearing, y0 + rise, x1 - bearing, y1 - rise) for x0, y0, x1, y1 in loose)
    return chars(text, x, y, height=height, tight_boxes=tight)


def block_lines(blocks) -> list[list[str]]:
    """Each block's line texts."""
    return [[found.text for found in item.lines] for item in blocks]


class TestTextBlocks:
    """Blocks in reading order, end to end: columns read in turn, lines merged into paragraphs as v4 merged them."""

    def test_two_column_prose_reads_column_by_column_under_a_spanning_heading(self, tmp_path):
        heading = TextRun("Chapter One: How the Woodland Alliance Came to Be", 72.0, 90.0, size=18.0)
        left = ("The woodland creatures gathered at", "dawn to plan the long campaign", "against the marquise.")
        right = ("Their sympathy spread from glade", "to glade, each clearing taking up", "the cause in turn.")
        items = (heading, *column(left, 72.0, 130.0), *column(right, 316.0, 130.0))

        assert block_texts(read_blocks(tmp_path, *items)) == [heading.text, " ".join(left), " ".join(right)]

    def test_three_columns_read_in_turn(self, tmp_path):
        first, second, third = ("one a", "one b", "one c"), ("two a", "two b", "two c"), ("three a", "three b")
        items = (*column(third, 396.0, 100.0), *column(second, 234.0, 100.0), *column(first, 72.0, 100.0))

        assert block_texts(read_blocks(tmp_path, *items)) == [" ".join(first), " ".join(second), " ".join(third)]

    def test_close_lines_of_one_size_are_one_paragraph_even_when_their_boxes_overlap(self, tmp_path):
        # 11 pt Helvetica boxes are 12.28 pt tall: at a 12 pt pitch neighbouring lines overlap by 0.28 pt.
        items = column(("Lines set", "solid read as", "one paragraph."), 72.0, 100.0, pitch=12.0)

        assert block_texts(read_blocks(tmp_path, *items)) == ["Lines set solid read as one paragraph."]

    @pytest.mark.parametrize(
        "pitch,texts",
        [
            # 11 pt lines 12.28 pt tall: a pitch of 29.7 pt leaves a 17.42 pt gap, 30 pt a 17.72 pt gap (1.6 em: 17.6).
            pytest.param(29.7, ["above below"], id="gap-under-1.6-em"),
            pytest.param(30.0, ["above", "below"], id="gap-over-1.6-em"),
        ],
    )
    def test_a_gap_over_1_6_em_starts_a_paragraph(self, tmp_path, pitch, texts):
        assert block_texts(read_blocks(tmp_path, *column(("above", "below"), 72.0, 100.0, pitch=pitch))) == texts

    @pytest.mark.parametrize(
        "size,texts",
        [
            pytest.param(11.5, ["body more"], id="0.5-pt-larger"),
            pytest.param(11.6, ["body", "more"], id="0.6-pt-larger"),
        ],
    )
    def test_a_font_size_0_6_pt_away_starts_a_paragraph(self, tmp_path, size, texts):
        items = (TextRun("body", 72.0, 100.0), TextRun("more", 72.0, 114.0, size=size))

        assert block_texts(read_blocks(tmp_path, *items)) == texts

    @pytest.mark.parametrize(
        "marked",
        [
            pytest.param(("• first", "• second"), id="bullets"),
            pytest.param(("» first", "» second"), id="guillemets"),
            pytest.param(("1. first", "2. second"), id="numbers"),
            pytest.param(("a) first", "b) second"), id="letters"),
            pytest.param(("(a) first", "(b) second"), id="bracketed-letters"),
            pytest.param(("I Battle. first", "II Ambush. second"), id="bare-roman-before-a-capital"),
        ],
    )
    def test_a_line_starting_with_a_list_marker_starts_a_paragraph(self, tmp_path, marked):
        items = column(("Intro text", *marked), 72.0, 100.0)

        assert block_texts(read_blocks(tmp_path, *items)) == ["Intro text", *marked]

    @pytest.mark.parametrize(
        "numbered",
        [
            pytest.param(("1.1.1 Precedence. first", "1.1.2 Cannot. second"), id="rule-numbers"),
            pytest.param(("A.5.1 Choose. first", "G.24 Remove. second"), id="appendix-numbers"),
        ],
    )
    def test_a_line_starting_with_a_section_number_starts_a_paragraph(self, tmp_path, numbered):
        items = column(("Intro text", *numbered), 72.0, 100.0)

        assert block_texts(read_blocks(tmp_path, *items)) == ["Intro text", *numbered]

    @pytest.mark.parametrize(
        "second",
        [
            # v4's marker took a bare number for one: Root's Law wraps "... take" / "3 quest cards ..." (p. 10).
            pytest.param("3 quest cards, and place them face up near you.", id="bare-number"),
            pytest.param("2024. The rules were revised for every game.", id="year"),
            pytest.param("-5 °C is the lowest rated temperature.", id="minus-sign"),
            pytest.param("I think the rule is clear.", id="pronoun"),
            pytest.param("CIVIL ENGINEERING TEXTS ARE PROSE TOO.", id="roman-letters-of-a-word"),
        ],
    )
    def test_a_line_starting_with_no_marker_continues_its_paragraph(self, tmp_path, second):
        items = column(("Each player draws a hand and takes", second), 72.0, 100.0)

        assert block_texts(read_blocks(tmp_path, *items)) == [f"Each player draws a hand and takes {second}"]

    @pytest.mark.parametrize(
        "fonts",
        [
            pytest.param(("Helvetica-Bold", "Helvetica"), id="bold-then-regular"),
            pytest.param(("Helvetica", "Helvetica-Bold"), id="regular-then-bold"),
        ],
    )
    def test_a_line_whose_boldness_differs_from_the_last_starts_a_paragraph(self, tmp_path, fonts):
        # v5 adds this to v4's merge: a bold heading set at the body's size, directly over it, stays its own block.
        items = (
            TextRun("Overview", 72.0, 100.0, font=fonts[0]),
            TextRun("The woodland is vast.", 72.0, 114.0, font=fonts[1]),
        )

        assert block_texts(read_blocks(tmp_path, *items)) == ["Overview", "The woodland is vast."]

    def test_lines_that_are_all_bold_merge_as_any_others(self, tmp_path):
        items = column(("A bold paragraph", "set over two lines"), 72.0, 100.0, font="Helvetica-Bold")

        assert block_texts(read_blocks(tmp_path, *items)) == ["A bold paragraph set over two lines"]

    def test_lines_without_horizontal_overlap_stay_apart(self, tmp_path):
        items = (TextRun("left", 72.0, 100.0), TextRun("right", 300.0, 114.0))

        assert block_texts(read_blocks(tmp_path, *items)) == ["left", "right"]

    def test_a_page_turned_by_rotate_90_reads_its_paragraph_as_written(self, tmp_path):
        items = column(("first line", "second line", "third line"), 72.0, 100.0)

        assert block_texts(read_blocks(tmp_path, *items, rotate=90)) == ["first line second line third line"]

    def test_the_lattice_tables_text_is_not_reused(self, tmp_path):
        xs, ys = (50.0, 150.0, 250.0), (40.0, 60.0, 80.0)
        items = rules(xs, ys) + written((("Part", "Qty"), ("Bolt", "4")), xs, ys) + (TextRun("After", 50.0, 120.0),)

        assert block_texts(read_blocks(tmp_path, *items)) == ["After"]

    def test_a_turned_line_never_joins_a_paragraph_of_another_turn(self):
        # Read in mixed frames, the falling label would pass every other test of the merge: its turn keeps it apart.
        pieces = (chars("alpha", 50.0, 10.0), falling("dn", 40.0, 40.0))

        assert [[found.text for found in item.lines] for item in text_blocks(pieces, NO_TABLES)] == [["alpha"], ["dn"]]

    def test_a_trailing_space_does_not_narrow_a_gutter(self):
        # 6 pt chars: the left lines' ink ends at 138, their trailing spaces at 144; the right column starts at 152.
        # The XY-cut reads ink, as v4's glyph bounds did: a 14 pt gutter, though the boxes leave only 8 pt.
        left = tuple(chars("abcdefghijklmnopqrstuvw ", 0.0, 14.0 * row) for row in range(3))
        right = tuple(chars("ABCDEFGHIJKLMNOPQRSTUVWX", 152.0, 14.0 * row) for row in range(3))

        blocks = text_blocks(left + right, NO_TABLES)

        assert [[found.text for found in item.lines] for item in blocks] == [
            [piece.text for piece in left],
            [piece.text for piece in right],
        ]

    def test_a_gutter_is_measured_between_glyphs_not_their_advances(self):
        # The left lines' advances end at 144 and the right column's start at 155: 11 pt, no gutter. Their glyphs
        # (tight boxes) sit 1 pt in from each side of their advances: 13 pt of ink-free gutter, as v4 measured it.
        left = tuple(inked("abcdefghijklmnopqrstuvwx", 0.0, 14.0 * row) for row in range(3))
        right = tuple(inked("ABCDEFGHIJKLMNOPQRSTUVWX", 155.0, 14.0 * row) for row in range(3))

        blocks = text_blocks(left + right, NO_TABLES)

        assert block_lines(blocks) == [[piece.text for piece in left], [piece.text for piece in right]]

    def test_lines_set_solid_under_a_spanning_heading_join_its_band_only_if_their_ink_does(self):
        # Boxes 13.4 pt tall (an 11 pt font's ascent to descent) at a 12 pt pitch overlap, so each column's first
        # line would start inside the heading's band and read across the columns with it; their ink (2 pt in
        # from top and bottom) does not overlap.
        heading = inked("A HEADING OVER BOTH COLUMNS OF THE PAGE AND MORE", 0.0, 0.0, height=13.4)
        left = tuple(inked(f"left column line {row}", 0.0, 12.0 * (row + 1), height=13.4) for row in range(4))
        right = tuple(inked(f"right column line {row}", 180.0, 12.0 * (row + 1), height=13.4) for row in range(4))

        blocks = text_blocks((heading, *left, *right), NO_TABLES)

        assert block_lines(blocks) == [[heading.text], [piece.text for piece in left], [piece.text for piece in right]]

    def test_columns_closer_than_12_pt_between_advances_read_in_turn_when_their_ink_is_not(self, tmp_path):
        # The left lines end in "1" and the right ones start with it: a tabular figure, narrower than its
        # advance by a side bearing at each edge. Their advances leave 10 pt; their ink over 12 pt.
        size, pitch, left_x = 20.0, 24.0, 72.0
        left = ("Rule 1.1", "Rule 2.1", "Rule 3.1")
        right_x = after(left[0], left_x, size=size) + 10.0
        right = ("1st apple", "1st pear", "1st plum")
        page, pieces = page_of(
            tmp_path,
            *column(left, left_x, 100.0, pitch=pitch, size=size),
            *column(right, right_x, 100.0, pitch=pitch, size=size),
        )

        # The glyphs come from the font pdfium substitutes for Helvetica, which differs by platform: the
        # fixture holds only while the ink gutter is at least 13 pt wide.
        ends = [piece.span.tight_boxes[piece.end - 1][2] for piece in pieces if piece.span.text in left]
        starts = [piece.span.tight_boxes[piece.start][0] for piece in pieces if piece.span.text in right]
        assert min(starts) - max(ends) >= 13.0
        blocks = text_blocks(pieces, find_lattice_tables(page.segments.segments, pieces))

        assert block_texts(blocks) == [" ".join(left), " ".join(right)]

    def test_invalid_arguments_raise_as_for_text_lines(self):
        with pytest.raises(TypeError, match=r"^lattice must be a LatticeResult, got NoneType$"):
            text_blocks((), None)


def stepped_pieces(steps: int, rows_per_step: int) -> tuple[SpanPiece, ...]:
    """One-char pieces filling each box of a ``steps``-box staircase, ``rows_per_step`` lines of them deep."""
    pieces = []
    for x0, _, x1, _ in staircase(steps):
        pieces += [chars("x", x0, 14.0 * row, advance=x1 - x0) for row in range(rows_per_step)]
    return tuple(pieces)


class TestTextBlocksBounds:
    """Adversarial pages of 10 000 or more tiny pieces lay out in under 2 s: the work is O(depth x n log n)."""

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_staircase_driving_the_xy_cut_to_its_depth_cap(self):
        pieces = stepped_pieces(40, 250)

        started = time.perf_counter()
        blocks = text_blocks(pieces, NO_TABLES)
        elapsed = time.perf_counter() - started

        assert len(pieces) == 10_000
        assert len(blocks) == layout.MAX_PARTITION_DEPTH + 1
        assert sum(found.text.count("x") for found in blocks) == 10_000
        assert elapsed < 2.0, f"{elapsed:.2f} s"

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_grid_of_20_000_one_char_pieces(self):
        # Every char 24 pt clear of the next: a gutter for the XY-cut, and over 1.8 em of 11 pt, so a cell of its own.
        pieces = tuple(chars("x", 30.0 * col, 14.0 * row) for row in range(100) for col in range(200))

        started = time.perf_counter()
        blocks = text_blocks(pieces, NO_TABLES)
        elapsed = time.perf_counter() - started

        assert sum(len(found.cells) for item in blocks for found in item.lines) == 20_000
        assert elapsed < 2.0, f"{elapsed:.2f} s"
