"""Tests for the v5 stream pass's tables and page layout (#368, A4b): stream tables, mastheads, reading order.

The corpus has no borderless table (#368), so these fixtures are the stream
tables' ground truth. Unit tests build TextLine values directly: boxes are
displayed, top-left ``(x0, y0, x1, y1)`` points. End-to-end tests build PDFs
with the ReportLab toolkit (``pdf_tests.builders``) and read them through
``extract_page``, ``assess_page``, ``find_lattice_tables`` and
``stream_layout``. Boxes come from the font pdfium substitutes for
Helvetica, which differs by platform: expected geometry is read from
pdfium's own boxes in the same run.
"""

import dataclasses
import pickle
import re
import time
from collections import Counter

import pytest

from datagrunt.pdf import layout
from datagrunt.pdf.layout import (
    LatticeResult,
    StreamParagraph,
    StreamTable,
    TextCell,
    find_lattice_tables,
    stream_layout,
    text_lines,
)
from pdf_tests.builders import TextRun
from pdf_tests.test_layout_classify import described as described_text
from pdf_tests.test_layout_lattice import box, chars, falling, rising, rules, written
from pdf_tests.test_layout_stream import cell, column, line, page_of

NO_TABLES = LatticeResult(tables=(), used_pieces=())
PARA = "This paragraph runs across the whole width of the page so that it spans every column."
PARTS = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"), ("Washer", "100", "0.01"))
XS = (72.0, 220.0, 360.0)


def laid_out(tmp_path, *items, rotate: int = 0):
    """stream_layout of page 1, through extract_page, assess_page and find_lattice_tables."""
    page, pieces = page_of(tmp_path, *items, rotate=rotate)
    lattice = find_lattice_tables(page.segments.segments, pieces)
    return stream_layout(pieces, lattice, width_pt=page.geometry.width_pt, height_pt=page.geometry.height_pt)


def described(items) -> list[tuple]:
    """Each item as a plain tuple: a table as its grid and header flag, text items as test_layout_classify does."""
    return [
        ("table", item.grid(), item.has_header) if isinstance(item, StreamTable) else described_text([item])[0]
        for item in items
    ]


def table_runs(rows, xs=XS, top: float = 140.0, *, pitch: float = 14.0, **overrides) -> tuple[TextRun, ...]:
    """One TextRun per non-empty cell text, each row's cells from ``xs``, rows ``pitch`` apart from ``top``."""
    return tuple(
        TextRun(text, x, top + pitch * index, **overrides)
        for index, row in enumerate(rows)
        for text, x in zip(row, xs)
        if text
    )


def body_text(top: float = 130.0) -> tuple[TextRun, ...]:
    """Three lines of 10 pt prose from x 72: one paragraph, the page's body size."""
    texts = ("The woodland is vast and its creatures many,", "each with its own purpose and", "its own ambitions.")
    return column(texts, 72.0, top, pitch=12.0, size=10.0)


def masthead(tmp_path, gap_over: float, rows: int = 2, **overrides):
    """Masthead rows and their runs: two columns ``gap_over`` points further apart than 0.40 of a 612 pt page.

    The left column's end is read from pdfium in this run: the font it substitutes for Helvetica sets it.
    """
    left = ("ACME Industrial Supply", "1 Harbour Road, Leith", "Edinburgh EH6 6LX")[:rows]
    right = ("Invoice: 10442", "Date: 2024-03-01", "Terms: 30 days")[:rows]
    probe = tmp_path / "probe"
    probe.mkdir(exist_ok=True)
    _, pieces = page_of(probe, *table_runs(tuple((text,) for text in left), (72.0,), top=60.0, **overrides))
    x = max(piece.bbox[2] for piece in pieces) + 0.40 * 612.0 + gap_over
    grid = tuple(zip(left, right))
    return grid, table_runs(grid, (72.0, x), top=60.0, **overrides)


def down_cell(text: str, x: float, y: float) -> TextCell:
    """A TextCell of ``text`` running down the page from ``(x, y)``, 6 pt a char, boxed exactly."""
    return TextCell(text=text, bbox=box(x, y, x + 12.0, y + 6.0 * len(text)), pieces=(falling(text, x, y),))


def up_cell(text: str, x: float, y: float) -> TextCell:
    """A TextCell of ``text`` running up the page from ``(x, y)``, 6 pt a char, boxed exactly."""
    return TextCell(text=text, bbox=box(x, y - 6.0 * len(text), x + 12.0, y), pieces=(rising(text, x, y),))


class TestStreamTables:
    """Runs of 2 or more lines of 2 or more cells, in consistent X-bands, are tables (spec §3.3 Pass B)."""

    def test_a_borderless_3_column_table_between_paragraphs(self, tmp_path):
        items = laid_out(tmp_path, TextRun(PARA, 72.0, 100.0), *table_runs(PARTS), TextRun(PARA, 72.0, 230.0))

        assert described(items) == [("paragraph", PARA), ("table", PARTS, True), ("paragraph", PARA)]

    def test_a_table_alone_on_its_page_reads_by_rows(self, tmp_path):
        # Read as text, the XY-cut cuts at the gaps between the columns: "Part Bolt Nut Washer", "Qty", ...
        assert described(laid_out(tmp_path, *table_runs(PARTS))) == [("table", PARTS, True)]

    def test_a_table_keeps_its_rows_boxes_and_column_centres(self, tmp_path):
        page, pieces = page_of(tmp_path, *table_runs(PARTS))
        lattice = find_lattice_tables(page.segments.segments, pieces)

        (table,) = stream_layout(pieces, lattice, width_pt=612.0, height_pt=792.0)

        # Each column's band runs from its cells' first start to their last end, as pdfium boxes them in this run
        # (the substitute font sets some advances: "Washer" ends 0.15 pt past its Helvetica width on macOS).
        cells = [found.cells for found in text_lines(pieces, lattice)]
        bands = [(min(row[col].bbox.x0 for row in cells), max(row[col].bbox.x1 for row in cells)) for col in range(3)]
        assert table.column_centres == tuple((start + end) / 2 for start, end in bands)
        assert (table.n_cols, table.columns) == (3, ((0, 1, 2),) * 4)
        assert [row.text for row in table.rows] == [" ".join(row) for row in PARTS]
        assert (table.bbox.x0, table.bbox.x1) == (72.0, bands[2][1])
        assert table.pieces == tuple(piece for row in table.rows for found in row.cells for piece in found.pieces)

    @pytest.mark.parametrize(
        "rows,has_header",
        [
            pytest.param(
                (("Name", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "", "0.05"), ("Washer", "10", "")),
                True,
                id="missing-body-cells",
            ),
            pytest.param(
                (("", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05")), False, id="missing-header-cell"
            ),
        ],
    )
    def test_a_ragged_table_leaves_its_missing_cells_empty(self, tmp_path, rows, has_header):
        assert described(laid_out(tmp_path, *table_runs(rows))) == [("table", rows, has_header)]

    def test_a_row_of_one_cell_ends_the_table(self, tmp_path):
        # Known limitation (spec: runs of multi-cell lines): a cell wrapped onto a line of its own splits the table.
        rows = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Spacer", "", ""), ("Nut", "12", "0.05"))
        rows += (("Washer", "100", "0.01"),)

        assert described(laid_out(tmp_path, *table_runs(rows))) == [
            ("table", rows[:2], True),
            ("paragraph", "Spacer"),
            ("table", rows[3:], True),
        ]

    def test_a_row_with_a_cell_across_two_columns_ends_the_table(self, tmp_path):
        # The title's cell reaches from the first column into the second: it fits neither, nor do the rows under it.
        rows = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"))
        title = (TextRun("A title running over the first two columns", 72.0, 168.0), TextRun("x", 360.0, 168.0))
        under = (("Nut", "12", "0.05"), ("Washer", "100", "0.01"))

        assert described(laid_out(tmp_path, *table_runs(rows), *title, *table_runs(under, top=182.0))) == [
            ("table", rows, True),
            ("paragraph", "A title running over the first two columns x"),
            ("table", under, True),
        ]

    def test_the_lattice_tables_text_never_enters_a_stream_table(self, tmp_path):
        xs, ys = (50.0, 150.0, 250.0), (40.0, 60.0, 80.0)
        items = rules(xs, ys) + written((("Part", "Qty"), ("Bolt", "4")), xs, ys) + table_runs(PARTS)

        assert described(laid_out(tmp_path, *items)) == [("table", PARTS, True)]

    def test_a_table_on_a_page_turned_by_rotate_90_reads_as_written(self, tmp_path):
        (table,) = laid_out(tmp_path, *table_runs(PARTS), rotate=90)

        # Along its rows the turned table runs down the displayed page: its column centres are display y.
        bands = [
            [
                found.bbox
                for row, cols in zip(table.rows, table.columns)
                for found, at in zip(row.cells, cols)
                if at == col
            ]
            for col in range(3)
        ]
        assert table.grid() == PARTS
        assert {row.turn for row in table.rows} == {90}
        assert table.column_centres == tuple((min(b.y0 for b in band) + max(b.y1 for b in band)) / 2 for band in bands)

    def test_a_table_read_up_the_page_has_its_column_centres_in_reading_order(self):
        # Text rising up the page reads bottom to top: its first column is the lower one (display y 376-400).
        rows = [line(up_cell("Name", x, 400.0), up_cell("Value", x, 200.0), turn=270) for x in (10.0, 30.0)]

        (found,) = layout._row_structures(rows, width=612.0, height=792.0)

        assert found.items[0].grid() == (("Name", "Value"), ("Name", "Value"))
        assert found.items[0].column_centres == (388.0, 185.0)


class TestBands:
    """Cells cluster into X-bands by interval projection: a cell joins the band it overlaps or touches, no slack."""

    @pytest.mark.parametrize(
        "x0,columns",
        [
            # Row 1's first cell ends at x 34; row 2's starts at 34 (touching: one band) or 34.5 (a band of its own).
            pytest.param(34.0, ((0, 1), (0, 1)), id="touching-joins"),
            pytest.param(34.5, ((0, 2), (1, 2)), id="any-gap-separates"),
        ],
    )
    def test_a_cell_joins_a_band_it_touches(self, x0, columns):
        first = line(cell("Name", 10.0, 10.0), cell("Value", 200.0, 10.0))
        second = line(cell("Code", x0, 30.0), cell("Price", 200.0, 30.0))

        (found,) = layout._row_structures([first, second], width=612.0, height=792.0)

        assert found.items[0].columns == columns

    def test_a_line_that_fits_no_band_ends_the_run(self):
        # Row 3's wide cell reaches both bands of the run; row 4's two cells both fall in row 3's first band.
        rows = [
            line(cell("Name", 10.0, 10.0), cell("Value", 200.0, 10.0)),
            line(cell("Bolt", 10.0, 30.0), cell("4", 200.0, 30.0)),
            line(cell("Wide" * 10, 10.0, 50.0), cell("x", 300.0, 50.0)),
            line(cell("Nut", 10.0, 70.0), cell("12", 200.0, 70.0)),
        ]

        structures = layout._row_structures(rows, width=612.0, height=792.0)

        assert [[row.text for row in found.items[0].rows] for found in structures] == [["Name Value", "Bolt 4"]]

    def test_a_line_whose_cells_overlap_fits_no_band(self):
        # Never so from text_lines, whose cells are over 1.8 em apart; a hand-made line must not break the bands.
        # Row 2's cells (x 40-64 and 50-80) overlap each other and meet neither of row 1's bands.
        rows = [
            line(cell("Name", 10.0, 10.0), cell("Value", 200.0, 10.0)),
            line(cell("Name", 40.0, 30.0), cell("Value", 50.0, 30.0)),
        ]

        assert layout._row_structures(rows, width=612.0, height=792.0) == []

    def test_a_line_of_another_turn_ends_the_run(self):
        upright = [line(cell("Name", 10.0, 10.0 + 40.0 * at), cell("Value", 200.0, 10.0 + 40.0 * at)) for at in (0, 1)]
        turned = line(down_cell("Name", 10.0, 100.0), down_cell("Value", 10.0, 300.0), turn=90)

        assert layout._row_structures([upright[0], turned, upright[1]], width=612.0, height=792.0) == []


class TestRejections:
    """Text in rows that is no table: a masthead, a key-value list, prose (spec §3.3 Pass B)."""

    def test_a_2_row_masthead_wider_than_0_40_of_the_page_is_pairs_and_paragraphs(self, tmp_path):
        _, runs = masthead(tmp_path, 1.0)

        assert described(laid_out(tmp_path, *runs, TextRun(PARA, 72.0, 120.0))) == [
            ("paragraph", "ACME Industrial Supply"),
            ("key_value", "Invoice", "10442"),
            ("paragraph", "1 Harbour Road, Leith"),
            ("key_value", "Date", "2024-03-01"),
            ("paragraph", PARA),
        ]

    def test_a_masthead_never_heads_its_page(self, tmp_path):
        # Bold and larger than the body, its cells would be headings as text; a masthead is pairs or paragraphs.
        _, runs = masthead(tmp_path, 1.0, font="Helvetica-Bold", size=13.0)

        assert [kind for kind, *_ in described(laid_out(tmp_path, *runs, *body_text()))] == [
            "paragraph",
            "key_value",
            "paragraph",
            "key_value",
            "paragraph",
        ]

    @pytest.mark.parametrize(
        "gap_over,rows",
        [pytest.param(-1.0, 2, id="gap-under-0.40-of-the-page"), pytest.param(1.0, 3, id="3-rows")],
    )
    def test_rows_with_a_narrower_gap_or_more_rows_are_a_table(self, tmp_path, gap_over, rows):
        grid, runs = masthead(tmp_path, gap_over, rows)

        assert described(laid_out(tmp_path, *runs)) == [("table", grid, True)]

    @pytest.mark.parametrize(
        "x,kind",
        [
            # "Name" ends at x 34: a gap of 400 pt is exactly 0.40 of a 1000 pt page, which is no masthead.
            pytest.param(434.0, StreamTable, id="exactly-0.40"),
            pytest.param(434.5, StreamParagraph, id="over-0.40"),
        ],
    )
    def test_a_masthead_gap_is_over_0_40_of_the_page(self, x, kind):
        rows = [line(cell("Name", 10.0, y), cell("Value", x, y)) for y in (10.0, 30.0)]

        (found,) = layout._row_structures(rows, width=1000.0, height=792.0)

        assert type(found.items[0]) is kind

    @pytest.mark.parametrize(
        "height,kinds",
        [
            # The rows run down the page: their gap (201 pt) is weighed against the page's height, not its width.
            pytest.param(500.0, [StreamParagraph] * 4, id="over-0.40-of-the-height"),
            pytest.param(2000.0, [StreamTable], id="under-0.40-of-the-height"),
        ],
    )
    def test_a_turned_masthead_weighs_its_gap_against_the_page_along_its_rows(self, height, kinds):
        rows = [line(down_cell("Name", x, 10.0), down_cell("Value", x, 235.0), turn=90) for x in (10.0, 30.0)]

        (found,) = layout._row_structures(rows, width=2000.0, height=height)

        assert [type(item) for item in found.items] == kinds

    @pytest.mark.parametrize("alone", [True, False], ids=["alone", "between-paragraphs"])
    def test_a_key_value_list_is_pairs_not_a_table(self, tmp_path, alone):
        runs = table_runs((("Weight:", "5 kg"), ("Colour:", "red"), ("Size:", "large")), (72.0, 200.0))
        around = () if alone else (TextRun(PARA, 72.0, 100.0), TextRun(PARA, 72.0, 230.0))
        pairs = [("key_value", "Weight", "5 kg"), ("key_value", "Colour", "red"), ("key_value", "Size", "large")]

        found = described(laid_out(tmp_path, *runs, *around))

        assert found == (pairs if alone else [("paragraph", PARA), *pairs, ("paragraph", PARA)])

    @pytest.mark.parametrize(
        "rows",
        [
            pytest.param((("Weight", "5 kg"), ("Colour", "red"), ("Size", "large")), id="keys-without-colons"),
            pytest.param((("Weight:", "5", "kg"), ("Colour:", "red", "-")), id="three-cells"),
            pytest.param((("Weight: 5", "kg"), ("Colour: red", "-")), id="values-in-the-key-cells"),
        ],
    )
    def test_rows_that_are_not_one_key_and_one_value_are_a_table(self, tmp_path, rows):
        assert described(laid_out(tmp_path, *table_runs(rows, (72.0, 200.0, 300.0)))) == [("table", rows, True)]

    def test_two_column_prose_reads_column_by_column(self, tmp_path):
        left = ("The woodland creatures gathered at", "dawn to plan the long campaign", "against the marquise.")
        right = ("Their sympathy spread from glade", "to glade, each clearing taking up", "the cause in turn.")
        items = (*column(left, 72.0, 130.0), *column(right, 316.0, 130.0))

        assert described(laid_out(tmp_path, *items)) == [("paragraph", " ".join(left)), ("paragraph", " ".join(right))]

    def test_two_page_columns_of_short_lines_on_one_baseline_grid_are_no_table(self, tmp_path):
        # Root's Law p. 31: an index in two columns, each running on past the lines the other shares its baselines
        # with. The XY-cut reads the columns apart, each holding text no row holds: page columns, not a table.
        left = ("Battle, 4.3", "ambush in, 4.3.1", "defenseless, 4.3.5", "Cards, 2.1", "Clearings, 2.2", "Craft, 4.1")
        right = ("Pieces", "limits of, 1.5.1", "ownership of, 1.5.3", "Rivers, 2.3", "Rule, 2.5", "Ruins, 2.2.4")
        items = (*column(left, 72.0, 100.0), *column(right, 330.0, 100.0))
        # Their last lines sit 8 pt apart: on no shared line (overlap 0.35 of a line), each close under its column.
        items += (TextRun("Dominance, 3.3", 72.0, 196.0), TextRun("Setup, 5.1", 330.0, 188.0))

        assert described(laid_out(tmp_path, *items)) == [
            ("paragraph", " ".join((*left, "Dominance, 3.3"))),
            ("paragraph", " ".join((*right, "Setup, 5.1"))),
        ]

    def test_a_table_whose_first_column_has_a_title_over_it_stays_a_table(self, tmp_path):
        # The title shares the first column's XY-cut segment; the other columns' segments hold only the table.
        items = laid_out(tmp_path, TextRun("Parts list", 72.0, 120.0), *table_runs(PARTS))

        assert described(items) == [("paragraph", "Parts list"), ("table", PARTS, True)]

    def test_a_table_the_xy_cut_leaves_whole_stays_a_table_beside_free_text(self, tmp_path):
        # The long second column covers the middle of the page, so the XY-cut finds no gutter: one segment,
        # shared with the paragraph under the table, holds both columns. They are not page columns.
        code = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        rows = (("A", code), ("B", code))

        assert described(laid_out(tmp_path, *table_runs(rows, (72.0, 130.0)), TextRun(PARA, 72.0, 200.0))) == [
            ("table", rows, True),
            ("paragraph", PARA),
        ]

    def test_a_key_value_list_in_page_columns_stays_pairs(self, tmp_path):
        # Only tables are tested for page columns: a key cell and its colon say the rows are pairs.
        runs = table_runs((("Weight:", "5 kg"), ("Colour:", "red")), (72.0, 330.0), top=100.0)
        after = (TextRun("Notes on the left", 72.0, 136.0), TextRun("Notes on the right", 330.0, 128.0))

        found = described(laid_out(tmp_path, *runs, *after))

        assert [item for item in found if item[0] == "key_value"] == [
            ("key_value", "Weight", "5 kg"),
            ("key_value", "Colour", "red"),
        ]

    @pytest.mark.parametrize(
        "first,kind",
        [
            # Two rows of two cells: the right ones of 1 word, the left ones of 4 words, or of 3 and 4 words.
            pytest.param("one two three four", "paragraph", id="half-the-cells-of-4-words"),
            pytest.param("one two three", "table", id="fewer"),
        ],
    )
    def test_rows_whose_cells_are_half_running_text_are_prose(self, tmp_path, first, kind):
        rows = ((first, "x"), ("five six seven eight", "y"))

        assert described(laid_out(tmp_path, *table_runs(rows, (72.0, 300.0))))[0][0] == kind


class TestStreamLayout:
    """The page's stream text in reading order: tables and rows kept whole, the rest classified."""

    def test_every_char_is_read_once(self, tmp_path):
        _, top = masthead(tmp_path, 1.0)
        xs, ys = (50.0, 150.0, 250.0), (300.0, 320.0, 340.0)
        ruled = rules(xs, ys) + written((("Part", "Qty"), ("Bolt", "4")), xs, ys)
        items = (*top, TextRun("Overview", 72.0, 110.0, size=16.0), *body_text(), *table_runs(PARTS, top=200.0))
        page, pieces = page_of(tmp_path, *items, *ruled, TextRun("• last", 72.0, 400.0))
        lattice = find_lattice_tables(page.segments.segments, pieces)

        found = stream_layout(pieces, lattice, width_pt=page.geometry.width_pt, height_pt=page.geometry.height_pt)

        def chars_of(source):
            return Counter(char for piece in source for char in piece.text if not char.isspace())

        visible = chars_of(piece for piece in pieces if not piece.hidden_reasons)
        assert chars_of(piece for item in found for piece in item.pieces) == visible - chars_of(lattice.used_pieces)
        assert [type(item).__name__ for item in found] == [
            "StreamParagraph",
            "StreamKeyValue",
            "StreamParagraph",
            "StreamKeyValue",
            "StreamHeading",
            "StreamParagraph",
            "StreamTable",
            "StreamList",
        ]

    def test_the_body_size_is_read_from_the_text_outside_the_rows(self, tmp_path):
        # The table's 160 chars of 12 pt outweigh the body's 10 pt: read with them, the body size would be 12 pt
        # and an 11.5 pt line no heading. Read without them, it is 1.15 times the body.
        rows = (("Washer M12", "100", "0.01"),) * 10
        table = table_runs(rows, top=200.0, pitch=16.0, size=12.0)
        items = (TextRun("Parts", 72.0, 100.0, size=11.5), *body_text(), *table)

        assert described(laid_out(tmp_path, *items))[0] == ("heading", 1, "Parts")

    def test_a_list_never_runs_across_a_table(self, tmp_path):
        items = (TextRun("• one", 72.0, 100.0), *table_runs(PARTS, top=130.0), TextRun("• two", 72.0, 200.0))

        assert described(laid_out(tmp_path, *items)) == [
            ("list", False, [("•", "one")]),
            ("table", PARTS, True),
            ("list", False, [("•", "two")]),
        ]

    def test_no_text_no_items(self):
        assert stream_layout((), NO_TABLES, width_pt=612.0, height_pt=792.0) == ()

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param(
                {"pieces": [chars("x", 0.0, 0.0)]}, TypeError, "pieces must be a tuple, got list", id="pieces"
            ),
            pytest.param({"lattice": None}, TypeError, "lattice must be a LatticeResult, got NoneType", id="lattice"),
            pytest.param({"width_pt": 0.0}, ValueError, "width_pt must be > 0, got 0.0", id="width-zero"),
            pytest.param(
                {"height_pt": "1"}, TypeError, "height_pt must be an int or float, got '1' (str)", id="height"
            ),
        ],
    )
    def test_invalid_argument_raises(self, overrides, error, message):
        fields = {"pieces": (), "lattice": NO_TABLES, "width_pt": 612.0, "height_pt": 792.0}

        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            stream_layout(**(fields | overrides))


class TestStreamLayoutBounds:
    """Adversarial pages of 20 000 pieces lay out in under 2 s: banding is O(c log c), the XY-cut O(D n log n)."""

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_grid_of_20_000_one_char_pieces_is_one_table(self):
        # Every char 24 pt clear of the next: over 1.8 em of 11 pt, so a cell of its own, in a column of its own.
        pieces = tuple(chars("x", 30.0 * col, 14.0 * row) for row in range(100) for col in range(200))

        started = time.perf_counter()
        (table,) = stream_layout(pieces, NO_TABLES, width_pt=6000.0, height_pt=1400.0)
        elapsed = time.perf_counter() - started

        assert (len(table.rows), table.n_cols) == (100, 200)
        assert elapsed < 2.0, f"{elapsed:.2f} s"

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_20_000_pieces_in_lines_that_never_fit_the_line_before(self):
        # Odd lines' first cell (x 0-60) spans both cells of an even line (x 0-10, 40-46); an even line's two
        # cells both fall in an odd line's first band. Every line ends a run and starts one.
        pieces = []
        for row in range(10_000):
            if row % 2:
                pieces += [chars("abcdefghij", 0.0, 14.0 * row), chars("k", 100.0, 14.0 * row)]
            else:
                pieces += [chars("ab", 0.0, 14.0 * row, advance=5.0), chars("c", 40.0, 14.0 * row)]

        started = time.perf_counter()
        found = stream_layout(tuple(pieces), NO_TABLES, width_pt=612.0, height_pt=140_100.0)
        elapsed = time.perf_counter() - started

        assert not [item for item in found if isinstance(item, StreamTable)]
        assert sum(len(item.pieces) for item in found) == 20_000
        assert elapsed < 2.0, f"{elapsed:.2f} s"


def two_rows():
    """Two lines of two cells: Part | Qty, Bolt | 4."""
    return (
        line(cell("Part", 10.0, 10.0), cell("Qty", 100.0, 10.0)),
        line(cell("Bolt", 10.0, 30.0), cell("4", 100.0, 30.0)),
    )


def table(**overrides) -> StreamTable:
    """A valid StreamTable of two_rows, with ``overrides``."""
    fields = {"rows": two_rows(), "columns": ((0, 1), (0, 1)), "column_centres": (22.0, 109.0)}
    return StreamTable(**(fields | {"bbox": box(10.0, 10.0, 124.0, 42.0)} | overrides))


class TestStreamTableRecord:
    """StreamTable is a frozen, slotted, picklable, validated record; its grid, header and pieces derive from it."""

    def test_pickle_round_trip_frozen_and_slotted(self):
        original = table()

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.rows = ()
        assert not hasattr(original, "__dict__")

    def test_grid_header_and_pieces(self):
        rows = (two_rows()[0], line(cell("4", 100.0, 30.0)))
        found = table(rows=rows, columns=((0, 1), (1,)))

        assert found.grid() == (("Part", "Qty"), ("", "4"))
        assert found.has_header
        assert found.n_cols == 2
        assert found.pieces == rows[0].cells[0].pieces + rows[0].cells[1].pieces + rows[1].cells[0].pieces

    def test_a_blank_first_row_cell_means_no_header(self):
        rows = (line(cell("Qty", 100.0, 10.0)), two_rows()[1])

        assert not table(rows=rows, columns=((1,), (0, 1))).has_header

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"rows": list(two_rows())}, TypeError, "rows must be a tuple, got list", id="rows-list"),
            pytest.param(
                {"rows": two_rows()[:1], "columns": ((0, 1),)},
                ValueError,
                "rows must hold at least 2 rows, got 1",
                id="one-row",
            ),
            pytest.param({"columns": [(0, 1), (0, 1)]}, TypeError, "columns must be a tuple, got list", id="list"),
            pytest.param(
                {"columns": ((0, 1),)}, ValueError, "columns must hold one entry per row (2), got 1", id="count"
            ),
            pytest.param(
                {"columns": ((0, 1), (0,))},
                ValueError,
                "columns[1] must place each of the row's 2 cells, got (0,)",
                id="row-count",
            ),
            pytest.param(
                {"columns": ((0, 1), (1, 0))},
                ValueError,
                "columns[1] must rise strictly within 0..1, got (1, 0)",
                id="not-rising",
            ),
            pytest.param(
                {"columns": ((0, 1), (0, 2))},
                ValueError,
                "columns[1] must rise strictly within 0..1, got (0, 2)",
                id="past-the-last",
            ),
            pytest.param(
                {"columns": ((0, 1), (0, True))},
                TypeError,
                "columns[1][1] must be an int, got True (bool)",
                id="column-bool",
            ),
            pytest.param(
                {"column_centres": [22.0, 109.0]},
                TypeError,
                "column_centres must be a tuple, got list",
                id="centres-list",
            ),
            pytest.param(
                {"column_centres": (22.0,)},
                ValueError,
                "column_centres must hold at least 2 columns, got 1",
                id="one-column",
            ),
            pytest.param(
                {"column_centres": (22.0, 109.0, 200.0)},
                ValueError,
                "every column must hold a cell: column 2 holds none",
                id="empty-column",
            ),
            pytest.param(
                {"column_centres": (22.0, float("nan"))},
                ValueError,
                "column_centres[1] must be finite, got nan",
                id="centre-nan",
            ),
            pytest.param({"bbox": None}, TypeError, "bbox must be a BBox, got NoneType", id="bbox-none"),
            pytest.param(
                {"bbox": box(10.0, 10.0, 50.0, 42.0)},
                ValueError,
                "rows[0].bbox must lie within the bbox, got BBox(x0=10.0, y0=10.0, x1=118.0, y1=22.0) in "
                "BBox(x0=10.0, y0=10.0, x1=50.0, y1=42.0)",
                id="row-outside",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            table(**overrides)
