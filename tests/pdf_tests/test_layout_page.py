"""Tests for the v5 page layout's units (#368, A4b group 3): lattice tables and figures in the reading order.

Unit tests build SpanPiece and LatticeTable values directly: boxes are
displayed, top-left ``(x0, y0, x1, y1)`` points.
"""

import ast
import dataclasses
import inspect
import pickle
import time
from pathlib import Path

import pytest

from datagrunt.pdf import assembly, layout
from datagrunt.pdf.layout import FigureSlot, LatticeResult, LatticeTable, PageLayout, StreamParagraph, page_layout
from datagrunt.pdf.pdfium_engine import extract_page, open_document, recover_dropped_text
from datagrunt.pdf.quality import assess_page
from pdf_tests.test_layout_apart import LEFT, RIGHT, column_of, hidden
from pdf_tests.test_layout_lattice import box, cell, chars
from pdf_tests.test_layout_stream import block, line
from pdf_tests.test_layout_stream import cell as text_cell

NO_TABLES = LatticeResult(tables=(), used_pieces=())
W, H = 400.0, 300.0


def laid_out(*pieces, lattice=NO_TABLES, figures=(), width: float = W, height: float = H) -> tuple:
    """page_layout's items for ``pieces`` with ``lattice`` and ``figures`` on a ``width`` x ``height`` pt page."""
    found = page_layout(pieces, (), lattice, width_pt=width, height_pt=height, figures=figures)
    assert found.warnings == ()
    return found.items


def named(items) -> list[str]:
    """Each item as a name: a paragraph's text (a column paragraph's as "left" / "right"), else its kind."""
    columns = {" ".join([LEFT] * count): f"left{count}" for count in (3, 4, 5, 6)}
    columns |= {" ".join([RIGHT] * count): f"right{count}" for count in (3, 4, 5, 6)}
    found = []
    for item in items:
        if isinstance(item, StreamParagraph):
            found.append(columns.get(item.text, item.text))
        elif isinstance(item, FigureSlot):
            found.append(f"figure{item.index}")
        elif isinstance(item, LatticeTable):
            found.append("lattice")
        else:
            found.append(type(item).__name__)
    return found


def ruled(x0: float, y0: float, x1: float, y1: float) -> LatticeTable:
    """A 2 x 2 lattice table filling ``(x0, y0)`` to ``(x1, y1)``, its cells blank."""
    mid_x, mid_y = (x0 + x1) / 2, (y0 + y1) / 2
    cells = (
        dataclasses.replace(cell(0, 0), bbox=box(x0, y0, mid_x, mid_y)),
        dataclasses.replace(cell(0, 1), bbox=box(mid_x, y0, x1, mid_y)),
        dataclasses.replace(cell(1, 0), bbox=box(x0, mid_y, mid_x, y1)),
        dataclasses.replace(cell(1, 1), bbox=box(mid_x, mid_y, x1, y1)),
    )
    return LatticeTable(bbox=box(x0, y0, x1, y1), n_rows=2, n_cols=2, cells=cells)


class TestFigureSlot:
    """FigureSlot is a frozen, slotted, picklable, validated record of where a figure reads."""

    def test_pickle_round_trip_frozen_and_slotted(self):
        original = FigureSlot(index=2, bbox=box(1.0, 2.0, 3.0, 4.0))

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.index = 1
        assert not hasattr(original, "__dict__")

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"index": -1, "bbox": box(0.0, 0.0, 1.0, 1.0)}, ValueError, "index", id="negative"),
            pytest.param({"index": True, "bbox": box(0.0, 0.0, 1.0, 1.0)}, TypeError, "index", id="bool"),
            pytest.param({"index": 0, "bbox": (0.0, 0.0, 1.0, 1.0)}, TypeError, "bbox", id="tuple-box"),
        ],
    )
    def test_invalid_field_raises(self, fields, error, message):
        with pytest.raises(error, match=message):
            FigureSlot(**fields)


class TestUnits:
    """Lattice tables and figures join the XY-cut as single units: dealt by geometry, never text (v4's rule)."""

    def test_a_figure_reads_where_it_lies(self):
        items = laid_out(
            chars("Above", 10.0, 10.0), chars("Below", 10.0, 160.0), figures=(box(10.0, 40.0, 200.0, 140.0),)
        )

        assert named(items) == ["Above", "figure0", "Below"]
        assert items[1] == FigureSlot(index=0, bbox=box(10.0, 40.0, 200.0, 140.0))

    def test_a_figure_beside_a_paragraph_never_splits_it(self):
        # v4 read images apart from text blocks: a figure beside a paragraph reads after it, the paragraph whole.
        lines = tuple(chars(text, 10.0, 10.0 + 14.0 * at) for at, text in enumerate(("one two", "three four", "five")))

        items = laid_out(*lines, figures=(box(200.0, 20.0, 300.0, 40.0),))

        assert named(items) == ["one two three four five", "figure0"]

    def test_a_figure_level_with_a_paragraph_reads_after_it(self):
        items = laid_out(
            chars("Level", 10.0, 10.0), chars("Below", 10.0, 200.0), figures=(box(200.0, 10.0, 300.0, 40.0),)
        )

        assert named(items) == ["Level", "figure0", "Below"]

    def test_a_figure_between_list_items_ends_the_list(self):
        bullets = (chars("\u2022 first", 10.0, 10.0), chars("\u2022 second", 10.0, 100.0))

        items = laid_out(*bullets, figures=(box(10.0, 40.0, 100.0, 80.0),))

        assert [type(item).__name__ for item in items] == ["StreamList", "FigureSlot", "StreamList"]

    def test_a_lattice_table_reads_where_it_lies(self):
        lattice = LatticeResult(tables=(ruled(10.0, 40.0, 200.0, 140.0),), used_pieces=())

        items = laid_out(chars("Above", 10.0, 10.0), chars("Below", 10.0, 160.0), lattice=lattice)

        assert named(items) == ["Above", "lattice", "Below"]
        assert items[1] is lattice.tables[0]

    def test_figures_keep_their_indices(self):
        figures = (box(10.0, 200.0, 60.0, 250.0), box(10.0, 10.0, 60.0, 60.0))

        assert named(laid_out(figures=figures)) == ["figure1", "figure0"]

    def test_a_figure_beside_short_text_reads_by_its_top(self):
        # v4's _filter_text_items: units add no width to a region, so text 6 pt wide is not cut away from them.
        items = laid_out(chars("A", 10.0, 10.0), chars("B", 10.0, 100.0), figures=(box(200.0, 40.0, 300.0, 60.0),))

        assert named(items) == ["A", "figure0", "B"]

    def test_units_alone_are_no_columns(self):
        # A region of no text is not cut (v4): figures read by their tops, then in order.
        figures = (box(10.0, 10.0, 100.0, 60.0), box(200.0, 10.0, 300.0, 60.0), box(10.0, 200.0, 100.0, 250.0))

        assert named(laid_out(figures=figures)) == ["figure0", "figure1", "figure2"]

    def test_a_unit_never_makes_a_column(self):
        # Text lies on one side of the gutter only, so there are no columns to cut (A4b group 1's fix of v4):
        # a figure on the other side changes nothing, and the wide lines read as one paragraph, not a band each.
        wide = tuple(chars("this is a wide line of prose text", 100.0, 10.0 + 14.0 * at) for at in range(3))

        items = laid_out(*wide, chars("Index", 260.0, 100.0), figures=(box(20.0, 200.0, 60.0, 240.0),))

        assert named(items) == [" ".join(["this is a wide line of prose text"] * 3), "Index", "figure0"]

    def test_a_figure_across_the_gutter_cuts_the_columns_into_bands(self):
        top = column_of(LEFT, 10.0, 3) + column_of(RIGHT, 200.0, 3)
        below = tuple(chars(LEFT, 10.0, 150.0 + 14.0 * at) for at in range(3))
        below += tuple(chars(RIGHT, 200.0, 150.0 + 14.0 * at) for at in range(3))

        items = laid_out(*top, *below, figures=(box(10.0, 70.0, 326.0, 130.0),))

        assert named(items) == ["left3", "right3", "figure0", "left3", "right3"]

    def test_a_figure_across_the_gutter_beside_column_text_cuts_no_band(self):
        # Column text flanks the figure (lines start beside it): a band there would read the columns' lines
        # across the gutter as one. It reads in the column of its centre instead, after that column's paragraph.
        items = laid_out(
            *column_of(LEFT, 10.0, 6), *column_of(RIGHT, 200.0, 6), figures=(box(100.0, 30.0, 250.0, 60.0),)
        )

        assert named(items) == ["left6", "right6", "figure0"]

    def test_a_figure_level_with_column_lines_across_the_gutter_cuts_no_band(self):
        # The figure starts where a line of each column does: those lines start beside it.
        items = laid_out(
            *column_of(LEFT, 10.0, 6), *column_of(RIGHT, 200.0, 6), figures=(box(100.0, 38.0, 250.0, 50.0),)
        )

        assert named(items) == ["left6", "right6", "figure0"]

    def test_a_figure_ending_where_column_lines_start_still_cuts_a_band(self):
        top = column_of(LEFT, 10.0, 3) + column_of(RIGHT, 200.0, 3)
        below = tuple(chars(LEFT, 10.0, 130.0 + 14.0 * at) for at in range(3))
        below += tuple(chars(RIGHT, 200.0, 130.0 + 14.0 * at) for at in range(3))

        items = laid_out(*top, *below, figures=(box(10.0, 70.0, 326.0, 130.0),))

        assert named(items) == ["left3", "right3", "figure0", "left3", "right3"]

    def test_a_far_figure_never_moves_the_gutter_window(self):
        # A region's extent is its text's (v4): a figure far right on a wide page leaves the columns' gutter found.
        items = laid_out(
            *column_of(LEFT, 10.0), *column_of(RIGHT, 200.0), figures=(box(1500.0, 10.0, 1990.0, 60.0),), width=2000.0
        )

        assert named(items) == ["left4", "right4", "figure0"]

    def test_columns_without_the_figure_read_down_each_column(self):
        top = column_of(LEFT, 10.0, 3) + column_of(RIGHT, 200.0, 3)
        below = tuple(chars(LEFT, 10.0, 150.0 + 14.0 * at) for at in range(3))
        below += tuple(chars(RIGHT, 200.0, 150.0 + 14.0 * at) for at in range(3))

        assert named(laid_out(*top, *below)) == ["left3", "left3", "right3", "right3"]

    def test_a_figure_off_the_page_reads_by_its_nearest_item(self):
        items = laid_out(
            chars("First", 10.0, 10.0), chars("Second", 10.0, 200.0), figures=(box(-300.0, 205.0, -100.0, 260.0),)
        )

        assert named(items) == ["First", "Second", "figure0"]

    def test_hidden_text_and_off_page_figures_read_by_position_together(self):
        items = laid_out(
            chars("First", 10.0, 10.0),
            hidden("SECRET", 10.0, 230.0),
            chars("Second", 10.0, 200.0),
            figures=(box(-300.0, 210.0, -100.0, 220.0),),
        )

        # Both read after Second, their nearest item, top to bottom.
        assert named(items) == ["First", "Second", "figure0", "ApartLine"]

    def test_hidden_text_on_a_background_figure_reads_with_the_text_it_touches(self):
        # The page-sized figure touches the line as the right column does, and its centre is the nearer (57 pt
        # against 64): the paragraph still anchors it, as hidden text reads with the visible text it lies in. The
        # figure is the columns' background, so it reads before them (#409), not between them.
        columns = (*column_of(LEFT, 10.0, 6), *column_of(RIGHT, 200.0, 6))

        items = laid_out(*columns, hidden("SECRET", 200.0, 90.0), figures=(box(0.0, 0.0, W, H),))

        assert named(items) == ["figure0", "left6", "right6", "ApartLine"]

    def test_hidden_text_on_a_figure_alone_reads_with_it(self):
        items = laid_out(
            chars("Text", 10.0, 10.0), hidden("SECRET", 210.0, 210.0), figures=(box(200.0, 200.0, 300.0, 250.0),)
        )

        assert named(items) == ["Text", "figure0", "ApartLine"]

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_20_000_figure_boxes_lay_out_in_linear_time(self):
        # A quarter reach past the page's left edge: one pass splits them from the rest. Comparing each with every
        # on-page slot (a tuple's "in") took 5.3 s on a laptop.
        boxes = tuple(
            box(
                5.0 * (at % 200) - 500.0 * (at % 2),
                20.0 + 5.0 * (at // 200),
                5.0 * (at % 200) + 4.0,
                24.0 + 5.0 * (at // 200),
            )
            for at in range(20_000)
        )

        started = time.perf_counter()
        found = page_layout(
            (chars("Visible body text", 10.0, 2.0),), (), NO_TABLES, width_pt=1000.0, height_pt=600.0, figures=boxes
        )
        elapsed = time.perf_counter() - started

        slots = [item for item in found.items if isinstance(item, FigureSlot)]
        assert sorted(slot.index for slot in slots) == list(range(20_000))
        assert elapsed < 2.0, f"{elapsed:.2f} s"

    def test_off_page_figures_past_the_placement_bound_are_named_alone(self, monkeypatch):
        monkeypatch.setattr(layout, "MAX_APART_PAIRS", 1)
        figures = (box(-300.0, 10.0, -100.0, 60.0), box(-300.0, 100.0, -100.0, 160.0))

        found = page_layout((chars("Body", 10.0, 10.0),), (), NO_TABLES, width_pt=W, height_pt=H, figures=figures)

        assert found.warnings == (
            "2 off-page figure(s) read after the page's visible text: placing them by position would compare "
            "2 pairs, over the bound of 1",
        )

    def test_a_unit_is_placed_once_when_a_structure_starts_above_the_paragraph_before_it(self):
        # A structure is placed by its ink, a paragraph by its loose box, so the two tops can run back a little.
        paragraph = block(line(text_cell("Body", 10.0, 50.0)))
        structure = layout._Rows(rows=(line(text_cell("Row", 10.0, 40.0)),), items=())
        unit = FigureSlot(index=0, bbox=box(10.0, 45.0, 20.0, 46.0))

        slots = layout._with_units([[paragraph], structure], [(45.0, unit)], 0)

        assert slots == [unit, [paragraph], structure]

    def test_figures_must_be_a_tuple_of_boxes(self):
        with pytest.raises(TypeError, match="figures"):
            page_layout((), (), NO_TABLES, width_pt=W, height_pt=H, figures=[box(0.0, 0.0, 1.0, 1.0)])

    def test_page_layout_holds_tables_and_figure_slots(self):
        slot, ruling = FigureSlot(index=0, bbox=box(0.0, 0.0, 1.0, 1.0)), ruled(0.0, 0.0, 20.0, 20.0)

        assert PageLayout(items=(slot, ruling), warnings=()).items == (slot, ruling)


def five_lines(text: str, x: float) -> tuple:
    """Five visible lines of ``text`` from ``x``, 17 pt apart from y 10: one paragraph whose ink ends at y 90."""
    return tuple(chars(text, x, 10.0 + 17.0 * at) for at in range(5))


class TestBackgrounds:
    """A figure covering 90% of a region the XY-cut cuts is its background: read first, never dealt to a column (#409).

    A4b dealt every unit to a column by its centre, so a page-sized background read between the columns.
    """

    def test_a_page_sized_figure_reads_before_the_columns(self):
        items = laid_out(*column_of(LEFT, 10.0, 6), *column_of(RIGHT, 200.0, 6), figures=(box(0.0, 0.0, W, H),))

        assert named(items) == ["figure0", "left6", "right6"]

    @pytest.mark.parametrize(
        "bottom,expected",
        [
            # The columns' ink spans x 10-326 and y 10-90. The figure runs from y 0 across x 10-326 to ``bottom``:
            # 72 of the 80 pt is 90% (the float product is exact), 71.2 pt 89%, dealt by its centre (x 168) to the
            # right column, where it reads by its top, as before #409.
            pytest.param(82.0, ["figure0", "left5", "right5"], id="90-percent"),
            pytest.param(81.2, ["left5", "figure0", "right5"], id="89-percent"),
        ],
    )
    def test_a_figure_covering_90_percent_of_the_region_is_its_background(self, bottom, expected):
        items = laid_out(*five_lines(LEFT, 10.0), *five_lines(RIGHT, 200.0), figures=(box(10.0, 0.0, 326.0, bottom),))

        assert named(items) == expected

    def test_a_figure_under_one_column_is_that_columns_background(self):
        # The right half (x 200-400) splits into two columns of its own, of 4-word lines (prose, not a table). The
        # figure covers 53% of the page's text, so the first cut deals it to the right half by its centre; it covers
        # all of the right half's, so it reads first there, not in the half's right column by its centre (x 302.5).
        inner = (*column_of("one two six ten", 200.0, 6), *column_of("red tan sky sea", 310.0, 6))

        items = laid_out(*column_of(LEFT, 10.0, 6), *inner, figures=(box(195.0, 0.0, 410.0, 100.0),), width=500.0)

        assert named(items) == [
            "left6",
            "figure0",
            " ".join(["one two six ten"] * 6),
            " ".join(["red tan sky sea"] * 6),
        ]

    def test_a_lattice_table_over_the_columns_is_dealt_as_before(self):
        # Only a figure is a background: a ruled table covering the columns is dealt by its centre.
        lattice = LatticeResult(tables=(ruled(0.0, 0.0, W, H),), used_pieces=())

        items = laid_out(*column_of(LEFT, 10.0, 6), *column_of(RIGHT, 200.0, 6), lattice=lattice)

        assert named(items) == ["left6", "lattice", "right6"]

    def test_a_figure_clear_of_the_text_covers_none_of_it(self):
        # Below and right of the columns' ink (x 10-326, y 10-92): no overlap either way, which is none (the product
        # of the two shortfalls, 674 x 908 pt, would pass 90% of the text's 316 x 82 pt).
        columns = (*column_of(LEFT, 10.0, 6), *column_of(RIGHT, 200.0, 6))

        items = laid_out(*columns, figures=(box(1000.0, 1000.0, 1100.0, 1100.0),), width=2000.0, height=2000.0)

        assert named(items) == ["left6", "right6", "figure0"]

    def test_a_region_whose_text_has_no_area_has_no_background(self):
        # Two runs of no height cut at x 165: of no area, nothing is 90% covered (0 >= 0.9 x 0 would make the far
        # figure a background), so the figure is dealt by its centre (x 200) to the right, as any unit.
        boxes = [(10.0, 10.0, 130.0, 10.0), (200.0, 10.0, 326.0, 10.0), (0.0, 0.0, 400.0, 300.0)]

        assert layout._xy_cut(boxes, [20, 21, 0], [True, True, False], [False, False, True]) == [[0], [1, 2]]

    def test_a_figure_under_a_region_not_cut_reads_by_its_top(self):
        # One column is no cut, so the figure reads where it starts, as any unit, though it covers 94% of the text
        # (y 15-92 of 10-92): no column is dealt there, and a figure between a title and its caption covers most of
        # theirs too.
        items = laid_out(*column_of(LEFT, 10.0, 6), figures=(box(0.0, 15.0, W, H),))

        assert named(items) == ["left6", "figure0"]


LAW = Path(__file__).resolve().parents[2] / "data" / "pdfs" / "Root_Base_Law_Oct_2025.pdf"


@pytest.mark.corpus
@pytest.mark.skipif(not LAW.exists(), reason="Root_Base corpus not present under data/pdfs/ (local-only).")
def test_root_law_page_4_reads_its_background_first_then_column_by_column():
    # A4b's page-sized background (figure 0) read between the columns: it reads first now, then the left column's
    # text, then the right's.
    handle = open_document(LAW)
    raw = extract_page(handle, 3)
    recovered = recover_dropped_text(handle, 3) if raw.text.stats.dropped_text_objects else None
    assessment = assess_page(raw, recovered=recovered)
    lattice = layout.find_lattice_tables(raw.segments.segments, assessment.pieces)
    figures = tuple(draft.bbox for draft in assembly.page_figures(raw.images, raw.geometry, source=LAW).drafts)
    width, height = raw.geometry.width_pt, raw.geometry.height_pt

    found = page_layout(
        assessment.pieces, assessment.recovered_pieces, lattice, width_pt=width, height_pt=height, figures=figures
    )

    first = found.items[0]
    assert isinstance(first, FigureSlot) and first.index == 0
    assert (first.bbox.x1 - first.bbox.x0) * (first.bbox.y1 - first.bbox.y0) > 0.99 * width * height
    sides = [item.bbox.x0 > width / 2 for item in found.items if isinstance(item, layout._STREAM)]
    assert sides == sorted(sides) and False in sides and True in sides


# Statements that open a block of control flow: each one inside another nests a level deeper.
BLOCKS = (ast.For, ast.AsyncFor, ast.While, ast.If, ast.With, ast.AsyncWith, ast.Try, ast.Match)


def nesting(node: ast.AST, level: int = 0) -> int:
    """The deepest nesting of control-flow blocks under ``node``: an ``elif`` is its ``if``'s level, not one deeper."""
    deepest = level
    for field, value in ast.iter_fields(node):
        children = value if isinstance(value, list) else [value]
        for child in children:
            if not isinstance(child, ast.AST) or isinstance(child, (ast.FunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            elif_ = isinstance(node, ast.If) and field == "orelse" and len(children) == 1 and isinstance(child, ast.If)
            deepest = max(deepest, nesting(child, level + (isinstance(child, BLOCKS) and not elif_)))
    return deepest


class TestNesting:
    """Flat flow (the clean-code canon): no function of the layout or the assembly nests over 3 blocks."""

    @pytest.mark.parametrize("module", [layout, assembly], ids=lambda module: module.__name__)
    def test_no_function_nests_over_3_blocks(self, module):
        tree = ast.parse(inspect.getsource(module))
        functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]

        assert {node.name: nesting(node) for node in functions if nesting(node) > 3} == {}

    def test_the_measure_counts_blocks_in_blocks_and_not_elif(self):
        flat = ast.parse("def f():\n    if a:\n        pass\n    elif b:\n        pass\n    elif c:\n        pass\n")
        deep = ast.parse("def f():\n for a in b:\n  for c in d:\n   while e:\n    if f:\n     pass\n")

        assert (nesting(flat.body[0]), nesting(deep.body[0])) == (1, 4)
