"""Tests for the v5 page layout's units (#368, A4b group 3): lattice tables and figures in the reading order.

Unit tests build SpanPiece and LatticeTable values directly: boxes are
displayed, top-left ``(x0, y0, x1, y1)`` points.
"""

import dataclasses
import pickle
import time

import pytest

from datagrunt.pdf import layout
from datagrunt.pdf.layout import FigureSlot, LatticeResult, LatticeTable, PageLayout, StreamParagraph, page_layout
from pdf_tests.test_layout_apart import LEFT, RIGHT, column_of, hidden
from pdf_tests.test_layout_lattice import box, cell, chars

NO_TABLES = LatticeResult(tables=(), used_pieces=())
W, H = 400.0, 300.0


def laid_out(*pieces, lattice=NO_TABLES, figures=(), width: float = W, height: float = H) -> tuple:
    """page_layout's items for ``pieces`` with ``lattice`` and ``figures`` on a ``width`` x ``height`` pt page."""
    found = page_layout(pieces, (), lattice, width_pt=width, height_pt=height, figures=figures)
    assert found.warnings == ()
    return found.items


def named(items) -> list[str]:
    """Each item as a name: a paragraph's text (a column paragraph's as "left" / "right"), else its kind."""
    columns = {" ".join([LEFT] * count): f"left{count}" for count in (3, 4, 6)}
    columns |= {" ".join([RIGHT] * count): f"right{count}" for count in (3, 4, 6)}
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
        # against 64): the paragraph still anchors it, as hidden text reads with the visible text it lies in.
        columns = (*column_of(LEFT, 10.0, 6), *column_of(RIGHT, 200.0, 6))

        items = laid_out(*columns, hidden("SECRET", 200.0, 90.0), figures=(box(0.0, 0.0, W, H),))

        assert named(items) == ["left6", "figure0", "right6", "ApartLine"]

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

    def test_figures_must_be_a_tuple_of_boxes(self):
        with pytest.raises(TypeError, match="figures"):
            page_layout((), (), NO_TABLES, width_pt=W, height_pt=H, figures=[box(0.0, 0.0, 1.0, 1.0)])

    def test_page_layout_holds_tables_and_figure_slots(self):
        slot, ruling = FigureSlot(index=0, bbox=box(0.0, 0.0, 1.0, 1.0)), ruled(0.0, 0.0, 20.0, 20.0)

        assert PageLayout(items=(slot, ruling), warnings=()).items == (slot, ruling)
