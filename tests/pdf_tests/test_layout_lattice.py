"""Tests for the v5 vector-lattice table pass (#367, A4a).

Unit tests build RawSegment and SpanPiece values directly: boxes are
displayed, top-left ``(x0, y0, x1, y1)`` points.
"""

import dataclasses
import pickle
import re

import pytest

from datagrunt.pdf import layout, quality
from datagrunt.pdf._path_segments import RawSegment
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.layout import LatticeCell, LatticeResult, LatticeTable
from datagrunt.pdf.quality import SpanPiece
from pdf_tests.test_pdfium_engine import ENGINE_MODULES, imported_modules
from pdf_tests.test_quality import raw_span


def box(x0: float, y0: float, x1: float, y1: float) -> BBox:
    """A1's BBox of ``(x0, y0, x1, y1)``."""
    return BBox(x0=x0, y0=y0, x1=x1, y1=y1)


def cell(row: int = 0, col: int = 0, text: str = "", *, row_span: int = 1, col_span: int = 1, bbox=None):
    """A LatticeCell on a grid of 10 pt squares from (0, 0), its box from its position unless given."""
    if bbox is None:
        bbox = box(col * 10.0, row * 10.0, (col + col_span) * 10.0, (row + row_span) * 10.0)
    return LatticeCell(row=row, col=col, row_span=row_span, col_span=col_span, bbox=bbox, text=text)


def table(cells, n_rows: int, n_cols: int, *, bbox=None) -> LatticeTable:
    """A LatticeTable of ``cells`` on the 10 pt grid, its box the whole grid unless given."""
    bbox = box(0.0, 0.0, n_cols * 10.0, n_rows * 10.0) if bbox is None else bbox
    return LatticeTable(bbox=bbox, n_rows=n_rows, n_cols=n_cols, cells=cells)


def plain(texts) -> LatticeTable:
    """A table of one unmerged cell per text, ``texts`` a list of rows."""
    cells = tuple(cell(r, c, text) for r, row in enumerate(texts) for c, text in enumerate(row))
    return table(cells, len(texts), len(texts[0]))


# A 3 x 3 table: a header cell across two columns, a cell down two rows.
#   +-------------+-----+
#   | Group       | C   |
#   +------+------+-----+
#   | Tall | b1   | c1  |
#   |      +------+-----+
#   |      | b2   | c2  |
#   +------+------+-----+
MERGED = (
    cell(0, 0, "Group", col_span=2),
    cell(0, 2, "C"),
    cell(1, 0, "Tall", row_span=2),
    cell(1, 1, "b1"),
    cell(1, 2, "c1"),
    cell(2, 1, "b2"),
    cell(2, 2, "c2"),
)


class TestModuleImports:
    """layout is pure and internal: no pdfium, no v4 stack, nothing exported."""

    def test_imports_no_pdfium_and_no_v4_stack(self):
        banned = ("pypdfium2", "fitz", "pymupdf", "pdfplumber", "datagrunt.core.pdf_io", "datagrunt.pdf_api")

        assert not [name for name in imported_modules(layout) if name.lower().startswith(banned)]

    def test_imports_only_the_raw_fact_types_quality_and_a1(self):
        allowed = {
            "datagrunt.pdf._path_segments",
            "datagrunt.pdf._validate",
            "datagrunt.pdf.elements",
            "datagrunt.pdf.quality",
        }

        assert {name for name in imported_modules(layout) if name.startswith("datagrunt")} == allowed

    @pytest.mark.parametrize("module", [*ENGINE_MODULES, quality], ids=lambda module: module.__name__)
    def test_the_engine_and_quality_never_import_layout(self, module):
        assert "datagrunt.pdf.layout" not in imported_modules(module)

    def test_is_not_exported(self):
        import datagrunt
        import datagrunt.pdf

        for package in (datagrunt, datagrunt.pdf):
            assert not {"LatticeCell", "LatticeTable", "LatticeResult", "find_lattice_tables"} & set(dir(package))


class TestLatticeCell:
    """LatticeCell is a frozen, slotted, picklable, validated record."""

    def test_pickle_round_trip(self):
        original = cell(1, 2, "Value", row_span=2, col_span=3)

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = cell()

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.text = "x"
        assert not hasattr(value, "__dict__")

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"row": True}, TypeError, "row must be an int, got True (bool)", id="row-bool"),
            pytest.param({"row": -1}, ValueError, "row must be >= 0, got -1", id="row-negative"),
            pytest.param({"col": 1.0}, TypeError, "col must be an int, got 1.0 (float)", id="col-float"),
            pytest.param({"col": -1}, ValueError, "col must be >= 0, got -1", id="col-negative"),
            pytest.param({"row_span": 0}, ValueError, "row_span must be >= 1, got 0", id="row-span-zero"),
            pytest.param({"col_span": 0}, ValueError, "col_span must be >= 1, got 0", id="col-span-zero"),
            pytest.param({"col_span": False}, TypeError, "col_span must be an int, got False (bool)", id="span-bool"),
            pytest.param({"bbox": (0.0, 0.0, 1.0, 1.0)}, TypeError, "bbox must be a BBox, got tuple", id="bbox-tuple"),
            pytest.param({"text": None}, TypeError, "text must be a str, got NoneType", id="text-none"),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        fields = {"row": 0, "col": 0, "row_span": 1, "col_span": 1, "bbox": box(0, 0, 10, 10), "text": ""}

        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            LatticeCell(**(fields | overrides))


class TestLatticeTable:
    """LatticeTable's cells tile its grid exactly once; grid() and has_header follow v4."""

    def test_pickle_round_trip(self):
        original = table(MERGED, 3, 3)

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = table(MERGED, 3, 3)

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.n_rows = 4
        assert not hasattr(value, "__dict__")

    def test_grid_puts_a_merged_cells_text_top_left_and_blanks_what_it_spans(self):
        # v4 / pdfplumber's convention: no text is duplicated into the positions a merged cell covers.
        assert table(MERGED, 3, 3).grid() == (
            ("Group", "", "C"),
            ("Tall", "b1", "c1"),
            ("", "b2", "c2"),
        )

    def test_has_header_when_every_row_0_cell_is_non_blank(self):
        assert plain([["Name", "Value"], ["a", "1"]]).has_header is True

    @pytest.mark.parametrize(
        "texts",
        [
            pytest.param([["Name", " "], ["a", "1"]], id="blank-header-cell"),
            pytest.param([["Name", ""], ["a", "1"]], id="empty-header-cell"),
            pytest.param([["Name", "Value"]], id="one-row"),
        ],
    )
    def test_has_no_header_with_a_blank_row_0_cell_or_a_single_row(self, texts):
        assert plain(texts).has_header is False

    def test_a_header_cell_merged_across_columns_leaves_blank_grid_cells_so_no_header(self):
        # The grid holds "" where "Group" spans, as v4 held None.
        assert table(MERGED, 3, 3).has_header is False

    @pytest.mark.parametrize(
        "cells,n_rows,n_cols,error,message",
        [
            pytest.param(MERGED, 0, 3, ValueError, "n_rows must be >= 1, got 0", id="no-rows"),
            pytest.param(MERGED, 3, True, TypeError, "n_cols must be an int, got True (bool)", id="cols-bool"),
            pytest.param(list(MERGED), 3, 3, TypeError, "cells must be a tuple, got list", id="cells-list"),
            pytest.param((cell(), "x"), 1, 2, TypeError, "cells[1] must be a LatticeCell, got str", id="cell-str"),
            pytest.param(
                (cell(0, 0), cell(0, 1, col_span=2)),
                1,
                2,
                ValueError,
                "cells[1] at row 0, col 1 spans 1 x 2 past the 1 x 2 grid",
                id="past-the-columns",
            ),
            pytest.param(
                (cell(0, 0, row_span=2), cell(0, 1)),
                1,
                2,
                ValueError,
                "cells[0] at row 0, col 0 spans 2 x 1 past the 1 x 2 grid",
                id="past-the-rows",
            ),
            pytest.param(
                (cell(0, 1), cell(0, 0)),
                1,
                2,
                ValueError,
                "cells must be in row-major order of their top-left positions: cells[1] is at row 0, col 0",
                id="out-of-order",
            ),
            pytest.param(
                (cell(0, 0), cell(0, 0)),
                1,
                1,
                ValueError,
                "cells must be in row-major order of their top-left positions: cells[1] is at row 0, col 0",
                id="duplicate",
            ),
            pytest.param(
                (cell(0, 0, col_span=2), cell(1, 0, col_span=2)),
                2,
                2,
                ValueError,
                "every grid column must start a cell: column 1 starts none",
                id="redundant-column",
            ),
            pytest.param(
                (cell(0, 0, row_span=2), cell(0, 1, row_span=2)),
                2,
                2,
                ValueError,
                "every grid row must start a cell: row 1 starts none",
                id="redundant-row",
            ),
            pytest.param(
                (cell(0, 0), cell(0, 1, row_span=2), cell(1, 0, col_span=2)),
                2,
                2,
                ValueError,
                "cells must tile the 2 x 2 grid exactly once: cells[2] at row 1, col 0 overlaps another cell",
                id="overlap",
            ),
            pytest.param(
                (cell(0, 0), cell(0, 1), cell(1, 0), cell(2, 0), cell(2, 1)),
                3,
                2,
                ValueError,
                "cells must tile the 3 x 2 grid exactly once: cells[4] at row 2, col 1 leaves a gap",
                id="gap-above",
            ),
            pytest.param(
                (cell(0, 0), cell(0, 1), cell(1, 0)),
                2,
                2,
                ValueError,
                "cells must tile the 2 x 2 grid exactly once: row 1, col 1 is in no cell",
                id="gap-at-the-end",
            ),
        ],
    )
    def test_invalid_grid_raises(self, cells, n_rows, n_cols, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            table(cells, n_rows, n_cols)

    def test_a_cell_box_outside_the_table_box_raises(self):
        cells = (cell(0, 0), cell(0, 1, bbox=box(10.0, 0.0, 20.5, 10.0)))

        with pytest.raises(ValueError, match=r"^cells\[1\]\.bbox must lie within the table's bbox"):
            table(cells, 1, 2)

    def test_table_bbox_must_be_a_bbox(self):
        with pytest.raises(TypeError, match="^bbox must be a BBox, got tuple$"):
            table((cell(),), 1, 1, bbox=(0.0, 0.0, 10.0, 10.0))


def used(text: str = "Abcd", start: int = 0, end: int = 4, reasons=()) -> SpanPiece:
    """A SpanPiece of ``text[start:end]``."""
    return SpanPiece(span=raw_span(text), start=start, end=end, hidden_reasons=reasons)


class TestLatticeResult:
    """LatticeResult holds the tables and the visible text they consumed."""

    def test_pickle_round_trip(self):
        original = LatticeResult(tables=(table(MERGED, 3, 3),), used_pieces=(used(),))

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        value = LatticeResult(tables=(), used_pieces=())

        with pytest.raises(dataclasses.FrozenInstanceError):
            value.tables = ()
        assert not hasattr(value, "__dict__")

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"tables": [table(MERGED, 3, 3)]}, TypeError, "tables must be a tuple, got list", id="list"),
            pytest.param({"tables": (cell(),)}, TypeError, "tables[0] must be a LatticeTable, got LatticeCell", id="t"),
            pytest.param({"used_pieces": ("Abcd",)}, TypeError, "used_pieces[0] must be a SpanPiece, got str", id="p"),
            pytest.param(
                {"used_pieces": (used(), used(reasons=("invisible",)))},
                ValueError,
                "used_pieces[1] is hidden (('invisible',)): hidden text never enters a table",
                id="hidden",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        fields = {"tables": (), "used_pieces": ()}

        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            LatticeResult(**(fields | overrides))


def seg(orientation: str, position: float, start: float, end: float, **overrides) -> RawSegment:
    """A RawSegment: a 0.5 pt black stroke unless ``overrides`` say otherwise."""
    fields = {
        "orientation": orientation,
        "position": position,
        "start": start,
        "end": end,
        "width": 0.5,
        "kind": "stroke",
        "object_index": 0,
        "dashed": False,
        "paint_rgba": (0, 0, 0, 255),
    }
    return RawSegment(**(fields | overrides))


def h(y: float, x0: float, x1: float, **overrides) -> RawSegment:
    """A horizontal segment at ``y`` from ``x0`` to ``x1``."""
    return seg("h", y, x0, x1, **overrides)


def v(x: float, y0: float, y1: float, **overrides) -> RawSegment:
    """A vertical segment at ``x`` from ``y0`` to ``y1``."""
    return seg("v", x, y0, y1, **overrides)


def snapped(*segments: RawSegment):
    """The snapped, merged lines as ``(horizontal, vertical)`` lists of ``(position, start, end)``."""
    horizontal, vertical = layout._snapped_lines(segments)
    return [tuple(line) for line in horizontal], [tuple(line) for line in vertical]


class TestSnapping:
    """Positions within 2 pt of each other snap to their mean, per orientation (chained, as pdfplumber's)."""

    def test_positions_within_2_pt_snap_to_their_mean(self):
        assert snapped(h(100.0, 10.0, 90.0), h(101.0, 10.0, 90.0), h(102.0, 10.0, 90.0)) == ([(101.0, 10.0, 90.0)], [])

    def test_exactly_2_pt_apart_snap(self):
        assert snapped(v(100.0, 10.0, 90.0), v(102.0, 10.0, 90.0)) == ([], [(101.0, 10.0, 90.0)])

    def test_more_than_2_pt_apart_stay_apart(self):
        assert snapped(v(100.0, 10.0, 90.0), v(102.25, 10.0, 90.0)) == ([], [(100.0, 10.0, 90.0), (102.25, 10.0, 90.0)])

    def test_snapping_chains_through_close_neighbours(self):
        # 100 and 103 are 3 pt apart, but each is within 2 pt of 101.5: one position, their mean.
        got = snapped(h(103.0, 10.0, 90.0), h(100.0, 10.0, 90.0), h(101.5, 10.0, 90.0))

        assert got == ([(101.5, 10.0, 90.0)], [])

    def test_snapping_is_per_page_so_lines_far_apart_along_the_axis_share_a_position(self):
        got = snapped(h(100.0, 0.0, 50.0), h(101.0, 200.0, 250.0))

        assert got == ([(100.5, 0.0, 50.0), (100.5, 200.0, 250.0)], [])

    def test_orientations_never_snap_together(self):
        assert snapped(h(100.0, 10.0, 90.0), v(101.0, 10.0, 90.0)) == ([(100.0, 10.0, 90.0)], [(101.0, 10.0, 90.0)])


class TestCollinearMerge:
    """Snapped lines at one position merge where they overlap, touch or leave a gap of at most 2 pt."""

    @pytest.mark.parametrize(
        "second,expected",
        [
            pytest.param((40.0, 120.0), [(10.0, 120.0)], id="overlapping"),
            pytest.param((20.0, 30.0), [(10.0, 50.0)], id="contained"),
            pytest.param((50.0, 120.0), [(10.0, 120.0)], id="touching"),
            pytest.param((52.0, 120.0), [(10.0, 120.0)], id="gap-of-2-pt"),
            pytest.param((52.25, 120.0), [(10.0, 50.0), (52.25, 120.0)], id="gap-over-2-pt"),
        ],
    )
    def test_merge(self, second, expected):
        horizontal, _ = snapped(h(100.0, *second), h(100.0, 10.0, 50.0))

        assert horizontal == [(100.0, start, end) for start, end in expected]

    def test_a_chain_of_short_pieces_is_one_line(self):
        pieces = [v(40.0, 10.0 + 12.0 * index, 22.0 + 12.0 * index) for index in range(10)]

        assert snapped(*reversed(pieces)) == ([], [(40.0, 10.0, 130.0)])

    def test_snapped_pieces_merge(self):
        # A rule drawn in two pieces 1 pt apart in y: one line at the mean.
        assert snapped(h(100.0, 10.0, 60.0), h(101.0, 55.0, 120.0)) == ([(100.5, 10.0, 120.0)], [])

    def test_every_kind_dash_and_colour_rules(self):
        # v4's pdfplumber (and the A0 prototype) rule a table with every edge: fill edges, thin fills,
        # dashed and white strokes alike.
        got = snapped(
            h(100.0, 10.0, 40.0, kind="fill_edge", width=0.0),
            h(100.0, 40.0, 70.0, kind="thin_fill", width=2.0),
            h(100.0, 70.0, 100.0, dashed=True),
            h(100.0, 100.0, 130.0, paint_rgba=(255, 255, 255, 255)),
        )

        assert got == ([(100.0, 10.0, 130.0)], [])

    def test_no_segments_no_lines(self):
        assert snapped() == ([], [])
