"""Tests for the v5 vector-lattice table pass (#367, A4a).

Unit tests build RawSegment and SpanPiece values directly: boxes are
displayed, top-left ``(x0, y0, x1, y1)`` points.
"""

import dataclasses
import pickle
import re
import time

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


def components(*segments: RawSegment):
    """Each connected group of rules as ``(horizontal, vertical)`` lists of ``(position, start, end)``."""
    groups = layout._components(*layout._snapped_lines(segments))
    return [([tuple(line) for line in hs], [tuple(line) for line in vs]) for hs, vs in groups]


def ruled_grid(xs, ys) -> list[RawSegment]:
    """Full-length rules at ``xs`` (vertical) and ``ys`` (horizontal): every cell closed."""
    return [h(y, xs[0], xs[-1]) for y in ys] + [v(x, ys[0], ys[-1]) for x in xs]


def dashed_mesh() -> list[RawSegment]:
    """14 920 dashes (12 pt on, 3 pt off: too far apart to merge) crossing in one mesh.

    100 rows of 75 horizontal dashes 8 pt apart and 140 columns of 53
    vertical dashes 8 pt apart, every other row and column shifted half a
    dash so that the crossings link them all: 55.6 million
    horizontal-vertical pairs, but each vertical dash crosses only the
    dashes of the two or three rows it spans.
    """
    rows = [
        h(6.0 + 8.0 * row, 10.0 + 15.0 * dash + 7.5 * (row % 2), 22.0 + 15.0 * dash + 7.5 * (row % 2))
        for row in range(100)
        for dash in range(75)
    ]
    columns = [
        v(14.0 + 8.0 * col, 10.0 + 15.0 * dash + 7.5 * (col % 2), 22.0 + 15.0 * dash + 7.5 * (col % 2))
        for col in range(140)
        for dash in range(53)
    ]
    return rows + columns


class TestComponents:
    """Rules that cross (within 2 pt) group by a sweep line and union-find; a group needs both orientations."""

    def test_a_ruled_grid_is_one_component(self):
        segments = ruled_grid((10.0, 50.0, 90.0), (20.0, 40.0, 60.0))

        assert components(*segments) == [
            (
                [(20.0, 10.0, 90.0), (40.0, 10.0, 90.0), (60.0, 10.0, 90.0)],
                [(10.0, 20.0, 60.0), (50.0, 20.0, 60.0), (90.0, 20.0, 60.0)],
            )
        ]

    @pytest.mark.parametrize(
        "vertical,meets",
        [
            pytest.param(v(50.0, 102.0, 150.0), True, id="starts-2-pt-below"),
            pytest.param(v(50.0, 102.25, 150.0), False, id="starts-over-2-pt-below"),
            pytest.param(v(50.0, 40.0, 98.0), True, id="ends-2-pt-above"),
            pytest.param(v(50.0, 40.0, 97.75), False, id="ends-over-2-pt-above"),
            pytest.param(v(12.0, 40.0, 150.0), True, id="2-pt-past-the-start"),
            pytest.param(v(11.75, 40.0, 150.0), False, id="over-2-pt-past-the-start"),
            pytest.param(v(92.0, 40.0, 150.0), True, id="2-pt-past-the-end"),
            pytest.param(v(92.25, 40.0, 150.0), False, id="over-2-pt-past-the-end"),
        ],
    )
    def test_rules_meet_within_2_pt_of_each_others_ends(self, vertical, meets):
        got = components(h(100.0, 14.0, 90.0), vertical)

        assert len(got) == (1 if meets else 0)

    def test_parallel_rules_never_connect_on_their_own(self):
        assert components(h(100.0, 10.0, 90.0), h(101.5, 10.0, 90.0), h(130.0, 10.0, 90.0)) == []

    def test_rules_connect_through_a_chain_of_crossings(self):
        # h1 - v1 - h2 - v2: no rule crosses every other, yet all are one group.
        got = components(h(10.0, 0.0, 50.0), v(40.0, 0.0, 100.0), h(90.0, 30.0, 200.0), v(180.0, 80.0, 300.0))

        assert got == [([(10.0, 0.0, 50.0), (90.0, 30.0, 200.0)], [(40.0, 0.0, 100.0), (180.0, 80.0, 300.0)])]

    def test_separate_grids_are_separate_components_in_order_of_their_first_horizontal_rule(self):
        lower = ruled_grid((10.0, 50.0), (300.0, 340.0))
        upper = ruled_grid((200.0, 260.0), (20.0, 60.0))

        got = components(*lower, *upper)

        assert [hs[0][0] for hs, _ in got] == [20.0, 300.0]
        assert [len(hs) + len(vs) for hs, vs in got] == [4, 4]

    def test_a_dashed_mesh_of_15_000_segments_groups_within_1_second(self):
        # The bound under test: O((H + V) log(H + V) + I), not the pairwise O(H x V).
        segments = dashed_mesh()

        started = time.perf_counter()
        got = layout._components(*layout._snapped_lines(segments))
        elapsed = time.perf_counter() - started

        assert len(segments) == 14_920
        # One group: every vertical dash, and every horizontal dash but the 76 that cross none.
        assert [(len(hs), len(vs)) for hs, vs in got] == [(7_424, 7_420)]
        assert elapsed < 1.0, f"{elapsed:.2f} s"


def grid_of(*segments: RawSegment):
    """The one component's grid as ``(n_rows, n_cols, [(row, col, row_span, col_span, box), ...])``, or None."""
    groups = layout._components(*layout._snapped_lines(segments))
    assert len(groups) == 1, f"expected one component, got {len(groups)}"
    grid = layout._grid(*groups[0])
    if grid is None:
        return None
    n_rows, n_cols, positions = layout._grid_positions(grid.boxes)
    return n_rows, n_cols, [(*position, cell_box) for position, cell_box in zip(positions, grid.boxes)]


def frame(x0: float, y0: float, x1: float, y1: float) -> list[RawSegment]:
    """The four sides of a box."""
    return [h(y0, x0, x1), h(y1, x0, x1), v(x0, y0, y1), v(x1, y0, y1)]


class TestGrid:
    """Grid lines are the component's positions; cells are the units joined across absent internal edges."""

    def test_a_ruled_3_x_3_grid_has_nine_unit_cells(self):
        got = grid_of(*ruled_grid((10.0, 50.0, 90.0, 130.0), (20.0, 40.0, 60.0, 80.0)))

        expected = [
            (row, col, 1, 1, (10.0 + 40.0 * col, 20.0 + 20.0 * row, 50.0 + 40.0 * col, 40.0 + 20.0 * row))
            for row in range(3)
            for col in range(3)
        ]
        assert got == (3, 3, expected)

    def test_a_header_cell_across_the_columns_spans_them(self):
        # The column rules start below the header row.
        segments = frame(0.0, 0.0, 90.0, 60.0) + [h(20.0, 0.0, 90.0), h(40.0, 0.0, 90.0)]
        segments += [v(30.0, 20.0, 60.0), v(60.0, 20.0, 60.0)]

        n_rows, n_cols, cells = grid_of(*segments)

        assert (n_rows, n_cols) == (3, 3)
        assert cells[0] == (0, 0, 1, 3, (0.0, 0.0, 90.0, 20.0))
        assert [cell[:4] for cell in cells[1:]] == [(row, col, 1, 1) for row in (1, 2) for col in range(3)]

    def test_a_cell_down_two_rows_spans_them(self):
        # The second row rule starts right of the first column.
        segments = frame(0.0, 0.0, 90.0, 60.0) + [h(20.0, 0.0, 90.0), h(40.0, 30.0, 90.0)]
        segments += [v(30.0, 0.0, 60.0), v(60.0, 0.0, 60.0)]

        n_rows, n_cols, cells = grid_of(*segments)

        assert (n_rows, n_cols) == (3, 3)
        assert [cell[:4] for cell in cells] == [
            (0, 0, 1, 1),
            (0, 1, 1, 1),
            (0, 2, 1, 1),
            (1, 0, 2, 1),
            (1, 1, 1, 1),
            (1, 2, 1, 1),
            (2, 1, 1, 1),
            (2, 2, 1, 1),
        ]
        assert cells[3][4] == (0.0, 20.0, 30.0, 60.0)

    @pytest.mark.parametrize(
        "bottom,split",
        [pytest.param(58.0, True, id="ends-2-pt-short"), pytest.param(57.75, False, id="ends-over-2-pt-short")],
    )
    def test_a_rule_covers_an_edge_to_within_2_pt_of_its_end(self, bottom, split):
        segments = frame(0.0, 0.0, 90.0, 60.0) + [h(20.0, 0.0, 90.0), h(40.0, 0.0, 90.0)]
        segments += [v(30.0, 0.0, 60.0), v(60.0, 0.0, bottom)]

        n_rows, n_cols, cells = grid_of(*segments)

        last_row = [cell[:4] for cell in cells if cell[0] == 2]
        assert last_row == ([(2, 0, 1, 1), (2, 1, 1, 1), (2, 2, 1, 1)] if split else [(2, 0, 1, 1), (2, 1, 1, 2)])

    def test_a_rule_that_bounds_no_cell_adds_no_column(self):
        # A 12 pt stub hangs from the top rule inside the first cell: it is in the component (it meets the
        # top rule) but closes no cell, so the grid is the plain 2 x 2 one.
        plain_grid = frame(0.0, 0.0, 90.0, 60.0) + [h(30.0, 0.0, 90.0), v(45.0, 0.0, 60.0)]

        assert grid_of(*plain_grid, v(20.0, 0.0, 12.0)) == grid_of(*plain_grid)

    @pytest.mark.parametrize(
        "segments",
        [
            pytest.param(frame(0.0, 0.0, 600.0, 780.0), id="page-border-frame"),
            pytest.param(frame(0.0, 0.0, 90.0, 60.0) + [h(30.0, 0.0, 90.0)], id="two-rows-one-column"),
            pytest.param(frame(0.0, 0.0, 90.0, 60.0) + [v(30.0, 0.0, 60.0), v(60.0, 0.0, 60.0)], id="one-row"),
            pytest.param(
                # The stub makes two columns of units, but it bounds no cell: still one column.
                frame(0.0, 0.0, 90.0, 60.0) + [h(30.0, 0.0, 90.0), v(45.0, 0.0, 12.0)],
                id="one-column-and-a-stub",
            ),
        ],
    )
    def test_fewer_than_2_rows_or_2_columns_is_no_grid(self, segments):
        # v4's drop_layout_tables minimum, now geometric: a frame or a single row or column of boxes.
        assert grid_of(*segments) is None

    def test_a_merged_region_that_is_not_a_rectangle_is_no_grid(self):
        # The inner rules stop at the centre: the three other units join in an L, which no
        # (row, col, row_span, col_span) cell can hold; the text is left to the stream pass.
        segments = frame(0.0, 0.0, 100.0, 100.0) + [h(50.0, 0.0, 50.0), v(50.0, 0.0, 50.0)]

        assert grid_of(*segments) is None

    def test_a_box_inside_a_cell_touching_its_border_is_no_grid(self):
        # The cell around the box is a U, not a rectangle.
        segments = ruled_grid((0.0, 100.0, 200.0), (0.0, 100.0, 200.0)) + frame(20.0, 0.0, 60.0, 40.0)

        assert grid_of(*segments) is None


class TestGridPositions:
    """Cell boxes rank into grid positions: every distinct box edge is a grid line."""

    def test_positions_rank_the_box_edges(self):
        boxes = ((0.0, 0.0, 90.0, 20.0), (0.0, 20.0, 30.0, 60.0), (30.0, 20.0, 90.0, 40.0), (30.0, 40.0, 90.0, 60.0))

        assert layout._grid_positions(boxes) == (3, 2, [(0, 0, 1, 2), (1, 0, 2, 1), (1, 1, 1, 1), (2, 1, 1, 1)])
