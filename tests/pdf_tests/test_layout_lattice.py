"""Tests for the v5 vector-lattice table pass (#367, A4a).

Unit tests build RawSegment and SpanPiece values directly: boxes are
displayed, top-left ``(x0, y0, x1, y1)`` points. End-to-end tests build PDFs
with the ReportLab toolkit (``pdf_tests.builders``), whose coordinates are
top-left points in the unrotated page space, and read them through
``extract_page``, ``assess_page`` and ``find_lattice_tables``.
"""

import dataclasses
import pickle
import re
import time

import pytest

from datagrunt.pdf import layout, quality
from datagrunt.pdf._path_segments import RawSegment
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.layout import LatticeCell, LatticeResult, LatticeTable, find_lattice_tables
from datagrunt.pdf.pdfium_engine import extract_page, open_document
from datagrunt.pdf.quality import SpanPiece, assess_page
from pdf_tests.builders import FormXObject, Line, PageSpec, Rect, TextRun, build_pdf
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
            "datagrunt.pdf._pdfium_common",
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


def chars(text: str, x: float, y: float, *, advance: float = 6.0, height: float = 12.0, **overrides) -> SpanPiece:
    """A visible piece of ``text`` in a row of ``advance``-wide chars from ``(x, y)`` (a whole span)."""
    boxes = tuple((x + i * advance, y, x + (i + 1) * advance, y + height) for i in range(len(text)))
    return SpanPiece(span=raw_span(text, boxes, **overrides), start=0, end=len(text), hidden_reasons=())


def falling(text: str, x: float, y: float, *, advance: float = 6.0, width: float = 12.0) -> SpanPiece:
    """A visible piece of ``text`` running down from ``(x, y)``: text a quarter turn clockwise."""
    boxes = tuple((x, y + i * advance, x + width, y + (i + 1) * advance) for i in range(len(text)))
    return SpanPiece(span=raw_span(text, boxes), start=0, end=len(text), hidden_reasons=())


def rising(text: str, x: float, y: float, *, advance: float = 6.0, width: float = 12.0) -> SpanPiece:
    """A visible piece of ``text`` running up from ``(x, y)``: text a quarter turn anticlockwise."""
    boxes = tuple((x, y - (i + 1) * advance, x + width, y - i * advance) for i in range(len(text)))
    return SpanPiece(span=raw_span(text, boxes), start=0, end=len(text), hidden_reasons=())


def texts(segments, *pieces: SpanPiece):
    """Each component grid's cell texts, in cell order, read in each cell's own direction."""
    grids = [layout._grid(*group) for group in layout._components(*layout._snapped_lines(segments))]
    assert all(grid is not None for grid in grids)
    runs = layout._cell_runs(grids, pieces)
    return [[layout._cell_text(cell_runs, 0) for cell_runs in grid_runs] for grid_runs in runs]


# A 2 x 2 grid of 100 x 50 pt cells from (0, 0).
TWO_BY_TWO = ruled_grid((0.0, 100.0, 200.0), (0.0, 50.0, 100.0))


class TestCellText:
    """A visible char belongs to the cell holding its box centre; a cell's text reads line by line."""

    def test_each_cell_holds_the_chars_centred_in_it(self):
        got = texts(TWO_BY_TWO, chars("a", 10.0, 10.0), chars("b", 110.0, 10.0), chars("c", 10.0, 60.0))

        assert got == [["a", "b", "c", ""]]

    @pytest.mark.parametrize(
        "x,left,right",
        [
            # From x 70, 'E' spans 94..100: it ends on the border, its centre (97) is left of it.
            pytest.param(70.0, "ABCDE", "FGH", id="ends-on-the-border"),
            # From x 74, 'E' spans 98..104: it starts left of the border, its centre (101) is right of it.
            pytest.param(74.0, "ABCD", "EFGH", id="centred-past-the-border"),
        ],
    )
    def test_a_piece_crossing_a_border_splits_at_its_chars_by_their_centres(self, x, left, right):
        assert texts(TWO_BY_TWO, chars("ABCDEFGH", x, 10.0)) == [[left, right, "", ""]]

    def test_a_char_centred_outside_every_grid_is_in_no_cell(self):
        got = texts(TWO_BY_TWO, chars("xy", 200.0, 10.0), chars("z", -4.0, 120.0))

        assert got == [["", "", "", ""]]

    def test_hidden_pieces_never_enter_a_cell(self):
        hidden = dataclasses.replace(chars("secret", 10.0, 10.0), hidden_reasons=("invisible",))

        assert texts(TWO_BY_TWO, hidden, chars("shown", 110.0, 10.0)) == [["", "shown", "", ""]]

    def test_lines_read_top_to_bottom_and_join_with_one_space(self):
        got = texts(TWO_BY_TWO, chars("second", 10.0, 24.0), chars("first", 10.0, 6.0))

        assert got == [["first second", "", "", ""]]

    @pytest.mark.parametrize(
        "gap,joined",
        [
            pytest.param(2.4, "leftright", id="gap-under-0.22-em"),
            pytest.param(2.5, "left right", id="gap-over-0.22-em"),
        ],
    )
    def test_runs_on_a_line_read_left_to_right_joined_by_the_gap(self, gap, joined):
        # 11 pt text: a gap of at most 0.22 em (2.42 pt) joins with nothing, a wider one with a space.
        right = chars("right", 10.0 + 4 * 6.0 + gap, 10.0)

        assert texts(TWO_BY_TWO, right, chars("left", 10.0, 10.0)) == [[joined, "", "", ""]]

    def test_a_raised_run_overlapping_the_line_by_45_percent_or_more_stays_on_it(self):
        # '2' is 6 pt tall and overlaps the 12 pt line by 2.76 pt: 0.46 of the smaller height.
        got = texts(TWO_BY_TWO, chars("m", 10.0, 10.0), chars("2", 16.0, 6.76, height=6.0))

        assert got == [["m2", "", "", ""]]

    def test_a_run_overlapping_the_line_by_less_than_45_percent_starts_a_line(self):
        # 2.64 pt: 0.44 of the smaller height.
        got = texts(TWO_BY_TWO, chars("m", 10.0, 10.0), chars("2", 16.0, 6.64, height=6.0))

        assert got == [["2 m", "", "", ""]]

    def test_generated_breaks_become_one_space(self):
        piece = chars("NameValue", 10.0, 10.0, generated_breaks=(4,))

        assert texts(TWO_BY_TWO, piece) == [["Name Value", "", "", ""]]

    def test_a_generated_break_where_the_cell_splits_the_piece_adds_nothing(self):
        piece = chars("abcdefghij", 70.0, 10.0, generated_breaks=(5,))

        assert texts(TWO_BY_TWO, piece) == [["abcde", "fghij", "", ""]]

    def test_text_is_otherwise_verbatim(self):
        assert texts(TWO_BY_TWO, chars(" a  b\t", 10.0, 10.0)) == [[" a  b\t", "", "", ""]]

    def test_a_run_keeps_its_span_order(self):
        # pdfium gives right-to-left text in logical order: its chars run leftward; kept as given.
        boxes = tuple((60.0 - 6.0 * (i + 1), 10.0, 60.0 - 6.0 * i, 22.0) for i in range(4))
        piece = SpanPiece(span=raw_span("שלום", boxes), start=0, end=4, hidden_reasons=())

        assert texts(TWO_BY_TWO, piece) == [["שלום", "", "", ""]]

    @pytest.mark.parametrize(
        "pieces",
        [
            pytest.param((falling("second", 10.0, 5.0), falling("first", 30.0, 5.0)), id="falling"),
            pytest.param((rising("first", 10.0, 45.0), rising("second", 30.0, 45.0)), id="rising"),
        ],
    )
    def test_turned_text_reads_along_its_own_lines(self, pieces):
        # Text a quarter turn clockwise reads its lines right to left; anticlockwise, left to right.
        assert texts(TWO_BY_TWO, *pieces) == [["first second", "", "", ""]]

    def test_the_smallest_grid_holding_a_char_takes_it(self):
        inner = ruled_grid((20.0, 50.0, 80.0), (10.0, 25.0, 40.0))

        got = texts(TWO_BY_TWO + inner, chars("in", 22.0, 12.0), chars("out", 10.0, 60.0))

        # Components come in order of their first horizontal rule: the outer grid's (y 0) first.
        assert got == [["", "", "out", ""], ["in", "", "", ""]]


def read_tables(tmp_path, *items, rotate: int = 0) -> LatticeResult:
    """find_lattice_tables over page 1 of a 400 x 300 pt PDF of ``items``, through extract_page and assess_page."""
    path = build_pdf(tmp_path / "page.pdf", [PageSpec(width=400.0, height=300.0, items=items, rotate=rotate)])
    page = extract_page(open_document(path), 0)
    return find_lattice_tables(page.segments.segments, assess_page(page).pieces)


def rules(xs, ys, *, width: float = 0.5) -> tuple[Line, ...]:
    """Stroked rules at ``xs`` (vertical) and ``ys`` (horizontal), each across the whole grid."""
    across = tuple(Line(xs[0], y, xs[-1], y, width=width) for y in ys)
    return across + tuple(Line(x, ys[0], x, ys[-1], width=width) for x in xs)


def filled_rules(xs, ys) -> tuple[Rect, ...]:
    """The same rules drawn as 1 pt thin filled rectangles, centred on the grid lines."""
    across = tuple(Rect(xs[0], y - 0.5, xs[-1] - xs[0], 1.0, fill_rgb=(0.0, 0.0, 0.0), stroke=False) for y in ys)
    return across + tuple(Rect(x - 0.5, ys[0], 1.0, ys[-1] - ys[0], fill_rgb=(0.0, 0.0, 0.0), stroke=False) for x in xs)


def written(texts, xs, ys, **overrides) -> tuple[TextRun, ...]:
    """Each non-empty text in its grid cell, 5 pt in from its left rule and 14 pt down from its top rule."""
    return tuple(
        TextRun(text, xs[col] + 5.0, ys[row] + 14.0, **overrides)
        for row, line in enumerate(texts)
        for col, text in enumerate(line)
        if text
    )


def shape(result: LatticeResult):
    """Each table as ``(rounded bbox, grid, has_header)``: pdfium points are 32-bit floats."""
    return [
        (tuple(round(value, 2) for value in (t.bbox.x0, t.bbox.y0, t.bbox.x1, t.bbox.y1)), t.grid(), t.has_header)
        for t in result.tables
    ]


def spans_of(table: LatticeTable):
    """Each cell as ``(row, col, row_span, col_span, text)``."""
    return [(item.row, item.col, item.row_span, item.col_span, item.text) for item in table.cells]


XS, YS = (50.0, 150.0, 250.0, 350.0), (40.0, 60.0, 80.0, 100.0)
SPEC = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"))
SPEC_GRID = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"))


class TestFindLatticeTables:
    """Ruled tables found end to end: extract_page's segments and assess_page's pieces."""

    def test_a_ruled_3_x_3_grid_is_one_table(self, tmp_path):
        result = read_tables(tmp_path, *rules(XS, YS), *written(SPEC, XS, YS))

        assert shape(result) == [((50.0, 40.0, 350.0, 100.0), SPEC_GRID, True)]
        assert (result.tables[0].n_rows, result.tables[0].n_cols) == (3, 3)
        first = result.tables[0].cells[0].bbox
        assert tuple(round(value, 2) for value in (first.x0, first.y0, first.x1, first.y1)) == (50.0, 40.0, 150.0, 60.0)

    def test_thin_filled_rectangles_rule_a_table(self, tmp_path):
        result = read_tables(tmp_path, *filled_rules(XS, YS), *written(SPEC, XS, YS))

        assert shape(result) == [((50.0, 40.0, 350.0, 100.0), SPEC_GRID, True)]

    def test_a_header_across_the_columns_is_one_cell_spanning_them(self, tmp_path):
        items = [Line(XS[0], y, XS[-1], y) for y in YS] + [Line(x, YS[0], x, YS[-1]) for x in (XS[0], XS[-1])]
        items += [Line(x, YS[1], x, YS[-1]) for x in XS[1:-1]]
        texts_ = (("Fasteners", "", ""), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"))

        result = read_tables(tmp_path, *items, *written(texts_, XS, YS))

        table = result.tables[0]
        assert spans_of(table)[0] == (0, 0, 1, 3, "Fasteners")
        assert table.grid() == (("Fasteners", "", ""), ("Bolt", "4", "0.10"), ("Nut", "12", "0.05"))
        assert table.has_header is False

    def test_a_cell_down_two_rows_spans_them(self, tmp_path):
        items = [Line(XS[0], y, XS[-1], y) for y in (YS[0], YS[1], YS[-1])] + [Line(XS[1], YS[2], XS[-1], YS[2])]
        items += [Line(x, YS[0], x, YS[-1]) for x in XS]
        texts_ = (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("", "12", "0.05"))

        result = read_tables(tmp_path, *items, *written(texts_, XS, YS))

        assert spans_of(result.tables[0])[3] == (1, 0, 2, 1, "Bolt")
        assert result.tables[0].grid() == (("Part", "Qty", "Price"), ("Bolt", "4", "0.10"), ("", "12", "0.05"))

    def test_a_cell_of_two_lines_reads_them_joined_by_a_space(self, tmp_path):
        ys = (40.0, 60.0, 100.0)
        items = rules(XS[:3], ys) + written((("Part", "Note"),), XS, ys)
        items += (TextRun("Hex bolt,", 55.0, 74.0), TextRun("zinc plated", 55.0, 88.0), TextRun("M8", 155.0, 74.0))

        result = read_tables(tmp_path, *items)

        assert result.tables[0].grid() == (("Part", "Note"), ("Hex bolt, zinc plated", "M8"))

    def test_text_crossing_a_cell_border_splits_and_is_used_once(self, tmp_path):
        # One text object across the column rule at x 150: each char goes to the cell holding its centre.
        items = rules(XS[:3], YS) + written((("Left", "Right"), ("a", "b")), XS, YS)
        items += (TextRun("ABCDEFGHIJKL", 100.0, 94.0),)

        result = read_tables(tmp_path, *items)

        left, right = result.tables[0].grid()[2]
        assert left + right == "ABCDEFGHIJKL"
        assert left and right
        crossing = [piece for piece in result.used_pieces if piece.span.text == "ABCDEFGHIJKL"]
        assert [(piece.start, piece.end) for piece in crossing] == [(0, 12)]

    def test_a_table_in_a_scaled_form_xobject_is_found_at_its_page_position(self, tmp_path):
        xs, ys = (0.0, 60.0, 120.0), (0.0, 20.0, 40.0)
        form = FormXObject(
            40.0, 30.0, 120.0, 40.0, items=rules(xs, ys) + written((("A1", "B1"), ("A2", "B2")), xs, ys), scale=1.5
        )

        result = read_tables(tmp_path, form)

        assert shape(result) == [((40.0, 30.0, 220.0, 90.0), (("A1", "B1"), ("A2", "B2")), True)]

    @pytest.mark.parametrize("rotate", [90, 270])
    def test_a_table_turned_by_rotate_reads_in_the_order_it_was_written(self, tmp_path, rotate):
        # v4 parity (#391): /Rotate turns the table on the displayed page; its cells still read as written.
        result = read_tables(tmp_path, *rules(XS, YS), *written(SPEC, XS, YS), rotate=rotate)

        assert [table.grid() for table in result.tables] == [SPEC_GRID]
        assert result.tables[0].has_header is True

    def test_a_table_upside_down_reads_in_display_order(self, tmp_path):
        # Known limitation: pdfium's boxes cannot tell upside-down text from right-to-left text (both
        # advance leftward), so /Rotate 180 reads the displayed grid; each cell's text stays as written.
        result = read_tables(tmp_path, *rules(XS, YS), *written(SPEC, XS, YS), rotate=180)

        assert result.tables[0].grid() == tuple(tuple(reversed(row)) for row in reversed(SPEC_GRID))

    def test_a_page_border_frame_is_no_table_and_uses_no_text(self, tmp_path):
        items = (Rect(10.0, 10.0, 380.0, 280.0), TextRun("Inside the frame", 50.0, 50.0))

        assert read_tables(tmp_path, *items) == LatticeResult(tables=(), used_pieces=())

    def test_an_empty_ruled_grid_is_decoration(self, tmp_path):
        assert read_tables(tmp_path, *rules(XS[:3], YS[:3])) == LatticeResult(tables=(), used_pieces=())

    def test_two_tables_come_top_to_bottom(self, tmp_path):
        lower_ys = (200.0, 220.0, 240.0)
        items = rules(XS[:3], lower_ys) + written((("Low", "er"), ("x", "y")), XS, lower_ys)
        items += rules(XS[:3], YS[:3]) + written((("Up", "per"), ("p", "q")), XS, YS)

        result = read_tables(tmp_path, *items)

        assert [table.grid()[0] for table in result.tables] == [("Up", "per"), ("Low", "er")]

    def test_hidden_text_in_a_cell_stays_out_of_the_table(self, tmp_path):
        items = rules(XS[:3], YS[:3]) + written((("Name", "Value"), ("a", "1")), XS, YS)
        items += (TextRun("secret", 160.0, 74.0, render_mode=3),)

        result = read_tables(tmp_path, *items)

        assert result.tables[0].grid() == (("Name", "Value"), ("a", "1"))
        assert "secret" not in [piece.text for piece in result.used_pieces]

    def test_used_pieces_are_the_cells_text_and_nothing_outside(self, tmp_path):
        items = (TextRun("Parts list", 50.0, 30.0),) + rules(XS[:3], YS[:3])
        items += written((("Name", "Value"), ("a", "1")), XS, YS)

        result = read_tables(tmp_path, *items)

        assert [piece.text for piece in result.used_pieces] == ["Name", "Value", "a", "1"]
        assert all(piece.hidden_reasons == () for piece in result.used_pieces)

    def test_a_blank_header_cell_means_no_header(self, tmp_path):
        result = read_tables(tmp_path, *rules(XS[:3], YS[:3]), *written((("Name", ""), ("a", "1")), XS, YS))

        assert result.tables[0].has_header is False


def cell_rectangles(count: int, size: float) -> tuple[RawSegment, ...]:
    """A ``count`` x ``count`` table drawn as one stroked rectangle per cell: four sides each."""
    segments = []
    for row in range(count):
        for col in range(count):
            x0, y0 = 10.0 + col * size, 10.0 + row * size
            segments += [
                h(y0, x0, x0 + size),
                h(y0 + size, x0, x0 + size),
                v(x0, y0, y0 + size),
                v(x0 + size, y0, y0 + size),
            ]
    return tuple(segments)


class TestFindLatticeTablesBounds:
    """A page of ~15 000 segments takes under 1 s; the arguments are checked."""

    def test_a_60_x_60_table_drawn_cell_by_cell_with_text_in_every_cell(self):
        segments = cell_rectangles(60, 12.0)
        pieces = tuple(
            chars("x", 13.0 + col * 12.0, 11.0 + row * 12.0, height=10.0) for row in range(60) for col in range(60)
        )

        started = time.perf_counter()
        result = find_lattice_tables(segments, pieces)
        elapsed = time.perf_counter() - started

        assert len(segments) == 14_400
        assert [(table.n_rows, table.n_cols) for table in result.tables] == [(60, 60)]
        assert len(result.used_pieces) == 3_600
        assert elapsed < 1.0, f"{elapsed:.2f} s"

    def test_a_dashed_mesh_of_15_000_segments(self):
        segments = tuple(dashed_mesh())

        started = time.perf_counter()
        result = find_lattice_tables(segments, ())
        elapsed = time.perf_counter() - started

        assert result == LatticeResult(tables=(), used_pieces=())
        assert elapsed < 1.0, f"{elapsed:.2f} s"

    @pytest.mark.parametrize(
        "segments,pieces,error,message",
        [
            pytest.param([h(1.0, 0.0, 20.0)], (), TypeError, "segments must be a tuple, got list", id="segments-list"),
            pytest.param((), ("text",), TypeError, "pieces[0] must be a SpanPiece, got str", id="pieces-str"),
        ],
    )
    def test_invalid_argument_raises(self, segments, pieces, error, message):
        with pytest.raises(error, match=f"^{re.escape(message)}$"):
            find_lattice_tables(segments, pieces)

    def test_a_piece_passed_twice_raises(self):
        piece = chars("x", 10.0, 10.0)

        with pytest.raises(ValueError, match=r"^pieces\[1\] is pieces\[0\] again: each piece is the page's text once$"):
            find_lattice_tables((), (piece, piece))
