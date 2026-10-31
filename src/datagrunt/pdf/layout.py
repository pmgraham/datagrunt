"""Page layout for the v5 PDF document model (#367, A4a: the vector-lattice table pass; #368, A4b: the stream pass).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads the engine's segments (:class:`~datagrunt.pdf._path_segments.RawSegment`)
and A3's span pieces (:class:`~datagrunt.pdf.quality.SpanPiece`) and calls
no pdfium function. Boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.

The lattice pass (:func:`find_lattice_tables`, spec §3.3 step 3, Pass A)
builds ruled tables from a page's segments and puts each visible char in
the cell holding its box centre. Its :class:`LatticeResult` names the text
the tables used, so the stream pass (A4b, #368) lays out only the rest;
element ids and reading order come there too, so the tables are
intermediate :class:`LatticeTable` values. Its work per page is bounded
(:data:`MAX_CROSSINGS`, :data:`MAX_GRID_UNITS`, :data:`MAX_GRIDS`): what a
bound leaves out keeps its text for the stream pass, and the result says so.

The stream pass (spec §3.3 step 3, Pass B) lays out the visible text the
lattice tables did not use: :func:`text_lines` reads it as lines
(:class:`TextLine`) of cells (:class:`TextCell`), each keeping the char
ranges it reads; :func:`text_blocks` reads it in reading order, as
paragraphs (:class:`TextBlock`), by v4's XY-cut and line merge, ported
(v5 imports no v4 code). Hidden text is laid out apart (A4b group 3).

Known limitations (each a fact this pure pass cannot see):

- Clip paths are not applied to segments (A2): a rule clipped away rules.
- Every segment rules, whatever its colour, alpha or dash (v4's
  pdfplumber and the A0 prototype read every edge): a white, transparent
  or dotted rule rules a table as a black one does.
- A table drawn upside down reads in display order: pdfium's boxes cannot
  tell upside-down text from right-to-left text.
- A cell that is not a rectangle (an L, or the U around a box touching a
  cell border) leaves its whole grid to the stream pass.
"""

import math
import re
from bisect import bisect_left, bisect_right, insort
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import groupby
from typing import NamedTuple

from datagrunt.pdf._path_segments import Box, RawSegment
from datagrunt.pdf._pdfium_common import union_box
from datagrunt.pdf._validate import int_at_least, non_blank_str, positive_float, strict_bool, typed_tuple
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.quality import SpanPiece

__all__ = [
    "LatticeCell",
    "LatticeResult",
    "LatticeTable",
    "TextBlock",
    "TextCell",
    "TextLine",
    "find_lattice_tables",
    "text_blocks",
    "text_lines",
]

# Lines this close snap to one position, join end to end, and meet (spec §3.3 Pass A).
SNAP_PT = 2.0

# A table has at least this many rows and columns of cells (v4's drop_layout_tables minimum).
_MIN_GRID = 2

# Page bounds: a hostile page cannot make the pass quadratic. Nothing is dropped: a component left
# out keeps its text for the stream pass, and LatticeResult counts it so the caller can warn. The
# corpus (data/pdfs, 95 pages) peaks at 674 crossings, a 201-unit component (422 units on the page)
# and 3 grids on a page; its largest table is 34 grid positions.
# Grid units a page may build, all components together: a 500 x 500 grid.
MAX_GRID_UNITS = 250_000
# Crossings the sweep may report on a page: twice the 251 001 of a full 500 x 500 grid.
MAX_CROSSINGS = 500_000
# Grids a page may build: every visible piece is checked against each.
MAX_GRIDS = 100

# A run joins a line when they overlap across it by this share of the smaller height (spec §3.3 Pass B).
_LINE_OVERLAP = 0.45
# Runs on a line join with nothing across a gap of at most this many ems, else with a space (spec §3.3 Pass B).
_JOIN_GAP_EM = 0.22
# ... and with nothing next to degree and punctuation glyphs, whatever the gap (spec §3.3 Pass B): after
# an opening bracket, quote or degree sign ("(see", "°C"), before a closing one or punctuation ("20°", "Hi,").
_TIGHT_AFTER = frozenset("([{\u2018\u201c\u00b0")
_TIGHT_BEFORE = frozenset(")]},.;:!?%\u2019\u201d\u00b0")
# The stream pass splits a line into cells across a gap of more than this many ems (spec §3.3 Pass B).
_CELL_GAP_EM = 1.8
# Reading turns: the clockwise angle a run's text advances at on the displayed page. Text advancing
# leftward reads across too: right-to-left text comes in logical order, as upside-down text would.
_ACROSS, _DOWN, _UP = 0, 90, 270
# The turns, in the order a tie between them is broken.
_TURNS = (_ACROSS, _DOWN, _UP)

# v4's XY-cut (layout_sorter.PageLayoutSorter), ported in _xy_cut. Regions nest at most this deep: past
# it, a region's items are one segment (v4's MAX_PARTITION_DEPTH).
MAX_PARTITION_DEPTH = 32
# A gutter is a run of at least this many 1 pt bins (v4's gutter_tolerance) ...
_GUTTER_PT = 12
# ... each holding a weight (chars) of at most this ...
_FREE_WEIGHT = 1
# ... in this middle share of the region's width.
_GUTTER_WINDOW = (0.25, 0.75)
# Items wider than this share of the region add no weight: a full-width line spans the gutter.
_WIDE_SHARE = 0.7
# A region narrower than this, or of fewer items, is not cut.
_MIN_CUT_WIDTH_PT = 100.0
_MIN_CUT_ITEMS = 3
# An item spans the gutter when it reaches more than this past its centre on both sides ...
_SPAN_REACH_PT = 5.0
# ... and is shorter than this share of the region's height.
_SPAN_HEIGHT_SHARE = 0.8

# v4's paragraph merge (text_block_builder.TextBlockBuilder._merge_lines), ported in _paragraphs. A line
# starting with one of v4's list markers (text_block_builder.LIST_MARKER_REGEX) starts a paragraph.
_LIST_MARKER = re.compile(
    r"^([•\-\*]|\d+(\.\d+)*\.?|[IVXLCDM]{2,}|[IVXLCDM]\.|[IVXLCDM]\s+[A-Z]|[a-zA-Z]\.|[a-zA-Z]\))(\s|$)"
)
# A paragraph's lines differ from its first line's font size by less than this ...
_SAME_SIZE_PT = 0.6
# ... and each starts at most this many ems (of that size, at least 1 pt) below the one before.
_PARAGRAPH_GAP_EM = 1.6

# Sweep events at one x: horizontal rules open, then vertical rules query, then horizontal rules close.
_OPEN, _QUERY, _CLOSE = 0, 1, 2


def _check_bbox(name: str, value: object) -> BBox:
    """Return ``value`` if it is an A1 BBox."""
    if not isinstance(value, BBox):
        raise TypeError(f"{name} must be a BBox, got {type(value).__name__}")
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class LatticeCell:
    """One cell of a lattice table: a rectangle of grid positions and its text.

    Attributes:
        row: The cell's top grid row, 0-based.
        col: Its left grid column, 0-based.
        row_span: How many grid rows it covers: more than 1 for a cell
            merged down (rowspan).
        col_span: How many grid columns it covers: more than 1 for a cell
            merged across (colspan).
        bbox: The cell's box on the displayed page.
        text: The cell's text; ``""`` for an empty cell.
    """

    row: int
    col: int
    row_span: int
    col_span: int
    bbox: BBox
    text: str

    def __post_init__(self) -> None:
        int_at_least("row", self.row, 0)
        int_at_least("col", self.col, 0)
        int_at_least("row_span", self.row_span, 1)
        int_at_least("col_span", self.col_span, 1)
        _check_bbox("bbox", self.bbox)
        if not isinstance(self.text, str):
            raise TypeError(f"text must be a str, got {type(self.text).__name__}")


@dataclass(frozen=True, slots=True, kw_only=True)
class LatticeTable:
    """A ruled table: cells that tile an ``n_rows`` x ``n_cols`` grid exactly once.

    Every grid row and column starts some cell (no grid line bounds no
    cell), so ``n_rows`` and ``n_cols`` are at most the cell count.
    Validation is O(cells + n_cols + the cells' total column span).

    Attributes:
        bbox: The table's box on the displayed page; every cell's box lies
            within it.
        n_rows: Grid rows, at least 1.
        n_cols: Grid columns, at least 1.
        cells: The cells in row-major order of their top-left positions.
    """

    bbox: BBox
    n_rows: int
    n_cols: int
    cells: tuple[LatticeCell, ...]

    def __post_init__(self) -> None:
        _check_bbox("bbox", self.bbox)
        int_at_least("n_rows", self.n_rows, 1)
        int_at_least("n_cols", self.n_cols, 1)
        typed_tuple("cells", self.cells, LatticeCell)
        self._check_placement()
        self._check_every_line_starts_a_cell()
        self._check_tiling()

    def _check_placement(self) -> None:
        """Each cell inside the grid and the table's box, in row-major order."""
        table = self.bbox
        previous = (-1, -1)
        for index, item in enumerate(self.cells):
            if item.row + item.row_span > self.n_rows or item.col + item.col_span > self.n_cols:
                where = f"cells[{index}] at row {item.row}, col {item.col} spans {item.row_span} x {item.col_span}"
                raise ValueError(f"{where} past the {self.n_rows} x {self.n_cols} grid")
            if (item.row, item.col) <= previous:
                order = "cells must be in row-major order of their top-left positions"
                raise ValueError(f"{order}: cells[{index}] is at row {item.row}, col {item.col}")
            previous = (item.row, item.col)
            inner = item.bbox
            if inner.x0 < table.x0 or inner.y0 < table.y0 or inner.x1 > table.x1 or inner.y1 > table.y1:
                raise ValueError(f"cells[{index}].bbox must lie within the table's bbox, got {inner} in {table}")

    def _check_every_line_starts_a_cell(self) -> None:
        """Every grid row and column starts a cell: no grid line bounds no cell (and the grid is bounded)."""
        for name, count, starts in (
            ("row", self.n_rows, {item.row for item in self.cells}),
            ("column", self.n_cols, {item.col for item in self.cells}),
        ):
            if len(starts) != count:
                # starts lies in 0..count - 1 (placement is checked) and misses one: the first gap in its order.
                ordered = sorted(starts)
                missing = next((index for index, start in enumerate(ordered) if index != start), len(ordered))
                raise ValueError(f"every grid {name} must start a cell: {name} {missing} starts none")

    def _check_tiling(self) -> None:
        """The cells cover every grid position exactly once.

        Cells are in row-major order, so a column's next free row only
        grows: each cell must start at the next free row of every column
        it covers, and every column must end full.
        """
        next_free = [0] * self.n_cols
        exactly_once = f"cells must tile the {self.n_rows} x {self.n_cols} grid exactly once"
        for index, item in enumerate(self.cells):
            for column in range(item.col, item.col + item.col_span):
                if next_free[column] != item.row:
                    fault = "overlaps another cell" if next_free[column] > item.row else "leaves a gap"
                    raise ValueError(f"{exactly_once}: cells[{index}] at row {item.row}, col {item.col} {fault}")
                next_free[column] = item.row + item.row_span
        for column, row in enumerate(next_free):
            if row != self.n_rows:
                raise ValueError(f"{exactly_once}: row {row}, col {column} is in no cell")

    def grid(self) -> tuple[tuple[str, ...], ...]:
        """The cell texts by grid position: a merged cell's text at its top-left, ``""`` where it spans.

        v4's (pdfplumber's) convention: no text is duplicated.
        """
        rows = [[""] * self.n_cols for _ in range(self.n_rows)]
        for item in self.cells:
            rows[item.row][item.col] = item.text
        return tuple(tuple(row) for row in rows)

    @property
    def has_header(self) -> bool:
        """v4's rule: more than one row, and every row-0 grid cell non-blank."""
        return self.n_rows > 1 and all(text.strip() for text in self.grid()[0])


@dataclass(frozen=True, slots=True, kw_only=True)
class LatticeResult:
    """One page's lattice tables and the visible text they consumed.

    Attributes:
        tables: The page's tables, top to bottom, then left to right.
        used_pieces: The char ranges of visible pieces whose chars lie in a
            table's cells, as pieces of the same spans: A4b lays out only
            the rest of the page's text.
        skipped_grids: Components left out, their text to the stream pass,
            because they would pass the page's :data:`MAX_GRID_UNITS` or
            :data:`MAX_GRIDS`: a warning for the page.
        crossing_cap_reached: True when the page's rules cross more than
            :data:`MAX_CROSSINGS` times: the sweep stopped, so the page has
            no lattice tables and all its text goes to the stream pass.
    """

    tables: tuple[LatticeTable, ...]
    used_pieces: tuple[SpanPiece, ...]
    skipped_grids: int = 0
    crossing_cap_reached: bool = False

    def __post_init__(self) -> None:
        typed_tuple("tables", self.tables, LatticeTable)
        typed_tuple("used_pieces", self.used_pieces, SpanPiece)
        for index, item in enumerate(self.used_pieces):
            if item.hidden_reasons:
                raise ValueError(
                    f"used_pieces[{index}] is hidden ({item.hidden_reasons!r}): hidden text never enters a table"
                )
        int_at_least("skipped_grids", self.skipped_grids, 0)
        if strict_bool("crossing_cap_reached", self.crossing_cap_reached) and (self.tables or self.used_pieces):
            raise ValueError("a page past the crossing cap has no lattice tables and uses no text")


def find_lattice_tables(segments: tuple[RawSegment, ...], pieces: tuple[SpanPiece, ...]) -> LatticeResult:
    """The page's ruled tables and the visible text they hold (spec §3.3 step 3, Pass A).

    1. Rules: every segment, snapped (positions within :data:`SNAP_PT`
       chain to their mean) and merged along each position across gaps of
       at most :data:`SNAP_PT` (:func:`_snapped_lines`).
    2. Components: rules crossing within :data:`SNAP_PT`, grouped by a
       sweep line and union-find (:func:`_components`).
    3. Grids: a component's positions cut it into units; units joined
       across absent edges are one cell, which spans rows or columns. No
       table when a cell is not a rectangle or the grid has fewer than 2
       rows or 2 columns (:func:`_grid`).
    4. Text: each visible char goes to the cell holding its box centre
       (in the smallest grid holding it), so a piece crossing a cell
       border splits at its chars; a cell's text reads line by line
       (:func:`_cell_text`). A grid whose every cell is blank is
       decoration, not a table.

    A table whose text all advances one way down or up the page (one
    turned by /Rotate 90 or 270, or drawn turned) reads as it was written:
    its rows run along that text's lines (v4 parity, #391). Text advancing
    leftward reads as displayed: pdfium's boxes cannot tell upside-down
    text from right-to-left text.

    Tables come in the order of their top rules: top to bottom, then left
    to right.

    Bounds, per page: the sweep stops past :data:`MAX_CROSSINGS`
    crossings (the page then has no lattice tables); components build
    grids in order while their units (X - 1) x (Y - 1), for X vertical and
    Y horizontal positions, fit the :data:`MAX_GRID_UNITS` left and fewer
    than :data:`MAX_GRIDS` grids are built; any other is left out, its
    text kept for the stream pass, and counted. So the cost is
    O(n log n + MAX_CROSSINGS + MAX_GRID_UNITS + pieces x MAX_GRIDS +
    chars x (G + log(X + Y))) for n segments and the G grids near a piece.

    Args:
        segments: The page's segments (``PageSegments.segments``).
        pieces: The page's pieces (``PageAssessment.pieces``); hidden ones
            never enter a table.

    Raises:
        TypeError: If ``segments`` is not a tuple of RawSegment or
            ``pieces`` is not a tuple of SpanPiece.
        ValueError: If a piece is passed twice.
    """
    typed_tuple("segments", segments, RawSegment)
    typed_tuple("pieces", pieces, SpanPiece)
    _check_distinct(pieces)
    groups = _components(*_snapped_lines(segments))
    if groups is None:
        return LatticeResult(tables=(), used_pieces=(), crossing_cap_reached=True)
    grids, skipped = _bounded_grids(groups)
    tables: list[LatticeTable] = []
    used: list[_Run] = []
    for grid, cell_runs in zip(grids, _cell_runs(grids, pieces)):
        table = _table(grid, cell_runs)
        if table is not None:
            tables.append(table)
            used.extend(run for runs in cell_runs for run in runs)
    return LatticeResult(tables=tuple(tables), used_pieces=_used_pieces(used, pieces), skipped_grids=skipped)


def _check_distinct(pieces: tuple[SpanPiece, ...]) -> None:
    """Require each piece object once: a piece is a run of the page's text, which is read once."""
    seen: dict[int, int] = {}
    for index, item in enumerate(pieces):
        first = seen.setdefault(id(item), index)
        if first != index:
            raise ValueError(f"pieces[{index}] is pieces[{first}] again: each piece is the page's text once")


class _Line(NamedTuple):
    """A snapped, merged rule: its position across its axis, and where it starts and ends along it."""

    position: float
    start: float
    end: float


def _snapped_lines(segments: Iterable[RawSegment]) -> tuple[list[_Line], list[_Line]]:
    """The horizontal and vertical rules of ``segments``, snapped and merged.

    Per orientation, positions chain into clusters (sorted, each within
    :data:`SNAP_PT` of the one before, as pdfplumber snaps) and each
    cluster's segments move to its mean; then the segments at each
    position merge where they overlap, touch or leave a gap of at most
    :data:`SNAP_PT`. Every segment rules, whatever its kind, dash or
    colour (v4's pdfplumber reads every edge). Lines come sorted by
    position, then start. O(n log n) for n segments.
    """
    horizontal = [item for item in segments if item.orientation == "h"]
    vertical = [item for item in segments if item.orientation == "v"]
    return _snap_and_merge(horizontal), _snap_and_merge(vertical)


def _snap_and_merge(segments: list[RawSegment]) -> list[_Line]:
    """One orientation's segments, snapped to their clusters' mean positions and merged along them."""
    lines: list[_Line] = []
    for cluster in _position_clusters(sorted(segments, key=lambda item: item.position)):
        position = math.fsum(item.position for item in cluster) / len(cluster)
        lines.extend(_merged(position, sorted((item.start, item.end) for item in cluster)))
    return lines


def _position_clusters(ordered: list[RawSegment]) -> Iterator[list[RawSegment]]:
    """Yield runs of position-sorted segments, each within :data:`SNAP_PT` of the one before."""
    cluster: list[RawSegment] = []
    for item in ordered:
        if cluster and item.position - cluster[-1].position > SNAP_PT:
            yield cluster
            cluster = []
        cluster.append(item)
    if cluster:
        yield cluster


def _merged(position: float, extents: list[tuple[float, float]]) -> list[_Line]:
    """Start-sorted ``(start, end)`` extents at ``position``, joined across gaps of at most :data:`SNAP_PT`."""
    lines = []
    start, end = extents[0]
    for next_start, next_end in extents[1:]:
        if next_start - end <= SNAP_PT:
            end = max(end, next_end)
            continue
        lines.append(_Line(position, start, end))
        start, end = next_start, next_end
    lines.append(_Line(position, start, end))
    return lines


_Component = tuple[list[_Line], list[_Line]]


def _components(horizontal: list[_Line], vertical: list[_Line]) -> list[_Component] | None:
    """The groups of rules connected by crossings, each as its horizontal and vertical rules.

    Two rules cross when each reaches the other's position within
    :data:`SNAP_PT` (:func:`_crossings`); groups are the connected
    components of that relation (union-find). A group without both
    orientations rules no cell and is left out. Groups come in order of
    their first horizontal rule, each with its rules in input order.
    None when the rules cross more than :data:`MAX_CROSSINGS` times: the
    sweep stops at the first crossing past it. O((H + V) log(H + V) + I)
    for H horizontal and V vertical rules with I <= MAX_CROSSINGS + 1
    crossings read (union-find is near-constant per crossing).
    """
    groups = _UnionFind(len(horizontal) + len(vertical))
    for count, (h_index, v_index) in enumerate(_crossings(horizontal, vertical), 1):
        if count > MAX_CROSSINGS:
            return None
        groups.union(h_index, len(horizontal) + v_index)
    members: dict[int, _Component] = {}
    for index, line in enumerate(horizontal):
        members.setdefault(groups.find(index), ([], []))[0].append(line)
    for index, line in enumerate(vertical):
        root = groups.find(len(horizontal) + index)
        if root in members:
            members[root][1].append(line)
    return [group for group in members.values() if group[1]]


def _crossings(horizontal: list[_Line], vertical: list[_Line]) -> Iterator[tuple[int, int]]:
    """Yield ``(horizontal index, vertical index)`` for each pair of rules that cross within :data:`SNAP_PT`.

    A sweep over x: a horizontal rule is active from ``start - SNAP_PT`` to
    ``end + SNAP_PT`` in a list sorted by y, and each vertical rule
    reports, by bisection, the active rules whose y lies within its own
    extent widened by ``SNAP_PT`` at both ends. O((H + V) log(H + V) + I):
    one sort of 2H + V events, one bisection per open, close and query,
    and each crossing reported once (opening and closing also shift the
    list, a C memmove of at most H references).
    """
    events = [(line.start - SNAP_PT, _OPEN, index) for index, line in enumerate(horizontal)]
    events += [(line.end + SNAP_PT, _CLOSE, index) for index, line in enumerate(horizontal)]
    events += [(line.position, _QUERY, index) for index, line in enumerate(vertical)]
    events.sort()
    active: list[tuple[float, int]] = []
    for _, kind, index in events:
        if kind == _OPEN:
            insort(active, (horizontal[index].position, index))
        elif kind == _CLOSE:
            del active[bisect_left(active, (horizontal[index].position, index))]
        else:
            line = vertical[index]
            low = bisect_left(active, (line.start - SNAP_PT, -1))
            high = bisect_right(active, (line.end + SNAP_PT, len(horizontal)))
            for _, h_index in active[low:high]:
                yield h_index, index


class _Grid(NamedTuple):
    """The cells a component's rules close.

    Attributes:
        xs: The component's distinct vertical-rule positions, sorted: the
            unit columns' boundaries.
        ys: Its distinct horizontal-rule positions, sorted: the unit rows'
            boundaries.
        unit_cells: Each unit's cell (an index into ``boxes``), row-major
            over ``len(ys) - 1`` rows of ``len(xs) - 1`` units.
        boxes: Each cell's box, in row-major order of its top-left unit.
    """

    xs: tuple[float, ...]
    ys: tuple[float, ...]
    unit_cells: tuple[int, ...]
    boxes: tuple[Box, ...]


def _bounded_grids(groups: list[_Component]) -> tuple[list[_Grid], int]:
    """The components' grids, in order, within the page's bounds, and how many components were left out.

    A component is left out when its units would pass the
    :data:`MAX_GRID_UNITS` the page has left, or :data:`MAX_GRIDS` grids
    are built; one built (or rejected by :func:`_grid`) spends its units.
    """
    grids: list[_Grid] = []
    skipped, budget = 0, MAX_GRID_UNITS
    for horizontal, vertical in groups:
        units = _unit_count(horizontal, vertical)
        if units > budget or len(grids) >= MAX_GRIDS:
            skipped += 1
            continue
        budget -= units
        grid = _grid(horizontal, vertical)
        if grid is not None:
            grids.append(grid)
    return grids, skipped


def _unit_count(horizontal: list[_Line], vertical: list[_Line]) -> int:
    """The units a component's grid would have: (X - 1) x (Y - 1) for its X vertical and Y horizontal positions."""
    across = len({line.position for line in vertical}) - 1
    down = len({line.position for line in horizontal}) - 1
    return max(across, 0) * max(down, 0)


def _grid(horizontal: list[_Line], vertical: list[_Line]) -> _Grid | None:
    """The cells one component's rules close, or None if they rule no table.

    The component's distinct positions cut its extent into units; a unit
    edge is present when a rule at its position reaches both its ends
    within :data:`SNAP_PT`. Units joined across absent internal edges are
    one cell, so a cell spans rows or columns where rules are missing. An
    edge on the grid's border may be absent: the border closes the cell.
    None when a cell is not a rectangle (no row, column and span can hold
    it), or when the cells' distinct edges leave fewer than two rows or
    columns (a frame, or one row or column of boxes: v4's
    ``drop_layout_tables`` minimum); a position that bounds no cell is no
    grid line. O(X Y + R log X Y) for X vertical and Y horizontal
    positions and R rules: :func:`_bounded_grids` bounds X Y per page.
    """
    xs = tuple(sorted({line.position for line in vertical}))
    ys = tuple(sorted({line.position for line in horizontal}))
    if len(xs) <= _MIN_GRID or len(ys) <= _MIN_GRID:
        return None
    rows, cols = len(ys) - 1, len(xs) - 1
    across = _covered_edges(horizontal, ys, xs)
    down = _covered_edges(vertical, xs, ys)
    found = _rectangles(_unit_regions(across, down, rows, cols), cols)
    if found is None:
        return None
    spans, unit_cells = found
    boxes = tuple((xs[left], ys[top], xs[right + 1], ys[bottom + 1]) for top, left, bottom, right in spans)
    n_rows, n_cols, _ = _grid_positions(boxes)
    if n_rows < _MIN_GRID or n_cols < _MIN_GRID:
        return None
    return _Grid(xs=xs, ys=ys, unit_cells=unit_cells, boxes=boxes)


def _covered_edges(lines: list[_Line], positions: tuple[float, ...], along: tuple[float, ...]) -> bytearray:
    """Which unit edges ``lines`` cover: 1 at ``k * (len(along) - 1) + j`` for edge ``along[j]..along[j + 1]``.

    ``positions`` are the lines' own distinct positions, sorted (``k``
    indexes them). A line covers an edge when it reaches both ends of it
    within :data:`SNAP_PT`.
    """
    count = len(along) - 1
    covered = bytearray(len(positions) * count)
    rank = {position: index for index, position in enumerate(positions)}
    for line in lines:
        first = bisect_left(along, line.start - SNAP_PT)
        last = bisect_right(along, line.end + SNAP_PT) - 1
        if last > first:
            offset = rank[line.position] * count
            covered[offset + first : offset + last] = b"\x01" * (last - first)
    return covered


def _unit_regions(across: bytearray, down: bytearray, rows: int, cols: int) -> list[int]:
    """Each unit's region (a union-find root), row-major: units join across absent internal edges.

    ``across`` holds the horizontal edges (``rows + 1`` lines of ``cols``),
    ``down`` the vertical ones (``cols + 1`` lines of ``rows``).
    """
    regions = _UnionFind(rows * cols)
    for row in range(rows):
        for col in range(cols):
            unit = row * cols + col
            if col + 1 < cols and not down[(col + 1) * rows + row]:
                regions.union(unit, unit + 1)
            if row + 1 < rows and not across[(row + 1) * cols + col]:
                regions.union(unit, unit + cols)
    return [regions.find(unit) for unit in range(rows * cols)]


def _rectangles(roots: list[int], cols: int) -> tuple[list[tuple[int, int, int, int]], tuple[int, ...]] | None:
    """Each region's unit bounds ``(top, left, bottom, right)`` and each unit's region, or None if one is no rectangle.

    Regions are numbered in row-major order of first appearance, which is
    the order of their top-left units when every region is a rectangle.
    """
    order: dict[int, int] = {}
    bounds: list[list[int]] = []
    unit_cells = []
    for unit, root in enumerate(roots):
        row, col = divmod(unit, cols)
        index = order.setdefault(root, len(order))
        if index == len(bounds):
            bounds.append([row, col, row, col, 0])
        found = bounds[index]
        found[1], found[2], found[3] = min(found[1], col), max(found[2], row), max(found[3], col)
        found[4] += 1
        unit_cells.append(index)
    if any(units != (bottom - top + 1) * (right - left + 1) for top, left, bottom, right, units in bounds):
        return None
    return [(top, left, bottom, right) for top, left, bottom, right, _ in bounds], tuple(unit_cells)


def _grid_positions(boxes: tuple[Box, ...]) -> tuple[int, int, list[tuple[int, int, int, int]]]:
    """The grid the cell ``boxes`` make: ``(n_rows, n_cols, [(row, col, row_span, col_span), ...])``.

    Every distinct box edge is a grid line (so none bounds no cell); each
    box's position and spans are the ranks of its edges among them.
    """
    columns = {x: index for index, x in enumerate(sorted({x for box in boxes for x in (box[0], box[2])}))}
    rows = {y: index for index, y in enumerate(sorted({y for box in boxes for y in (box[1], box[3])}))}
    positions = [(rows[y0], columns[x0], rows[y1] - rows[y0], columns[x1] - columns[x0]) for x0, y0, x1, y1 in boxes]
    return len(rows) - 1, len(columns) - 1, positions


class _UnionFind:
    """Disjoint sets of ``0..size - 1``: union by size, path halving (each find walks O(log n) parents)."""

    __slots__ = ("_parent", "_size")

    def __init__(self, size: int) -> None:
        self._parent = list(range(size))
        self._size = [1] * size

    def find(self, item: int) -> int:
        """The representative of ``item``'s set."""
        parent = self._parent
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(self, first: int, second: int) -> None:
        """Join the sets of ``first`` and ``second``."""
        first, second = self.find(first), self.find(second)
        if first == second:
            return
        if self._size[first] < self._size[second]:
            first, second = second, first
        self._parent[second] = first
        self._size[first] += self._size[second]


class _Run(NamedTuple):
    """Consecutive chars ``piece.span.text[start:end]`` of one visible piece, all centred in one cell."""

    piece: SpanPiece
    start: int
    end: int


def _cell_runs(grids: list[_Grid], pieces: Iterable[SpanPiece]) -> list[list[list[_Run]]]:
    """The runs of visible chars in each cell of each grid, in piece order.

    A char belongs to the smallest grid (by extent) whose extent holds the
    centre of its box, edges included, and to the cell of the unit
    holding it there, so a piece crossing a cell border splits at its
    chars. Hidden pieces are in no cell. O(chars x G + chars log(X + Y))
    for G grids near each piece.
    """
    runs: list[list[list[_Run]]] = [[[] for _ in grid.boxes] for grid in grids]
    by_area = sorted(range(len(grids)), key=lambda index: _extent_area(grids[index]))
    for piece in pieces:
        if piece.hidden_reasons:
            continue
        nearby = [index for index in by_area if _meets(_extent(grids[index]), piece.bbox)]
        for (grid_index, cell_index), start, end in _located_runs(piece, grids, nearby):
            runs[grid_index][cell_index].append(_Run(piece, start, end))
    return runs


def _extent(grid: _Grid) -> Box:
    """The box a grid's units cover."""
    return grid.xs[0], grid.ys[0], grid.xs[-1], grid.ys[-1]


def _extent_area(grid: _Grid) -> float:
    """The area a grid's units cover."""
    x0, y0, x1, y1 = _extent(grid)
    return (x1 - x0) * (y1 - y0)


def _meets(first: Box, second: Box) -> bool:
    """True if two boxes meet, edges included."""
    return first[0] <= second[2] and second[0] <= first[2] and first[1] <= second[3] and second[1] <= first[3]


def _located_runs(
    piece: SpanPiece, grids: list[_Grid], nearby: list[int]
) -> Iterator[tuple[tuple[int, int], int, int]]:
    """Yield ``((grid, cell), start, end)`` for each maximal run of the piece's chars centred in one cell."""
    if not nearby:
        return
    boxes = piece.span.char_boxes
    places = [_place(grids, nearby, boxes[offset]) for offset in range(piece.start, piece.end)]
    offset = piece.start
    for place, group in groupby(places):
        count = sum(1 for _ in group)
        if place is not None:
            yield place, offset, offset + count
        offset += count


def _place(grids: list[_Grid], nearby: list[int], box: Box) -> tuple[int, int] | None:
    """``(grid, cell)`` of the first of the ``nearby`` grids whose extent holds the centre of ``box``, if any."""
    x, y = (box[0] + box[2]) / 2, (box[1] + box[3]) / 2
    for index in nearby:
        xs, ys = grids[index].xs, grids[index].ys
        if xs[0] <= x <= xs[-1] and ys[0] <= y <= ys[-1]:
            col = min(bisect_right(xs, x), len(xs) - 1) - 1
            row = min(bisect_right(ys, y), len(ys) - 1) - 1
            return index, grids[index].unit_cells[row * (len(xs) - 1) + col]
    return None


def _cell_text(runs: list[_Run], default_turn: int) -> str:
    """A cell's text: its runs in lines, read in the direction its text advances.

    The direction is the one the cell's runs of two or more chars share,
    else ``default_turn``. In that frame, runs overlapping a line by at
    least :data:`_LINE_OVERLAP` of the smaller height are on it; lines read
    top to bottom and join with one space; runs on a line read left to
    right and join as the stream pass joins them (:func:`_separator`:
    nothing across a gap of at most :data:`_JOIN_GAP_EM` ems of the larger
    font or next to a degree or punctuation glyph, else one space); a
    run keeps its span's char order, with one space at each break pdfium
    generated inside it. Text is otherwise verbatim. O(r log r) for r runs.
    """
    if not runs:
        return ""
    turn = _reading_turn(runs, default_turn)
    placed = sorted(((_turned(_run_box(run), turn), run) for run in runs), key=lambda item: (item[0][1], item[0][0]))
    return " ".join(_line_text(line) for line in _lines(placed))


def _reading_turn(runs: list[_Run], default: int) -> int:
    """The one reading turn the runs with a direction share, else ``default``."""
    turns = {turn for run in runs if (turn := _run_turn(run)) is not None}
    return turns.pop() if len(turns) == 1 else default


def _run_turn(run: _Run) -> int | None:
    """The turn a run's text advances at, from its first char's centre to its last; None for one char."""
    if run.end - run.start < 2:
        return None
    boxes = run.piece.span.char_boxes
    first, last = boxes[run.start], boxes[run.end - 1]
    dx = (last[0] + last[2] - first[0] - first[2]) / 2
    dy = (last[1] + last[3] - first[1] - first[3]) / 2
    if dx == 0 and dy == 0:
        return None
    if abs(dx) >= abs(dy):
        return _ACROSS
    return _DOWN if dy > 0 else _UP


def _turned(box: Box, turn: int) -> Box:
    """``box`` in the frame where text advancing at ``turn`` reads left to right, its lines top to bottom.

    Text advancing down (a quarter turn clockwise) has its top toward +x:
    ``(u, v) = (y, -x)``. Text advancing up: ``(u, v) = (-y, x)``.
    """
    x0, y0, x1, y1 = box
    if turn == _DOWN:
        return y0, -x1, y1, -x0
    if turn == _UP:
        return -y1, x0, -y0, x1
    return box


def _run_box(run: _Run) -> Box:
    """The union of a run's char boxes."""
    return union_box(run.piece.span.char_boxes[run.start : run.end])


def _lines(placed: list[tuple[Box, _Run]]) -> list[list[tuple[Box, _Run]]]:
    """Runs (with their turned boxes, sorted by top) grouped into lines by their overlap across them."""
    lines: list[list[tuple[Box, _Run]]] = []
    top = bottom = 0.0
    for item in placed:
        _, y0, _, y1 = item[0]
        if lines and _overlap_share(top, bottom, y0, y1) >= _LINE_OVERLAP:
            lines[-1].append(item)
            bottom = max(bottom, y1)
            continue
        lines.append([item])
        top, bottom = y0, y1
    return lines


def _overlap_share(top: float, bottom: float, y0: float, y1: float) -> float:
    """How much ``top..bottom`` and ``y0..y1`` overlap, as a share of the smaller (touching counts for a point)."""
    overlap = min(bottom, y1) - max(top, y0)
    smaller = min(bottom - top, y1 - y0)
    if smaller <= 0.0:
        return 1.0 if overlap >= 0.0 else 0.0
    return overlap / smaller


def _line_text(line: list[tuple[Box, _Run]]) -> str:
    """A line's runs left to right, each joined to the one before by :func:`_separator`."""
    ordered = sorted(line, key=lambda item: item[0][0])
    parts = [_run_text(ordered[0][1])]
    for before, item in zip(ordered, ordered[1:]):
        parts.append(_separator(before, item))
        parts.append(_run_text(item[1]))
    return "".join(parts)


def _separator(before: tuple[Box, _Run], after: tuple[Box, _Run]) -> str:
    """What joins two neighbouring runs on a line, each with its turned box (spec §3.3 Pass B).

    Nothing across a gap of at most :data:`_JOIN_GAP_EM` ems of the larger
    font, after a glyph in :data:`_TIGHT_AFTER` or before one in
    :data:`_TIGHT_BEFORE` (degree and punctuation glyphs); else one space.
    """
    (left_box, left), (right_box, right) = before, after
    em = max(abs(left.piece.span.font_size_pt), abs(right.piece.span.font_size_pt))
    if right_box[0] - left_box[2] <= _JOIN_GAP_EM * em:
        return ""
    last, first = left.piece.span.text[left.end - 1], right.piece.span.text[right.start]
    return "" if last in _TIGHT_AFTER or first in _TIGHT_BEFORE else " "


def _run_text(run: _Run) -> str:
    """A run's chars in span order, with one space at each generated break inside it."""
    span = run.piece.span
    breaks = span.generated_breaks
    inside = breaks[bisect_right(breaks, run.start) : bisect_left(breaks, run.end)]
    bounds = (run.start, *inside, run.end)
    return " ".join(span.text[start:end] for start, end in zip(bounds, bounds[1:]))


def _table(grid: _Grid, cell_runs: list[list[_Run]]) -> LatticeTable | None:
    """A grid's table, read in its text's turn, or None if every cell is blank (decoration)."""
    turn = _reading_turn([run for runs in cell_runs for run in runs], _ACROSS)
    texts = [_cell_text(runs, turn) for runs in cell_runs]
    if not any(text.strip() for text in texts):
        return None
    n_rows, n_cols, positions = _grid_positions(tuple(_turned(box, turn) for box in grid.boxes))
    cells = sorted(zip(positions, grid.boxes, texts))
    x0, y0, x1, y1 = _extent(grid)
    return LatticeTable(
        bbox=BBox(x0=x0, y0=y0, x1=x1, y1=y1),
        n_rows=n_rows,
        n_cols=n_cols,
        cells=tuple(
            LatticeCell(row=row, col=col, row_span=rows, col_span=cols, bbox=_bbox(box), text=text)
            for (row, col, rows, cols), box, text in cells
        ),
    )


def _bbox(box: Box) -> BBox:
    """A1's BBox of a box."""
    return BBox(x0=box[0], y0=box[1], x1=box[2], y1=box[3])


def _used_pieces(runs: list[_Run], pieces: tuple[SpanPiece, ...]) -> tuple[SpanPiece, ...]:
    """The char ranges ``runs`` cover, as visible pieces: per piece in order, maximal and in char order."""
    ranges: dict[int, list[tuple[int, int]]] = {}
    for run in runs:
        ranges.setdefault(id(run.piece), []).append((run.start, run.end))
    used = []
    for piece in pieces:
        for start, end in _joined(sorted(ranges.get(id(piece), []))):
            used.append(SpanPiece(span=piece.span, start=start, end=end, hidden_reasons=()))
    return tuple(used)


def _joined(extents: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Sorted, disjoint ``(start, end)`` char ranges with touching ones joined."""
    joined: list[tuple[int, int]] = []
    for start, end in extents:
        if joined and joined[-1][1] == start:
            joined[-1] = (joined[-1][0], end)
        else:
            joined.append((start, end))
    return joined


# The stream pass (spec §3.3 step 3, Pass B; #368, A4b).


def _outside(inner: Box, outer: BBox) -> bool:
    """True if ``inner`` reaches past ``outer`` on any side."""
    return inner[0] < outer.x0 or inner[1] < outer.y0 or inner[2] > outer.x1 or inner[3] > outer.y1


def _plain(bbox: BBox) -> Box:
    """A BBox as a box tuple."""
    return bbox.x0, bbox.y0, bbox.x1, bbox.y1


@dataclass(frozen=True, slots=True, kw_only=True)
class TextCell:
    """A stretch of a stream line between gaps wider than 1.8 em: one column's text on the line.

    Attributes:
        text: Its runs in order along the line, joined as :func:`text_lines`
            joins them; never blank.
        bbox: The union of its pieces' boxes on the displayed page.
        pieces: The char ranges it reads, in their order along the line.
    """

    text: str
    bbox: BBox
    pieces: tuple[SpanPiece, ...]

    def __post_init__(self) -> None:
        non_blank_str("text", self.text)
        _check_bbox("bbox", self.bbox)
        typed_tuple("pieces", self.pieces, SpanPiece)
        if not self.pieces:
            raise ValueError("pieces must hold at least one piece")
        for index, item in enumerate(self.pieces):
            if _outside(item.bbox, self.bbox):
                raise ValueError(f"pieces[{index}] must lie within the bbox, got {item.bbox} in {self.bbox}")


@dataclass(frozen=True, slots=True, kw_only=True)
class TextLine:
    """A line of the page's stream text: its cells, read along the line.

    Attributes:
        cells: Its cells in order along the line (left to right in the
            line's own frame).
        bbox: The union of its cells' boxes on the displayed page.
        turn: The clockwise angle, in degrees, its text advances at on the
            displayed page: 0 (across; right-to-left text too), 90 (down)
            or 270 (up).
        font_size_pt: The font size (magnitude, to 0.1 pt) most of its chars
            are drawn at; a tie goes to the size met first along the line.
        is_bold: True if every run on it is from a bold span.
    """

    cells: tuple[TextCell, ...]
    bbox: BBox
    turn: int
    font_size_pt: float
    is_bold: bool

    def __post_init__(self) -> None:
        typed_tuple("cells", self.cells, TextCell)
        if not self.cells:
            raise ValueError("cells must hold at least one cell")
        _check_bbox("bbox", self.bbox)
        for index, item in enumerate(self.cells):
            if _outside(_plain(item.bbox), self.bbox):
                raise ValueError(f"cells[{index}].bbox must lie within the bbox, got {item.bbox} in {self.bbox}")
        if int_at_least("turn", self.turn, 0) not in _TURNS:
            raise ValueError(f"turn must be 0, 90 or 270, got {self.turn}")
        object.__setattr__(self, "font_size_pt", positive_float("font_size_pt", self.font_size_pt))
        strict_bool("is_bold", self.is_bold)

    @property
    def text(self) -> str:
        """Its cells' texts joined with one space."""
        return " ".join(item.text for item in self.cells)


def text_lines(pieces: tuple[SpanPiece, ...], lattice: LatticeResult) -> tuple[TextLine, ...]:
    """The page's visible text outside its lattice tables, as lines (spec §3.3 step 3, Pass B).

    1. Runs: each visible piece's chars that no lattice table used, cut
       where two neighbouring chars leave a gap over :data:`_CELL_GAP_EM`
       ems of the span's font (where pdfium generates a break across a
       ``TJ`` gap between columns). Whitespace-only runs hold no text and
       are left out; hidden pieces are not laid out here.
    2. Turns: a run reads in the direction its text advances (see
       :func:`find_lattice_tables`); a one-char run takes the page's turn,
       the one most chars advance at (ties: across, down, up). Each turn's
       runs make lines in their own frame, so a label up the margin is a
       line of its own, not glue between the page's lines.
    3. Lines: runs overlapping a line across it by at least
       :data:`_LINE_OVERLAP` of the smaller height are on it (A4a's rule).
    4. Cells: along a line, runs read in order of their boxes (a run keeps
       its span's char order, so right-to-left text reads as pdfium gives
       it); a gap over :data:`_CELL_GAP_EM` ems of the larger font starts a
       cell; inside a cell, runs join as A4a's cells do
       (:func:`_separator`), with one space at each break pdfium generated
       inside a run.

    Lines come in order of their tops, then their left edges, in the frame
    of the page's turn. O(c log c) for c chars.

    Args:
        pieces: The page's pieces (``PageAssessment.pieces``).
        lattice: Their lattice tables (``find_lattice_tables(segments,
            pieces)``): its used pieces share the pieces' spans.

    Raises:
        TypeError: If ``pieces`` is not a tuple of SpanPiece or ``lattice``
            is not a LatticeResult.
        ValueError: If a piece is passed twice, or a used piece of
            ``lattice`` is not a char range inside one visible piece.
    """
    runs = _stream_runs(pieces, lattice)
    return tuple(_ordered_lines(runs, _page_turn(runs)))


# A run with the turn its text advances at; None for one char.
_TurnedRun = tuple[int | None, _Run]


def _stream_runs(pieces: tuple[SpanPiece, ...], lattice: LatticeResult) -> list[_TurnedRun]:
    """The runs the stream pass lays out (:func:`text_lines`, step 1), in piece order, each with its turn."""
    typed_tuple("pieces", pieces, SpanPiece)
    if not isinstance(lattice, LatticeResult):
        raise TypeError(f"lattice must be a LatticeResult, got {type(lattice).__name__}")
    _check_distinct(pieces)
    used = _used_ranges(lattice.used_pieces)
    runs: list[_TurnedRun] = []
    matched: set[int] = set()
    for piece in pieces:
        if piece.hidden_reasons:
            continue
        for start, end in _free_ranges(piece, used.get(id(piece.span), []), matched):
            runs.extend(_cut_at_gaps(_Run(piece, start, end)))
    if len(matched) != len(lattice.used_pieces):
        missing = min(set(range(len(lattice.used_pieces))) - matched)
        where = "is not visible text of these pieces: the lattice result is of another page"
        raise ValueError(f"lattice.used_pieces[{missing}] {where}")
    return [item for item in runs if not _run_chars(item[1]).isspace()]


def _used_ranges(used: tuple[SpanPiece, ...]) -> dict[int, list[tuple[int, int, int]]]:
    """Each span's used char ranges as ``(start, end, index into used)``, sorted, keyed by the span's identity."""
    ranges: dict[int, list[tuple[int, int, int]]] = {}
    for index, item in enumerate(used):
        ranges.setdefault(id(item.span), []).append((item.start, item.end, index))
    for found in ranges.values():
        found.sort()
    return ranges


def _free_ranges(piece: SpanPiece, ranges: list[tuple[int, int, int]], matched: set[int]) -> list[tuple[int, int]]:
    """The piece's char ranges outside its span's used ``ranges``; adds the indices of those inside to ``matched``.

    A used range counts only inside the piece and after the one before it:
    any other matches nothing, so :func:`_stream_runs` rejects it.
    O(log u + the ranges inside) for u used ranges of the span.
    """
    free: list[tuple[int, int]] = []
    cursor = piece.start
    at = bisect_left(ranges, (piece.start,))
    while at < len(ranges) and ranges[at][0] < piece.end:
        start, end, index = ranges[at]
        at += 1
        if end > piece.end or start < cursor:
            continue
        if start > cursor:
            free.append((cursor, start))
        cursor = end
        matched.add(index)
    if cursor < piece.end:
        free.append((cursor, piece.end))
    return free


def _cut_at_gaps(run: _Run) -> list[_TurnedRun]:
    """``run`` cut where two neighbouring chars leave a gap over :data:`_CELL_GAP_EM` ems, each part with its turn."""
    turn = _run_turn(run)
    if turn is None:
        return [(None, run)]
    span = run.piece.span
    boxes = [_turned(box, turn) for box in span.char_boxes[run.start : run.end]]
    limit = _CELL_GAP_EM * abs(span.font_size_pt)
    cuts = [run.start + at for at in range(1, len(boxes)) if _separation(boxes[at - 1], boxes[at]) > limit]
    bounds = (run.start, *cuts, run.end)
    return [(turn, _Run(run.piece, start, end)) for start, end in zip(bounds, bounds[1:])]


def _separation(first: Box, second: Box) -> float:
    """The gap between two boxes along a line, whichever comes first (right-to-left chars run leftward)."""
    return max(second[0] - first[2], first[0] - second[2])


def _run_chars(run: _Run) -> str:
    """A run's chars, as its span holds them."""
    return run.piece.span.text[run.start : run.end]


def _page_turn(runs: list[_TurnedRun]) -> int:
    """The turn most of the runs' chars advance at (ties: across, down, up); across for none."""
    chars = dict.fromkeys(_TURNS, 0)
    for turn, run in runs:
        if turn is not None:
            chars[turn] += run.end - run.start
    return max(_TURNS, key=chars.__getitem__)


def _ordered_lines(runs: list[_TurnedRun], page_turn: int) -> list[TextLine]:
    """The runs' lines in order of their tops, then left edges, in the frame of ``page_turn``."""

    def key(found: TextLine) -> tuple[float, float]:
        x0, y0, _, _ = _turned(_plain(found.bbox), page_turn)
        return y0, x0

    return sorted(_stream_lines(runs, page_turn), key=key)


def _stream_lines(runs: list[_TurnedRun], page_turn: int) -> list[TextLine]:
    """The runs' lines, each turn's in its own frame (:func:`text_lines`, steps 2 to 4), unordered."""
    by_turn: dict[int, list[tuple[Box, _Run]]] = {}
    for turn, run in runs:
        own = page_turn if turn is None else turn
        by_turn.setdefault(own, []).append((_turned(_run_box(run), own), run))
    lines: list[TextLine] = []
    for own, placed in by_turn.items():
        placed.sort(key=lambda item: (item[0][1], item[0][0]))
        lines.extend(_text_line(found, own) for found in _lines(placed))
    return lines


def _text_line(line: list[tuple[Box, _Run]], turn: int) -> TextLine:
    """One line's TextLine: its runs (with their boxes turned by ``turn``) in cells along it."""
    ordered = sorted(line, key=lambda item: item[0][0])
    cells = tuple(_text_cell(group) for group in _cell_groups(ordered))
    runs = [run for _, run in ordered]
    sizes: dict[float, int] = {}
    for run in runs:
        size = round(abs(run.piece.span.font_size_pt), 1)
        sizes[size] = sizes.get(size, 0) + run.end - run.start
    return TextLine(
        cells=cells,
        bbox=_bbox(union_box(tuple(_run_box(run) for run in runs))),
        turn=turn,
        font_size_pt=max(sizes, key=sizes.__getitem__),
        is_bold=all(run.piece.span.is_bold for run in runs),
    )


def _cell_groups(ordered: list[tuple[Box, _Run]]) -> list[list[tuple[Box, _Run]]]:
    """A line's runs, sorted along it, split where one starts over :data:`_CELL_GAP_EM` ems past all before it.

    The em is the larger font of the run and the one before it.
    """
    groups = [[ordered[0]]]
    reach = ordered[0][0][2]
    for (_, previous), item in zip(ordered, ordered[1:]):
        box, run = item
        em = max(abs(previous.piece.span.font_size_pt), abs(run.piece.span.font_size_pt))
        if box[0] - reach > _CELL_GAP_EM * em:
            groups.append([item])
        else:
            groups[-1].append(item)
        reach = max(reach, box[2])
    return groups


def _text_cell(group: list[tuple[Box, _Run]]) -> TextCell:
    """One cell's TextCell: its runs (sorted along the line) joined, boxed and kept as char ranges."""
    runs = [run for _, run in group]
    return TextCell(
        text=_line_text(group),
        bbox=_bbox(union_box(tuple(_run_box(run) for run in runs))),
        pieces=tuple(
            SpanPiece(span=run.piece.span, start=run.start, end=run.end, hidden_reasons=run.piece.hidden_reasons)
            for run in runs
        ),
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class TextBlock:
    """A paragraph of the page's stream text: lines v4's merge joins (see :func:`text_blocks`).

    Attributes:
        lines: Its lines, top to bottom in their own frame.
        bbox: The union of its lines' boxes on the displayed page.
    """

    lines: tuple[TextLine, ...]
    bbox: BBox

    def __post_init__(self) -> None:
        typed_tuple("lines", self.lines, TextLine)
        if not self.lines:
            raise ValueError("lines must hold at least one line")
        _check_bbox("bbox", self.bbox)
        for index, item in enumerate(self.lines):
            if _outside(_plain(item.bbox), self.bbox):
                raise ValueError(f"lines[{index}].bbox must lie within the bbox, got {item.bbox} in {self.bbox}")

    @property
    def text(self) -> str:
        """Its lines' texts joined with one space."""
        return " ".join(item.text for item in self.lines)


def text_blocks(pieces: tuple[SpanPiece, ...], lattice: LatticeResult) -> tuple[TextBlock, ...]:
    """The page's visible text outside its lattice tables, as paragraphs in reading order (spec §3.3 Pass B).

    The runs of :func:`text_lines` (its step 1) are cut into reading-order
    segments by v4's XY-cut (:func:`_xy_cut`) in the frame of the page's
    turn, so columns read in turn and a heading spanning them reads first.
    The cut reads each run's ink (:func:`_ink_box`) on both axes, as v4's
    glyph bounds did: gutters, spanning, column sides and bands. Lines,
    joins, cells and the paragraph merge keep the loose boxes, as the
    lattice pass does.

    Each segment's runs make lines as :func:`text_lines` makes them, and
    its lines merge into paragraphs by v4's rule (:func:`_paragraphs`).
    Blocks come segment by segment, each segment's top to bottom.

    Cost: O(c log c) for the runs and lines of c chars, and the XY-cut's
    O(D r log r) for r runs and D = :data:`MAX_PARTITION_DEPTH` + 1.

    Args:
        pieces: The page's pieces (``PageAssessment.pieces``).
        lattice: Their lattice tables (``find_lattice_tables(segments,
            pieces)``): its used pieces share the pieces' spans.

    Raises:
        TypeError: If ``pieces`` is not a tuple of SpanPiece or ``lattice``
            is not a LatticeResult.
        ValueError: If a piece is passed twice, or a used piece of
            ``lattice`` is not a char range inside one visible piece.
    """
    runs = _stream_runs(pieces, lattice)
    turn = _page_turn(runs)
    boxes = [_turned(_ink_box(run), turn) for _, run in runs]
    blocks: list[TextBlock] = []
    for segment in _xy_cut(boxes, [run.end - run.start for _, run in runs]):
        blocks.extend(_paragraphs(_ordered_lines([runs[index] for index in segment], turn)))
    return tuple(blocks)


def _ink_box(run: _Run) -> Box:
    """Where a run's ink is: the union of its non-whitespace chars' tight boxes (a stream run has one).

    Tight boxes are pdfium's glyph bounds (``RawSpan.tight_boxes``), whose
    union for a text object is the object bounds v4's XY-cut measured.
    Loose boxes run each glyph's full advance, a side bearing past its ink
    at each edge, and the font's ascent to descent, so they narrow a
    gutter (by 1 to 2 pt on the corpus) and make lines set solid overlap.
    A whitespace char draws no ink: pdfium stands in its advance along the
    baseline, and a trailing space would narrow the gutter after it.
    """
    span = run.piece.span
    return union_box(tuple(span.tight_boxes[at] for at in range(run.start, run.end) if not span.text[at].isspace()))


# An XY-cut region: its items (indices into the boxes) and its depth; None for a segment already read.
_Region = tuple[list[int], int | None]


def _xy_cut(boxes: list[Box], weights: list[int]) -> list[list[int]]:
    """The items' reading-order segments: lists of indices into ``boxes``, in reading order.

    v4's recursive XY-cut (``layout_sorter.PageLayoutSorter.partition``),
    ported to a stack of regions: each region is cut into parts read in
    order (:func:`_cut_region`), or is a segment. ``weights`` (chars per
    item) make the density histogram. Boxes are in the frame the page
    reads in: left to right, lines top to bottom.

    Bound: a region at depth :data:`MAX_PARTITION_DEPTH` is not cut, and
    the regions at one depth hold disjoint items (each cut deals its
    region's items out once), so at most D = MAX_PARTITION_DEPTH + 1
    levels each cut at most n items: O(D n log n) for n items, with no
    recursion.

    Known limitations:

    - v4's: a heading over the gutter narrower than :data:`_WIDE_SHARE` of
      its region adds weight to it, so the region is not cut and its
      columns read as one segment.
    - v4's: each line spanning the gutter is a band of its own, so a
      full-width paragraph above or between columns reads in order but
      as one block per line.
    """
    segments: list[list[int]] = []
    stack: list[_Region] = [(list(range(len(boxes))), 0)] if boxes else []
    while stack:
        items, depth = stack.pop()
        parts = None if depth is None else _cut_region(boxes, weights, items, depth)
        if parts is None:
            segments.append(items)
        else:
            stack.extend(reversed(parts))
    return segments


def _cut_region(boxes: list[Box], weights: list[int], items: list[int], depth: int) -> list[_Region] | None:
    """A region's parts in reading order (v4's ``partition`` step), or None to read it as one segment.

    No cut at :data:`MAX_PARTITION_DEPTH`, for fewer than
    :data:`_MIN_CUT_ITEMS` items, a region under :data:`_MIN_CUT_WIDTH_PT`
    wide or one with no gutter (:func:`_widest_gutter`). The items not
    spanning the gutter (:func:`_spans`) are its columns' text: unless
    they lie on both sides of it (by box centre), there are no columns to
    cut. Else the spanning items cut the columns into bands
    (:func:`_bands`), or with none the columns split into the two sides.

    v4 asked for text on both sides only before that split. Asked before
    the bands too (a v5 fix): a region of lines each wider than
    :data:`_WIDE_SHARE` of it (justified prose drawn a line per run) has
    weight nowhere, so v4 found a gutter that every line spans and read
    each line as a band of its own, which no paragraph merge could join.
    """
    if depth >= MAX_PARTITION_DEPTH or len(items) < _MIN_CUT_ITEMS:
        return None
    x0, y0, x1, y1 = union_box(tuple(boxes[index] for index in items))
    if x1 - x0 < _MIN_CUT_WIDTH_PT:
        return None
    gutter = _widest_gutter(boxes, weights, items, x0, x1 - x0)
    if gutter is None:
        return None
    spanning = [index for index in items if _spans(boxes[index], gutter, y1 - y0)]
    across = set(spanning)
    columns = [index for index in items if index not in across]
    left = [index for index in columns if (boxes[index][0] + boxes[index][2]) / 2 < gutter]
    if not left or len(left) == len(columns):
        return None
    if not spanning:
        chosen = set(left)
        return [(left, depth + 1), ([index for index in columns if index not in chosen], depth + 1)]
    return _bands(boxes, spanning, columns, depth)


def _widest_gutter(boxes: list[Box], weights: list[int], items: list[int], left: float, width: float) -> float | None:
    """The centre of the region's widest gutter, or None (v4's density histogram and ``_find_widest_gutter``).

    v4's histogram has a 1 pt bin per whole point of x: an item adds its
    weight to the bins from ``floor(x0)`` up to ``floor(x1)`` (items wider
    than :data:`_WIDE_SHARE` of the region add none). A gutter is a run of
    at least :data:`_GUTTER_PT` bins of weight at most :data:`_FREE_WEIGHT`
    inside :data:`_GUTTER_WINDOW` of the region; the widest wins, the
    leftmost of equals. The bins are read run-length encoded
    (:func:`_free_runs`): O(k log k) for k items, whatever their width.
    """
    covers = []
    for index in items:
        x0, _, x1, _ = boxes[index]
        start, end = math.floor(x0), math.floor(x1)
        if x1 - x0 <= _WIDE_SHARE * width and end > start:
            covers.append((start, end, weights[index]))
    low = math.floor(left + _GUTTER_WINDOW[0] * width)
    high = max(low, math.floor(left + _GUTTER_WINDOW[1] * width))
    widest = max(_free_runs(covers, low, high), key=lambda run: run[1] - run[0], default=None)
    if widest is None or widest[1] - widest[0] < _GUTTER_PT:
        return None
    return widest[0] + (widest[1] - widest[0]) / 2


def _free_runs(covers: list[tuple[int, int, int]], low: int, high: int) -> Iterator[tuple[int, int]]:
    """Yield, left to right, each maximal run ``[start, end)`` of bins in ``[low, high)`` of weight at most 1.

    ``covers`` are ``(first bin, end bin, weight)``; the weight changes
    only where one starts or ends.
    """
    deltas: dict[int, int] = {}
    for start, end, weight in covers:
        deltas[start] = deltas.get(start, 0) + weight
        deltas[end] = deltas.get(end, 0) - weight
    weight = sum(delta for point, delta in deltas.items() if point <= low)
    run_start = low if weight <= _FREE_WEIGHT else None
    for point in sorted(point for point in deltas if low < point < high):
        weight += deltas[point]
        if weight <= _FREE_WEIGHT and run_start is None:
            run_start = point
        elif weight > _FREE_WEIGHT and run_start is not None:
            yield run_start, point
            run_start = None
    if run_start is not None and run_start < high:
        yield run_start, high


def _spans(box: Box, gutter: float, height: float) -> bool:
    """True if ``box`` spans the gutter (v4's ``_split_columns_and_spanning``)."""
    reaches = box[0] < gutter - _SPAN_REACH_PT and box[2] > gutter + _SPAN_REACH_PT
    return reaches and box[3] - box[1] < _SPAN_HEIGHT_SHARE * height


def _bands(boxes: list[Box], spanning: list[int], columns: list[int], depth: int) -> list[_Region]:
    """v4's ``_slice_y_bands``: the spanning items' intervals cut the column items into bands by their tops.

    In order, for each interval: the column items above it (a region),
    then the interval's items with the column items whose tops fall inside
    it (a segment, by top then left); then the column items below the last
    interval (a region). The bands tile every top, so no item is left over.
    """
    intervals = _spanning_intervals(boxes, spanning)
    edges = [edge for top, bottom, _ in intervals for edge in (top, bottom)]
    bands: list[list[int]] = [[] for _ in range(len(edges) + 1)]
    for index in columns:
        bands[bisect_right(edges, boxes[index][1])].append(index)
    parts: list[_Region] = []
    for at, (_, _, members) in enumerate(intervals):
        if bands[2 * at]:
            parts.append((bands[2 * at], depth + 1))
        band = sorted(members + bands[2 * at + 1], key=lambda index: (boxes[index][1], boxes[index][0]))
        parts.append((band, None))
    if bands[-1]:
        parts.append((bands[-1], depth + 1))
    return parts


def _spanning_intervals(boxes: list[Box], spanning: list[int]) -> list[tuple[float, float, list[int]]]:
    """The spanning items, by top, grouped while they overlap down the page: ``(top, bottom, items)`` each."""
    intervals: list[tuple[float, float, list[int]]] = []
    for index in sorted(spanning, key=lambda item: boxes[item][1]):
        _, top, _, bottom = boxes[index]
        if intervals and top <= intervals[-1][1]:
            first, last, members = intervals[-1]
            members.append(index)
            intervals[-1] = (first, max(last, bottom), members)
        else:
            intervals.append((top, bottom, [index]))
    return intervals


def _paragraphs(lines: list[TextLine]) -> list[TextBlock]:
    """Lines (in reading order) merged into paragraphs: v4's ``_merge_lines``, ported (see :func:`_continues`)."""
    groups: list[list[TextLine]] = []
    extents: list[tuple[float, float]] = []
    for line in lines:
        box = _turned(_plain(line.bbox), line.turn)
        if groups and _continues(groups[-1], extents[-1], line, box):
            groups[-1].append(line)
            extents[-1] = (min(extents[-1][0], box[0]), max(extents[-1][1], box[2]))
        else:
            groups.append([line])
            extents.append((box[0], box[2]))
    return [
        TextBlock(lines=tuple(group), bbox=_bbox(union_box(tuple(_plain(item.bbox) for item in group))))
        for group in groups
    ]


def _continues(group: list[TextLine], extent: tuple[float, float], line: TextLine, box: Box) -> bool:
    """True if ``line`` (``box`` in its own frame) continues the paragraph ``group`` (v4's ``_should_merge``).

    v4's rule: the line does not start with a list marker; its font size
    is within :data:`_SAME_SIZE_PT` of the paragraph's first line; it
    starts at most :data:`_PARAGRAPH_GAP_EM` ems below the last line; it
    overlaps the paragraph's extent across. v5 adds: the same turn, the
    geometry read in its frame. v4 asked a gap of at least 0: its object
    boxes were tight, but pdfium's char boxes run ascent to descent, so
    lines set solid overlap; v5 asks the line to start below the last
    line's top instead. Sizes are to 0.1 pt and their difference is
    rounded so, so float error cannot move it across the threshold.
    """
    first, last = group[0], group[-1]
    if line.turn != first.turn or _LIST_MARKER.match(line.text.lstrip()):
        return False
    if round(abs(line.font_size_pt - first.font_size_pt), 1) >= _SAME_SIZE_PT:
        return False
    _, last_top, _, last_bottom = _turned(_plain(last.bbox), line.turn)
    below = last_top < box[1] and box[1] - last_bottom <= _PARAGRAPH_GAP_EM * max(first.font_size_pt, 1.0)
    return below and box[0] <= extent[1] and box[2] >= extent[0]
