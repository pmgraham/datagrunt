"""Page layout for the v5 PDF document model (#367, A4a: the vector-lattice table pass).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads the engine's segments (:class:`~datagrunt.pdf._path_segments.RawSegment`)
and A3's span pieces (:class:`~datagrunt.pdf.quality.SpanPiece`) and calls
no pdfium function. Boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.

The lattice pass (:func:`find_lattice_tables`, spec §3.3 step 3, Pass A)
builds ruled tables from a page's segments and puts each visible char in
the cell holding its box centre. Its :class:`LatticeResult` names the text
the tables used, so the stream pass (A4b, #368) lays out only the rest;
element ids and reading order come there too, so the tables are
intermediate :class:`LatticeTable` values.

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
from bisect import bisect_left, bisect_right, insort
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import groupby
from typing import NamedTuple

from datagrunt.pdf._path_segments import Box, RawSegment
from datagrunt.pdf._pdfium_common import union_box
from datagrunt.pdf._validate import int_at_least, typed_tuple
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.quality import SpanPiece

__all__ = ["LatticeCell", "LatticeResult", "LatticeTable", "find_lattice_tables"]

# Lines this close snap to one position, join end to end, and meet (spec §3.3 Pass A).
SNAP_PT = 2.0

# A table has at least this many rows and columns of cells (v4's drop_layout_tables minimum).
_MIN_GRID = 2

# A run joins a line when they overlap across it by this share of the smaller height (spec §3.3 Pass B).
_LINE_OVERLAP = 0.45
# Runs on a line join with nothing across a gap of at most this many ems, else with a space (spec §3.3 Pass B).
_JOIN_GAP_EM = 0.22
# Reading turns: the clockwise angle a run's text advances at on the displayed page. Text advancing
# leftward reads across too: right-to-left text comes in logical order, as upside-down text would.
_ACROSS, _DOWN, _UP = 0, 90, 270

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
    """

    tables: tuple[LatticeTable, ...]
    used_pieces: tuple[SpanPiece, ...]

    def __post_init__(self) -> None:
        typed_tuple("tables", self.tables, LatticeTable)
        typed_tuple("used_pieces", self.used_pieces, SpanPiece)
        for index, item in enumerate(self.used_pieces):
            if item.hidden_reasons:
                raise ValueError(
                    f"used_pieces[{index}] is hidden ({item.hidden_reasons!r}): hidden text never enters a table"
                )


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

    Cost: O(n log n + I) for n segments and I crossings, plus O(X Y) per
    component of X by Y positions, plus O(chars x G + chars log(X + Y))
    for G grids near each piece.

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
    found = (_grid(*group) for group in _components(*_snapped_lines(segments)))
    grids = [grid for grid in found if grid is not None]
    tables: list[LatticeTable] = []
    used: list[_Run] = []
    for grid, cell_runs in zip(grids, _cell_runs(grids, pieces)):
        table = _table(grid, cell_runs)
        if table is not None:
            tables.append(table)
            used.extend(run for runs in cell_runs for run in runs)
    return LatticeResult(tables=tuple(tables), used_pieces=_used_pieces(used, pieces))


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


def _components(horizontal: list[_Line], vertical: list[_Line]) -> list[_Component]:
    """The groups of rules connected by crossings, each as its horizontal and vertical rules.

    Two rules cross when each reaches the other's position within
    :data:`SNAP_PT` (:func:`_crossings`); groups are the connected
    components of that relation (union-find). A group without both
    orientations rules no cell and is left out. Groups come in order of
    their first horizontal rule, each with its rules in input order.
    O((H + V) log(H + V) + I) for H horizontal and V vertical rules with
    I crossings (union-find is near-constant per crossing).
    """
    groups = _UnionFind(len(horizontal) + len(vertical))
    for h_index, v_index in _crossings(horizontal, vertical):
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
    positions and R rules.
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
    right and join with nothing across a gap of at most
    :data:`_JOIN_GAP_EM` ems of the larger font, else with one space; a
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
    """A line's runs left to right, joined by the gap between them (see :func:`_cell_text`)."""
    ordered = sorted(line, key=lambda item: item[0][0])
    parts = [_run_text(ordered[0][1])]
    for (before, previous), (box, run) in zip(ordered, ordered[1:]):
        em = max(abs(previous.piece.span.font_size_pt), abs(run.piece.span.font_size_pt))
        parts.append("" if box[0] - before[2] <= _JOIN_GAP_EM * em else " ")
        parts.append(_run_text(run))
    return "".join(parts)


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
