"""Page layout for the v5 PDF document model (#367, A4a: the vector-lattice table pass).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads the engine's segments (:class:`~datagrunt.pdf._path_segments.RawSegment`)
and A3's span pieces (:class:`~datagrunt.pdf.quality.SpanPiece`) and calls
no pdfium function. Boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.
"""

import math
from bisect import bisect_left, bisect_right, insort
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import NamedTuple

from datagrunt.pdf._path_segments import Box, RawSegment
from datagrunt.pdf._validate import int_at_least, typed_tuple
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.quality import SpanPiece

__all__ = ["LatticeCell", "LatticeResult", "LatticeTable"]

# Lines this close snap to one position, join end to end, and meet (spec §3.3 Pass A).
SNAP_PT = 2.0

# A table has at least this many rows and columns of cells (v4's drop_layout_tables minimum).
_MIN_GRID = 2

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


Component = tuple[list[_Line], list[_Line]]


def _components(horizontal: list[_Line], vertical: list[_Line]) -> list[Component]:
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
    members: dict[int, Component] = {}
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
