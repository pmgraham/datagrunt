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
(v5 imports no v4 code); :func:`classify_blocks` reads the blocks as
headings, key-value pairs, lists and paragraphs (:class:`StreamHeading`,
:class:`StreamKeyValue`, :class:`StreamList`, :class:`StreamParagraph`),
each keeping the char ranges it reads. :func:`stream_layout` puts it
together: borderless tables (:class:`StreamTable`), mastheads and
key-value lists found on the lines and read whole, the rest classified,
all in reading order. :func:`page_layout` adds the page's hidden and
recovered text, laid out apart as lines (:class:`ApartLine`) and placed
among the visible items by position, so it never changes their order.

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
from fractions import Fraction
from itertools import groupby
from typing import NamedTuple

from datagrunt.pdf._path_segments import Box, RawSegment
from datagrunt.pdf._pdfium_common import union_box
from datagrunt.pdf._validate import (
    finite_float,
    int_at_least,
    non_blank_str,
    positive_float,
    str_tuple,
    strict_bool,
    typed_tuple,
)
from datagrunt.pdf.elements import BBox, HiddenReason, check_hidden_reasons
from datagrunt.pdf.quality import SpanPiece

__all__ = [
    "ApartLine",
    "LatticeCell",
    "LatticeResult",
    "LatticeTable",
    "PageLayout",
    "StreamHeading",
    "StreamKeyValue",
    "StreamList",
    "StreamListItem",
    "StreamParagraph",
    "StreamTable",
    "TextBlock",
    "TextCell",
    "TextLine",
    "classify_blocks",
    "find_lattice_tables",
    "page_layout",
    "stream_layout",
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
# starting with a list marker or a section number starts a paragraph (v4's LIST_MARKER_REGEX, revised: see
# _starts_paragraph). Unordered markers: the spec's bullet, hyphen and asterisk, the » Root's Learn to Play
# bullets with, and Unicode's bullet glyphs; a hyphen or asterisk only before whitespace.
_BULLETS = "•‣⁃◦∙▪●»"
# An ordinal: a number of up to 3 digits, a letter, or a Roman numeral of 2 to 5 of i, v and x.
_ORDINAL = r"(?:\d{1,3}|[A-Za-z]|[ivx]{2,5}|[IVX]{2,5})"
# Ordered markers: an ordinal before "." or ")", or in brackets; or a bare upper-case Roman numeral before a
# capital (Root's Law: "I  Battle."). Each before whitespace or the end of the text.
_LIST_MARKER = re.compile(
    rf"(?P<bullet>[{_BULLETS}]|[-*](?=\s))"
    rf"|(?P<ordinal>\({_ORDINAL}\)|{_ORDINAL}[.)]|[IVX]{{1,5}}(?=\s+[A-Z]))(?=\s|$)"
)
# A section number: numbers joined by dots, or a letter and numbers ("1.1.1", "A.5.1", "G.24").
_SECTION_NUMBER = re.compile(r"(?:\d+|[A-Z])(?:\.\d+)+\.?(?=\s|$)")
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
        """v4's rule (:func:`_has_header`)."""
        return _has_header(self.grid())


def _has_header(grid: tuple[tuple[str, ...], ...]) -> bool:
    """v4's rule: more than one row, and every row-0 grid cell non-blank."""
    return len(grid) > 1 and all(text.strip() for text in grid[0])


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


def _check_distinct(pieces: tuple[SpanPiece, ...], name: str = "pieces") -> None:
    """Require each piece object once: a piece is a run of the page's text, which is read once."""
    seen: dict[int, int] = {}
    for index, item in enumerate(pieces):
        first = seen.setdefault(id(item), index)
        if first != index:
            raise ValueError(f"{name}[{index}] is {name}[{first}] again: each piece is the page's text once")


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
        _check_sources(_check_bbox("bbox", self.bbox), self.pieces)


def _check_sources(bbox: BBox, pieces: object) -> None:
    """Require ``pieces`` to be a non-empty tuple of SpanPiece, each within ``bbox``."""
    checked = typed_tuple("pieces", pieces, SpanPiece)
    if not checked:
        raise ValueError("pieces must hold at least one piece")
    for index, item in enumerate(checked):
        if _outside(item.bbox, bbox):
            raise ValueError(f"pieces[{index}] must lie within the bbox, got {item.bbox} in {bbox}")


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
        _check_line_shape(self.cells, self.bbox, self.turn)
        object.__setattr__(self, "font_size_pt", positive_float("font_size_pt", self.font_size_pt))
        strict_bool("is_bold", self.is_bold)

    @property
    def text(self) -> str:
        """Its cells' texts joined with one space."""
        return " ".join(item.text for item in self.cells)


def _check_line_shape(cells: object, bbox: object, turn: object) -> None:
    """Require a line's cells (a non-empty tuple of TextCell, each within ``bbox``), its BBox and its turn."""
    checked = typed_tuple("cells", cells, TextCell)
    if not checked:
        raise ValueError("cells must hold at least one cell")
    outer = _check_bbox("bbox", bbox)
    for index, item in enumerate(checked):
        if _outside(_plain(item.bbox), outer):
            raise ValueError(f"cells[{index}].bbox must lie within the bbox, got {item.bbox} in {outer}")
    if int_at_least("turn", turn, 0) not in _TURNS:
        raise ValueError(f"turn must be 0, 90 or 270, got {turn}")


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
    return [_text_line(found, own) for found, own in _grouped_lines(runs, page_turn)]


def _grouped_lines(runs: list[_TurnedRun], page_turn: int) -> Iterator[tuple[list[tuple[Box, _Run]], int]]:
    """Yield each line of the runs (its runs with their boxes turned) and its turn: each turn's in its own frame."""
    by_turn: dict[int, list[tuple[Box, _Run]]] = {}
    for turn, run in runs:
        own = page_turn if turn is None else turn
        by_turn.setdefault(own, []).append((_turned(_run_box(run), own), run))
    for own, placed in by_turn.items():
        placed.sort(key=lambda item: (item[0][1], item[0][0]))
        for found in _lines(placed):
            yield found, own


def _line_cells(line: list[tuple[Box, _Run]]) -> tuple[tuple[TextCell, ...], BBox, list[_Run]]:
    """One line's cells along it, its box on the displayed page, and its runs in order along it."""
    ordered = sorted(line, key=lambda item: item[0][0])
    runs = [run for _, run in ordered]
    cells = tuple(_text_cell(group) for group in _cell_groups(ordered))
    return cells, _bbox(union_box(tuple(_run_box(run) for run in runs))), runs


def _text_line(line: list[tuple[Box, _Run]], turn: int) -> TextLine:
    """One line's TextLine: its runs (with their boxes turned by ``turn``) in cells along it."""
    cells, bbox, runs = _line_cells(line)
    sizes: dict[float, int] = {}
    for run in runs:
        size = round(abs(run.piece.span.font_size_pt), 1)
        sizes[size] = sizes.get(size, 0) + run.end - run.start
    return TextLine(
        cells=cells,
        bbox=bbox,
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

    v4's rule: the line does not start a paragraph by its text
    (:func:`_starts_paragraph`); its font size is within
    :data:`_SAME_SIZE_PT` of the paragraph's first line; it starts at most
    :data:`_PARAGRAPH_GAP_EM` ems below the last line; it overlaps the
    paragraph's extent across. v5 adds: the same turn, the geometry read in
    its frame; the same boldness as the last line (:attr:`TextLine.is_bold`),
    so a bold heading set at the body's size, directly over it, stays a
    block of its own (on the corpus this splits 18 blocks, each a bold
    question, heading or label from the text under it). v4 asked a gap of
    at least 0: its object boxes were tight, but pdfium's char boxes run
    ascent to descent, so lines set solid overlap; v5 asks the line to
    start below the last line's top instead. Sizes are to 0.1 pt and their
    difference is rounded so, so float error cannot move it across the
    threshold.
    """
    first, last = group[0], group[-1]
    if line.turn != first.turn or line.is_bold != last.is_bold or _starts_paragraph(line.text.lstrip()):
        return False
    if round(abs(line.font_size_pt - first.font_size_pt), 1) >= _SAME_SIZE_PT:
        return False
    _, last_top, _, last_bottom = _turned(_plain(last.bbox), line.turn)
    below = last_top < box[1] and box[1] - last_bottom <= _PARAGRAPH_GAP_EM * max(first.font_size_pt, 1.0)
    return below and box[0] <= extent[1] and box[2] >= extent[0]


def _starts_paragraph(text: str) -> bool:
    """True if ``text`` starts with a list marker or a section number, so its line starts a paragraph.

    v4's markers (``text_block_builder.LIST_MARKER_REGEX``), revised:

    - A bare number is no marker (v4's was): Root's Law wraps lines
      before "3 quest cards", "20 warriors" and "1 card"; only "1." or
      "1)" mark items. A number of 4 digits or more is none either.
    - Roman numerals are i, v and x only, and a bare upper-case one must
      precede a capital ("I  Battle." in Root's Law), so words such as
      "CIVIL" or "MIX" are none (v4 took any run of I, V, X, L, C, D, M).
    - Added: bracketed ordinals ("(a)", "(iv)"; the spec's), Unicode's
      bullet glyphs, and » (Root's Learn to Play bullets with it), the
      bullets also before no space; letter-led section numbers
      ("A.5.1", Root's Law's appendices).
    """
    return _LIST_MARKER.match(text) is not None or _SECTION_NUMBER.match(text) is not None


# Classification (spec §3.3 step 3, Pass B).

# A heading's size is at least this share of the page's body size (spec) ...
_HEADING_RATIO = Fraction(115, 100)
# ... and its text is at most this many chars: corpus headings run to 49; text set as large but longer was always
# prose (68 to 581 chars).
_HEADING_MAX_CHARS = 60
# Levels run from 1 (the page's largest heading size) to this.
_MAX_LEVEL = 6
# A key is at most this many chars (spec) and holds none of these (sentence-ending punctuation).
_MAX_KEY_CHARS = 40
_SENTENCE_END = frozenset(".!?")
# A key ends at the first colon before whitespace or the end of the line: "10:30" or "https://" holds none.
_KEY_END = re.compile(r":(?=\s|$)")


@dataclass(frozen=True, slots=True, kw_only=True)
class _StreamText:
    """Fields shared by the classified text items: their box and the char ranges they read.

    Attributes:
        bbox: The union of the item's lines' boxes on the displayed page.
        pieces: The char ranges it reads, in reading order: every char of
            its lines once, markers and separators included (Pass C).
    """

    bbox: BBox
    pieces: tuple[SpanPiece, ...]

    def __post_init__(self) -> None:
        _check_sources(_check_bbox("bbox", self.bbox), self.pieces)
        self._validate_fields()

    def _validate_fields(self) -> None:
        """Validate the subclass's own fields (A1's ``_Element`` hook)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamHeading(_StreamText):
    """A heading of the stream text, ``level`` 1-6: A1's ``Heading`` before its id and scope."""

    text: str
    level: int

    def _validate_fields(self) -> None:
        non_blank_str("text", self.text)
        if int_at_least("level", self.level, 1) > _MAX_LEVEL:
            raise ValueError(f"level must be <= {_MAX_LEVEL}, got {self.level}")


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamParagraph(_StreamText):
    """A paragraph of the stream text: A1's ``Paragraph`` before its id."""

    text: str

    def _validate_fields(self) -> None:
        non_blank_str("text", self.text)


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamKeyValue(_StreamText):
    """A ``Key: Value`` pair of the stream text: A1's ``KeyValue`` before its id.

    The colon after the key is in neither field (it is in ``pieces``);
    ``value`` may be empty.
    """

    key: str
    value: str

    def _validate_fields(self) -> None:
        non_blank_str("key", self.key)
        if not isinstance(self.value, str):
            raise TypeError(f"value must be a str, got {self.value!r} ({type(self.value).__name__})")


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamListItem(_StreamText):
    """One item of a stream list: its marker as written ("•", "1.", "(a)") and its text after it."""

    marker: str
    text: str

    def _validate_fields(self) -> None:
        non_blank_str("marker", self.marker)
        non_blank_str("text", self.text)


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamList:
    """A list of the stream text: A1's ``BulletList`` before its id.

    Attributes:
        items: Its items in reading order, one per marker.
        ordered: True if its markers are ordinals ("1.", "(a)", "IV."),
            False if bullets.
        bbox: The union of its items' boxes on the displayed page.
    """

    items: tuple[StreamListItem, ...]
    ordered: bool
    bbox: BBox

    def __post_init__(self) -> None:
        typed_tuple("items", self.items, StreamListItem)
        if not self.items:
            raise ValueError("items must hold at least one item")
        strict_bool("ordered", self.ordered)
        _check_bbox("bbox", self.bbox)
        for index, item in enumerate(self.items):
            if _outside(_plain(item.bbox), self.bbox):
                raise ValueError(f"items[{index}].bbox must lie within the bbox, got {item.bbox} in {self.bbox}")

    @property
    def pieces(self) -> tuple[SpanPiece, ...]:
        """Its items' pieces, in order."""
        return tuple(piece for item in self.items for piece in item.pieces)


StreamItem = StreamHeading | StreamParagraph | StreamKeyValue | StreamList


def classify_blocks(blocks: tuple[TextBlock, ...]) -> tuple[StreamItem, ...]:
    """The page's stream blocks (:func:`text_blocks`, in reading order) as headings, pairs, lists and paragraphs.

    Spec §3.3 step 3, Pass B; each block is, by the first rule it meets:

    1. A heading (:func:`_is_heading`): its first line's size is at least
       :data:`_HEADING_RATIO` times the page's body size
       (:func:`_body_size`; a ratio, never a division: :func:`_larger`),
       or it is one bold line; either way its text
       is at most :data:`_HEADING_MAX_CHARS` chars, does not end with a
       full stop and does not start with a bullet. Levels rank the page's
       heading sizes, largest first, from 1 to :data:`_MAX_LEVEL`.
    2. A list item: its text starts with a list marker
       (:data:`_LIST_MARKER`) and holds text after it; its continuation
       lines are the block's other lines (the merge starts a block at each
       marker). Neighbouring items, all bulleted or all numbered, are one
       list.
    3. Key-value pairs (:func:`_pairs_and_paragraphs`): its first line is
       ``Key: Value``, the key at most :data:`_MAX_KEY_CHARS` chars with no
       sentence-ending punctuation; each such line starts a pair, the
       lines after it continue its value while they wrap, and a paragraph
       then runs to the block's end.
    4. Else a paragraph.

    Items keep their blocks' order and every char of them once, in their
    ``pieces``. O(c + s log s) for c chars of s distinct sizes.

    Raises:
        TypeError: If ``blocks`` is not a tuple of TextBlock.
    """
    typed_tuple("blocks", blocks, TextBlock)
    return tuple(_classified_runs([list(blocks)])[0])


def _classified_runs(runs: list[list[TextBlock]]) -> list[list[StreamItem]]:
    """Each run of neighbouring blocks classified apart, with the body size and heading levels of them all.

    A list never runs from one run into the next: :func:`stream_layout`
    passes the page's paragraphs between its tables as runs.
    """
    every = [found for run in runs for found in run]
    body = _body_size(every)
    flags = [_is_heading(found, body) for found in every]
    levels = _heading_levels(_tenths(found.lines[0].font_size_pt) for found, flag in zip(every, flags) if flag)
    classified: list[list[StreamItem]] = []
    start = 0
    for run in runs:
        classified.append(_classified(run, flags[start : start + len(run)], levels))
        start += len(run)
    return classified


def _classified(blocks: list[TextBlock], headings: list[bool], levels: dict[int, int]) -> list[StreamItem]:
    """Neighbouring blocks classified (:func:`classify_blocks`), given which are headings and each size's level."""
    items: list[StreamItem] = []
    pending: list[tuple[StreamListItem, bool]] = []
    for found, is_heading in zip(blocks, headings):
        entry = None if is_heading else _list_item(found)
        if pending and (entry is None or entry[1] != pending[0][1]):
            items.append(_stream_list(pending))
            pending = []
        if entry is not None:
            pending.append(entry)
        elif is_heading:
            level = levels[_tenths(found.lines[0].font_size_pt)]
            items.append(
                StreamHeading(text=found.text, level=level, bbox=found.bbox, pieces=_lines_pieces(found.lines))
            )
        else:
            items.extend(_pairs_and_paragraphs(found))
    if pending:
        items.append(_stream_list(pending))
    return items


def _tenths(size: float) -> int:
    """A font size (to 0.1 pt, as lines hold it) in tenths of a point: exact for comparing."""
    return round(size * 10)


def _body_size(blocks: list[TextBlock]) -> int:
    """The page's body size in tenths: the upper median of its lines' sizes, each weighed by its non-blank chars.

    v4's median (``page_median_size``: the upper middle, not a mean),
    weighed by chars so a page's many short headings or labels do not move
    it. 0 for no blocks, and for a page whose text is mostly under 0.05 pt.
    """
    weights: dict[int, int] = {}
    for found in blocks:
        for item in found.lines:
            size = _tenths(item.font_size_pt)
            weights[size] = weights.get(size, 0) + sum(1 for char in item.text if not char.isspace())
    middle, seen = sum(weights.values()) // 2, 0
    for size in sorted(weights):
        seen += weights[size]
        if seen > middle:
            return size
    return 0


def _is_heading(found: TextBlock, body: int) -> bool:
    """True if the block is a heading (:func:`classify_blocks`, rule 1) on a page of ``body`` size (tenths)."""
    text = found.text.strip()
    marker = _LIST_MARKER.match(text)
    if len(text) > _HEADING_MAX_CHARS or text.endswith(".") or (marker is not None and marker["bullet"]):
        return False
    first = found.lines[0]
    if _larger(_tenths(first.font_size_pt), body):
        return True
    return len(found.lines) == 1 and first.is_bold


def _larger(size: int, body: int) -> bool:
    """True if ``size / body`` (both in tenths) is at least :data:`_HEADING_RATIO`, compared without dividing.

    Read as the ratio it is, also on a body of 0 tenths (text under
    0.05 pt): every size over 0 is infinitely larger, and 0 / 0 is no
    ratio at all, so equal sizes never make a heading. On a body over 0,
    ``size > body`` follows from the ratio.
    """
    return size > body and size >= _HEADING_RATIO * body


def _heading_levels(sizes: Iterable[int]) -> dict[int, int]:
    """Each heading size's level: its rank among the distinct sizes, largest first, at most :data:`_MAX_LEVEL`."""
    return {size: min(rank, _MAX_LEVEL) for rank, size in enumerate(sorted(set(sizes), reverse=True), 1)}


def _lines_pieces(lines: Iterable[TextLine]) -> tuple[SpanPiece, ...]:
    """The char ranges of ``lines``, in order."""
    return tuple(piece for item in lines for found in item.cells for piece in found.pieces)


def _list_item(found: TextBlock) -> tuple[StreamListItem, bool] | None:
    """The block as a list item and whether it is ordered, or None if it starts with no marker or holds nothing more."""
    text = found.text.lstrip()
    marker = _LIST_MARKER.match(text)
    if marker is None or not text[marker.end() :].strip():
        return None
    item = StreamListItem(
        marker=marker[0], text=text[marker.end() :].strip(), bbox=found.bbox, pieces=_lines_pieces(found.lines)
    )
    return item, marker["ordinal"] is not None


def _stream_list(entries: list[tuple[StreamListItem, bool]]) -> StreamList:
    """One list of neighbouring items, all bulleted or all numbered."""
    items = tuple(item for item, _ in entries)
    bbox = _bbox(union_box(tuple(_plain(item.bbox) for item in items)))
    return StreamList(items=items, ordered=entries[0][1], bbox=bbox)


def _key_value(text: str) -> tuple[str, str] | None:
    """``(key, value)`` of a ``Key: Value`` line, or None (:func:`classify_blocks`, rule 3)."""
    end = _KEY_END.search(text)
    if end is None:
        return None
    key = text[: end.start()].strip()
    if not key or len(key) > _MAX_KEY_CHARS or not _SENTENCE_END.isdisjoint(key):
        return None
    return key, text[end.end() :].strip()


def _pairs_and_paragraphs(found: TextBlock) -> list[StreamKeyValue | StreamParagraph]:
    """The block as key-value pairs and paragraphs (:func:`classify_blocks`, rules 3 and 4).

    A ``Key: Value`` line starts a pair; a line after it continues its
    value only if that line wrapped (:func:`_wrapped`), else it starts a
    paragraph. A paragraph runs to the block's end, so a colon later in a
    paragraph makes no pair, and a block whose first line is no pair is
    one paragraph. On the corpus datasheets, a paragraph set close under
    "Category: Fastener" merges into its block, though that line ends
    405 pt short of the column.
    """
    right = max(_turned(_plain(item.bbox), item.turn)[2] for item in found.lines)
    groups: list[tuple[tuple[str, str] | None, list[TextLine]]] = []
    for item in found.lines:
        parsed = _key_value(item.text)
        if groups and (groups[-1][0] is None or (parsed is None and _wrapped(groups[-1][1][-1], item, right))):
            groups[-1][1].append(item)
        else:
            groups.append((parsed, [item]))
    return [_pair_or_paragraph(parsed, lines) for parsed, lines in groups]


def _wrapped(before: TextLine, after: TextLine, right: float) -> bool:
    """True if ``before`` wrapped onto ``after``: ``after``'s first word would not fit between its end and ``right``.

    Read along the lines (their own frame), ``right`` the block's end.
    """
    return right - _turned(_plain(before.bbox), before.turn)[2] < _first_word_length(after)


def _first_word_length(found: TextLine) -> float:
    """How far the line's first word runs along it: its chars before the first whitespace after it starts."""
    boxes: list[Box] = []
    for piece in found.cells[0].pieces:
        for at in range(piece.start, piece.end):
            if not piece.span.text[at].isspace():
                boxes.append(_turned(piece.span.char_boxes[at], found.turn))
            elif boxes:
                return _length(boxes)
    return _length(boxes)


def _length(boxes: list[Box]) -> float:
    """How far ``boxes`` run across: from the first one's start to the furthest end."""
    return max(item[2] for item in boxes) - min(item[0] for item in boxes)


def _pair_or_paragraph(parsed: tuple[str, str] | None, lines: list[TextLine]) -> StreamKeyValue | StreamParagraph:
    """A pair over ``lines`` (``parsed`` from the first: key, then value and the other lines), or a paragraph."""
    bbox = _bbox(union_box(tuple(_plain(item.bbox) for item in lines)))
    if parsed is None:
        return StreamParagraph(text=" ".join(item.text for item in lines), bbox=bbox, pieces=_lines_pieces(lines))
    value = " ".join(part for part in (parsed[1], *(item.text.strip() for item in lines[1:])) if part)
    return StreamKeyValue(key=parsed[0], value=value, bbox=bbox, pieces=_lines_pieces(lines))


# Stream tables and the page's reading order (spec §3.3 step 3, Pass B).

# Rows fewer than this whose widest gap between columns passes this share of the page along them are a masthead
# (spec): pairs and paragraphs, not a table. On the corpus datasheets, 2-row mastheads gap 0.41 to 0.44 of the page.
_MASTHEAD_ROWS = 3
_MASTHEAD_GAP = 0.40
# Rows are prose, not a table, when at least this share of their cells hold at least this many words: on the
# corpus, every run of two-column prose lines has a share of 0.5 to 1.0; the fixture tables have none.
_PROSE_WORDS = 4
_PROSE_SHARE = Fraction(1, 2)


@dataclass(frozen=True, slots=True, kw_only=True)
class StreamTable:
    """A borderless table of the stream text: lines whose cells keep to consistent X-bands (spec §3.3 Pass B).

    Attributes:
        rows: Its lines, at least 2, top to bottom in their own frame.
        columns: Per row, the column of each of its cells, rising; every
            column holds a cell.
        column_centres: Each column's centre on the displayed page, along
            the rows: the middle of the band its cells span (display x for
            upright text, display y for text turned a quarter turn, so
            falling for text read up the page). At least 2.
        bbox: The union of its rows' boxes on the displayed page.
    """

    rows: tuple[TextLine, ...]
    columns: tuple[tuple[int, ...], ...]
    column_centres: tuple[float, ...]
    bbox: BBox

    def __post_init__(self) -> None:
        typed_tuple("rows", self.rows, TextLine)
        if len(self.rows) < _MIN_GRID:
            raise ValueError(f"rows must hold at least {_MIN_GRID} rows, got {len(self.rows)}")
        if not isinstance(self.column_centres, tuple):
            raise TypeError(f"column_centres must be a tuple, got {type(self.column_centres).__name__}")
        if len(self.column_centres) < _MIN_GRID:
            raise ValueError(f"column_centres must hold at least {_MIN_GRID} columns, got {len(self.column_centres)}")
        for index, centre in enumerate(self.column_centres):
            finite_float(f"column_centres[{index}]", centre)
        self._check_columns()
        _check_bbox("bbox", self.bbox)
        for index, item in enumerate(self.rows):
            if _outside(_plain(item.bbox), self.bbox):
                raise ValueError(f"rows[{index}].bbox must lie within the bbox, got {item.bbox} in {self.bbox}")

    def _check_columns(self) -> None:
        """One entry per row, placing each of its cells in a column, rising, with no column left empty."""
        if not isinstance(self.columns, tuple):
            raise TypeError(f"columns must be a tuple, got {type(self.columns).__name__}")
        if len(self.columns) != len(self.rows):
            raise ValueError(f"columns must hold one entry per row ({len(self.rows)}), got {len(self.columns)}")
        last = self.n_cols - 1
        held: set[int] = set()
        for index, (row, placed) in enumerate(zip(self.rows, self.columns)):
            if not isinstance(placed, tuple) or len(placed) != len(row.cells):
                raise ValueError(
                    f"columns[{index}] must place each of the row's {len(row.cells)} cells, got {placed!r}"
                )
            for at, column in enumerate(placed):
                int_at_least(f"columns[{index}][{at}]", column, 0)
            if any(after <= before for before, after in zip(placed, placed[1:])) or placed[-1] > last:
                raise ValueError(f"columns[{index}] must rise strictly within 0..{last}, got {placed!r}")
            held.update(placed)
        if len(held) <= last:
            missing = min(set(range(last + 1)) - held)
            raise ValueError(f"every column must hold a cell: column {missing} holds none")

    @property
    def n_cols(self) -> int:
        """Its column count."""
        return len(self.column_centres)

    def grid(self) -> tuple[tuple[str, ...], ...]:
        """The cell texts by row and column, ``""`` where a row has no cell."""
        grid = []
        for row, placed in zip(self.rows, self.columns):
            texts = [""] * self.n_cols
            for found, column in zip(row.cells, placed):
                texts[column] = found.text
            grid.append(tuple(texts))
        return tuple(grid)

    @property
    def has_header(self) -> bool:
        """A4a's rule (:func:`_has_header`): every cell of the first row non-blank."""
        return _has_header(self.grid())

    @property
    def pieces(self) -> tuple[SpanPiece, ...]:
        """The char ranges its rows read, row by row."""
        return _lines_pieces(self.rows)


StreamElement = StreamItem | StreamTable


def stream_layout(
    pieces: tuple[SpanPiece, ...], lattice: LatticeResult, *, width_pt: float, height_pt: float
) -> tuple[StreamElement, ...]:
    """The page's visible text outside its lattice tables, in reading order, as tables and classified text.

    Spec §3.3 step 3, Pass B:

    1. Lines: the page's stream lines (:func:`text_lines`).
    2. Rows (:func:`_row_structures`): each run of 2 or more neighbouring
       lines of 2 or more cells whose cells keep to consistent X-bands
       (:func:`_aligned_runs`) is, by the first rule it meets, a list of
       key-value pairs (every line a key cell ending in a colon and a
       value cell), a masthead (fewer than :data:`_MASTHEAD_ROWS` lines and
       its widest gap between columns over :data:`_MASTHEAD_GAP` of the
       page along them: a pair or paragraph per cell), prose (left to step
       3), or a :class:`StreamTable`, unless its columns are page columns
       (:func:`_without_page_columns`; left to step 3).
    3. Reading order: v4's XY-cut (:func:`text_blocks`) of the other
       text, each table, list or masthead in it as one item, so no gutter
       cuts it into columns; each segment's lines merge into paragraphs
       between the items, top to bottom.
    4. The paragraphs are classified (:func:`classify_blocks`), the body
       size and heading levels read over all of them, so text in rows
       never moves the body size; a list never runs across a table.

    Ids and positions among the lattice tables and figures come in A4b
    group 3. Every visible char outside the lattice tables is read once,
    in the items' ``pieces``.

    Cost: O(c log c) for c chars, plus two XY-cuts' O(D r log r) for r
    runs (one to find page columns, one to read the page).

    Known limitations:

    - Lines run across the page, so a borderless table beside other text
      on the same lines (in one column of two) is not found: its text
      reads as paragraphs.
    - A cell wrapped onto a line of its own ends its table (spec: runs of
      lines of 2 or more cells), as does a cell reaching across two
      columns.
    - A table whose cells are half or more running text (4 words or
      more, as a two-column glossary's) reads as prose.

    Args:
        pieces: The page's pieces (``PageAssessment.pieces``).
        lattice: Their lattice tables (``find_lattice_tables(segments,
            pieces)``).
        width_pt: The displayed page's width in points.
        height_pt: Its height in points.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: As :func:`text_lines`, or if a page side is not
            finite and greater than zero.
    """
    width, height = positive_float("width_pt", width_pt), positive_float("height_pt", height_pt)
    runs = _stream_runs(pieces, lattice)
    return tuple(_visible_items(runs, _page_turn(runs), width, height))


def _visible_items(runs: list[_TurnedRun], turn: int, width: float, height: float) -> list[StreamElement]:
    """The runs' tables and classified text in reading order (:func:`stream_layout`, steps 2 to 4)."""
    structures = _row_structures(_ordered_lines(runs, turn), width=width, height=height)
    structures = _without_page_columns(structures, runs, turn)
    slots = _reading_slots(runs, turn, structures)
    classified = iter(_classified_runs([slot for slot in slots if isinstance(slot, list)]))
    return [item for slot in slots for item in (next(classified) if isinstance(slot, list) else slot.items)]


class _Rows(NamedTuple):
    """Lines read whole: a stream table, or a masthead or key-value list as pairs and paragraphs."""

    rows: tuple[TextLine, ...]
    items: tuple[StreamElement, ...]


def _without_page_columns(structures: list[_Rows], runs: list[_TurnedRun], turn: int) -> list[_Rows]:
    """The structures but the tables whose columns are page columns: text on one baseline grid, not a table.

    v4's XY-cut of all the page's runs (as :func:`text_blocks` reads them)
    puts the table's cells in 2 or more segments, and every column of it
    shares a segment with text in no structure: each column runs on past
    the table, as two columns of an index do whose lines share baselines
    (Root's Law p. 31, a 19-row false table). A table with a column that
    shares segments only with structures (a title over its first column
    leaves the others alone) stays. O(D r log r) for r runs.
    """
    boxes = [_turned(_ink_box(run), turn) for _, run in runs]
    segment_of: dict[tuple[int, int, int], int] = {}
    for number, segment in enumerate(_xy_cut(boxes, [run.end - run.start for _, run in runs])):
        segment_of.update((_identity(runs[index][1]), number) for index in segment)
    taken = {_identity(piece) for found in structures for piece in _lines_pieces(found.rows)}
    free = {number for key, number in segment_of.items() if key not in taken}
    return [
        found
        for found in structures
        if not (isinstance(found.items[0], StreamTable) and _page_columns(found.items[0], segment_of, free))
    ]


def _page_columns(table: StreamTable, segment_of: dict[tuple[int, int, int], int], free: set[int]) -> bool:
    """True if the table's cells lie in 2 or more segments and each of its columns shares one with free text."""
    by_column: list[set[int]] = [set() for _ in range(table.n_cols)]
    for row, placed in zip(table.rows, table.columns):
        for found, column in zip(row.cells, placed):
            by_column[column].update(segment_of[_identity(piece)] for piece in found.pieces)
    return len(set().union(*by_column)) >= _MIN_GRID and all(segments & free for segments in by_column)


def _row_structures(lines: list[TextLine], *, width: float, height: float) -> list[_Rows]:
    """The tables, mastheads and key-value lists among the lines (:func:`stream_layout`, step 2), in order.

    The page's extent along a run's lines (``width`` for upright text,
    ``height`` for text turned a quarter turn) weighs a masthead's gap.
    """
    found = []
    for rows, starts, ends in _aligned_runs(lines):
        extent = width if rows[0].turn == _ACROSS else height
        structure = _row_structure(rows, starts, ends, extent)
        if structure is not None:
            found.append(structure)
    return found


def _aligned_runs(lines: list[TextLine]) -> Iterator[tuple[list[TextLine], list[float], list[float]]]:
    """Yield each run of 2 or more neighbouring lines of one turn whose cells fit X-bands, with its bands.

    Bands come as their starts and ends along the lines, sorted. A line
    that does not fit (:func:`_fit`), has one cell or another turn ends
    the run; one of 2 or more cells starts the next. O(c log c) for c
    cells: each cell is placed by bisection, and a band is inserted once.
    """
    rows: list[TextLine] = []
    starts: list[float] = []
    ends: list[float] = []
    for item in lines:
        spans = [_along(found.bbox, item.turn) for found in item.cells]
        several = len(spans) >= _MIN_GRID
        if several and rows and item.turn == rows[0].turn and _fit(starts, ends, spans):
            rows.append(item)
            continue
        if len(rows) >= _MIN_GRID:
            yield rows, starts, ends
        rows, starts, ends = [], [], []
        if several and _fit(starts, ends, spans):
            rows.append(item)
    if len(rows) >= _MIN_GRID:
        yield rows, starts, ends


def _along(bbox: BBox, turn: int) -> tuple[float, float]:
    """Where a box starts and ends along a line of ``turn``, in its frame."""
    x0, _, x1, _ = _turned(_plain(bbox), turn)
    return x0, x1


def _fit(starts: list[float], ends: list[float], spans: list[tuple[float, float]]) -> bool:
    """Add a line's cells (``spans`` along it, in order) to the bands, or leave the bands and return False.

    Interval projection with no slack: a cell joins the one band it
    overlaps or touches, widening it, or makes a band of its own. The line
    does not fit when its cells overlap, a cell meets two bands, or two
    cells meet one: its cells would not be one per column.
    """
    plan: list[tuple[int, bool, float, float]] = []
    reach, joined = -math.inf, -1
    for start, end in spans:
        low, high = bisect_left(ends, start), bisect_right(starts, end)
        if start <= reach or high - low > 1 or (high - low == 1 and low == joined):
            return False
        plan.append((low, high > low, start, end))
        reach, joined = end, (low if high > low else -1)
    for at, joins, start, end in reversed(plan):
        if joins:
            starts[at], ends[at] = min(starts[at], start), max(ends[at], end)
        else:
            starts.insert(at, start)
            ends.insert(at, end)
    return True


def _row_structure(rows: list[TextLine], starts: list[float], ends: list[float], extent: float) -> _Rows | None:
    """A run's lines as a key-value list, a masthead, or a table; None for prose (:func:`stream_layout`, step 2)."""
    if all(len(item.cells) == _MIN_GRID and _is_key(item.cells[0].text) for item in rows):
        return _Rows(tuple(rows), tuple(_pair_or_paragraph(_key_value(item.text), [item]) for item in rows))
    gap = max(start - end for end, start in zip(ends, starts[1:]))
    if len(rows) < _MASTHEAD_ROWS and gap > _MASTHEAD_GAP * extent:
        return _Rows(tuple(rows), tuple(_cell_item(found) for item in rows for found in item.cells))
    cells = [found for item in rows for found in item.cells]
    if sum(len(found.text.split()) >= _PROSE_WORDS for found in cells) >= _PROSE_SHARE * len(cells):
        return None
    return _Rows(tuple(rows), (_stream_table(rows, starts, ends),))


def _is_key(text: str) -> bool:
    """True if ``text`` is a key and its colon, nothing after: ``Weight:``."""
    parsed = _key_value(text)
    return parsed is not None and not parsed[1]


def _cell_item(found: TextCell) -> StreamKeyValue | StreamParagraph:
    """A masthead cell as a pair, if it reads ``Key: Value``, else a paragraph."""
    parsed = _key_value(found.text)
    if parsed is None:
        return StreamParagraph(text=found.text, bbox=found.bbox, pieces=found.pieces)
    return StreamKeyValue(key=parsed[0], value=parsed[1], bbox=found.bbox, pieces=found.pieces)


def _stream_table(rows: list[TextLine], starts: list[float], ends: list[float]) -> StreamTable:
    """A run's lines as a table: each cell in the band holding its start, each column centred on its band."""
    turn = rows[0].turn
    columns = tuple(
        tuple(bisect_right(starts, _along(found.bbox, turn)[0]) - 1 for found in item.cells) for item in rows
    )
    # Back to the displayed page: along a line read up it (turn 270), the frame's x is the page's y negated.
    sign = -1.0 if turn == _UP else 1.0
    centres = tuple(sign * (start + end) / 2 for start, end in zip(starts, ends))
    bbox = _bbox(union_box(tuple(_plain(item.bbox) for item in rows)))
    return StreamTable(rows=tuple(rows), columns=columns, column_centres=centres, bbox=bbox)


# A slot of the page's reading order: neighbouring paragraphs, or lines read whole.
_Slot = list[TextBlock] | _Rows


def _reading_slots(runs: list[_TurnedRun], turn: int, structures: list[_Rows]) -> list[_Slot]:
    """The page's paragraphs and row structures in reading order (:func:`stream_layout`, step 3).

    The XY-cut reads each structure as one item: the union of its runs'
    ink, weighing their chars. The other runs are items as in
    :func:`text_blocks`.
    """
    owner = {_identity(piece): index for index, found in enumerate(structures) for piece in _lines_pieces(found.rows)}
    free: list[_TurnedRun] = []
    members: list[list[_Run]] = [[] for _ in structures]
    for item in runs:
        at = owner.get(_identity(item[1]))
        if at is None:
            free.append(item)
        else:
            members[at].append(item[1])
    boxes = [_turned(_ink_box(run), turn) for _, run in free]
    boxes += [_turned(union_box(tuple(_ink_box(run) for run in found)), turn) for found in members]
    weights = [run.end - run.start for _, run in free]
    weights += [sum(run.end - run.start for run in found) for found in members]
    slots: list[_Slot] = []
    for segment in _xy_cut(boxes, weights):
        slots.extend(_segment_slots(segment, free, structures, boxes, turn))
    return slots


def _identity(item: _Run | SpanPiece) -> tuple[int, int, int]:
    """A run's identity on the page: its span's, and its char range there."""
    span = item.piece.span if isinstance(item, _Run) else item.span
    return id(span), item.start, item.end


def _segment_slots(
    segment: list[int], free: list[_TurnedRun], structures: list[_Rows], boxes: list[Box], turn: int
) -> list[_Slot]:
    """One XY-cut segment's slots: its runs' lines in paragraphs between its structures, top to bottom."""
    count = len(free)
    placed = sorted((boxes[index][1], index - count) for index in segment if index >= count)
    slots: list[_Slot] = []
    group: list[TextLine] = []
    at = 0
    for item in _ordered_lines([free[index] for index in segment if index < count], turn):
        top = _turned(_plain(item.bbox), turn)[1]
        while at < len(placed) and placed[at][0] <= top:
            if group:
                slots.append(_paragraphs(group))
                group = []
            slots.append(structures[placed[at][1]])
            at += 1
        group.append(item)
    if group:
        slots.append(_paragraphs(group))
    slots.extend(structures[index] for _, index in placed[at:])
    return slots


# Hidden and recovered text, and the page's reading order (A4b group 3).

# Each apart line is placed by comparing it with every visible item: past this many pairs, a page's apart lines read
# after its visible text instead, with a warning. The corpus (data/pdfs, 95 pages) needs at most 25 000.
MAX_APART_PAIRS = 1_000_000


@dataclass(frozen=True, slots=True, kw_only=True)
class ApartLine:
    """A line of hidden or recovered text, laid out apart from the page's visible text and never mixed with it.

    Its pieces all carry its ``hidden_reasons``: hidden pieces of the page's
    text that share them, or recovered pieces (a copy pdfium dropped though
    it is drawn has none). It holds no font size: apart text is never
    classified, and text drawn at ``Tf 0`` has no size to read.

    Attributes:
        cells: Its cells along the line, as :class:`TextLine`'s.
        bbox: The union of its cells' boxes on the displayed page.
        turn: The clockwise angle its text advances at: 0, 90 or 270.
        hidden_reasons: Its pieces' reasons, in A1's canonical order.
    """

    cells: tuple[TextCell, ...]
    bbox: BBox
    turn: int
    hidden_reasons: tuple[HiddenReason, ...]

    def __post_init__(self) -> None:
        _check_line_shape(self.cells, self.bbox, self.turn)
        check_hidden_reasons(self.hidden_reasons)
        for index, item in enumerate(self.cells):
            for at, piece in enumerate(item.pieces):
                if piece.hidden_reasons != self.hidden_reasons:
                    found = f"cells[{index}].pieces[{at}] has hidden_reasons {piece.hidden_reasons}"
                    raise ValueError(f"{found}, not the line's {self.hidden_reasons}")

    @property
    def text(self) -> str:
        """Its cells' texts joined with one space."""
        return " ".join(item.text for item in self.cells)


PageItem = StreamElement | ApartLine
_PAGE_ITEMS = (StreamHeading, StreamParagraph, StreamKeyValue, StreamList, StreamTable, ApartLine)


@dataclass(frozen=True, slots=True, kw_only=True)
class PageLayout:
    """A page's items in reading order (:func:`page_layout`), and the warnings laying them out raised.

    Attributes:
        items: Its visible items, as :func:`stream_layout` reads them, with
            its apart lines placed among them.
        warnings: Non-fatal notes for the page (``Page.warnings``).
    """

    items: tuple[PageItem, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            raise TypeError(f"items must be a tuple, got {type(self.items).__name__}")
        for index, item in enumerate(self.items):
            if not isinstance(item, _PAGE_ITEMS):
                raise TypeError(f"items[{index}] must be a page item, got {type(item).__name__}")
        str_tuple("warnings", self.warnings)


def page_layout(
    pieces: tuple[SpanPiece, ...],
    recovered: tuple[SpanPiece, ...],
    lattice: LatticeResult,
    *,
    width_pt: float,
    height_pt: float,
) -> PageLayout:
    """The page's text in reading order: its visible items, with its hidden and recovered text placed among them.

    1. Visible text: as :func:`stream_layout` reads it. Hidden and recovered
       text takes no part in it, so it cannot change the visible order.
    2. Apart lines (:func:`_apart_lines`): the page's hidden pieces and
       its recovered pieces, grouped by their hidden reasons, each group
       read as lines (:func:`text_lines`, steps 1 to 4), so a line holds
       pieces of one set of reasons and no visible text.
    3. Placement (:func:`_with_apart`): each apart line reads next to the
       visible item nearest it (the gap between their boxes, then between
       their centres, then reading order), before it when the line starts
       higher (in the frame of the page's turn), else after it; lines by
       one item, or on a page with none, read top to bottom, then left to
       right. Past :data:`MAX_APART_PAIRS` comparisons, they read after
       the visible items instead, with a warning.

    Cost: :func:`stream_layout`'s, the apart lines' O(c log c) for c chars,
    and O(a v) for a apart lines and v visible items (bounded above).

    Args:
        pieces: The page's pieces (``PageAssessment.pieces``).
        recovered: Its recovered pieces (``PageAssessment.recovered_pieces``).
        lattice: The pieces' lattice tables (``find_lattice_tables``).
        width_pt: The displayed page's width in points.
        height_pt: Its height in points.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: As :func:`stream_layout`, or if a recovered piece is
            passed twice or also among ``pieces``.
    """
    width, height = positive_float("width_pt", width_pt), positive_float("height_pt", height_pt)
    runs = _stream_runs(pieces, lattice)
    typed_tuple("recovered", recovered, SpanPiece)
    _check_distinct(recovered, "recovered")
    taken = {id(item) for item in pieces}
    for index, item in enumerate(recovered):
        if id(item) in taken:
            raise ValueError(f"recovered[{index}] is also among pieces: recovered text is never page text")
    turn = _page_turn(runs)
    visible: list[PageItem] = list(_visible_items(runs, turn, width, height))
    items, warnings = _with_apart(visible, _apart_lines(pieces, recovered, turn), turn)
    return PageLayout(items=tuple(items), warnings=warnings)


def _apart_lines(pieces: tuple[SpanPiece, ...], recovered: tuple[SpanPiece, ...], turn: int) -> list[ApartLine]:
    """The hidden pieces' and the recovered pieces' lines, each of one set of hidden reasons (:func:`page_layout`)."""
    groups: dict[tuple[HiddenReason, ...], list[_TurnedRun]] = {}
    for piece in (*(item for item in pieces if item.hidden_reasons), *recovered):
        for found in _cut_at_gaps(_Run(piece, piece.start, piece.end)):
            if not _run_chars(found[1]).isspace():
                groups.setdefault(piece.hidden_reasons, []).append(found)
    lines = []
    for reasons, runs in groups.items():
        for line, own in _grouped_lines(runs, turn):
            cells, bbox, _ = _line_cells(line)
            lines.append(ApartLine(cells=cells, bbox=bbox, turn=own, hidden_reasons=reasons))
    return lines


def _with_apart(visible: list[PageItem], apart: list[ApartLine], turn: int) -> tuple[list[PageItem], tuple[str, ...]]:
    """The visible items with the apart lines placed among them (:func:`page_layout`, step 3), and any warning."""
    if not apart:
        return visible, ()
    ordered = [item for _, _, item in sorted((_frame(item, turn)[1::-1], at, item) for at, item in enumerate(apart))]
    pairs = len(apart) * len(visible)
    if pairs > MAX_APART_PAIRS:
        where = f"{len(apart)} hidden or recovered line(s) read after the page's visible text"
        why = f"placing them by position would compare {pairs} pairs, over the bound of {MAX_APART_PAIRS}"
        return [*visible, *ordered], (f"{where}: {why}",)
    if not visible:
        return list(ordered), ()
    frames = [_frame(item, turn) for item in visible]
    before: list[list[PageItem]] = [[] for _ in visible]
    after: list[list[PageItem]] = [[] for _ in visible]
    for line in ordered:
        box = _frame(line, turn)
        anchor = min(range(len(frames)), key=lambda at: (_gap(box, frames[at]), _centre_gap(box, frames[at]), at))
        (before if box[1] < frames[anchor][1] else after)[anchor].append(line)
    placed: list[PageItem] = []
    for at, item in enumerate(visible):
        placed.extend((*before[at], item, *after[at]))
    return placed, ()


def _frame(item: PageItem, turn: int) -> Box:
    """An item's box in the frame of the page's ``turn``."""
    return _turned(_plain(item.bbox), turn)


def _gap(first: Box, second: Box) -> float:
    """The distance between two boxes: 0 when they meet."""
    across = max(second[0] - first[2], first[0] - second[2], 0.0)
    down = max(second[1] - first[3], first[1] - second[3], 0.0)
    return math.hypot(across, down)


def _centre_gap(first: Box, second: Box) -> float:
    """The distance between two boxes' centres."""
    return math.hypot(
        (first[0] + first[2] - second[0] - second[2]) / 2, (first[1] + first[3] - second[1] - second[3]) / 2
    )
