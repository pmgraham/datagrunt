"""Page layout for the v5 PDF document model (#367, A4a: the vector-lattice table pass).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads the engine's segments (:class:`~datagrunt.pdf._path_segments.RawSegment`)
and A3's span pieces (:class:`~datagrunt.pdf.quality.SpanPiece`) and calls
no pdfium function. Boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.
"""

from dataclasses import dataclass

from datagrunt.pdf._validate import int_at_least, typed_tuple
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.quality import SpanPiece

__all__ = ["LatticeCell", "LatticeResult", "LatticeTable"]


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
