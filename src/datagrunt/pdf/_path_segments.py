"""Axis-aligned segments from PDF path geometry, for the v5 pdfium engine (#365 T3).

Pure geometry: no pdfium. :mod:`datagrunt.pdf.pdfium_engine` reads each path
object's points, paint and composed page-space matrix from pdfium and hands
them here; this module keeps the straight, axis-aligned strokes and thin
filled rectangles that A4's lattice pass (#367) builds ruled tables from. It
imports nothing from the engine, so the engine imports (and re-exports)
:class:`RawSegment` from here without a cycle.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.
"""

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, NamedTuple

from datagrunt.pdf._validate import finite_float, int_at_least, rgba_tuple, strict_bool

Box = tuple[float, float, float, float]
Point = tuple[float, float]
Orientation = Literal["h", "v"]
SegmentKind = Literal["stroke", "thin_fill", "fill_edge"]

# A segment is at least this long (A4's lattice pass, #367).
MIN_LENGTH_PT = 12.0
# An edge is axis-aligned when its ends differ by less than this across the axis.
AXIS_TOLERANCE_PT = 0.5
# A filled rectangle at most this thick is a line (a rule drawn as a fill).
MAX_THIN_FILL_PT = 3.0

_ORIENTATIONS = ("h", "v")
_KINDS = ("stroke", "thin_fill", "fill_edge")


def _checked_choice(name: str, value: object, choices: tuple[str, ...]) -> str:
    """Return ``value`` if it is one of the str ``choices``."""
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, got {value!r} ({type(value).__name__})")
    if value not in choices:
        allowed = " or ".join(repr(choice) for choice in choices)
        raise ValueError(f"{name} must be {allowed}, got {value!r}")
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class RawSegment:
    """One axis-aligned line on the displayed page, from a stroke or a thin fill.

    Coordinates are displayed, top-left points (rotation and CropBox
    applied, as :func:`datagrunt.pdf.pdfium_engine.to_display_box` maps
    boxes), so a horizontal PDF line is vertical on a 90 or 270 page.

    Attributes:
        orientation: ``"h"`` (along x) or ``"v"`` (along y).
        position: The line's y for ``"h"``, its x for ``"v"``: the mean of its
            two ends' cross coordinates, or the centre of a thin fill.
        start: Where the line begins along its axis (``start < end``).
        end: Where the line ends along its axis.
        width: The stroke's thickness in page points (the ``w`` line width
            scaled across the line by every enclosing transform), or a thin
            fill's short side; 0 for a hairline and for every fill edge.
        kind: ``"stroke"`` for a stroked straight path edge, ``"thin_fill"``
            for a filled rectangle at most 3 pt thick (one segment along its
            length), ``"fill_edge"`` for a side of a thicker filled rectangle
            (a shaded cell's border, which v4's pdfplumber read as a rule;
            whether it rules a table is #367's call).
        object_index: The path object's position in the page's depth-first,
            paint-order object walk (forms count, then their contents): the
            same index a :class:`~datagrunt.pdf.pdfium_engine.RawSpan` uses.
        dashed: Whether a stroke's dash pattern leaves gaps (a dash array
            with a gap > 0, not all zero): dashed rules and dotted leaders.
            Always False for the fill kinds.
        paint_rgba: The colour and alpha, 0-255, the segment is painted in:
            the stroke colour for ``"stroke"``, the fill colour otherwise (a
            pattern reads as a colour pdfium picks). White or transparent
            rules are kept; #367/#368 judge them.
    """

    orientation: Orientation
    position: float
    start: float
    end: float
    width: float
    kind: SegmentKind
    object_index: int
    dashed: bool
    paint_rgba: tuple[int, int, int, int]

    def __post_init__(self) -> None:
        _checked_choice("orientation", self.orientation, _ORIENTATIONS)
        object.__setattr__(self, "position", finite_float("position", self.position))
        start, end = finite_float("start", self.start), finite_float("end", self.end)
        if not start < end:
            raise ValueError(f"start must be < end, got start {start!r}, end {end!r}")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "width", finite_float("width", self.width, lo=0.0))
        _checked_choice("kind", self.kind, _KINDS)
        int_at_least("object_index", self.object_index, 0)
        if strict_bool("dashed", self.dashed) and self.kind != "stroke":
            raise ValueError(f"dashed must be False for a {self.kind}: only a stroke has a dash pattern")
        rgba_tuple("paint_rgba", self.paint_rgba)


@dataclass(frozen=True, slots=True, kw_only=True)
class SegmentStats:
    """Counts of one page's path objects that gave no segments for a reason A3 records.

    Attributes:
        skipped_nonfinite_paths: Path objects skipped because their
            page-space geometry is not finite: their matrix to page space
            (pdfium keeps the CTM in 32-bit floats, so five ``1e9`` scales
            overflow it), a point, or the stroke width. A path inside a form
            whose matrix overflowed counts here.
    """

    skipped_nonfinite_paths: int

    def __post_init__(self) -> None:
        int_at_least("skipped_nonfinite_paths", self.skipped_nonfinite_paths, 0)


@dataclass(frozen=True, slots=True, kw_only=True)
class PageSegments:
    """One page's segments in object-walk order, with its skip counts."""

    segments: tuple[RawSegment, ...]
    stats: SegmentStats

    def __post_init__(self) -> None:
        if not isinstance(self.segments, tuple):
            raise TypeError(f"segments must be a tuple, got {type(self.segments).__name__}")
        for index, item in enumerate(self.segments):
            if not isinstance(item, RawSegment):
                raise TypeError(f"segments[{index}] must be a RawSegment, got {type(item).__name__}")
        if not isinstance(self.stats, SegmentStats):
            raise TypeError(f"stats must be a SegmentStats, got {type(self.stats).__name__}")


class Matrix(NamedTuple):
    """A PDF transform ``[a b c d e f]``: it maps (x, y) to (a x + c y + e, b x + d y + f)."""

    a: float
    b: float
    c: float
    d: float
    e: float
    f: float

    def then(self, outer: "Matrix") -> "Matrix":
        """This transform followed by ``outer`` (PDF's ``self x outer``)."""
        a, b, c, d, e, f = self
        return Matrix(
            a * outer.a + b * outer.c,
            a * outer.b + b * outer.d,
            c * outer.a + d * outer.c,
            c * outer.b + d * outer.d,
            e * outer.a + f * outer.c + outer.e,
            e * outer.b + f * outer.d + outer.f,
        )

    def apply(self, x: float, y: float) -> Point:
        """The point (x, y) maps to."""
        return self.a * x + self.c * y + self.e, self.b * x + self.d * y + self.f


IDENTITY = Matrix(1.0, 0.0, 0.0, 1.0, 0.0, 0.0)


class NonFiniteGeometry(ValueError):
    """A path's matrix to page space, a page-space point or its stroke width is not finite."""


class PathPoint(NamedTuple):
    """One point of a path object, in pdfium's order.

    ``kind`` is ``"move"`` (a subpath starts here), ``"line"`` (a straight
    edge ends here) or ``"curve"`` (one of a cubic Bézier's three points);
    ``closes`` marks the point after which the subpath closes back to its start.
    """

    kind: Literal["move", "line", "curve"]
    x: float
    y: float
    closes: bool = False


class PathPaint(NamedTuple):
    """How a path object is painted: stroked or filled, its ``w`` line width, dashes and colours."""

    stroked: bool
    filled: bool
    stroke_width: float
    dashed: bool
    stroke_rgba: tuple[int, int, int, int]
    fill_rgba: tuple[int, int, int, int]


def path_segments(
    points: Sequence[PathPoint],
    *,
    matrix: Matrix,
    paint: PathPaint,
    object_index: int,
    to_display: Callable[[Box], Box],
) -> list[RawSegment]:
    """The axis-aligned segments of one path object.

    Args:
        points: The path's points in its own space, as pdfium reads them.
        matrix: Maps the path's space to PDF user space (bottom-left,
            unrotated): its object matrix followed by every enclosing form's.
        paint: How the path is painted: a filled path has thin-fill and
            fill-edge segments, a stroked one stroke segments.
        object_index: The path object's index in the page's object walk.
        to_display: Maps a user-space ``(left, bottom, right, top)`` box to a
            displayed, top-left ``(x0, y0, x1, y1)`` box.

    Raises:
        NonFiniteGeometry: If ``matrix`` or the stroke width is not finite,
            or a point or an edge's stroke thickness overflows in page space.

    Returns:
        The filled rectangles' segments (:func:`_fill_segments`), then the stroked
        straight edges at least :data:`MIN_LENGTH_PT` long and axis-aligned
        within :data:`AXIS_TOLERANCE_PT` in user space, each in path order:
        PDF paints a path's fill before its stroke.
    """
    if not all(math.isfinite(value) for value in matrix):
        raise NonFiniteGeometry(f"its matrix to page space is not finite {tuple(matrix)}")
    if not math.isfinite(paint.stroke_width):
        raise NonFiniteGeometry(f"its stroke width is not finite ({paint.stroke_width})")
    closed = _explicit_closes(points)
    placed = [_placed(point, matrix) for point in closed]
    segments = []
    if paint.filled:
        segments.extend(_fill_segments(placed, object_index, paint, to_display))
    if not paint.stroked:
        return segments
    for local, page in zip(_straight_edges(closed), _straight_edges(placed)):
        line = _axis_line(*page)
        if line is not None:
            width = _stroke_thickness(paint.stroke_width, matrix, local, page)
            segments.append(_segment(line, width, "stroke", object_index, paint, to_display))
    return segments


def _placed(point: PathPoint, matrix: Matrix) -> PathPoint:
    """``point`` mapped by ``matrix``; NonFiniteGeometry if that overflows (or the matrix is not finite)."""
    x, y = matrix.apply(point.x, point.y)
    if not (math.isfinite(x) and math.isfinite(y)):
        raise NonFiniteGeometry(f"its page-space point {(x, y)} is not finite")
    return point._replace(x=x, y=y)


def _explicit_closes(points: Sequence[PathPoint]) -> list[PathPoint]:
    """``points`` with every closing edge drawn as a line back to its subpath's start.

    pdfium's parser already writes that line (the closing point is the start);
    a close flagged away from the start (pdfium's edit API) gets it here.
    """
    closed = []
    first: PathPoint | None = None
    for point in points:
        if point.kind == "move" or first is None:
            first = point
        if point.closes and (point.x, point.y) != (first.x, first.y):
            closed.append(point._replace(closes=False))
            closed.append(PathPoint("line", first.x, first.y, closes=True))
        else:
            closed.append(point)
    return closed


def _straight_edges(points: Sequence[PathPoint]) -> list[tuple[Point, Point]]:
    """Every straight edge, in path order, of points whose closes are explicit.

    A Bézier's points only move the current point: its curve is not an edge.
    """
    edges = []
    current: Point | None = None
    for point in points:
        here = (point.x, point.y)
        if point.kind == "line" and current is not None:
            edges.append((current, here))
        current = here
    return edges


def _axis_line(start: Point, end: Point) -> Box | None:
    """A user-space edge as a zero-thickness line box, or None if it is short or not axis-aligned."""
    (x0, y0), (x1, y1) = start, end
    if abs(y1 - y0) < AXIS_TOLERANCE_PT and abs(x1 - x0) >= MIN_LENGTH_PT:
        y = (y0 + y1) / 2
        return min(x0, x1), y, max(x0, x1), y
    if abs(x1 - x0) < AXIS_TOLERANCE_PT and abs(y1 - y0) >= MIN_LENGTH_PT:
        x = (x0 + x1) / 2
        return x, min(y0, y1), x, max(y0, y1)
    return None


def _stroke_thickness(width: float, matrix: Matrix, local: tuple[Point, Point], page: tuple[Point, Point]) -> float:
    """The thickness across a placed edge of a stroke ``width`` wide in the path's space.

    The matrix maps a unit square to a parallelogram of area |det|; along
    the edge it stretches lengths by page length / local length, so across
    it by |det| over that. A negative ``w`` strokes as a hairline (as
    pdfium draws it), so it is 0.
    """
    determinant = matrix.a * matrix.d - matrix.b * matrix.c
    thickness = max(width, 0.0) * abs(determinant) * math.dist(*local) / math.dist(*page)
    if not math.isfinite(thickness):
        raise NonFiniteGeometry(f"its stroke is not finitely wide across an edge ({thickness})")
    return thickness


def _fill_segments(
    points: Sequence[PathPoint], object_index: int, paint: PathPaint, to_display: Callable[[Box], Box]
) -> list[RawSegment]:
    """The segments of each filled axis-aligned rectangle among a path's subpaths.

    A rectangle at most :data:`MAX_THIN_FILL_PT` thick and at least
    :data:`MIN_LENGTH_PT` long is one ``thin_fill`` along its length; a
    thicker one gives each side at least :data:`MIN_LENGTH_PT` long as a
    zero-width ``fill_edge``: bottom, right, top, left in user space.
    """
    segments = []
    for subpath in _subpaths(points):
        box = _filled_rectangle(subpath)
        if box is None:
            continue
        left, bottom, right, top = box
        width, height = right - left, top - bottom
        if min(width, height) > MAX_THIN_FILL_PT:
            sides = (
                (left, bottom, right, bottom),
                (right, bottom, right, top),
                (left, top, right, top),
                (left, bottom, left, top),
            )
            segments += [
                _segment(side, 0.0, "fill_edge", object_index, paint, to_display)
                for side in sides
                if max(side[2] - side[0], side[3] - side[1]) >= MIN_LENGTH_PT
            ]
        elif max(width, height) < MIN_LENGTH_PT:
            continue
        elif width >= height:
            y = (bottom + top) / 2
            segments.append(_segment((left, y, right, y), height, "thin_fill", object_index, paint, to_display))
        else:
            x = (left + right) / 2
            segments.append(_segment((x, bottom, x, top), width, "thin_fill", object_index, paint, to_display))
    return segments


def _subpaths(points: Sequence[PathPoint]) -> list[list[PathPoint]]:
    """``points`` split where each subpath starts (at every move)."""
    subpaths: list[list[PathPoint]] = []
    for point in points:
        if point.kind == "move" or not subpaths:
            subpaths.append([])
        subpaths[-1].append(point)
    return subpaths


def _filled_rectangle(subpath: Sequence[PathPoint]) -> Box | None:
    """The user-space box a subpath fills, if it is an axis-aligned rectangle.

    That is: straight edges only, each axis-aligned within
    :data:`AXIS_TOLERANCE_PT` (a fill closes the subpath), enclosing the
    area of their bounding box (the shoelace area, within a tolerance-wide
    band around its perimeter). A zero-area rectangle qualifies: pdfium
    paints it as a hairline.
    """
    if len(subpath) < 2 or any(point.kind == "curve" for point in subpath):
        return None
    corners = [(point.x, point.y) for point in subpath]
    ring = list(zip(corners, corners[1:] + corners[:1]))
    if not all(abs(x1 - x0) < AXIS_TOLERANCE_PT or abs(y1 - y0) < AXIS_TOLERANCE_PT for (x0, y0), (x1, y1) in ring):
        return None
    xs, ys = [x for x, _ in corners], [y for _, y in corners]
    left, bottom, right, top = min(xs), min(ys), max(xs), max(ys)
    area = abs(sum(x0 * y1 - x1 * y0 for (x0, y0), (x1, y1) in ring)) / 2
    width, height = right - left, top - bottom
    if abs(area - width * height) > AXIS_TOLERANCE_PT * (width + height):
        return None
    return left, bottom, right, top


def _segment(
    line: Box,
    width: float,
    kind: SegmentKind,
    object_index: int,
    paint: PathPaint,
    to_display: Callable[[Box], Box],
) -> RawSegment:
    """The RawSegment of a user-space line, given as a zero-thickness box at least 12 pt long.

    The displayed box says the orientation: a quarter-turned page shows a
    horizontal user-space line vertically. A stroke takes the stroke's dash
    flag and colour, a fill segment the fill colour.
    """
    stroke = kind == "stroke"
    x0, y0, x1, y1 = to_display(line)
    orientation: Orientation
    if x1 - x0 > y1 - y0:
        orientation, position, start, end = "h", y0, x0, x1
    else:
        orientation, position, start, end = "v", x0, y0, y1
    return RawSegment(
        orientation=orientation,
        position=position,
        start=start,
        end=end,
        width=width,
        kind=kind,
        object_index=object_index,
        dashed=paint.dashed and stroke,
        paint_rgba=paint.stroke_rgba if stroke else paint.fill_rgba,
    )
