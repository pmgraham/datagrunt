"""Pure-Python geometry primitives for hidden-text detection (#395).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure stdlib
plus :mod:`datagrunt.pdf._boxes` and :mod:`datagrunt.pdf._validate` — zero
pypdfium2, Pillow, polars, or pyarrow imports.

Supports three hidden-text mechanisms:
- ``clipped``: point/glyph containment against nested rectangular and polygon
  clipping regions (even-odd or nonzero winding, with hairline tolerance).
- ``occluded``: paint-order occlusion against later opaque fills/images using
  ``x0``-sorted y-bands split into narrow (``<= 64 pt``) and wide (``> 64 pt``)
  lists so a single full-page background never degrades bisect to O(N*M).
- ``blank_glyph``: detection of non-whitespace, non-control/separator chars
  whose font glyph has no outline (pdfium's 1/1000-em stand-in box).
"""

import bisect
import math
import unicodedata
from dataclasses import dataclass
from typing import TypeAlias

from datagrunt.pdf._boxes import Box
from datagrunt.pdf._validate import (
    finite_float,
    finite_tuple,
    int_at_least,
    ordered_box,
    positive_float,
    strict_bool,
    typed_tuple,
)

__all__ = [
    "Box",
    "ClipPathPoly",
    "ClipRegion",
    "MAX_OCCLUSION_CHECKS",
    "Occluder",
    "OccluderIndex",
    "build_occluder_bands",
    "char_is_clipped",
    "char_is_occluded",
    "intersect_boxes",
    "intersect_clips",
    "is_blank_glyph_char",
    "point_in_clip",
    "point_in_polygons",
]

Point: TypeAlias = tuple[float, float]
Polygon: TypeAlias = tuple[Point, ...]
Polygons: TypeAlias = tuple[Polygon, ...]

# Hard per-page check cap for char_is_occluded so pathological pages fail open
# rather than burning quadratic time or false-hiding visible text.
MAX_OCCLUSION_CHECKS: int = 500_000

_EMPTY_BOX: Box = (0.0, 0.0, 0.0, 0.0)
_BOUNDARY_EPS: float = 1e-6
_BOUNDARY_EPS_SQ: float = _BOUNDARY_EPS * _BOUNDARY_EPS
_DEFAULT_CLIP_TOL: float = 0.5
_BAND_HEIGHT_PT: float = 32.0
_NARROW_WIDTH_PT: float = 64.0
_MAX_BANDS: int = 4096
_SAMPLE_FRACTIONS: tuple[float, float, float] = (0.2, 0.5, 0.8)
_OCCLUDED_MIN_HITS: int = 8
_MAX_OCCLUDED_MISSES: int = 9 - _OCCLUDED_MIN_HITS
_BLANK_GLYPH_MIN_EM_PT: float = 1.0
_BLANK_GLYPH_MAX_EM_RATIO: float = 0.0025
_IGNORED_UNICODE_PREFIXES: tuple[str, str] = ("C", "Z")


def _finite_box(name: str, value: object) -> Box:
    """Validate a 4-tuple of finite floats (allows empty ``x0 >= x1`` or ``y0 >= y1``)."""
    x0, y0, x1, y1 = finite_tuple(name, value, 4)
    return x0, y0, x1, y1


def _is_empty_box(box: Box) -> bool:
    """Return True if ``box`` has zero or negative width or height."""
    return box[0] >= box[2] or box[1] >= box[3]


def _validate_polygon(name: str, value: object) -> Polygon:
    """Validate one closed subpath as a tuple of finite ``(x, y)`` pairs."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple of points, got {type(value).__name__}")
    points: list[Point] = []
    for idx, pt in enumerate(value):
        px, py = finite_tuple(f"{name}[{idx}]", pt, 2)
        points.append((px, py))
    return tuple(points)


def _validate_polygons(name: str, value: object) -> Polygons:
    """Validate a tuple of closed display-space polygon subpaths."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple of polygons, got {type(value).__name__}")
    return tuple(_validate_polygon(f"{name}[{idx}]", poly) for idx, poly in enumerate(value))


def _check_budget(budget: object) -> list[int]:
    """Return the active mutable 1-element int budget counter, validating caller input."""
    if budget is None:
        return [MAX_OCCLUSION_CHECKS]
    if not isinstance(budget, list):
        raise TypeError(f"budget must be a list of one int or None, got {type(budget).__name__}")
    if len(budget) != 1:
        raise ValueError(f"budget must have length 1, got {len(budget)}")
    count = budget[0]
    if isinstance(count, bool) or not isinstance(count, int):
        raise TypeError(f"budget[0] must be an int, got {count!r} ({type(count).__name__})")
    return budget


@dataclass(frozen=True, slots=True, kw_only=True)
class ClipPathPoly:
    """One PDF clipping path in display space.

    ``polygons is None`` means the path is the exact axis-aligned rectangle
    ``bbox``; otherwise ``polygons`` holds closed display-space subpaths
    evaluated under ``even_odd`` (True = even-odd, False = nonzero winding).
    """

    bbox: Box
    polygons: tuple[tuple[tuple[float, float], ...], ...] | None = None
    even_odd: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "bbox", _finite_box("bbox", self.bbox))
        if self.polygons is not None:
            object.__setattr__(self, "polygons", _validate_polygons("polygons", self.polygons))
        strict_bool("even_odd", self.even_odd)


@dataclass(frozen=True, slots=True, kw_only=True)
class ClipRegion:
    """Intersection of one or more :class:`ClipPathPoly` clip paths.

    ``bbox`` is the intersected bounding box; if ``bbox`` is empty
    (``x0 >= x1`` or ``y0 >= y1``), every point and character is clipped.
    """

    bbox: Box
    paths: tuple[ClipPathPoly, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "bbox", _finite_box("bbox", self.bbox))
        typed_tuple("paths", self.paths, ClipPathPoly)


@dataclass(frozen=True, slots=True, kw_only=True)
class Occluder:
    """An opaque paint object (filled path or opaque image) in display space.

    ``polygons is None`` means a solid axis-aligned rectangle ``bbox``;
    otherwise ``polygons`` holds flattened display-space subpaths evaluated
    with ``even_odd`` (True = alternate, False = nonzero winding). If ``clip``
    is present, the occluder only covers points inside ``clip``.
    """

    object_index: int
    bbox: Box
    polygons: tuple[tuple[tuple[float, float], ...], ...] | None = None
    even_odd: bool = False
    clip: ClipRegion | None = None

    def __post_init__(self) -> None:
        int_at_least("object_index", self.object_index, 0)
        object.__setattr__(self, "bbox", _finite_box("bbox", self.bbox))
        if self.polygons is not None:
            object.__setattr__(self, "polygons", _validate_polygons("polygons", self.polygons))
        strict_bool("even_odd", self.even_odd)
        if self.clip is not None and not isinstance(self.clip, ClipRegion):
            raise TypeError(f"clip must be a ClipRegion or None, got {type(self.clip).__name__}")


@dataclass(frozen=True, slots=True, kw_only=True)
class _OccluderBand:
    """One horizontal page band split into narrow (<= 64 pt) and wide (> 64 pt) x0-sorted occluders."""

    narrow: tuple[Occluder, ...]
    narrow_x0s: tuple[float, ...]
    narrow_max_x1: tuple[float, ...]
    wide: tuple[Occluder, ...]
    wide_x0s: tuple[float, ...]
    wide_max_x1: tuple[float, ...]
    max_object_index: int


@dataclass(frozen=True, slots=True, kw_only=True)
class OccluderIndex:
    """Spatial index of page occluders partitioned into y-bands with narrow/wide x0-sorted lists."""

    band_height: float
    bands: tuple[_OccluderBand, ...]
    max_object_index: int
    total_occluders: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "band_height", positive_float("band_height", self.band_height))
        typed_tuple("bands", self.bands, _OccluderBand)
        int_at_least("max_object_index", self.max_object_index, -1)
        int_at_least("total_occluders", self.total_occluders, 0)


def intersect_boxes(a: Box, b: Box) -> Box:
    """Return the intersection of boxes ``a`` and ``b``, or ``(0.0, 0.0, 0.0, 0.0)`` when disjoint."""
    ax0, ay0, ax1, ay1 = _finite_box("a", a)
    bx0, by0, bx1, by1 = _finite_box("b", b)
    if ax0 >= ax1 or ay0 >= ay1 or bx0 >= bx1 or by0 >= by1:
        return _EMPTY_BOX
    x0 = max(ax0, bx0)
    y0 = max(ay0, by0)
    x1 = min(ax1, bx1)
    y1 = min(ay1, by1)
    if x0 >= x1 or y0 >= y1:
        return _EMPTY_BOX
    return (x0, y0, x1, y1)


def intersect_clips(a: ClipRegion | None, b: ClipRegion | None) -> ClipRegion | None:
    """Combine two optional clip regions cleanly into their intersection."""
    if a is not None and not isinstance(a, ClipRegion):
        raise TypeError(f"a must be a ClipRegion or None, got {type(a).__name__}")
    if b is not None and not isinstance(b, ClipRegion):
        raise TypeError(f"b must be a ClipRegion or None, got {type(b).__name__}")
    if a is None:
        return b
    if b is None:
        return a
    bbox = intersect_boxes(a.bbox, b.bbox)
    if _is_empty_box(bbox):
        return ClipRegion(bbox=_EMPTY_BOX, paths=())
    poly_paths = tuple(path for path in (*a.paths, *b.paths) if path.polygons is not None)
    if poly_paths:
        return ClipRegion(bbox=bbox, paths=poly_paths)
    return ClipRegion(bbox=bbox, paths=(ClipPathPoly(bbox=bbox, polygons=None),))


def _point_near_segment(x: float, y: float, ax: float, ay: float, bx: float, by: float) -> bool:
    """Return True if ``(x, y)`` lies within ``_BOUNDARY_EPS`` of closed segment ``(a, b)``."""
    min_x, max_x = (ax, bx) if ax <= bx else (bx, ax)
    if x < min_x - _BOUNDARY_EPS or x > max_x + _BOUNDARY_EPS:
        return False
    min_y, max_y = (ay, by) if ay <= by else (by, ay)
    if y < min_y - _BOUNDARY_EPS or y > max_y + _BOUNDARY_EPS:
        return False
    dx, dy = bx - ax, by - ay
    px, py = x - ax, y - ay
    seg_len_sq = dx * dx + dy * dy
    if seg_len_sq == 0.0:
        return px * px + py * py <= _BOUNDARY_EPS_SQ
    t = max(0.0, min(1.0, (px * dx + py * dy) / seg_len_sq))
    cx = px - t * dx
    cy = py - t * dy
    return cx * cx + cy * cy <= _BOUNDARY_EPS_SQ


def _edge_crosses_ray(x: float, y: float, ax: float, ay: float, bx: float, by: float) -> bool:
    """Return True if the +x horizontal ray from ``(x, y)`` crosses edge ``(a, b)``."""
    if (ay > y) == (by > y):
        return False
    return x < ax + (y - ay) * (bx - ax) / (by - ay)


def _edge_winding_delta(x: float, y: float, ax: float, ay: float, bx: float, by: float) -> int:
    """Return +1, -1, or 0 for one edge's contribution to the nonzero winding number."""
    if ay <= y < by:
        cross = (bx - ax) * (y - ay) - (x - ax) * (by - ay)
        return 1 if cross > 0.0 else 0
    if by <= y < ay:
        cross = (bx - ax) * (y - ay) - (x - ax) * (by - ay)
        return -1 if cross < 0.0 else 0
    return 0


def _polygon_parity(x: float, y: float, poly: Polygon) -> bool | None:
    """Return even-odd crossing parity of ``poly``, or None if ``(x, y)`` hits its boundary."""
    if len(poly) < 3:
        return False
    inside = False
    ax, ay = poly[-1]
    for bx, by in poly:
        if _point_near_segment(x, y, ax, ay, bx, by):
            return None
        if _edge_crosses_ray(x, y, ax, ay, bx, by):
            inside = not inside
        ax, ay = bx, by
    return inside


def _polygon_winding(x: float, y: float, poly: Polygon) -> int | None:
    """Return winding contribution of ``poly``, or None if ``(x, y)`` hits its boundary."""
    if len(poly) < 3:
        return 0
    winding = 0
    ax, ay = poly[-1]
    for bx, by in poly:
        if _point_near_segment(x, y, ax, ay, bx, by):
            return None
        winding += _edge_winding_delta(x, y, ax, ay, bx, by)
        ax, ay = bx, by
    return winding


def _point_in_polygons_unchecked(x: float, y: float, polygons: Polygons, even_odd: bool) -> bool:
    """Evaluate even-odd or nonzero winding membership for pre-validated inputs."""
    if even_odd:
        inside = False
        for poly in polygons:
            parity = _polygon_parity(x, y, poly)
            if parity is None:
                return True
            inside ^= parity
        return inside
    winding = 0
    for poly in polygons:
        delta = _polygon_winding(x, y, poly)
        if delta is None:
            return True
        winding += delta
    return winding != 0


def point_in_polygons(
    x: float,
    y: float,
    polygons: tuple[tuple[tuple[float, float], ...], ...],
    even_odd: bool,
) -> bool:
    """Return True if ``(x, y)`` is inside ``polygons`` (or within ``1e-6`` of any edge).

    Uses even-odd ray casting when ``even_odd`` is True, and nonzero winding
    number when False.
    """
    px = finite_float("x", x)
    py = finite_float("y", y)
    valid_polys = _validate_polygons("polygons", polygons)
    rule = strict_bool("even_odd", even_odd)
    return _point_in_polygons_unchecked(px, py, valid_polys, rule)


def _point_in_clip_path(x: float, y: float, path: ClipPathPoly, tol: float) -> bool:
    """Return True if ``(x, y)`` passes one :class:`ClipPathPoly` within tolerance ``tol``."""
    bx0, by0, bx1, by1 = path.bbox
    if bx0 >= bx1 or by0 >= by1:
        return False
    if x < bx0 - tol or x > bx1 + tol or y < by0 - tol or y > by1 + tol:
        return False
    if path.polygons is None:
        return True
    if _point_in_polygons_unchecked(x, y, path.polygons, path.even_odd):
        return True
    if tol <= 0.0:
        return False
    halo = ((x - tol, y), (x + tol, y), (x, y - tol), (x, y + tol))
    return any(_point_in_polygons_unchecked(hx, hy, path.polygons, path.even_odd) for hx, hy in halo)


def _point_in_clip_unchecked(x: float, y: float, clip: ClipRegion, tol: float) -> bool:
    """Evaluate clip membership for pre-validated inputs."""
    cx0, cy0, cx1, cy1 = clip.bbox
    if cx0 >= cx1 or cy0 >= cy1:
        return False
    if x < cx0 - tol or x > cx1 + tol or y < cy0 - tol or y > cy1 + tol:
        return False
    return all(_point_in_clip_path(x, y, path, tol) for path in clip.paths)


def point_in_clip(x: float, y: float, clip: ClipRegion, *, tol: float = _DEFAULT_CLIP_TOL) -> bool:
    """Return True if ``(x, y)`` lies inside ``clip`` (with ``tol`` hairline halo)."""
    px = finite_float("x", x)
    py = finite_float("y", y)
    if not isinstance(clip, ClipRegion):
        raise TypeError(f"clip must be a ClipRegion, got {type(clip).__name__}")
    slack = finite_float("tol", tol, lo=0.0)
    return _point_in_clip_unchecked(px, py, clip, slack)


def _sample_points(box: Box) -> tuple[Point, ...]:
    """Return the 3x3 = 9 inset sample points inside ``box``."""
    x0, y0, x1, y1 = box
    width = x1 - x0
    height = y1 - y0
    xs = tuple(x0 + frac * width for frac in _SAMPLE_FRACTIONS)
    ys = tuple(y0 + frac * height for frac in _SAMPLE_FRACTIONS)
    return tuple((px, py) for py in ys for px in xs)


def char_is_clipped(tight_box: Box, clip: ClipRegion | None, *, tol: float = _DEFAULT_CLIP_TOL) -> bool:
    """Return True if ``tight_box`` is completely clipped out by ``clip`` (0 of 9 sample points pass)."""
    box = ordered_box("tight_box", tight_box)
    if clip is not None and not isinstance(clip, ClipRegion):
        raise TypeError(f"clip must be a ClipRegion or None, got {type(clip).__name__}")
    slack = finite_float("tol", tol, lo=0.0)
    if clip is None:
        return False
    cx0, cy0, cx1, cy1 = clip.bbox
    if cx0 >= cx1 or cy0 >= cy1:
        return True
    tx0, ty0, tx1, ty1 = box
    if tx1 + slack < cx0 or tx0 - slack > cx1 or ty1 + slack < cy0 or ty0 - slack > cy1:
        return True
    return not any(_point_in_clip_unchecked(px, py, clip, slack) for px, py in _sample_points(box))


def _band_index(y: float, band_height: float, num_bands: int) -> int:
    """Map display-space ``y`` to its clamped band index in ``[0, num_bands - 1]``."""
    raw = math.floor(y / band_height)
    return max(0, min(num_bands - 1, raw))


def _sort_band_entries(
    entries: list[tuple[float, float, Occluder]],
) -> tuple[
    tuple[Occluder, ...],
    tuple[float, ...],
    tuple[float, ...],
]:
    """Sort ``(x0, x1, occ)`` entries by ``x0`` and build parallel ``x0`` and prefix-max ``x1`` arrays."""
    if not entries:
        return (), (), ()
    entries.sort(key=lambda item: item[0])
    occs: list[Occluder] = []
    x0s: list[float] = []
    max_x1s: list[float] = []
    running_max = -math.inf
    for x0, x1, occ in entries:
        running_max = x1 if x1 > running_max else running_max
        occs.append(occ)
        x0s.append(x0)
        max_x1s.append(running_max)
    return tuple(occs), tuple(x0s), tuple(max_x1s)


def _effective_occluder_box(occ: Occluder) -> Box:
    """Return the effective painted bbox of ``occ`` (empty if no area can be painted)."""
    if occ.polygons is not None and not occ.polygons:
        return _EMPTY_BOX
    if occ.clip is None:
        return occ.bbox
    return intersect_boxes(occ.bbox, occ.clip.bbox)


def build_occluder_bands(occluders: tuple[Occluder, ...], page_height: float) -> OccluderIndex:
    """Build an ``x0``-sorted y-band :class:`OccluderIndex` with narrow (<= 64 pt) and wide (> 64 pt) splits."""
    valid_occs = typed_tuple("occluders", occluders, Occluder)
    height = finite_float("page_height", page_height, lo=0.0)
    num_bands = max(1, min(_MAX_BANDS, math.ceil(height / _BAND_HEIGHT_PT))) if height > 0.0 else 1
    band_height = max(_BAND_HEIGHT_PT, height / num_bands)
    narrow_buckets: list[list[tuple[float, float, Occluder]]] = [[] for _ in range(num_bands)]
    wide_buckets: list[list[tuple[float, float, Occluder]]] = [[] for _ in range(num_bands)]
    band_max_idx: list[int] = [-1] * num_bands
    global_max_idx = -1
    indexed_count = 0

    for occ in valid_occs:
        eff = _effective_occluder_box(occ)
        if _is_empty_box(eff):
            continue
        indexed_count += 1
        global_max_idx = max(global_max_idx, occ.object_index)
        target = narrow_buckets if (eff[2] - eff[0]) <= _NARROW_WIDTH_PT else wide_buckets
        b0 = _band_index(eff[1], band_height, num_bands)
        b1 = _band_index(eff[3], band_height, num_bands)
        entry = (eff[0], eff[2], occ)
        for band_i in range(b0, b1 + 1):
            target[band_i].append(entry)
            if occ.object_index > band_max_idx[band_i]:
                band_max_idx[band_i] = occ.object_index

    bands = tuple(_build_band(narrow_buckets[i], wide_buckets[i], band_max_idx[i]) for i in range(num_bands))
    return OccluderIndex(
        band_height=band_height,
        bands=bands,
        max_object_index=global_max_idx,
        total_occluders=indexed_count,
    )


def _build_band(
    narrow_entries: list[tuple[float, float, Occluder]],
    wide_entries: list[tuple[float, float, Occluder]],
    max_object_index: int,
) -> _OccluderBand:
    """Freeze one band's narrow and wide sorted occluder arrays."""
    narrow, narrow_x0s, narrow_max_x1 = _sort_band_entries(narrow_entries)
    wide, wide_x0s, wide_max_x1 = _sort_band_entries(wide_entries)
    return _OccluderBand(
        narrow=narrow,
        narrow_x0s=narrow_x0s,
        narrow_max_x1=narrow_max_x1,
        wide=wide,
        wide_x0s=wide_x0s,
        wide_max_x1=wide_max_x1,
        max_object_index=max_object_index,
    )


def _occluder_covers_point(px: float, py: float, occ: Occluder) -> bool:
    """Return True if ``occ`` covers ``(px, py)``, respecting ``occ.clip`` and ``occ.polygons``."""
    ox0, oy0, ox1, oy1 = occ.bbox
    if px < ox0 or px > ox1 or py < oy0 or py > oy1:
        return False
    if occ.clip is not None and not _point_in_clip_unchecked(px, py, occ.clip, 0.0):
        return False
    if occ.polygons is None:
        return True
    return _point_in_polygons_unchecked(px, py, occ.polygons, occ.even_odd)


def _slice_covers_point(
    px: float,
    py: float,
    span_object_index: int,
    occs: tuple[Occluder, ...],
    max_x1s: tuple[float, ...],
    lo: int,
    hi: int,
    budget: list[int],
) -> bool | None:
    """Return True if any later occluder in ``occs[lo:hi]`` covers ``(px, py)``, False if none, None on budget cap."""
    for idx in range(hi - 1, lo - 1, -1):
        if max_x1s[idx] < px:
            return False
        if budget[0] <= 0:
            return None
        budget[0] -= 1
        occ = occs[idx]
        if occ.object_index > span_object_index and _occluder_covers_point(px, py, occ):
            return True
    return False


def _band_covers_point(
    px: float,
    py: float,
    span_object_index: int,
    band: _OccluderBand,
    budget: list[int],
) -> bool | None:
    """Test whether any occluder in ``band`` with ``object_index > span_object_index`` covers ``(px, py)``."""
    if band.max_object_index <= span_object_index:
        return False
    if band.narrow:
        lo = bisect.bisect_left(band.narrow_x0s, px - _NARROW_WIDTH_PT)
        hi = bisect.bisect_right(band.narrow_x0s, px)
        hit = _slice_covers_point(px, py, span_object_index, band.narrow, band.narrow_max_x1, lo, hi, budget)
        if hit is not False:
            return hit
    if band.wide:
        hi = bisect.bisect_right(band.wide_x0s, px)
        return _slice_covers_point(px, py, span_object_index, band.wide, band.wide_max_x1, 0, hi, budget)
    return False


def char_is_occluded(
    tight_box: Box,
    span_object_index: int,
    index: OccluderIndex,
    budget: list[int] | None = None,
) -> bool:
    """Return True if >= 8 of 9 inset sample points of ``tight_box`` are covered by later occluders.

    Stops safely and fails open to ``False`` if ``budget`` reaches 0.
    """
    box = ordered_box("tight_box", tight_box)
    span_idx = int_at_least("span_object_index", span_object_index, 0)
    if not isinstance(index, OccluderIndex):
        raise TypeError(f"index must be an OccluderIndex, got {type(index).__name__}")
    remaining = _check_budget(budget)
    if remaining[0] <= 0 or index.total_occluders == 0 or span_idx >= index.max_object_index:
        return False
    num_bands = len(index.bands)
    covered = 0
    misses = 0
    for px, py in _sample_points(box):
        band = index.bands[_band_index(py, index.band_height, num_bands)]
        hit = _band_covers_point(px, py, span_idx, band, remaining)
        if hit is None:
            return False
        covered += int(hit)
        misses += int(not hit)
        if covered >= _OCCLUDED_MIN_HITS:
            return True
        if misses > _MAX_OCCLUDED_MISSES:
            return False
    return covered >= _OCCLUDED_MIN_HITS


def is_blank_glyph_char(char: str, tight_box: Box, em_size_pt: float) -> bool:
    """Return True if ``char`` is a non-whitespace/non-control glyph whose outline collapsed to a stand-in box."""
    if not isinstance(char, str):
        raise TypeError(f"char must be a str, got {char!r} ({type(char).__name__})")
    if len(char) != 1:
        raise ValueError(f"char must be a single character, got length {len(char)}")
    box = ordered_box("tight_box", tight_box)
    em_pt = finite_float("em_size_pt", em_size_pt)
    if char == "\ufffd" or char.isspace() or unicodedata.category(char)[0] in _IGNORED_UNICODE_PREFIXES:
        return False
    if em_pt < _BLANK_GLYPH_MIN_EM_PT:
        return False
    return min(box[2] - box[0], box[3] - box[1]) <= _BLANK_GLYPH_MAX_EM_RATIO * em_pt
