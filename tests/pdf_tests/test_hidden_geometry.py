"""Unit tests for Issue #395 hidden-text geometry primitives and HiddenReason extensions."""

import ast
import math
from pathlib import Path
from typing import get_args

import pytest

import datagrunt.pdf._hidden_geometry as hidden_geom
from datagrunt.pdf._hidden_geometry import (
    MAX_OCCLUSION_CHECKS,
    Box,
    ClipPathPoly,
    ClipRegion,
    Occluder,
    OccluderIndex,
    build_occluder_bands,
    char_is_clipped,
    char_is_occluded,
    intersect_boxes,
    intersect_clips,
    is_blank_glyph_char,
    point_in_clip,
    point_in_polygons,
)
from datagrunt.pdf.elements import HIDDEN_REASONS, HiddenReason, check_hidden_reasons

_FORBIDDEN_IMPORTS = frozenset({"pypdfium2", "PIL", "polars", "pyarrow"})


def _rect_poly(x0: float, y0: float, x1: float, y1: float, *, reverse: bool = False) -> tuple[tuple[float, float], ...]:
    """Return a closed rectangle vertex ring in clockwise or counter-clockwise order."""
    ring = ((x0, y0), (x1, y0), (x1, y1), (x0, y1))
    return tuple(reversed(ring)) if reverse else ring


def _diamond_poly(cx: float, cy: float, r: float) -> tuple[tuple[float, float], ...]:
    """Return a diamond polygon centered at ``(cx, cy)`` with half-diagonal ``r``."""
    return ((cx, cy - r), (cx + r, cy), (cx, cy + r), (cx - r, cy))


def _circle_poly(cx: float, cy: float, r: float, segments: int = 32) -> tuple[tuple[float, float], ...]:
    """Return a regular polygon approximation of a circle."""
    return tuple(
        (cx + r * math.cos(2.0 * math.pi * i / segments), cy + r * math.sin(2.0 * math.pi * i / segments))
        for i in range(segments)
    )


def _rect_clip(box: Box) -> ClipRegion:
    """Build a single-path axis-aligned rectangular ClipRegion."""
    return ClipRegion(bbox=box, paths=(ClipPathPoly(bbox=box, polygons=None),))


class TestHiddenReasonsContract:
    def test_canonical_order_appends_three_reasons(self) -> None:
        expected = (
            "off_page",
            "invisible",
            "tiny_font",
            "transparent",
            "matches_background",
            "zero_scale",
            "duplicate",
            "superseded",
            "unchecked_background",
            "occluded",
            "clipped",
            "blank_glyph",
        )
        assert HIDDEN_REASONS == expected
        assert get_args(HiddenReason) == expected

    def test_check_hidden_reasons_accepts_and_orders_new_reasons(self) -> None:
        check_hidden_reasons(("occluded",))
        check_hidden_reasons(("clipped",))
        check_hidden_reasons(("blank_glyph",))
        check_hidden_reasons(("unchecked_background", "occluded", "clipped", "blank_glyph"))
        with pytest.raises(ValueError, match="canonical order"):
            check_hidden_reasons(("clipped", "occluded"))
        with pytest.raises(ValueError, match="canonical order"):
            check_hidden_reasons(("occluded", "unchecked_background"))


class TestModulePurity:
    def test_no_pdfium_pil_or_polars_imports(self) -> None:
        source_path = Path(hidden_geom.__file__)
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported_roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".")[0] for alias in node.names)
            if isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_roots.add(node.module.split(".")[0])
        assert imported_roots.isdisjoint(_FORBIDDEN_IMPORTS)


class TestDataclassValidation:
    def test_clip_path_poly_validation(self) -> None:
        poly = ClipPathPoly(bbox=(0.0, 0.0, 10.0, 10.0), polygons=None)
        assert poly.even_odd is False
        with pytest.raises(AttributeError):
            poly.even_odd = True  # type: ignore[misc]
        with pytest.raises(ValueError, match="finite"):
            ClipPathPoly(bbox=(0.0, 0.0, float("nan"), 10.0), polygons=None)
        with pytest.raises(TypeError, match="even_odd"):
            ClipPathPoly(bbox=(0.0, 0.0, 10.0, 10.0), polygons=None, even_odd=1)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="polygons"):
            ClipPathPoly(bbox=(0.0, 0.0, 10.0, 10.0), polygons=[((0.0, 0.0),)])  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="tuple of points"):
            ClipPathPoly(bbox=(0.0, 0.0, 10.0, 10.0), polygons=([(0.0, 0.0)],))  # type: ignore[arg-type]

    def test_clip_region_and_occluder_validation(self) -> None:
        with pytest.raises(TypeError, match="paths"):
            ClipRegion(bbox=(0.0, 0.0, 10.0, 10.0), paths=("bad",))  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="object_index"):
            Occluder(object_index=-1, bbox=(0.0, 0.0, 10.0, 10.0))
        with pytest.raises(TypeError, match="object_index"):
            Occluder(object_index=True, bbox=(0.0, 0.0, 10.0, 10.0))
        with pytest.raises(TypeError, match="clip"):
            Occluder(object_index=0, bbox=(0.0, 0.0, 10.0, 10.0), clip="bad")  # type: ignore[arg-type]


class TestIntersectBoxesAndClips:
    @pytest.mark.parametrize(
        "a,b,expected",
        [
            ((0.0, 0.0, 10.0, 10.0), (5.0, 5.0, 15.0, 15.0), (5.0, 5.0, 10.0, 10.0)),
            ((0.0, 0.0, 10.0, 10.0), (2.0, 3.0, 8.0, 7.0), (2.0, 3.0, 8.0, 7.0)),
            ((-20.0, -10.0, 5.0, 5.0), (-5.0, -5.0, 20.0, 20.0), (-5.0, -5.0, 5.0, 5.0)),
            ((0.0, 0.0, 10.0, 10.0), (20.0, 0.0, 30.0, 10.0), (0.0, 0.0, 0.0, 0.0)),
            ((0.0, 0.0, 10.0, 10.0), (10.0, 0.0, 20.0, 10.0), (0.0, 0.0, 0.0, 0.0)),
            ((0.0, 0.0, 0.0, 10.0), (0.0, 0.0, 10.0, 10.0), (0.0, 0.0, 0.0, 0.0)),
            ((10.0, 0.0, 5.0, 10.0), (0.0, 0.0, 20.0, 10.0), (0.0, 0.0, 0.0, 0.0)),
        ],
    )
    def test_intersect_boxes(self, a: Box, b: Box, expected: Box) -> None:
        assert intersect_boxes(a, b) == expected

    def test_intersect_clips_none_and_rect_chains(self) -> None:
        clip_a = _rect_clip((0.0, 0.0, 100.0, 100.0))
        clip_b = _rect_clip((20.0, 30.0, 80.0, 120.0))
        assert intersect_clips(None, None) is None
        assert intersect_clips(clip_a, None) == clip_a
        assert intersect_clips(None, clip_b) == clip_b

        combined = intersect_clips(clip_a, clip_b)
        assert combined is not None
        assert combined.bbox == (20.0, 30.0, 80.0, 100.0)

        disjoint = intersect_clips(clip_a, _rect_clip((120.0, 0.0, 200.0, 100.0)))
        assert disjoint is not None
        assert disjoint.bbox == (0.0, 0.0, 0.0, 0.0)

    def test_intersect_clips_preserves_nested_polygons(self) -> None:
        diamond_a = ClipPathPoly(bbox=(0.0, 0.0, 100.0, 100.0), polygons=(_diamond_poly(50.0, 50.0, 50.0),))
        diamond_b = ClipPathPoly(bbox=(40.0, 0.0, 140.0, 100.0), polygons=(_diamond_poly(90.0, 50.0, 50.0),))
        combined = intersect_clips(
            ClipRegion(bbox=diamond_a.bbox, paths=(diamond_a,)),
            ClipRegion(bbox=diamond_b.bbox, paths=(diamond_b,)),
        )
        assert combined is not None
        assert combined.bbox == (40.0, 0.0, 100.0, 100.0)
        assert point_in_clip(70.0, 50.0, combined, tol=0.0) is True
        assert point_in_clip(50.0, 50.0, combined, tol=0.0) is True
        assert point_in_clip(30.0, 50.0, combined, tol=0.0) is False
        assert point_in_clip(70.0, 10.0, combined, tol=0.0) is False

    def test_intersect_clips_rejects_bad_types(self) -> None:
        with pytest.raises(TypeError, match="a"):
            intersect_clips("bad", None)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="b"):
            intersect_clips(None, "bad")  # type: ignore[arg-type]


class TestPointInPolygons:
    def test_hollow_frame_opposite_winding_both_rules(self) -> None:
        outer = _rect_poly(0.0, 0.0, 100.0, 100.0, reverse=False)
        inner_opposite = _rect_poly(20.0, 20.0, 80.0, 80.0, reverse=True)
        frame = (outer, inner_opposite)
        for even_odd in (True, False):
            assert point_in_polygons(10.0, 50.0, frame, even_odd) is True
            assert point_in_polygons(50.0, 10.0, frame, even_odd) is True
            assert point_in_polygons(50.0, 50.0, frame, even_odd) is False
            assert point_in_polygons(110.0, 50.0, frame, even_odd) is False

    def test_hollow_frame_same_winding_diverges_by_rule(self) -> None:
        outer = _rect_poly(0.0, 0.0, 100.0, 100.0, reverse=False)
        inner_same = _rect_poly(20.0, 20.0, 80.0, 80.0, reverse=False)
        frame = (outer, inner_same)
        assert point_in_polygons(50.0, 50.0, frame, even_odd=True) is False
        assert point_in_polygons(50.0, 50.0, frame, even_odd=False) is True

    def test_boundary_tolerance_one_micro_point(self) -> None:
        tri = (((0.0, 0.0), (10.0, 0.0), (0.0, 10.0)),)
        for even_odd in (True, False):
            assert point_in_polygons(0.0, 0.0, tri, even_odd) is True
            assert point_in_polygons(5.0, 5.0, tri, even_odd) is True
            assert point_in_polygons(5.0 + 5e-7, 5.0, tri, even_odd) is True
            assert point_in_polygons(-5e-7, 5.0, tri, even_odd) is True
            assert point_in_polygons(-2e-6, 5.0, tri, even_odd) is False
            assert point_in_polygons(5.0 + 3e-6, 5.0 + 3e-6, tri, even_odd) is False

    def test_concave_vertex_ray_and_degenerate_subpaths(self) -> None:
        u_shape = (
            (
                (0.0, 0.0),
                (30.0, 0.0),
                (30.0, 30.0),
                (20.0, 30.0),
                (20.0, 10.0),
                (10.0, 10.0),
                (10.0, 30.0),
                (0.0, 30.0),
            ),
        )
        for even_odd in (True, False):
            assert point_in_polygons(5.0, 10.0, u_shape, even_odd) is True
            assert point_in_polygons(15.0, 20.0, u_shape, even_odd) is False
            assert point_in_polygons(25.0, 10.0, u_shape, even_odd) is True
            assert point_in_polygons(15.0, 5.0, u_shape, even_odd) is True
        assert point_in_polygons(5.0, 5.0, (), False) is False
        assert point_in_polygons(5.0, 5.0, (((0.0, 0.0), (10.0, 10.0)),), True) is False
        assert point_in_polygons(5.0, 5.0, (((0.0, 0.0), (10.0, 10.0)),), False) is False
        closed_tri = (((0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (0.0, 0.0)),)
        assert point_in_polygons(2.0, 2.0, closed_tri, False) is True
        assert point_in_polygons(0.0, 0.0, closed_tri, False) is True

    def test_point_in_polygons_rejects_bad_inputs(self) -> None:
        tri = (((0.0, 0.0), (10.0, 0.0), (0.0, 10.0)),)
        with pytest.raises(ValueError, match="finite"):
            point_in_polygons(float("inf"), 1.0, tri, False)
        with pytest.raises(TypeError, match="even_odd"):
            point_in_polygons(1.0, 1.0, tri, 0)  # type: ignore[arg-type]


class TestClipping:
    def test_none_zero_area_and_disjoint_clips(self) -> None:
        char_box = (10.0, 10.0, 20.0, 22.0)
        assert char_is_clipped(char_box, None) is False
        zero_area = ClipRegion(bbox=(10.0, 10.0, 10.0, 22.0), paths=())
        inverted = ClipRegion(bbox=(20.0, 10.0, 10.0, 22.0), paths=())
        assert char_is_clipped(char_box, zero_area, tol=0.5) is True
        assert char_is_clipped(char_box, inverted, tol=0.5) is True
        assert point_in_clip(10.0, 15.0, zero_area, tol=0.5) is False
        assert char_is_clipped(char_box, _rect_clip((25.0, 10.0, 80.0, 50.0))) is True

    def test_partial_clip_char_is_not_flagged_clipped(self) -> None:
        char_box = (10.0, 10.0, 20.0, 22.0)
        assert char_is_clipped(char_box, _rect_clip((15.0, 0.0, 100.0, 100.0))) is False
        assert char_is_clipped(char_box, _rect_clip((17.5, 19.0, 100.0, 100.0))) is False

    def test_boundary_tolerance_for_rect_and_polygon_clips(self) -> None:
        hairline_char = (9.6, 10.0, 9.9, 22.0)
        rect_clip = _rect_clip((10.0, 10.0, 50.0, 30.0))
        assert char_is_clipped(hairline_char, rect_clip, tol=0.5) is False
        assert char_is_clipped(hairline_char, rect_clip, tol=0.0) is True
        assert char_is_clipped((8.8, 10.0, 9.2, 22.0), rect_clip, tol=0.5) is True

        diamond = ClipPathPoly(bbox=(0.0, 0.0, 100.0, 100.0), polygons=(_diamond_poly(50.0, 50.0, 50.0),))
        poly_clip = ClipRegion(bbox=diamond.bbox, paths=(diamond,))
        # Point (24.7, 25.0) is 0.3 pt left of edge x + y = 50 -> within tol=0.5 halo, outside tol=0.0.
        assert point_in_clip(24.7, 25.0, poly_clip, tol=0.5) is True
        assert point_in_clip(24.7, 25.0, poly_clip, tol=0.0) is False
        # Char in top-left corner of diamond bbox (outside diamond): clipped!
        assert char_is_clipped((2.0, 2.0, 10.0, 12.0), poly_clip, tol=0.5) is True
        # Char centered in diamond: visible!
        assert char_is_clipped((45.0, 45.0, 55.0, 55.0), poly_clip, tol=0.5) is False

    def test_clip_rejects_bad_inputs(self) -> None:
        clip = _rect_clip((0.0, 0.0, 100.0, 100.0))
        with pytest.raises(ValueError, match="tol"):
            char_is_clipped((1.0, 1.0, 5.0, 5.0), clip, tol=-0.1)
        with pytest.raises(ValueError, match="x0 <= x1"):
            char_is_clipped((10.0, 1.0, 5.0, 5.0), clip)
        with pytest.raises(TypeError, match="clip"):
            char_is_clipped((1.0, 1.0, 5.0, 5.0), "bad")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="clip"):
            point_in_clip(1.0, 1.0, "bad")  # type: ignore[arg-type]
        empty_sub = ClipRegion(
            bbox=(0.0, 0.0, 100.0, 100.0), paths=(ClipPathPoly(bbox=(10.0, 10.0, 10.0, 20.0), polygons=None),)
        )
        assert point_in_clip(10.0, 15.0, empty_sub, tol=0.5) is False
        sub_box = ClipRegion(
            bbox=(0.0, 0.0, 100.0, 100.0), paths=(ClipPathPoly(bbox=(50.0, 50.0, 80.0, 80.0), polygons=None),)
        )
        assert point_in_clip(10.0, 10.0, sub_box, tol=0.5) is False


class TestOcclusion:
    def test_paint_order_and_full_occlusion(self) -> None:
        occ = Occluder(object_index=5, bbox=(0.0, 0.0, 100.0, 100.0))
        index = build_occluder_bands((occ,), page_height=200.0)
        char_box = (10.0, 10.0, 20.0, 22.0)
        assert char_is_occluded(char_box, 4, index) is True
        assert char_is_occluded(char_box, 5, index) is False
        assert char_is_occluded(char_box, 6, index) is False
        assert char_is_occluded(char_box, 0, build_occluder_bands((), page_height=200.0)) is False

    def test_split_two_box_occlusion(self) -> None:
        left = Occluder(object_index=2, bbox=(0.0, 0.0, 15.0, 40.0))
        right = Occluder(object_index=3, bbox=(15.0, 0.0, 40.0, 40.0))
        index = build_occluder_bands((left, right), page_height=100.0)
        assert char_is_occluded((10.0, 10.0, 20.0, 22.0), 1, index) is True
        assert char_is_occluded((10.0, 10.0, 20.0, 22.0), 2, index) is False

    def test_eight_of_nine_threshold_and_strikethrough(self) -> None:
        char_box = (10.0, 10.0, 20.0, 22.0)
        # 1-pt strikethrough across the middle (y in [15.5, 16.5]): covers 3/9 points -> NOT occluded.
        strike = Occluder(object_index=2, bbox=(0.0, 15.5, 40.0, 16.5))
        assert char_is_occluded(char_box, 1, build_occluder_bands((strike,), 100.0)) is False
        # Cover all except the bottom-right sample point (18.0, 19.6) -> 8/9 covered -> occluded.
        l_poly = (((0.0, 0.0), (40.0, 0.0), (40.0, 19.0), (17.0, 19.0), (17.0, 40.0), (0.0, 40.0)),)
        occ_8 = Occluder(object_index=2, bbox=(0.0, 0.0, 40.0, 40.0), polygons=l_poly)
        assert char_is_occluded(char_box, 1, build_occluder_bands((occ_8,), 100.0)) is True
        # Leave 2 points uncovered (x=18.0 at y=16.0 and y=19.6) -> 7/9 covered -> NOT occluded.
        l_poly_7 = (((0.0, 0.0), (40.0, 0.0), (40.0, 15.0), (17.0, 15.0), (17.0, 40.0), (0.0, 40.0)),)
        occ_7 = Occluder(object_index=2, bbox=(0.0, 0.0, 40.0, 40.0), polygons=l_poly_7)
        assert char_is_occluded(char_box, 1, build_occluder_bands((occ_7,), 100.0)) is False

    def test_hollow_frame_occluder_even_odd_and_winding(self) -> None:
        outer = _rect_poly(0.0, 0.0, 100.0, 40.0, reverse=False)
        inner_opp = _rect_poly(5.0, 5.0, 95.0, 35.0, reverse=True)
        inner_same = _rect_poly(5.0, 5.0, 95.0, 35.0, reverse=False)
        inside_char = (20.0, 12.0, 30.0, 24.0)
        border_char = (0.5, 12.0, 4.5, 24.0)

        for even_odd in (True, False):
            occ = Occluder(object_index=2, bbox=(0.0, 0.0, 100.0, 40.0), polygons=(outer, inner_opp), even_odd=even_odd)
            idx = build_occluder_bands((occ,), 100.0)
            assert char_is_occluded(inside_char, 1, idx) is False
            assert char_is_occluded(border_char, 1, idx) is True

        occ_same_eo = Occluder(
            object_index=2, bbox=(0.0, 0.0, 100.0, 40.0), polygons=(outer, inner_same), even_odd=True
        )
        occ_same_nz = Occluder(
            object_index=2, bbox=(0.0, 0.0, 100.0, 40.0), polygons=(outer, inner_same), even_odd=False
        )
        assert char_is_occluded(inside_char, 1, build_occluder_bands((occ_same_eo,), 100.0)) is False
        assert char_is_occluded(inside_char, 1, build_occluder_bands((occ_same_nz,), 100.0)) is True

    def test_clipped_occluder_respects_clip_exactly(self) -> None:
        circle = ClipPathPoly(bbox=(20.0, 20.0, 60.0, 60.0), polygons=(_circle_poly(40.0, 40.0, 20.0),))
        clip = ClipRegion(bbox=circle.bbox, paths=(circle,))
        occ = Occluder(object_index=5, bbox=(0.0, 0.0, 200.0, 200.0), clip=clip)
        index = build_occluder_bands((occ,), page_height=200.0)
        assert char_is_occluded((35.0, 35.0, 45.0, 45.0), 1, index) is True
        assert char_is_occluded((21.0, 21.0, 25.0, 25.0), 1, index) is False
        assert char_is_occluded((60.2, 35.0, 68.0, 45.0), 1, index) is False

        empty_clipped = Occluder(
            object_index=5,
            bbox=(0.0, 0.0, 200.0, 200.0),
            clip=ClipRegion(bbox=(0.0, 0.0, 0.0, 0.0), paths=()),
        )
        assert char_is_occluded((35.0, 35.0, 45.0, 45.0), 1, build_occluder_bands((empty_clipped,), 200.0)) is False

    def test_budget_exhaustion_fails_open(self) -> None:
        occs = tuple(Occluder(object_index=i + 1, bbox=(0.0, 0.0, 100.0, 100.0)) for i in range(10))
        index = build_occluder_bands(occs, page_height=100.0)
        assert MAX_OCCLUSION_CHECKS == 500_000
        zero_budget = [0]
        assert char_is_occluded((10.0, 10.0, 20.0, 22.0), 0, index, budget=zero_budget) is False
        assert zero_budget == [0]

        small_budget = [3]
        assert char_is_occluded((10.0, 10.0, 20.0, 22.0), 0, index, budget=small_budget) is False
        assert small_budget[0] == 0

    def test_narrow_wide_band_split_and_scaling(self) -> None:
        giant_bg = Occluder(object_index=1, bbox=(0.0, 0.0, 612.0, 792.0))
        narrow_boxes = tuple(
            Occluder(object_index=i + 2, bbox=(float(i * 12), 100.0, float(i * 12 + 10), 120.0)) for i in range(500)
        )
        index = build_occluder_bands((giant_bg, *narrow_boxes), page_height=792.0)
        assert isinstance(index, OccluderIndex)
        assert index.total_occluders == 501
        band = index.bands[int(100.0 // index.band_height)]
        assert len(band.wide) == 1
        assert len(band.narrow) == 500

        # Query near the far right (x ~ 5880): giant_bg is skipped/misses x1, narrow bisect skips ~494 boxes.
        budget = [MAX_OCCLUSION_CHECKS]
        assert char_is_occluded((5881.0, 104.0, 5889.0, 116.0), 1, index, budget=budget) is True
        checks_used = MAX_OCCLUSION_CHECKS - budget[0]
        assert checks_used < 60

    def test_off_page_and_extreme_page_heights(self) -> None:
        neg_occ = Occluder(object_index=2, bbox=(-50.0, -50.0, 50.0, -5.0))
        far_occ = Occluder(object_index=3, bbox=(0.0, 900.0, 100.0, 1000.0))
        idx = build_occluder_bands((neg_occ, far_occ), page_height=792.0)
        assert char_is_occluded((-30.0, -30.0, -10.0, -10.0), 1, idx) is True
        assert char_is_occluded((10.0, 920.0, 20.0, 940.0), 1, idx) is True

        idx_zero = build_occluder_bands((neg_occ,), page_height=0.0)
        assert len(idx_zero.bands) == 1
        idx_huge = build_occluder_bands((far_occ,), page_height=10_000_000.0)
        assert len(idx_huge.bands) <= 4096
        assert char_is_occluded((10.0, 920.0, 20.0, 940.0), 1, idx_huge) is True

    def test_occlusion_rejects_bad_inputs(self) -> None:
        index = build_occluder_bands((), page_height=100.0)
        with pytest.raises(ValueError, match="page_height"):
            build_occluder_bands((), page_height=-1.0)
        with pytest.raises(ValueError, match="span_object_index"):
            char_is_occluded((0.0, 0.0, 10.0, 10.0), -1, index)
        with pytest.raises(TypeError, match="index"):
            char_is_occluded((0.0, 0.0, 10.0, 10.0), 0, "bad")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="budget"):
            char_is_occluded((0.0, 0.0, 10.0, 10.0), 0, index, budget=(1,))  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="budget"):
            char_is_occluded((0.0, 0.0, 10.0, 10.0), 0, index, budget=[True])
        with pytest.raises(ValueError, match="budget"):
            char_is_occluded((0.0, 0.0, 10.0, 10.0), 0, index, budget=[1, 2])
        empty_poly_occ = Occluder(object_index=5, bbox=(0.0, 0.0, 50.0, 50.0), polygons=())
        far_later = Occluder(object_index=9, bbox=(0.0, 150.0, 50.0, 180.0))
        idx = build_occluder_bands((empty_poly_occ, far_later), page_height=200.0)
        assert idx.total_occluders == 1
        assert char_is_occluded((10.0, 10.0, 20.0, 20.0), 1, idx) is False


class TestBlankGlyphChar:
    @pytest.mark.parametrize(
        "char",
        [" ", "\t", "\n", "\r", "\u00a0", "\u200b", "\u00ad", "\u2028", "\x00", "\ufffd"],
    )
    def test_whitespace_control_separator_and_replacement_never_flagged(self, char: str) -> None:
        assert is_blank_glyph_char(char, (10.0, 10.0, 16.0, 10.0), 12.0) is False

    def test_pdfium_standin_box_flagged_horizontal_and_vertical(self) -> None:
        assert is_blank_glyph_char("A", (10.0, 10.0, 16.0, 10.012), 12.0) is True
        assert is_blank_glyph_char("A", (10.0, 10.0, 10.012, 16.0), 12.0) is True
        assert is_blank_glyph_char("$", (10.0, 10.0, 10.0, 10.0), 12.0) is True
        # Exact threshold: 0.0025 * 12.0 = 0.03 pt.
        assert is_blank_glyph_char("A", (10.0, 10.0, 16.0, 10.03), 12.0) is True
        assert is_blank_glyph_char("A", (10.0, 10.0, 16.0, 10.031), 12.0) is False

    @pytest.mark.parametrize(
        "char,box",
        [
            ("_", (10.0, 10.0, 16.0, 10.8)),
            ("-", (10.0, 10.0, 14.0, 10.9)),
            (".", (10.0, 10.0, 11.2, 11.2)),
            ("|", (10.0, 10.0, 10.8, 22.0)),
            ("l", (10.0, 10.0, 10.9, 20.0)),
        ],
    )
    def test_thin_punctuation_and_narrow_glyphs_not_flagged(self, char: str, box: Box) -> None:
        assert is_blank_glyph_char(char, box, 12.0) is False

    def test_sub_point_em_size_not_flagged(self) -> None:
        assert is_blank_glyph_char("A", (10.0, 10.0, 10.0, 10.0), 0.99) is False
        assert is_blank_glyph_char("A", (10.0, 10.0, 10.0, 10.0), 0.0) is False
        assert is_blank_glyph_char("A", (10.0, 10.0, 10.0, 10.0), 1.0) is True

    def test_blank_glyph_rejects_bad_inputs(self) -> None:
        with pytest.raises(ValueError, match="single character"):
            is_blank_glyph_char("AB", (0.0, 0.0, 1.0, 1.0), 12.0)
        with pytest.raises(TypeError, match="char"):
            is_blank_glyph_char(65, (0.0, 0.0, 1.0, 1.0), 12.0)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="finite"):
            is_blank_glyph_char("A", (0.0, 0.0, 1.0, 1.0), float("nan"))
