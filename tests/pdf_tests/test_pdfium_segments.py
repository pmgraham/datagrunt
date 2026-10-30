"""Tests for the v5 pypdfium2 engine's axis-aligned segments (#365 T3).

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``), whose
coordinates are top-left points in the unrotated page space, or written by
hand (``hand_written_pdf``) for paths ReportLab cannot draw: curves, dashes,
``cm`` transforms and nested forms with a /Matrix. Hand-written content is in
PDF user space (bottom-left origin) on a 400 x 300 pt page, so a display y is
``300 - y``.
"""

import ast
import dataclasses
import pickle
import re
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import PDFCorruptError, _path_segments
from datagrunt.pdf._path_segments import IDENTITY, Matrix, NonFiniteGeometry, PathPaint, PathPoint, path_segments
from datagrunt.pdf.pdfium_engine import (
    PageSegments,
    RawSegment,
    SegmentStats,
    open_document,
    page_segments,
    page_text,
)
from pdf_tests.builders import FormXObject, Line, PageSpec, Rect, break_page, build_pdf, hand_written_pdf

BLACK = (0.0, 0.0, 0.0)
HELVETICA = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
# pdfium keeps the CTM in 32-bit floats: five of these overflow it to (inf, nan, nan, inf, 0, 0).
OVERFLOW = b"1000000000 0 0 1000000000 0 0 cm " * 5

# RawSegment fields for a valid horizontal stroke.
VALID = {
    "orientation": "h",
    "position": 20.0,
    "start": 10.0,
    "end": 110.0,
    "width": 0.5,
    "kind": "stroke",
    "object_index": 0,
    "dashed": False,
    "paint_rgba": (0, 0, 0, 255),
}


def segment(**overrides) -> RawSegment:
    """A valid RawSegment with ``overrides`` applied."""
    return RawSegment(**{**VALID, **overrides})


def one_page(tmp_path: Path, *items, rotate: int = 0, crop_box=None) -> Path:
    """A one-page 400 x 300 pt PDF holding ``items``."""
    spec = PageSpec(width=400.0, height=300.0, items=items, rotate=rotate, crop_box=crop_box)
    return build_pdf(tmp_path / "page.pdf", [spec])


def content_page(tmp_path: Path, content: bytes, *, extra: tuple[bytes, ...] = (), resources: bytes = b"") -> Path:
    """A hand-written 400 x 300 pt page drawing ``content``; ``extra`` objects are numbered from 5."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R /Resources << %s >> >>" % resources,
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        *extra,
    ]
    return hand_written_pdf(tmp_path / "hand.pdf", objects)


def segments_of(path: Path) -> tuple[RawSegment, ...]:
    """The segments of page 1 of ``path``."""
    return page_segments(open_document(path), 0).segments


def rows(segments: tuple[RawSegment, ...]) -> list[tuple]:
    """Each segment as a tuple, coordinates rounded to 3 decimals (pdfium points are 32-bit floats)."""
    return [
        (
            item.orientation,
            round(item.position, 3),
            round(item.start, 3),
            round(item.end, 3),
            round(item.width, 3),
            item.kind,
            item.object_index,
        )
        for item in segments
    ]


class TestModuleImports:
    """The segment geometry is pure: no pdfium, and no v4 PDF stack."""

    def test_imports_only_the_standard_library_and_validators(self):
        tree = ast.parse(Path(_path_segments.__file__).read_text(encoding="utf-8"))
        imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        imported |= {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}

        assert imported == {"math", "collections.abc", "dataclasses", "typing", "datagrunt.pdf._validate"}


class TestRawSegment:
    """RawSegment is a frozen, slotted, picklable, validated record."""

    @pytest.mark.parametrize("kind", ["stroke", "thin_fill", "fill_edge"])
    def test_pickle_round_trip(self, kind):
        original = segment(orientation="v", kind=kind, object_index=7)

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        item = segment()

        with pytest.raises(dataclasses.FrozenInstanceError):
            item.position = 1.0  # type: ignore[misc]
        assert not hasattr(item, "__dict__")

    def test_int_coordinates_and_width_are_stored_as_floats(self):
        item = segment(position=20, start=10, end=110, width=0)

        assert (item.position, item.start, item.end, item.width) == (20.0, 10.0, 110.0, 0.0)
        assert all(type(value) is float for value in (item.position, item.start, item.end, item.width))

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"orientation": "x"}, ValueError, "orientation must be 'h' or 'v', got 'x'", id="orientation"),
            pytest.param(
                {"orientation": None}, TypeError, "orientation must be a str, got None", id="orientation-type"
            ),
            pytest.param({"position": float("nan")}, ValueError, "position must be finite", id="position-nan"),
            pytest.param({"start": float("inf")}, ValueError, "start must be finite", id="start-inf"),
            pytest.param({"end": "1"}, TypeError, "end must be an int or float", id="end-type"),
            pytest.param({"start": 110.0}, ValueError, "start must be < end, got start 110.0, end 110.0", id="empty"),
            pytest.param(
                {"start": 120.0}, ValueError, "start must be < end, got start 120.0, end 110.0", id="reversed"
            ),
            pytest.param({"width": -0.5}, ValueError, "width must be >= 0.0, got -0.5", id="width-negative"),
            pytest.param({"width": True}, TypeError, "width must be an int or float, got True", id="width-bool"),
            pytest.param(
                {"kind": "fill"},
                ValueError,
                "kind must be 'stroke' or 'thin_fill' or 'fill_edge', got 'fill'",
                id="kind",
            ),
            pytest.param({"kind": 1}, TypeError, "kind must be a str, got 1", id="kind-type"),
            pytest.param({"object_index": -1}, ValueError, "object_index must be >= 0, got -1", id="index-negative"),
            pytest.param({"object_index": True}, TypeError, "object_index must be an int, got True", id="index-bool"),
            pytest.param({"dashed": 1}, TypeError, "dashed must be a bool, got 1", id="dashed-type"),
            pytest.param(
                {"kind": "thin_fill", "dashed": True},
                ValueError,
                "dashed must be False for a thin_fill: only a stroke has a dash pattern",
                id="dashed-fill",
            ),
            pytest.param(
                {"paint_rgba": (0, 0, 0)}, ValueError, "paint_rgba must have 4 channels, got 3", id="rgba-size"
            ),
            pytest.param(
                {"paint_rgba": (0, 0, 256, 0)}, ValueError, "paint_rgba[2] must be in 0..255", id="rgba-range"
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            segment(**overrides)


class TestStrokedLines:
    """Stroked straight path edges at least 12 pt long and axis-aligned within 0.5 pt."""

    def test_ruled_grid_gives_one_segment_per_line(self, tmp_path):
        ys, xs = (50.0, 80.0, 110.0, 140.0), (40.0, 140.0, 240.0, 340.0)
        lines = [Line(40.0, y, 340.0, y) for y in ys] + [Line(x, 50.0, x, 140.0) for x in xs]

        got = segments_of(one_page(tmp_path, *lines))

        horizontal = [("h", y, 40.0, 340.0, 0.5, "stroke", index) for index, y in enumerate(ys)]
        vertical = [("v", x, 50.0, 140.0, 0.5, "stroke", 4 + index) for index, x in enumerate(xs)]
        assert rows(got) == horizontal + vertical

    def test_lines_shorter_than_12_pt_are_ignored(self, tmp_path):
        lines = (Line(10.0, 20.0, 22.0, 20.0), Line(10.0, 40.0, 21.75, 40.0), Line(50.0, 60.0, 50.0, 72.0))
        lines += (Line(70.0, 60.0, 70.0, 71.75),)

        got = segments_of(one_page(tmp_path, *lines))

        assert rows(got) == [("h", 20.0, 10.0, 22.0, 0.5, "stroke", 0), ("v", 50.0, 60.0, 72.0, 0.5, "stroke", 2)]

    def test_axis_alignment_tolerates_under_half_a_point(self, tmp_path):
        # PDF y 100 -> 100.25 drifts 0.25 pt: kept at the mean; 0.5 pt is a diagonal.
        content = b"10 100 m 110 100.25 l S 10 50 m 110 50.5 l S 200 10 m 200.25 110 l S 250 10 m 250.5 110 l S"

        got = segments_of(content_page(tmp_path, content))

        assert rows(got) == [
            ("h", 199.875, 10.0, 110.0, 1.0, "stroke", 0),
            ("v", 200.125, 190.0, 290.0, 1.0, "stroke", 2),
        ]

    def test_diagonals_and_curves_are_ignored(self, tmp_path):
        # A curve is ignored even when it is drawn straight; the edge after it
        # starts at the curve's end point.
        content = b"10 10 m 100 100 l S 10 200 m 50 200 90 200 130 200 c S"
        content += b" 10 250 m 100 250 l 150 290 200 290 250 250 c 250 150 l S"

        got = segments_of(content_page(tmp_path, content))

        assert rows(got) == [("h", 50.0, 10.0, 100.0, 1.0, "stroke", 2), ("v", 250.0, 50.0, 150.0, 1.0, "stroke", 2)]

    def test_unstroked_paths_give_no_stroke_segments(self, tmp_path):
        # A fill-only straight path, a clip-only path and an unpainted one.
        content = b"10 150 m 200 150 l 200 160 l f 10 200 m 300 200 l W n 10 250 m 300 250 l n"
        filled = Rect(50.0, 50.0, 200.0, 100.0, fill_rgb=BLACK, stroke=False)

        assert segments_of(content_page(tmp_path, content)) == ()
        # A large filled rectangle gives only its fill's edges (TestFillEdges).
        assert {item.kind for item in segments_of(one_page(tmp_path, filled))} == {"fill_edge"}

    def test_a_stroked_rectangle_gives_its_four_sides(self, tmp_path):
        got = segments_of(one_page(tmp_path, Rect(50.0, 100.0, 100.0, 40.0)))

        # pdfium reads ``re`` as its bottom-left (PDF) corner, then anticlockwise.
        assert rows(got) == [
            ("h", 140.0, 50.0, 150.0, 0.5, "stroke", 0),
            ("v", 150.0, 100.0, 140.0, 0.5, "stroke", 0),
            ("h", 100.0, 50.0, 150.0, 0.5, "stroke", 0),
            ("v", 50.0, 100.0, 140.0, 0.5, "stroke", 0),
        ]

    @pytest.mark.parametrize("operators", [b"s", b"h S", b"b", b"h B"])
    def test_closing_a_subpath_strokes_its_closing_side(self, tmp_path, operators):
        got = segments_of(content_page(tmp_path, b"10 10 m 100 10 l 100 50 l 10 50 l " + operators))
        strokes = [row for row in rows(got) if row[5] == "stroke"]

        assert strokes[-1] == ("v", 10.0, 250.0, 290.0, 1.0, "stroke", 0)
        assert len(strokes) == 4

    def test_each_subpath_of_one_path_gives_its_segments(self, tmp_path):
        got = segments_of(content_page(tmp_path, b"10 10 m 100 10 l 10 30 m 100 30 l S"))

        assert rows(got) == [("h", 290.0, 10.0, 100.0, 1.0, "stroke", 0), ("h", 270.0, 10.0, 100.0, 1.0, "stroke", 0)]

    def test_a_dashed_line_is_one_segment(self, tmp_path):
        # As v4's pdfplumber reads it: a dashed rule still rules a table.
        got = segments_of(content_page(tmp_path, b"[3 2] 0 d 10 10 m 100 10 l S"))

        assert rows(got) == [("h", 290.0, 10.0, 100.0, 1.0, "stroke", 0)]

    def test_zero_width_stroke_is_a_hairline(self, tmp_path):
        got = segments_of(content_page(tmp_path, b"0 w 10 10 m 100 10 l S"))

        assert rows(got) == [("h", 290.0, 10.0, 100.0, 0.0, "stroke", 0)]


class TestThinFills:
    """Filled rectangles at most 3 pt thick and at least 12 pt long are one segment along their length."""

    def test_thin_filled_rects_give_one_segment_along_their_long_side(self, tmp_path):
        bars = (
            Rect(50.0, 60.0, 100.0, 2.0, fill_rgb=BLACK, stroke=False),
            Rect(200.0, 50.0, 1.5, 80.0, fill_rgb=BLACK, stroke=False),
        )

        got = segments_of(one_page(tmp_path, *bars))

        assert rows(got) == [
            ("h", 61.0, 50.0, 150.0, 2.0, "thin_fill", 0),
            ("v", 200.75, 50.0, 130.0, 1.5, "thin_fill", 1),
        ]

    def test_thin_means_at_most_3_pt_thick_and_at_least_12_pt_long(self, tmp_path):
        content = b"10 10 100 3 re f 10 30 100 3.25 re f 10 50 12 2 re f 10 70 11.75 2 re f 200 10 3 100 re f"

        got = segments_of(content_page(tmp_path, content))

        # The 3.25 pt rectangle gives fill edges instead (TestFillEdges).
        assert [row for row in rows(got) if row[5] == "thin_fill"] == [
            ("h", 288.5, 10.0, 110.0, 3.0, "thin_fill", 0),
            ("h", 249.0, 10.0, 22.0, 2.0, "thin_fill", 2),
            ("v", 201.5, 190.0, 290.0, 3.0, "thin_fill", 4),
        ]

    def test_each_thin_rect_of_one_filled_path_is_a_segment(self, tmp_path):
        got = segments_of(content_page(tmp_path, b"10 10 50 2 re 100 100 2 30 re f"))

        assert rows(got) == [
            ("h", 289.0, 10.0, 60.0, 2.0, "thin_fill", 0),
            ("v", 101.0, 170.0, 200.0, 2.0, "thin_fill", 0),
        ]

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(b"10 10 m 110 10 l 110 13 l 60 13 l 60 11 l 10 11 l f", id="l-shape"),
            pytest.param(b"10 50 m 110 50 l 112 52 l 12 52 l f", id="parallelogram"),
            pytest.param(b"10 100 m 110 100 l 110 102 l 80 102 40 102 10 102 c f", id="curved-side"),
            pytest.param(b"10 150 m 110 150 l 10 150 l 10 152 l f", id="no-area"),
        ],
    )
    def test_a_thin_fill_that_is_not_a_rectangle_gives_no_segment(self, tmp_path, content):
        assert segments_of(content_page(tmp_path, content)) == ()

    def test_a_zero_area_fill_is_a_zero_width_segment(self, tmp_path):
        path = content_page(tmp_path, b"0 0 0 rg 50 150 300 0 re f")

        # Observed: pdfium paints a zero-area fill as a hairline, so it rules a line.
        bitmap = pdfium.PdfDocument(path)[0].render(scale=1).to_pil().convert("L")
        assert [bitmap.getpixel((200, y)) < 255 for y in (148, 149, 150, 151)] == [False, False, True, False]
        assert rows(segments_of(path)) == [("h", 150.0, 50.0, 350.0, 0.0, "thin_fill", 0)]

    def test_a_filled_and_stroked_thin_rect_gives_its_fill_then_its_long_sides(self, tmp_path):
        # PDF paints a path's fill before its stroke; the 2 pt sides are too short.
        got = segments_of(content_page(tmp_path, b"50 150 100 2 re B"))

        assert rows(got) == [
            ("h", 149.0, 50.0, 150.0, 2.0, "thin_fill", 0),
            ("h", 150.0, 50.0, 150.0, 1.0, "stroke", 0),
            ("h", 148.0, 50.0, 150.0, 1.0, "stroke", 0),
        ]


class TestFillEdges:
    """Each side (>= 12 pt) of a filled rectangle thicker than 3 pt is a zero-width fill_edge."""

    def test_a_table_of_shaded_cells_gives_every_cell_edge(self, tmp_path):
        # A 3 x 3 table drawn only as shaded cells, 1 pt apart (as v4's pdfplumber still finds).
        lefts, bottoms = (50, 150, 250), (100, 120, 140)
        cells = b" ".join(b"%d %d 99 19 re f" % (x, y) for y in bottoms for x in lefts)

        got = segments_of(content_page(tmp_path, b"0.9 g " + cells))

        expected = []
        for index, (y, x) in enumerate((y, x) for y in bottoms for x in lefts):
            top, bottom = 300.0 - (y + 19), 300.0 - y
            expected += [
                ("h", bottom, float(x), x + 99.0, 0.0, "fill_edge", index),
                ("v", x + 99.0, top, bottom, 0.0, "fill_edge", index),
                ("h", top, float(x), x + 99.0, 0.0, "fill_edge", index),
                ("v", float(x), top, bottom, 0.0, "fill_edge", index),
            ]
        assert rows(got) == expected

    def test_thin_fills_stay_thin_fill(self, tmp_path):
        got = segments_of(content_page(tmp_path, b"10 10 100 3 re f 10 50 100 3.25 re f"))

        # 3 pt thick is a thin fill; 3.25 pt is a rectangle with two long edges.
        assert rows(got) == [
            ("h", 288.5, 10.0, 110.0, 3.0, "thin_fill", 0),
            ("h", 250.0, 10.0, 110.0, 0.0, "fill_edge", 1),
            ("h", 246.75, 10.0, 110.0, 0.0, "fill_edge", 1),
        ]

    def test_a_filled_and_stroked_rectangle_gives_its_fill_edges_then_its_strokes(self, tmp_path):
        got = segments_of(content_page(tmp_path, b"10 10 100 50 re B"))

        assert [item.kind for item in got] == ["fill_edge"] * 4 + ["stroke"] * 4
        assert rows(got)[:4] == [(row[0], row[1], row[2], row[3], 0.0, "fill_edge", 0) for row in rows(got)[4:]]


def paints(segments: tuple[RawSegment, ...]) -> list[tuple]:
    """Each segment's kind, dash flag and paint colour."""
    return [(item.kind, item.dashed, item.paint_rgba) for item in segments]


class TestPaint:
    """Each segment carries its dash flag and the colour it is painted in, for #367/#368 to judge."""

    @pytest.mark.parametrize(
        "pattern,dashed",
        [
            pytest.param(b"[]", False, id="solid"),
            pytest.param(b"[3 2]", True, id="dashes"),
            pytest.param(b"[3]", True, id="odd-length"),
            pytest.param(b"[0 4]", True, id="dots"),
            pytest.param(b"[4 0]", False, id="no-gaps"),
            pytest.param(b"[0 0]", False, id="all-zero"),
        ],
    )
    def test_dashed_means_the_dash_pattern_leaves_gaps(self, tmp_path, pattern, dashed):
        # Observed: pdfium strokes [4 0] and [0 0] solid; [0 4] draws only round-cap dots.
        got = segments_of(content_page(tmp_path, pattern + b" 0 d 10 10 m 100 10 l S"))

        assert paints(got) == [("stroke", dashed, (0, 0, 0, 255))]

    def test_strokes_carry_the_stroke_colour_and_fills_the_fill_colour(self, tmp_path):
        alpha = b"<< /Type /ExtGState /CA 0.5 /ca 0.25 >>"
        content = b"1 0 0 RG 0 0 1 rg [3 2] 0 d 10 10 100 2 re 10 50 100 40 re B /G gs 0.9 g 10 200 100 20 re f"
        path = content_page(tmp_path, content, resources=b"/ExtGState << /G 5 0 R >>", extra=(alpha,))

        got = paints(segments_of(path))

        blue, red, grey = (0, 0, 255, 255), (255, 0, 0, 255), (230, 230, 230, 64)
        expected = [("thin_fill", False, blue)] + [("fill_edge", False, blue)] * 4 + [("stroke", True, red)] * 6
        assert got == expected + [("fill_edge", False, grey)] * 4


def form_xobject(content: bytes, *, matrix: bytes = b"[1 0 0 1 0 0]", resources: bytes = b"") -> bytes:
    """A 400 x 300 Form XObject object drawing ``content``."""
    header = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Matrix %s /Resources << %s >> /Length %d >>"
    return header % (matrix, resources, len(content)) + b"\nstream\n%s\nendstream" % content


def nested_line_forms_page(tmp_path: Path, depth: int) -> Path:
    """A page drawing form 1, which strokes its line and draws form 2, and so on ``depth`` deep.

    Form k's 20 pt line starts at its own place (4 columns of 20 rows).
    """
    forms = []
    for level in range(depth):
        x, y = 10 + 90 * (level % 4), 10 + 7 * (level // 4)
        body = b"%d %d m %d %d l S" % (x, y, x + 20, y)
        resources = b""
        if level < depth - 1:
            body += b" /X Do"
            resources = b"/XObject << /X %d 0 R >>" % (6 + level)
        forms.append(form_xobject(body, resources=resources))
    return content_page(tmp_path, b"/X Do", resources=b"/XObject << /X 5 0 R >>", extra=tuple(forms))


def renders_dark(path: Path, x: int, ys: tuple[int, ...]) -> list[bool]:
    """Whether pdfium paints anything at each pixel (x, y) of page 1, rendered at 72 dpi."""
    bitmap = pdfium.PdfDocument(path)[0].render(scale=1).to_pil().convert("L")
    return [bitmap.getpixel((x, y)) < 255 for y in ys]


class TestTransforms:
    """Every enclosing transform (object matrix, cm, form /Matrix) maps paths to page space."""

    def test_paths_in_a_scaled_form_land_at_page_positions_with_scaled_widths(self, tmp_path):
        lines = (Line(10.0, 20.0, 110.0, 20.0), Line(10.0, 5.0, 10.0, 55.0))
        bar = Rect(20.0, 30.0, 50.0, 1.0, fill_rgb=BLACK, stroke=False)
        form = FormXObject(x=100.0, y=50.0, w=150.0, h=60.0, items=(*lines, bar), scale=2.0)

        got = segments_of(one_page(tmp_path, form, Line(20.0, 250.0, 120.0, 250.0)))

        # Form (u, v) at scale 2 from the form's top-left (100, 50) is page (100 + 2u, 50 + 2v).
        assert rows(got) == [
            ("h", 90.0, 120.0, 320.0, 1.0, "stroke", 1),
            ("v", 120.0, 60.0, 160.0, 1.0, "stroke", 2),
            ("h", 111.0, 140.0, 240.0, 2.0, "thin_fill", 3),
            ("h", 250.0, 20.0, 120.0, 0.5, "stroke", 4),
        ]

    def test_nested_forms_compose_every_matrix(self, tmp_path):
        # Observed pdfium behaviour: a path's matrix is its CTM inside its form
        # (that form's /Matrix included), and a form object's matrix is the CTM
        # at its Do; neither includes an enclosing form's.
        inner = form_xobject(b"2 0 0 2 0 0 cm 0 0 m 30 0 l 0 0 m 0 20 l S 0 30 20 0.25 re f", matrix=b"[1 0 0 1 5 7]")
        outer = form_xobject(b"1 0 0 1 10 0 cm /Y Do", matrix=b"[3 0 0 3 0 0]", resources=b"/XObject << /Y 6 0 R >>")
        path = content_page(
            tmp_path, b"1 0 0 1 100 50 cm /X Do", resources=b"/XObject << /X 5 0 R >>", extra=(outer, inner)
        )

        got = segments_of(path)

        # Page point = translate(100, 50) . scale 3 . translate(10, 0) . translate(5, 7) . scale 2 of the path point:
        # (0, 0) -> (145, 71), (30, 0) -> (325, 71), (0, 20) -> (145, 191); widths scale 6x.
        assert rows(got) == [
            ("h", 229.0, 145.0, 325.0, 6.0, "stroke", 2),
            ("v", 145.0, 109.0, 229.0, 6.0, "stroke", 2),
            ("h", 48.25, 145.0, 265.0, 1.5, "thin_fill", 3),
        ]

    def test_stroke_width_scales_across_each_line(self, tmp_path):
        # x scales 2 and y 3: a horizontal line's stroke spans y, a vertical one's x.
        got = segments_of(content_page(tmp_path, b"2 0 0 3 10 10 cm 4 w 0 0 m 50 0 l 0 0 m 0 50 l S"))

        assert rows(got) == [("h", 290.0, 10.0, 110.0, 12.0, "stroke", 0), ("v", 10.0, 140.0, 290.0, 8.0, "stroke", 0)]

    def test_a_quarter_turn_matrix_draws_a_horizontal_line_vertically(self, tmp_path):
        got = segments_of(content_page(tmp_path, b"0 1 -1 0 200 50 cm 2 w 0 0 m 100 0 l S"))

        assert rows(got) == [("v", 200.0, 150.0, 250.0, 2.0, "stroke", 0)]

    def test_a_skewing_matrix_makes_axis_lines_diagonal(self, tmp_path):
        content = b"q 1 0 0.5 1 0 0 cm 10 10 m 10 100 l S Q 1 0.5 0 1 0 0 cm 10 10 m 100 10 l S"

        assert segments_of(content_page(tmp_path, content)) == ()

    def test_a_negative_line_width_is_a_hairline(self, tmp_path):
        path = content_page(tmp_path, b"-6 w 50 150 m 350 150 l S")

        # Observed: pdfium keeps the negative width but strokes it as it does 0 w.
        assert renders_dark(path, 200, (146, 147, 153, 154)) == [False, False, False, False]
        assert rows(segments_of(path)) == [("h", 150.0, 50.0, 350.0, 0.0, "stroke", 0)]

    @pytest.mark.parametrize("number", [b"1e5", b"1" + b"0" * 38])
    def test_a_number_pdfium_cannot_read_is_zero_so_the_path_collapses(self, tmp_path, number):
        path = content_page(tmp_path, number + b" 0 0 1 0 0 cm 2 0 m 50 0 l S")
        page = pdfium.PdfDocument(path)[0]
        (line,) = page.get_objects()

        assert line.get_matrix().get() == (0.0, 0.0, 0.0, 1.0, 0.0, 0.0)
        assert segments_of(path) == ()

    def test_every_form_level_pdfium_parses_is_walked(self, tmp_path):
        # Observed: pdfium parses forms 40 levels deep and ignores deeper ones.
        got = segments_of(nested_line_forms_page(tmp_path, 80))

        expected = []
        for level in range(40):
            x, y = 10 + 90 * (level % 4), 10 + 7 * (level // 4)
            expected.append(("h", 300.0 - y, float(x), x + 20.0, 1.0, "stroke", 2 * level + 1))
        assert rows(got) == expected


ROTATIONS = [0, 90, 180, 270]
# Top-left (50, 20)-(350, 260) on the 400 x 300 page: PDF CropBox (50, 40)-(350, 280).
CROPS = [pytest.param(None, id="no-crop"), pytest.param((50.0, 20.0, 350.0, 260.0), id="offset-crop")]


def displayed_row(start, end, width, kind, index, rotation, crop_box) -> tuple:
    """The rows() tuple of the line from top-left page point ``start`` to ``end`` on the displayed page."""
    x0, y0, x1, y1 = crop_box or (0.0, 0.0, 400.0, 300.0)
    across, up = x1 - x0, y1 - y0

    def turned(u, v):
        u, v = u - x0, v - y0
        return {0: (u, v), 90: (up - v, u), 180: (across - u, up - v), 270: (v, across - u)}[rotation]

    (a, b), (c, d) = turned(*start), turned(*end)
    if b == d:
        return ("h", b, min(a, c), max(a, c), width, kind, index)
    return ("v", a, min(b, d), max(b, d), width, kind, index)


class TestDisplaySpace:
    """Segments are in displayed, top-left space: /Rotate and the CropBox apply."""

    @pytest.mark.parametrize("crop_box", CROPS)
    @pytest.mark.parametrize("rotate", ROTATIONS)
    def test_rotation_and_crop_box(self, tmp_path, rotate, crop_box):
        items = (Line(60.0, 100.0, 160.0, 100.0), Line(200.0, 40.0, 200.0, 140.0))
        items += (Rect(250.0, 200.0, 80.0, 2.0, fill_rgb=BLACK, stroke=False),)

        got = segments_of(one_page(tmp_path, *items, rotate=rotate, crop_box=crop_box))

        # A horizontal PDF line is vertical on a 90 or 270 page.
        assert rows(got) == [
            displayed_row((60.0, 100.0), (160.0, 100.0), 0.5, "stroke", 0, rotate, crop_box),
            displayed_row((200.0, 40.0), (200.0, 140.0), 0.5, "stroke", 1, rotate, crop_box),
            displayed_row((250.0, 201.0), (330.0, 201.0), 2.0, "thin_fill", 2, rotate, crop_box),
        ]


def user_space(box):
    """A ``to_display`` that keeps user-space coordinates (y grows upward)."""
    return box


STROKE = PathPaint(
    stroked=True, filled=False, stroke_width=1.0, dashed=False, stroke_rgba=(0, 0, 0, 255), fill_rgba=(0, 0, 0, 255)
)


class TestPathGeometry:
    """path_segments over points pdfium never produces from a parsed page."""

    def test_a_close_flag_away_from_the_start_strokes_back_to_it(self):
        # pdfium's parser always appends the closing point itself; its edit API
        # (FPDFPath_Close) only flags the last point.
        points = [PathPoint("move", 0.0, 0.0), PathPoint("line", 100.0, 0.0), PathPoint("line", 100.0, 50.0)]
        points.append(PathPoint("line", 0.0, 50.0, closes=True))

        got = path_segments(points, matrix=IDENTITY, paint=STROKE, object_index=3, to_display=user_space)

        assert rows(tuple(got))[-1] == ("v", 0.0, 0.0, 50.0, 1.0, "stroke", 3)
        assert len(got) == 4

    def test_a_line_after_a_close_starts_at_the_subpath_start(self):
        points = [
            PathPoint("move", 0.0, 0.0),
            PathPoint("line", 0.0, 50.0),
            PathPoint("line", 100.0, 50.0, closes=True),
        ]
        points.append(PathPoint("line", 100.0, 0.0))

        got = path_segments(points, matrix=IDENTITY, paint=STROKE, object_index=0, to_display=user_space)

        assert rows(tuple(got)) == [
            ("v", 0.0, 0.0, 50.0, 1.0, "stroke", 0),
            ("h", 50.0, 0.0, 100.0, 1.0, "stroke", 0),
            ("h", 0.0, 0.0, 100.0, 1.0, "stroke", 0),
        ]

    def test_a_point_mapped_beyond_float_range_raises(self):
        points = [PathPoint("move", 1.0, 0.0), PathPoint("line", 100.0, 0.0)]
        huge = Matrix(1e308, 0.0, 0.0, 1.0, 0.0, 0.0)

        with pytest.raises(NonFiniteGeometry, match=re.escape("its page-space point (inf, 0.0) is not finite")):
            path_segments(points, matrix=huge, paint=STROKE, object_index=0, to_display=user_space)

    def test_a_non_finite_matrix_or_stroke_width_raises(self):
        points = [PathPoint("move", 1.0, 0.0), PathPoint("line", 100.0, 0.0)]
        message = "its matrix to page space is not finite (inf, 0.0, 0.0, 1.0, 0.0, 0.0)"

        with pytest.raises(NonFiniteGeometry, match=re.escape(message)):
            path_segments(points, matrix=IDENTITY._replace(a=INF), paint=STROKE, object_index=0, to_display=user_space)
        with pytest.raises(NonFiniteGeometry, match=re.escape("its stroke width is not finite (nan)")):
            nan_width = STROKE._replace(stroke_width=float("nan"))
            path_segments(points, matrix=IDENTITY, paint=nan_width, object_index=0, to_display=user_space)

    def test_a_stroke_width_scaled_beyond_float_range_raises(self):
        # Each point stays finite; the matrix's determinant (1e400) does not.
        points = [PathPoint("move", 0.0, 0.0), PathPoint("line", 1.0, 0.0)]
        huge = Matrix(1e200, 0.0, 0.0, 1e200, 0.0, 0.0)

        with pytest.raises(NonFiniteGeometry, match="its stroke is not finitely wide across an edge"):
            path_segments(points, matrix=huge, paint=STROKE, object_index=0, to_display=user_space)


@pytest.fixture
def page_closes(monkeypatch):
    """Record each explicit PdfPage close() call (not a parent's cascade)."""
    calls = []

    def counting_close(self, *args, _original=pdfium.PdfPage.close, **kwargs):
        if not kwargs.get("_by_parent", False):
            calls.append("PdfPage")
        return _original(self, *args, **kwargs)

    monkeypatch.setattr(pdfium.PdfPage, "close", counting_close)
    return calls


def failing(value):
    """A pdfium stand-in that reports failure by returning ``value``."""
    return lambda *args: value


def writes_fill_mode(mode):
    """FPDFPath_GetDrawMode succeeding with fill mode ``mode``."""

    def stand_in(path, fill_mode, stroke):
        fill_mode.value, stroke.value = mode, 1
        return 1

    return stand_in


def writes_width(width):
    def stand_in(path, out):
        out.value = width
        return 1

    return stand_in


def writes_matrix(*values):
    def stand_in(obj, matrix):
        matrix.a, matrix.b, matrix.c, matrix.d, matrix.e, matrix.f = values
        return 1

    return stand_in


def writes_nan_point(segment, x, y):
    x.value, y.value = float("nan"), 10.0
    return 1


INF = float("inf")

# (pdfium function, stand-in, message): each makes page_segments raise PDFCorruptError for page 1.
PDFIUM_FAILURES = [
    ("FPDFPageObj_GetDashCount", failing(-1), "page 1: pdfium cannot read the dash pattern of object 0"),
    ("FPDFPageObj_GetDashArray", failing(0), "page 1: pdfium cannot read the dash pattern of object 0"),
    ("FPDFPageObj_GetStrokeColor", failing(0), "page 1: pdfium cannot read the stroke colour of object 0"),
    ("FPDFPageObj_GetFillColor", failing(0), "page 1: pdfium cannot read the fill colour of object 0"),
    ("FPDFPath_GetDrawMode", failing(0), "page 1: pdfium cannot read the draw mode of object 0"),
    ("FPDFPath_GetDrawMode", writes_fill_mode(3), "page 1: pdfium cannot read the draw mode of object 0"),
    ("FPDFPageObj_GetStrokeWidth", failing(0), "page 1: pdfium cannot read the stroke width of object 0"),
    ("FPDFPageObj_GetMatrix", failing(0), "page 1: pdfium cannot read the matrix of object 0"),
    ("FPDFPath_CountSegments", failing(-1), "page 1: pdfium cannot count the path segments of object 0"),
    ("FPDFPath_GetPathSegment", failing(None), "page 1: pdfium cannot read path segment 0 of object 0"),
    ("FPDFPathSegment_GetType", failing(-1), "page 1: pdfium cannot read path segment 0 of object 0"),
    ("FPDFPathSegment_GetPoint", failing(0), "page 1: pdfium cannot read path segment 0 of object 0"),
    ("FPDFPage_CountObjects", failing(-1), "page 1: pdfium cannot count the objects of the page"),
    ("FPDFPage_GetObject", failing(None), "page 1: pdfium cannot read object 0 of the page"),
]

FORM_FAILURES = [
    ("FPDFPageObj_GetMatrix", failing(0), "page 1: pdfium cannot read the matrix of object 0"),
    ("FPDFFormObj_CountObjects", failing(-1), "page 1: pdfium cannot count the objects of form object 0"),
    ("FPDFFormObj_GetObject", failing(None), "page 1: pdfium cannot read object 0 of form object 0"),
]


# (pdfium function, stand-in): each gives the one path non-finite geometry, so it is skipped.
NON_FINITE = [
    pytest.param("FPDFPageObj_GetStrokeWidth", writes_width(float("nan")), id="width"),
    pytest.param("FPDFPathSegment_GetPoint", writes_nan_point, id="point"),
    pytest.param("FPDFPageObj_GetMatrix", writes_matrix(INF, 0.0, 0.0, 1.0, 0.0, 0.0), id="matrix"),
]


def stats(skipped: int) -> SegmentStats:
    return SegmentStats(skipped_nonfinite_paths=skipped)


class TestPageSegmentsAndStats:
    """PageSegments and SegmentStats are frozen, slotted, picklable, validated records."""

    def test_pickle_round_trip(self):
        original = PageSegments(segments=(segment(),), stats=stats(2))

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.stats = stats(0)  # type: ignore[misc]
        assert not hasattr(original, "__dict__") and not hasattr(original.stats, "__dict__")

    @pytest.mark.parametrize(
        "make,error,message",
        [
            pytest.param(lambda: stats(-1), ValueError, "skipped_nonfinite_paths must be >= 0, got -1", id="negative"),
            pytest.param(lambda: stats(True), TypeError, "skipped_nonfinite_paths must be an int, got True", id="bool"),
            pytest.param(
                lambda: PageSegments(segments=[segment()], stats=stats(0)),
                TypeError,
                "segments must be a tuple, got list",
                id="segments-list",
            ),
            pytest.param(
                lambda: PageSegments(segments=(segment(), "x"), stats=stats(0)),
                TypeError,
                "segments[1] must be a RawSegment, got str",
                id="segments-item",
            ),
            pytest.param(
                lambda: PageSegments(segments=(), stats=0),
                TypeError,
                "stats must be a SegmentStats, got int",
                id="stats-type",
            ),
        ],
    )
    def test_invalid_field_raises(self, make, error, message):
        with pytest.raises(error, match=re.escape(message)):
            make()


class TestNonFiniteGeometry:
    """A path whose page-space geometry is not finite is skipped and counted; the page survives."""

    def test_a_path_drawn_under_an_overflowing_cm_is_skipped(self, tmp_path):
        content = b"BT /F1 12 Tf 20 270 Td (Visible) Tj ET 10 250 m 200 250 l S q " + OVERFLOW + b"0 0 m 1 0 l S Q"
        path = content_page(tmp_path, content, resources=b"/Font << /F1 5 0 R >>", extra=(HELVETICA,))

        # Observed: pdfium reads the overflowed matrix and still renders the page.
        page = pdfium.PdfDocument(path)[0]
        assert [repr(value) for value in list(page.get_objects())[2].get_matrix().get()] == [
            "inf",
            "nan",
            "nan",
            "inf",
            "0.0",
            "0.0",
        ]
        assert renders_dark(path, 100, (50,)) == [True]
        got = page_segments(open_document(path), 0)

        assert rows(got.segments) == [("h", 50.0, 10.0, 200.0, 1.0, "stroke", 1)]
        assert got.stats == stats(1)
        assert [item.text for item in page_text(open_document(path), 0).spans] == ["Visible"]

    def test_every_path_in_a_form_drawn_under_an_overflowing_cm_is_skipped(self, tmp_path):
        form = form_xobject(b"10 10 m 100 10 l S 10 50 4 100 re f")
        content = b"q " + OVERFLOW + b"/X Do Q 10 250 m 200 250 l S"
        path = content_page(tmp_path, content, resources=b"/XObject << /X 5 0 R >>", extra=(form,))

        got = page_segments(open_document(path), 0)

        assert rows(got.segments) == [("h", 50.0, 10.0, 200.0, 1.0, "stroke", 3)]
        assert got.stats == stats(2)

    @pytest.mark.parametrize("function,stand_in", NON_FINITE)
    def test_non_finite_geometry_from_pdfium_skips_the_path(self, tmp_path, monkeypatch, function, stand_in):
        handle = open_document(content_page(tmp_path, b"10 10 m 110 10 l S"))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        assert page_segments(handle, 0) == PageSegments(segments=(), stats=stats(1))


class TestPageSegmentsErrors:
    """page_segments raises PDFCorruptError for the page when pdfium fails, and closes the page."""

    def test_closes_the_page(self, tmp_path, page_closes):
        page_segments(open_document(one_page(tmp_path, Line(10.0, 20.0, 110.0, 20.0))), 0)

        assert page_closes == ["PdfPage"]

    @pytest.mark.parametrize("function,stand_in,message", PDFIUM_FAILURES)
    def test_pdfium_failure_raises_for_the_page_and_closes_it(
        self, tmp_path, monkeypatch, page_closes, function, stand_in, message
    ):
        # Dashed, so the dash array is read too.
        handle = open_document(content_page(tmp_path, b"[3 2] 0 d 10 10 m 110 10 l S"))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        with pytest.raises(PDFCorruptError, match=re.escape(message)) as caught:
            page_segments(handle, 0)

        assert caught.value.page_number == 1
        assert page_closes == ["PdfPage"]

    @pytest.mark.parametrize("function,stand_in,message", FORM_FAILURES)
    def test_form_walk_failure_raises_for_the_page(self, tmp_path, monkeypatch, function, stand_in, message):
        form = FormXObject(x=72.0, y=72.0, w=100.0, h=40.0, items=(Line(5.0, 20.0, 95.0, 20.0),))
        handle = open_document(one_page(tmp_path, form))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        with pytest.raises(PDFCorruptError, match=re.escape(message)) as caught:
            page_segments(handle, 0)

        assert caught.value.page_number == 1

    def test_unloadable_page_raises_for_that_page_only(self, tmp_path):
        pages = [PageSpec(items=(Line(10.0, 20.0, 110.0 + n, 20.0),)) for n in (1, 2, 3)]
        handle = open_document(break_page(build_pdf(tmp_path / "three.pdf", pages), 2))

        with pytest.raises(PDFCorruptError, match="Failed to load page") as caught:
            page_segments(handle, 1)

        assert caught.value.page_number == 2
        assert [page_segments(handle, index).segments[0].end for index in (0, 2)] == [111.0, 113.0]

    @pytest.mark.parametrize(
        "index,error,message",
        [
            pytest.param(-1, IndexError, "index must be in 0..0, got -1", id="negative"),
            pytest.param(1, IndexError, "index must be in 0..0, got 1", id="past-end"),
            pytest.param(True, TypeError, "index must be an int, got True (bool)", id="bool"),
        ],
    )
    def test_invalid_index_raises(self, tmp_path, index, error, message):
        handle = open_document(one_page(tmp_path))

        with pytest.raises(error, match=re.escape(message)):
            page_segments(handle, index)

    def test_non_handle_and_closed_handle_raise(self, tmp_path):
        handle = open_document(one_page(tmp_path))
        handle.close()

        with pytest.raises(TypeError, match=re.escape("handle must be a DocumentHandle, got str")):
            page_segments("doc.pdf", 0)
        with pytest.raises(ValueError, match="document handle is closed"):
            page_segments(handle, 0)
