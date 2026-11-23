"""Adversarial security and CAD zero-false-positive regression suite for Issue #395.

Verifies all three hidden-text mechanisms end-to-end (PDF -> pdfium_engine -> quality -> PDFReader -> rationalize):
- ``occluded``: opaque fills, opaque raster images, split whiteout rects, non-rectangular polygons, while exempting
  earlier backgrounds, translucent/Multiply/SMask overlays, hollow border frames, 1 pt strikethrough/underline bars,
  and fills clipped away from the text.
- ``clipped``: out-of-clip text, zero-area clips, nested clip intersections, partial clips, Form XObject clips, and
  ``/Rotate 90`` clips, while keeping enclosing CAD viewport clips visible.
- ``blank_glyph``: Type3 ``0 0 0 0 d1`` and lying-``d1`` zero-paint glyphs with ``ToUnicode``, while keeping visible
  Type3 ``re f`` glyphs, mixed Type3 visible prefixes, and AutoCAD SHX ``3 Tr`` stroke-backed text visible.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any

import pytest
from pydantic import BaseModel

from datagrunt.pdf import PDFReader, quality
from datagrunt.pdf._hidden_geometry import ClipPathPoly, ClipRegion, Occluder
from datagrunt.pdf.elements import HIDDEN_REASONS, Paragraph
from datagrunt.pdf.pdfium_engine import RawPage, extract_page, open_document
from datagrunt.pdf.quality import SpanPiece, assess_page, classify_spans
from datagrunt.rationalization import CallableProvider, Key, ModelRequest, ProviderResponse, rationalize
from pdf_tests.builders import hand_written_pdf
from pdf_tests.test_quality import layout, raw_span, row

W, H = 400.0, 300.0
HELVETICA = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"

OPAQUE_IMG = (
    b"<< /Type /XObject /Subtype /Image /Width 2 /Height 2"
    b" /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length 12 >>\n"
    b"stream\n" + b"\xff\xff\xff" * 4 + b"\nendstream"
)
SMASK_ALPHA = (
    b"<< /Type /XObject /Subtype /Image /Width 2 /Height 2"
    b" /ColorSpace /DeviceGray /BitsPerComponent 8 /Length 4 >>\n"
    b"stream\n\x40\x40\x40\x40\nendstream"
)
SMASK_IMG = (
    b"<< /Type /XObject /Subtype /Image /Width 2 /Height 2"
    b" /ColorSpace /DeviceRGB /BitsPerComponent 8 /SMask 7 0 R /Length 12 >>\n"
    b"stream\n" + b"\xff\xff\xff" * 4 + b"\nendstream"
)
TRANSLUCENT_GS = b"<< /Type /ExtGState /ca 0.3 /CA 0.3 >>"
MULTIPLY_GS = b"<< /Type /ExtGState /BM /Multiply /ca 1.0 /CA 1.0 >>"


class _PartRecord(BaseModel):
    """Minimal schema for end-to-end rationalization exclusion verification."""

    part: Annotated[str, Key]
    name: str
    qty: int


def _stream_obj(data: bytes) -> bytes:
    """Wrap raw stream bytes in an uncompressed PDF stream dictionary."""
    return b"<< /Length %d >>\nstream\n%s\nendstream" % (len(data), data)


def _pdf_page(
    tmp_path: Path,
    name: str,
    content: bytes,
    *,
    extra: tuple[bytes, ...] = (),
    resources: bytes = b"",
    extra_fonts: bytes = b"",
    rotate: int = 0,
    width: int = 400,
    height: int = 300,
) -> Path:
    """Write a minimal single-page PDF with Helvetica ``/F1`` (obj 5) and optional ``extra`` objects from obj 6."""
    rot = f" /Rotate {rotate}".encode() if rotate else b""
    page_dict = (
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %d %d]%s /Contents 4 0 R"
        b" /Resources << /Font << /F1 5 0 R %s >> %s >> >>" % (width, height, rot, extra_fonts, resources)
    )
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        page_dict,
        _stream_obj(content),
        HELVETICA,
        *extra,
    ]
    return hand_written_pdf(tmp_path / name, objects)


def _to_unicode_cmap(entries: Sequence[tuple[int, str]]) -> bytes:
    """Build a minimal ToUnicode CMap mapping single-byte char codes to UTF-16BE hex strings."""
    lines = [
        b"/CIDInit /ProcSet findresource begin",
        b"12 dict begin",
        b"begincmap",
        b"/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def",
        b"/CMapName /Adobe-Identity-UCS def",
        b"/CMapType 2 def",
        b"1 begincodespacerange",
        b"<00> <FF>",
        b"endcodespacerange",
        f"{len(entries)} beginbfchar".encode("ascii"),
    ]
    for code, text in entries:
        utf16_hex = text.encode("utf-16-be").hex().upper()
        lines.append(f"<{code:02X}> <{utf16_hex}>".encode("ascii"))
    lines.extend([b"endbfchar", b"endcmap", b"CMapName currentdict /CMap defineresource pop", b"end", b"end"])
    return _stream_obj(b"\n".join(lines))


def _type3_font_objects(
    glyphs: Sequence[tuple[int, str, bytes, str]],
    *,
    base_obj: int = 6,
) -> tuple[bytes, ...]:
    """Build a Type3 font dict at ``base_obj``, its CharProc streams, and its ToUnicode CMap stream."""
    first_code = min(code for code, _, _, _ in glyphs)
    last_code = max(code for code, _, _, _ in glyphs)
    proc_map = {code: (name, base_obj + 1 + idx) for idx, (code, name, _, _) in enumerate(glyphs)}
    tounicode_obj = base_obj + 1 + len(glyphs)
    charprocs = b" ".join(f"/{name} {obj_num} 0 R".encode("ascii") for _, (name, obj_num) in proc_map.items())
    diffs = b" ".join(f"{code} /{name}".encode("ascii") for code, (name, _) in sorted(proc_map.items()))
    widths = b" ".join(b"600" for _ in range(first_code, last_code + 1))
    font_dict = (
        b"<< /Type /Font /Subtype /Type3 /FontBBox [0 0 1000 1000]"
        b" /FontMatrix [0.001 0 0 0.001 0 0]"
        b" /CharProcs << " + charprocs + b" >>"
        b" /Encoding << /Type /Encoding /Differences [" + diffs + b"] >>"
        b" /FirstChar %d /LastChar %d /Widths [" % (first_code, last_code) + widths + b"]"
        b" /ToUnicode %d 0 R >>" % tounicode_obj
    )
    proc_objs = tuple(_stream_obj(stream) for _, _, stream, _ in glyphs)
    cmap_obj = _to_unicode_cmap([(code, uni) for code, _, _, uni in glyphs])
    return (font_dict, *proc_objs, cmap_obj)


def _assess_pdf(path: Path) -> tuple[SpanPiece, ...]:
    """Extract page 0 of ``path`` and run ``assess_page``."""
    handle = open_document(path)
    try:
        raw = extract_page(handle, 0)
    finally:
        handle.close()
    assert isinstance(raw, RawPage)
    return assess_page(raw).pieces


def _piece_reasons_by_text(pieces: tuple[SpanPiece, ...]) -> list[tuple[str, tuple[str, ...]]]:
    """Project pieces to ``(stripped_text, hidden_reasons)`` pairs, omitting whitespace-only noise."""
    return [(p.text.strip(), p.hidden_reasons) for p in pieces if p.text.strip()]


def _read_element_reasons(path: Path, *, sandbox: bool = False) -> list[tuple[str, tuple[str, ...]]]:
    """Extract ``path`` via full ``PDFReader`` (including `#369` render-diff) and return ``(text, hidden_reasons)``."""
    doc = PDFReader(path, ocr="never", sandbox=sandbox).document
    return [(e.text.strip(), e.hidden_reasons) for e in doc.pages[0].elements if isinstance(e, Paragraph)]


class TestQualityUnit395:
    """Pure quality-stage unit tests for occluded, clipped, blank_glyph, budget bounds, and CAD exemption."""

    def test_split_adjacent_occluders_cover_multi_word_span(self) -> None:
        boxes = row(11, 72.0, 90.0)
        span = raw_span("TOP  SECRET", boxes, object_index=0)
        left_occ = Occluder(object_index=1, bbox=(70.0, 88.0, 105.0, 106.0))
        right_occ = Occluder(object_index=2, bbox=(105.0, 88.0, 142.0, 106.0))

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(left_occ, right_occ))
        assert layout(pieces) == [(0, 11, "TOP  SECRET", ("occluded",))]

    def test_hollow_polygon_and_strikethrough_do_not_occlude(self) -> None:
        boxes = row(6, 100.0, 100.0)
        span = raw_span("CELL42", boxes, object_index=0)
        outer = ((80.0, 80.0), (180.0, 80.0), (180.0, 130.0), (80.0, 130.0))
        inner = ((90.0, 90.0), (170.0, 90.0), (170.0, 120.0), (90.0, 120.0))
        hollow = Occluder(object_index=1, bbox=(80.0, 80.0, 180.0, 130.0), polygons=(outer, inner), even_odd=True)
        strike = Occluder(object_index=2, bbox=(95.0, 105.5, 145.0, 106.5))

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(hollow, strike))
        assert layout(pieces) == [(0, 6, "CELL42", ())]

    def test_clipped_occluder_away_from_text_leaves_text_visible(self) -> None:
        boxes = row(6, 72.0, 90.0)
        span = raw_span("SAFE99", boxes, object_index=0)
        far_clip = ClipRegion(bbox=(250.0, 0.0, 400.0, 300.0), paths=(ClipPathPoly(bbox=(250.0, 0.0, 400.0, 300.0)),))
        occ = Occluder(object_index=1, bbox=(0.0, 0.0, 400.0, 300.0), clip=far_clip)

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(occ,))
        assert layout(pieces) == [(0, 6, "SAFE99", ())]

    def test_diamond_occluder_hides_interior_and_preserves_bbox_corner(self) -> None:
        inside = raw_span("HIDDEN", row(6, 100.0, 145.0), object_index=0)
        corner = raw_span("SAFE", row(4, 175.0, 105.0), object_index=1)
        diamond = ((124.0, 90.0), (210.0, 150.0), (124.0, 210.0), (38.0, 150.0))
        occ = Occluder(object_index=2, bbox=(38.0, 90.0, 210.0, 210.0), polygons=(diamond,), even_odd=False)

        pieces = classify_spans((inside, corner), width_pt=W, height_pt=H, images=(), occluders=(occ,))
        assert _piece_reasons_by_text(pieces) == [("HIDDEN", ("occluded",)), ("SAFE", ())]

    def test_canonical_hidden_reason_order_with_new_reasons(self) -> None:
        cboxes = ((-30.0, 90.0, -24.0, 102.0),)
        standin = ((-30.0, 95.0, -24.0, 95.01),)
        span = raw_span("A", cboxes, tight_boxes=standin, fill_rgba=(0, 0, 0, 0), object_index=0)
        occ = Occluder(object_index=1, bbox=(-40.0, 80.0, 0.0, 110.0))

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(occ,))
        assert pieces[0].hidden_reasons == ("off_page", "transparent", "occluded", "blank_glyph")

    def test_shx_tr3_stroke_backed_with_zero_tight_boxes_stays_visible(self) -> None:
        cboxes = row(6, 100.0, 100.0, width=10.0, height=15.0)
        zero_tight = tuple((b[0], b[1], b[0], b[1]) for b in cboxes)
        strokes = tuple((b[0] + 1.0, b[1] + 1.0, b[2] - 1.0, b[3] - 1.0) for b in cboxes)
        span = raw_span("PN2045", cboxes, tight_boxes=zero_tight, render_mode=3, base_font_name="Txt")

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), glyph_boxes=strokes)
        assert layout(pieces) == [(0, 6, "PN2045", ())]

    def test_occlusion_budget_exhaustion_fails_open(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(quality, "MAX_OCCLUSION_CHECKS", 0)
        span = raw_span("SAFE", row(4, 72.0, 90.0), object_index=0)
        occ = Occluder(object_index=1, bbox=(60.0, 80.0, 140.0, 120.0))

        pieces = classify_spans((span,), width_pt=W, height_pt=H, images=(), occluders=(occ,))
        assert layout(pieces) == [(0, 4, "SAFE", ())]


class TestOcclusionEndToEnd395:
    """Real-PDF end-to-end occlusion detection and zero-false-positive controls."""

    @pytest.mark.parametrize(
        "rgb",
        [
            pytest.param(b"1 1 1", id="white-fill"),
            pytest.param(b"0 0 0", id="black-fill"),
            pytest.param(b"0.2 0.4 0.8", id="colored-fill"),
        ],
    )
    def test_opaque_filled_rect_after_text_is_occluded_and_before_is_visible(self, tmp_path: Path, rgb: bytes) -> None:
        after_pdf = _pdf_page(
            tmp_path,
            "after.pdf",
            b"BT /F1 12 Tf 72 150 Td (SECRET) Tj ET " + rgb + b" rg 65 140 120 28 re f",
        )
        before_pdf = _pdf_page(
            tmp_path,
            "before.pdf",
            rgb + b" rg 65 140 120 28 re f 0 0 0 rg BT /F1 12 Tf 72 150 Td (VISIBLE) Tj ET",
        )

        assert _piece_reasons_by_text(_assess_pdf(after_pdf)) == [("SECRET", ("occluded",))]
        assert _piece_reasons_by_text(_assess_pdf(before_pdf)) == [("VISIBLE", ())]

    @pytest.mark.parametrize("sandbox", [False, True], ids=["in-process", "sandbox"])
    def test_opaque_raster_image_after_text_is_occluded(self, tmp_path: Path, sandbox: bool) -> None:
        content = b"BT /F1 12 Tf 72 150 Td (SECRET) Tj ET q 140 0 0 40 60 135 cm /Im Do Q"
        path = _pdf_page(tmp_path, "img_occ.pdf", content, extra=(OPAQUE_IMG,), resources=b"/XObject << /Im 6 0 R >>")

        assert _read_element_reasons(path, sandbox=sandbox) == [("SECRET", ("occluded",))]

    @pytest.mark.parametrize(
        "gs_obj,color",
        [
            pytest.param(TRANSLUCENT_GS, b"1 1 0", id="translucent-ca-0.3"),
            pytest.param(MULTIPLY_GS, b"1 1 0", id="multiply-highlighter"),
        ],
    )
    def test_translucent_and_multiply_overlays_leave_text_visible(
        self, tmp_path: Path, gs_obj: bytes, color: bytes
    ) -> None:
        content = b"BT /F1 12 Tf 72 150 Td (HIGHLIGHTED) Tj ET q /GS gs " + color + b" rg 65 140 140 28 re f Q"
        path = _pdf_page(tmp_path, "overlay.pdf", content, extra=(gs_obj,), resources=b"/ExtGState << /GS 6 0 R >>")

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("HIGHLIGHTED", ())]

    @pytest.mark.parametrize("sandbox", [False, True], ids=["in-process", "sandbox"])
    def test_smask_image_after_text_leaves_text_visible(self, tmp_path: Path, sandbox: bool) -> None:
        content = b"BT /F1 12 Tf 72 150 Td (WATERMARK) Tj ET q 140 0 0 40 60 135 cm /Im Do Q"
        path = _pdf_page(
            tmp_path,
            "smask.pdf",
            content,
            extra=(SMASK_IMG, SMASK_ALPHA),
            resources=b"/XObject << /Im 6 0 R >>",
        )

        assert _read_element_reasons(path, sandbox=sandbox) == [("WATERMARK", ())]

    def test_two_adjacent_split_whiteout_rects_occlude_word(self, tmp_path: Path) -> None:
        content = b"BT /F1 12 Tf 72 150 Td (SECRET) Tj ET 1 1 1 rg 65 140 31 28 re f 1 1 1 rg 96 140 80 28 re f"
        path = _pdf_page(tmp_path, "split_occ.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("SECRET", ("occluded",))]

    @pytest.mark.parametrize(
        "frame_ops",
        [
            pytest.param(b"0 0 0 rg 80 135 160 40 re 86 141 148 28 re f*", id="even-odd-frame"),
            pytest.param(
                b"0 0 0 rg 80 135 m 240 135 l 240 175 l 80 175 l h 86 141 m 86 169 l 234 169 l 234 141 l h f",
                id="nonzero-frame",
            ),
        ],
    )
    def test_hollow_filled_border_frame_after_table_text_stays_visible(self, tmp_path: Path, frame_ops: bytes) -> None:
        content = b"BT /F1 12 Tf 100 150 Td (Table Cell 42) Tj ET " + frame_ops
        path = _pdf_page(tmp_path, "hollow.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("Table Cell 42", ())]

    @pytest.mark.parametrize(
        "bar_y",
        [
            pytest.param(b"153.5", id="strikethrough"),
            pytest.param(b"148.0", id="underline"),
        ],
    )
    def test_one_pt_strikethrough_and_underline_bar_stays_visible(self, tmp_path: Path, bar_y: bytes) -> None:
        content = b"BT /F1 12 Tf 72 150 Td (Revised Price) Tj ET 0 0 0 rg 70 " + bar_y + b" 120 1 re f"
        path = _pdf_page(tmp_path, "bar.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("Revised Price", ())]

    def test_non_rectangular_diamond_occludes_interior_but_not_bbox_corner(self, tmp_path: Path) -> None:
        content = (
            b"BT /F1 12 Tf 100 150 Td (SECRET) Tj ET "
            b"BT /F1 12 Tf 175 195 Td (SAFE) Tj ET "
            b"0.1 0.1 0.1 rg 124 210 m 210 154 l 124 98 l 38 154 l h f"
        )
        path = _pdf_page(tmp_path, "diamond.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("SECRET", ("occluded",)), ("SAFE", ())]

    def test_non_rectangular_triangle_occludes_interior_but_not_bbox_corner(self, tmp_path: Path) -> None:
        content = (
            b"BT /F1 12 Tf 100 135 Td (SECRET) Tj ET "
            b"BT /F1 12 Tf 45 200 Td (SAFE) Tj ET "
            b"0.1 0.1 0.1 rg 40 120 m 220 120 l 130 220 l h f"
        )
        path = _pdf_page(tmp_path, "triangle.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("SECRET", ("occluded",)), ("SAFE", ())]

    def test_clipped_later_fill_away_from_text_leaves_text_visible(self, tmp_path: Path) -> None:
        content = b"BT /F1 12 Tf 72 150 Td (UNTOUCHED) Tj ET q 250 0 150 300 re W n 1 1 1 rg 0 0 400 300 re f Q"
        path = _pdf_page(tmp_path, "clipped_fill.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("UNTOUCHED", ())]


class TestClippingEndToEnd395:
    """Real-PDF end-to-end clipping detection and CAD viewport zero-false-positive controls."""

    @pytest.mark.parametrize(
        "clip_ops",
        [
            pytest.param(b"250 100 100 50 re W n", id="outside-clip-rect"),
            pytest.param(b"0 0 0 0 re W n", id="zero-area-clip"),
            pytest.param(b"50 100 120 80 re W n 220 100 120 80 re W n", id="nested-disjoint-clip"),
            pytest.param(b"50 100 160 80 re W n 140 100 160 80 re W n", id="nested-intersection-excludes-text"),
        ],
    )
    def test_clipped_text_is_flagged(self, tmp_path: Path, clip_ops: bytes) -> None:
        content = b"q " + clip_ops + b" BT /F1 12 Tf 72 150 Td (CLIPPED) Tj ET Q"
        path = _pdf_page(tmp_path, "clipped.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("CLIPPED", ("clipped",))]

    def test_partial_clip_splits_visible_prefix_and_clipped_suffix(self, tmp_path: Path) -> None:
        # "VISIBLE" occupies x ~ [50, 96]; three spaces push "HIDDEN" past x ~ 106; clip stops at x = 102.
        content = b"q 40 130 62 40 re W n BT /F1 12 Tf 50 150 Td (VISIBLE   HIDDEN) Tj ET Q"
        path = _pdf_page(tmp_path, "partial_clip.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("VISIBLE", ()), ("HIDDEN", ("clipped",))]

    def test_form_xobject_bbox_and_outer_clip_propagation(self, tmp_path: Path) -> None:
        form_stream = b"BT /F1 12 Tf 8 15 Td (INBOX) Tj ET BT /F1 12 Tf 120 15 Td (OUTBOX) Tj ET"
        form_obj = (
            b"<< /Type /XObject /Subtype /Form /BBox [0 0 80 40]"
            b" /Resources << /Font << /F1 5 0 R >> >> /Length %d >>\nstream\n%s\nendstream"
            % (len(form_stream), form_stream)
        )
        content = b"q 1 0 0 1 40 160 cm /Fm Do Q q 0 0 30 300 re W n 1 0 0 1 100 60 cm /Fm Do Q"
        path = _pdf_page(tmp_path, "form_clip.pdf", content, extra=(form_obj,), resources=b"/XObject << /Fm 6 0 R >>")

        assert _piece_reasons_by_text(_assess_pdf(path)) == [
            ("INBOX", ()),
            ("OUTBOX", ("clipped",)),
            ("INBOX", ("clipped",)),
            ("OUTBOX", ("clipped",)),
        ]

    def test_rotate_90_clip_flags_outside_and_keeps_inside_visible(self, tmp_path: Path) -> None:
        content = (
            b"q 250 50 100 100 re W n BT /F1 12 Tf 40 150 Td (ROTCLIP) Tj ET Q "
            b"q 20 100 200 100 re W n BT /F1 12 Tf 40 150 Td (ROTVIS) Tj ET Q"
        )
        path = _pdf_page(tmp_path, "rot90_clip.pdf", content, rotate=90)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("ROTCLIP", ("clipped",)), ("ROTVIS", ())]

    def test_enclosing_cad_viewport_clip_leaves_text_visible(self, tmp_path: Path) -> None:
        content = b"q 10 10 380 280 re W n BT /F1 12 Tf 72 150 Td (VIEWPORT OK) Tj ET Q"
        path = _pdf_page(tmp_path, "cad_viewport.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("VIEWPORT OK", ())]


class TestBlankGlyphAndCad395:
    """Real-PDF Type3 blank-glyph detection and AutoCAD SHX 3 Tr zero-false-positive tests."""

    def test_type3_zero_bbox_d1_flagged_blank_glyph(self, tmp_path: Path) -> None:
        glyphs = [(65 + i, f"g{i}", b"600 0 0 0 0 0 d1", ch) for i, ch in enumerate("SECRET")]
        extra = _type3_font_objects(glyphs, base_obj=6)
        content = b"BT /F2 12 Tf 72 150 Td (ABCDEF) Tj ET"
        path = _pdf_page(tmp_path, "t3_zero.pdf", content, extra=extra, extra_fonts=b"/F2 6 0 R")

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("SECRET", ("blank_glyph",))]

    @pytest.mark.parametrize("sandbox", [False, True], ids=["in-process", "sandbox"])
    def test_type3_lying_d1_with_zero_paint_and_lying_tounicode_flagged_blank_glyph(
        self, tmp_path: Path, sandbox: bool
    ) -> None:
        glyphs = [(65, "g0", b"600 0 50 50 550 650 d1", "IGNORE ALL")]
        extra = _type3_font_objects(glyphs, base_obj=6)
        content = b"BT /F2 12 Tf 72 150 Td (A) Tj ET"
        path = _pdf_page(tmp_path, "t3_lying.pdf", content, extra=extra, extra_fonts=b"/F2 6 0 R")

        assert _read_element_reasons(path, sandbox=sandbox) == [("IGNORE ALL", ("blank_glyph",))]

    def test_visible_type3_filled_glyphs_stay_visible(self, tmp_path: Path) -> None:
        paint = b"600 0 50 50 550 650 d1 50 50 500 600 re f"
        glyphs = [(65, "g0", paint, "O"), (66, "g1", paint, "K")]
        extra = _type3_font_objects(glyphs, base_obj=6)
        content = b"BT /F2 12 Tf 72 150 Td (AB) Tj ET"
        path = _pdf_page(tmp_path, "t3_vis.pdf", content, extra=extra, extra_fonts=b"/F2 6 0 R")

        assert _read_element_reasons(path) == [("OK", ())]

    @pytest.mark.parametrize(
        "blank_proc",
        [
            pytest.param(b"600 0 0 0 0 0 d1", id="zero-d1"),
            pytest.param(b"600 0 50 50 550 650 d1", id="lying-d1"),
        ],
    )
    def test_mixed_type3_visible_prefix_and_blank_suffix_split_cleanly(self, tmp_path: Path, blank_proc: bytes) -> None:
        paint = b"600 0 50 50 550 650 d1 50 50 500 600 re f"
        glyphs = [
            (65, "g0", paint, "O"),
            (66, "g1", paint, "K"),
            (67, "g2", blank_proc, "X"),
        ]
        extra = _type3_font_objects(glyphs, base_obj=6)
        content = b"BT /F2 12 Tf 72 150 Td (ABC) Tj ET"
        path = _pdf_page(tmp_path, "t3_mixed.pdf", content, extra=extra, extra_fonts=b"/F2 6 0 R")

        assert _read_element_reasons(path) == [("OK", ()), ("X", ("blank_glyph",))]

    @pytest.mark.parametrize(
        "blank_proc",
        [
            pytest.param(b"600 0 0 0 0 0 d1", id="encoding-only-zero-d1"),
            pytest.param(b"600 0 50 50 550 650 d1", id="encoding-only-lying-d1"),
        ],
    )
    @pytest.mark.parametrize("sandbox", [False, True], ids=["in-process", "sandbox"])
    def test_type3_encoding_only_without_tounicode_cannot_bypass_blank_glyph(
        self, tmp_path: Path, blank_proc: bytes, sandbox: bool
    ) -> None:
        paint = b"600 0 50 50 550 650 d1 50 50 500 600 re f"
        font_dict = (
            b"<< /Type /Font /Subtype /Type3 /FontBBox [0 0 1000 1000]"
            b" /FontMatrix [0.001 0 0 0.001 0 0]"
            b" /CharProcs << /A 7 0 R /B 8 0 R >>"
            b" /Encoding << /Type /Encoding /Differences [65 /A /B] >>"
            b" /FirstChar 65 /LastChar 66 /Widths [600 600] >>"
        )
        extra = (font_dict, _stream_obj(paint), _stream_obj(blank_proc))
        content = b"BT /F2 12 Tf 72 150 Td (AB) Tj ET"
        path = _pdf_page(tmp_path, "t3_enc_only.pdf", content, extra=extra, extra_fonts=b"/F2 6 0 R")

        assert _read_element_reasons(path, sandbox=sandbox) == [("A", ()), ("B", ("blank_glyph",))]

    def test_autocad_shx_tr3_stroke_backed_span_stays_visible(self, tmp_path: Path) -> None:
        # Draw narrow vertical glyph strokes matching each 'H' of "HHHHHH" at (72, 150) 12 pt Helvetica.
        strokes = b" ".join(f"{73.5 + i * 8.66:.2f} 150.5 5.5 8.2 re S".encode("ascii") for i in range(6))
        content = b"0 0 0 RG 0.8 w " + strokes + b" BT /F1 12 Tf 3 Tr 72 150 Td (HHHHHH) Tj ET"
        path = _pdf_page(tmp_path, "shx_tr3.pdf", content)

        assert _piece_reasons_by_text(_assess_pdf(path)) == [("HHHHHH", ())]


def _build_e2e_395_pdf(tmp_path: Path) -> Path:
    """Build a single-page PDF holding visible text plus occluded, clipped, and blank_glyph canaries."""
    visible_line = "Part PN-395, Name Titanium Gasket, Qty 4"
    glyphs = [(65, "g0", b"600 0 50 50 550 650 d1", "CANARY-BLANK-395")]
    extra = _type3_font_objects(glyphs, base_obj=6)
    content = (
        b"BT /F1 12 Tf 40 250 Td (" + visible_line.encode("ascii") + b") Tj ET "
        b"BT /F1 12 Tf 40 200 Td (CANARY-OCCLUDED-395) Tj ET "
        b"1 1 1 rg 35 190 220 28 re f "
        b"q 0 0 0 0 re W n BT /F1 12 Tf 40 140 Td (CANARY-CLIPPED-395) Tj ET Q "
        b"BT /F2 12 Tf 40 80 Td (A) Tj ET"
    )
    return _pdf_page(tmp_path, "e2e_395.pdf", content, extra=extra, extra_fonts=b"/F2 6 0 R")


def _hidden_texts_by_reason(doc: Any) -> dict[str, list[str]]:
    """Collect hidden Paragraph texts on page 0 keyed by each of their hidden_reasons."""
    by_reason: dict[str, list[str]] = {}
    for elem in doc.pages[0].elements:
        if not (isinstance(elem, Paragraph) and elem.hidden_reasons):
            continue
        for reason in elem.hidden_reasons:
            by_reason.setdefault(reason, []).append(elem.text)
    return by_reason


def _capture_rationalize(doc: Any, visible_elem_id: str) -> tuple[Any, list[str]]:
    """Run ``rationalize()`` with a capturing CallableProvider grounded on ``visible_elem_id``."""
    answer: dict[str, Any] = {
        "records": [
            {
                "record": {"part": "PN-395", "name": "Titanium Gasket", "qty": 4},
                "grounding_quotes": ["Part PN-395", "Name Titanium Gasket", "Qty 4"],
                "source_element_ids": [visible_elem_id],
            }
        ]
    }
    captured: list[str] = []

    def fake_model(req: ModelRequest) -> ProviderResponse:
        captured.append(req.system_prompt + "\n" + req.user_envelope)
        return ProviderResponse(
            content=answer,
            raw_text=json.dumps(answer),
            finish="complete",
            input_tokens=50,
            output_tokens=20,
        )

    result = rationalize(
        doc,
        schema=_PartRecord,
        prompt="Extract part records.",
        models=[CallableProvider("callable:test-395", fake_model, family="openai")],
        min_families=1,
    )
    return result, captured


class TestPipelineAndRationalization395:
    """End-to-end PDFReader + rationalize() verifying flagging, Markdown omission, and LLM exclusion."""

    def test_reader_and_rationalize_exclude_all_three_new_hidden_reasons(self, tmp_path: Path) -> None:
        canaries = ("CANARY-OCCLUDED-395", "CANARY-CLIPPED-395", "CANARY-BLANK-395")
        reader = PDFReader(_build_e2e_395_pdf(tmp_path), ocr="never")
        doc = reader.document
        assert doc.pages[0].route == "digital"

        by_reason = _hidden_texts_by_reason(doc)
        assert any("CANARY-OCCLUDED-395" in t for t in by_reason.get("occluded", ()))
        assert any("CANARY-CLIPPED-395" in t for t in by_reason.get("clipped", ()))
        assert any("CANARY-BLANK-395" in t for t in by_reason.get("blank_glyph", ()))

        default_md = reader.to_markdown()
        hidden_md = reader.to_markdown(include_hidden=True)
        assert "PN-395" in default_md
        for canary in canaries:
            assert canary not in default_md
            assert canary in hidden_md
        for reason in ("occluded", "clipped", "blank_glyph"):
            assert reason in HIDDEN_REASONS
            assert f"<!-- hidden: {reason} -->" in hidden_md

        visible_elem = next(
            e for e in doc.pages[0].elements if isinstance(e, Paragraph) and not e.hidden_reasons and "PN-395" in e.text
        )
        result, captured = _capture_rationalize(doc, visible_elem.element_id)

        assert len(captured) == 1
        for canary in canaries:
            assert canary not in captured[0]
        assert "PN-395" in captured[0]
        counts = dict(result.telemetry.hidden_excluded_by_reason)
        assert counts.get("occluded", 0) >= 1
        assert counts.get("clipped", 0) >= 1
        assert counts.get("blank_glyph", 0) >= 1
        assert result.records == (_PartRecord(part="PN-395", name="Titanium Gasket", qty=4),)
