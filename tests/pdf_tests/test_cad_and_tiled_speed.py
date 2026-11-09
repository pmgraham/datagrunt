"""Regression, parity, and scaling tests for CAD drawings, micro-tiled PDFs, and multi-core corpus extraction."""

from __future__ import annotations

import dataclasses
import io
from pathlib import Path

import pytest
from PIL import Image
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

from datagrunt.pdf import pdfium_engine
from datagrunt.pdf._loaders import FigureRead, figure_pngs, read_figures
from datagrunt.pdf.exceptions import DatagruntPDFError
from datagrunt.pdf.pipeline import extract_document


def _make_tiled_pdf(path: Path, pages: int, tiles_per_page: int, px: int = 48) -> Path:
    """Create a multi-page PDF containing ``tiles_per_page`` distinct >=40px raster tiles per page."""
    c = canvas.Canvas(str(path), pagesize=(612, 792))
    for p in range(pages):
        c.setFont("Helvetica", 10)
        c.drawString(36, 760, f"Tiled sheet {p + 1}")
        for idx in range(tiles_per_page):
            img = Image.frombytes(
                "L",
                (px, px),
                bytes(((p * 31 + idx * 17 + k) & 0xFF) for k in range(px * px)),
            )
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            col = idx % 10
            row = idx // 10
            c.drawImage(ImageReader(buf), 36 + col * 52, 700 - row * 52, width=48, height=48)
        c.showPage()
    c.save()
    return path


class TestNode1FigureSessionBatching:
    """In-process figure reads load and walk each page once across the entire PDF."""

    def test_in_process_figure_pngs_opens_once_and_walks_once_per_page(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pdf_path = _make_tiled_pdf(tmp_path / "tiled.pdf", pages=3, tiles_per_page=30)
        doc = extract_document(pdf_path, ocr="never", min_image_dimension=40, sandbox=False)
        assert len(doc.figures) == 90  # exceeds FIGURE_BATCH (64)

        open_calls = 0
        load_page_calls = 0
        walk_calls = 0
        real_open = pdfium_engine.open_document
        real_load_page = pdfium_engine._load_page
        real_walk = pdfium_engine._page_objects

        def counting_open(*args, **kwargs):
            nonlocal open_calls
            open_calls += 1
            return real_open(*args, **kwargs)

        def counting_load_page(*args, **kwargs):
            nonlocal load_page_calls
            load_page_calls += 1
            return real_load_page(*args, **kwargs)

        def counting_walk(*args, **kwargs):
            nonlocal walk_calls
            walk_calls += 1
            return real_walk(*args, **kwargs)

        monkeypatch.setattr(pdfium_engine, "open_document", counting_open)
        monkeypatch.setattr(pdfium_engine, "_load_page", counting_load_page)
        monkeypatch.setattr(pdfium_engine, "_page_objects", counting_walk)

        batched_bytes = [loader() for loader in figure_pngs(doc.figures)]

        assert len(batched_bytes) == 90
        assert open_calls == 1
        assert load_page_calls == 3
        assert walk_calls == 3

        # Verify byte-for-byte parity with standalone single-item ImageLoader reads
        monkeypatch.undo()
        assert batched_bytes[0] == doc.figures[0].read_bytes()
        assert batched_bytes[45] == doc.figures[45].read_bytes()
        assert batched_bytes[-1] == doc.figures[-1].read_bytes()

    def test_figure_session_isolates_per_figure_tamper_and_walk_bound_errors(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pdf_path = _make_tiled_pdf(tmp_path / "tamper.pdf", pages=2, tiles_per_page=3)
        doc = extract_document(pdf_path, ocr="never", min_image_dimension=40, sandbox=False)
        reads: list[FigureRead] = [fig._loader.__self__ for fig in doc.figures]  # type: ignore[attr-defined]
        assert len(reads) == 6

        bad_hash_image = dataclasses.replace(reads[1].image, raw_sha256="0" * 64)
        mixed = [
            reads[0],
            dataclasses.replace(reads[1], image=bad_hash_image),
            reads[2],
            reads[3],
        ]
        outcomes = list(read_figures(mixed, "png"))
        assert isinstance(outcomes[0], bytes)
        assert isinstance(outcomes[1], DatagruntPDFError)
        assert "object" in str(outcomes[1])
        assert isinstance(outcomes[2], bytes)
        assert isinstance(outcomes[3], bytes)

        # Simulate WalkBoundHit on page 1: each figure on page 1 yields its own changed error, page 2 still succeeds
        real_walk = pdfium_engine._page_objects
        calls = 0

        def walk_fail_first_page(page_raw, *, max_objects):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise pdfium_engine.WalkBoundHit("simulated object bound exceeded")
            return real_walk(page_raw, max_objects=max_objects)

        monkeypatch.setattr(pdfium_engine, "_page_objects", walk_fail_first_page)
        bound_outcomes = list(read_figures([reads[0], reads[1], reads[3]], "raw"))
        assert isinstance(bound_outcomes[0], DatagruntPDFError)
        assert f"object {reads[0].object_index}" in str(bound_outcomes[0])
        assert isinstance(bound_outcomes[1], DatagruntPDFError)
        assert f"object {reads[1].object_index}" in str(bound_outcomes[1])
        assert isinstance(bound_outcomes[2], bytes)


class TestNode2CadPathAndFontFastPath:
    """Short CAD micro-paths cull via outer-mapped GetBounds without losing scaled paths; fonts cache per page."""

    def test_short_micro_paths_skip_segment_ffi_while_scaled_own_and_form_paths_survive(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import pypdfium2 as pdfium
        import pypdfium2.raw as pdfium_c

        pdf_file = tmp_path / "cad_micro.pdf"
        wrapper = pdfium.PdfDocument.new()
        raw_doc = wrapper.raw
        page = pdfium_c.FPDFPage_New(raw_doc, 0, 612.0, 792.0)

        # 1) 400 tiny 3pt CAD hatch/arrowhead strokes -> must be culled before FPDFPath_GetPathSegment
        for i in range(400):
            p = pdfium_c.FPDFPageObj_CreateNewPath(float(20 + (i % 40) * 12), float(50 + (i // 40) * 12))
            pdfium_c.FPDFPath_LineTo(p, float(23 + (i % 40) * 12), float(50 + (i // 40) * 12))
            pdfium_c.FPDFPath_SetDrawMode(p, pdfium_c.FPDF_FILLMODE_NONE, 1)
            pdfium_c.FPDFPage_InsertObject(page, p)

        # 2) A 0..2 local path scaled 10x by its OWN object matrix -> 20pt on page (MUST SURVIVE!)
        scaled_own = pdfium_c.FPDFPageObj_CreateNewPath(0.0, 0.0)
        pdfium_c.FPDFPath_LineTo(scaled_own, 2.0, 0.0)
        pdfium_c.FPDFPath_SetDrawMode(scaled_own, pdfium_c.FPDF_FILLMODE_NONE, 1)
        mat = pdfium_c.FS_MATRIX(10.0, 0.0, 0.0, 10.0, 100.0, 500.0)
        pdfium_c.FPDFPageObj_SetMatrix(scaled_own, mat)
        pdfium_c.FPDFPage_InsertObject(page, scaled_own)

        # 3) A normal 100pt horizontal rule -> MUST SURVIVE!
        long_rule = pdfium_c.FPDFPageObj_CreateNewPath(50.0, 600.0)
        pdfium_c.FPDFPath_LineTo(long_rule, 150.0, 600.0)
        pdfium_c.FPDFPath_SetDrawMode(long_rule, pdfium_c.FPDF_FILLMODE_NONE, 1)
        pdfium_c.FPDFPage_InsertObject(page, long_rule)

        pdfium_c.FPDFPage_GenerateContent(page)
        pdfium_c.FPDF_ClosePage(page)
        wrapper.save(str(pdf_file))
        wrapper.close()

        seg_ffi_calls = 0
        real_get_seg = pdfium_c.FPDFPath_GetPathSegment

        def counting_get_seg(path_obj, idx):
            nonlocal seg_ffi_calls
            seg_ffi_calls += 1
            return real_get_seg(path_obj, idx)

        monkeypatch.setattr(pdfium_c, "FPDFPath_GetPathSegment", counting_get_seg)

        with pdfium_engine.open_document(pdf_file) as handle:
            got = pdfium_engine.page_segments(handle, 0)

        # Both the 20pt scaled-own rule and the 100pt rule survive; the 400 3pt micro-paths never call GetPathSegment
        assert len(got.segments) == 2
        lengths = sorted(round(seg.end - seg.start, 2) for seg in got.segments)
        assert lengths == [20.0, 100.0]
        assert seg_ffi_calls <= 8, f"Expected <=8 GetPathSegment calls, got {seg_ffi_calls}"

    def test_per_page_font_cache_queries_base_font_name_once_per_distinct_font(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datagrunt.pdf import _pdfium_text

        pdf_file = tmp_path / "cad_callouts.pdf"
        c = canvas.Canvas(str(pdf_file), pagesize=(612, 792))
        for i in range(120):
            c.setFont("Helvetica-Bold" if i % 2 == 0 else "Helvetica", 8)
            c.drawString(30 + (i % 6) * 90, 750 - (i // 6) * 20, f"DIM-{i:03d}")
        c.showPage()
        c.save()

        font_queries = 0
        real_font_name = _pdfium_text._base_font_name

        def counting_font_name(font, label):
            nonlocal font_queries
            font_queries += 1
            return real_font_name(font, label)

        monkeypatch.setattr(_pdfium_text, "_base_font_name", counting_font_name)
        doc = extract_document(pdf_file, ocr="never", sandbox=False)
        md = doc.to_markdown()
        assert "DIM-000" in md and "DIM-119" in md
        assert font_queries <= 2, f"Expected <=2 font FFI queries on 120 callouts, got {font_queries}"


class TestNode3SharedRenderAndFastPng:
    """Bundle page rendering reuses one DocumentHandle, walks objects once per page, and encodes lossless PNG fast."""

    def test_grey_png_is_pixel_exact_across_edge_and_16mp_dimensions(self) -> None:
        import time

        from datagrunt.pdf._png import grey_png_bytes

        for w, h in ((1, 1), (7, 3), (257, 19)):
            raw = bytes(((i * 73 + 19) & 0xFF) for i in range(w * h))
            encoded = grey_png_bytes(w, h, raw)
            decoded = Image.open(io.BytesIO(encoded))
            assert decoded.mode == "L"
            assert decoded.size == (w, h)
            assert decoded.tobytes() == raw

        # 16MP CAD grayscale sheet speed + lossless pixel check
        w16, h16 = 4964, 3212
        cad_pixels = (b"\xff" * 4900 + b"\x00" * 64) * h16
        t0 = time.perf_counter()
        png16 = grey_png_bytes(w16, h16, cad_pixels)
        elapsed = time.perf_counter() - t0
        assert elapsed < 0.35, f"16MP grey_png_bytes took {elapsed:.3f}s"
        dec16 = Image.open(io.BytesIO(png16))
        assert dec16.size == (w16, h16)
        assert dec16.tobytes() == cad_pixels

    def test_write_bundle_reuses_one_document_handle_and_walks_once_per_rendered_page(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datagrunt.pdf.bundle import write_bundle

        pdf_file = tmp_path / "six_pages.pdf"
        c = canvas.Canvas(str(pdf_file), pagesize=(612, 792))
        for p in range(6):
            c.setFont("Helvetica", 12)
            c.drawString(72, 700, f"CAD drawing sheet {p + 1}")
            c.line(72, 680, 540, 680)
            c.showPage()
        c.save()

        doc = extract_document(pdf_file, ocr="never", sandbox=False)

        open_calls = 0
        walk_calls = 0
        real_open = pdfium_engine.open_document
        real_walk = pdfium_engine._page_objects

        def counting_open(*args, **kwargs):
            nonlocal open_calls
            open_calls += 1
            return real_open(*args, **kwargs)

        def counting_walk(*args, **kwargs):
            nonlocal walk_calls
            walk_calls += 1
            return real_walk(*args, **kwargs)

        monkeypatch.setattr(pdfium_engine, "open_document", counting_open)
        monkeypatch.setattr(pdfium_engine, "_page_objects", counting_walk)

        written = write_bundle(doc, tmp_path / "bundle", source=pdf_file, dpi=100, render_pages=True, sandbox=False)

        assert len(written.pages) == 6
        assert all(pg.page_image is not None and pg.page_image.image_path is not None for pg in written.pages)
        assert open_calls == 1, f"Expected 1 open_document across 6 rendered pages, got {open_calls}"
        assert walk_calls == 6, f"Expected 6 _page_objects walks (1/page), got {walk_calls}"
