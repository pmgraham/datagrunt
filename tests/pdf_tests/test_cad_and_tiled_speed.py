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


class TestNode4CorpusProcessScalingAndReuse:
    """extract_corpus scales across isolated workers, works from <stdin>, and reuse ignores workers."""

    def test_extract_corpus_max_workers_reuses_across_workers_param_and_bundle_dpi_change_rebuilds(
        self, tmp_path: Path
    ) -> None:
        from datagrunt.pdf.bundle import write_bundle
        from datagrunt.pdf.corpus import extract_corpus
        from datagrunt.pdf.pipeline import extract_document

        in_dir = tmp_path / "in_pdfs"
        in_dir.mkdir()
        for idx in range(6):
            p = in_dir / f"sheet_{idx:02d}.pdf"
            c = canvas.Canvas(str(p), pagesize=(612, 792))
            c.setFont("Helvetica", 11)
            c.drawString(72, 700, f"Engineering document {idx}")
            c.showPage()
            c.save()

        out_dir = tmp_path / "corpus_out"
        m1 = extract_corpus(in_dir, out_dir, max_workers=3, workers=1, ocr="never")
        assert len(m1.entries) == 6
        assert all(not e.reused for e in m1.entries)
        # Verify default Pillar 3: every page image is captured and saved in addition to JSON & Markdown
        for entry in m1.entries:
            assert (out_dir / entry.bundle_relpath / "page_images" / "page_0001.png").is_file()
            assert (out_dir / entry.bundle_relpath / "json" / "document.json").is_file()
            assert (out_dir / entry.bundle_relpath / "markdown" / "document.md").is_file()

        # Reuse with a different `workers` parameter must reuse all 6 bundles without rewriting
        m2 = extract_corpus(in_dir, out_dir, max_workers=2, workers=4, ocr="never", overwrite=False)
        assert len(m2.entries) == 6
        assert all(e.reused for e in m2.entries)

        # Direct write_bundle also reuses across workers changes, but rejects dpi mismatch
        one_pdf = in_dir / "sheet_00.pdf"
        doc = extract_document(one_pdf, ocr="never")
        bdir = tmp_path / "single_bundle"
        write_bundle(doc, bdir, source=one_pdf, dpi=150, workers=1)
        reused_doc = write_bundle(doc, bdir, source=one_pdf, dpi=150, workers=4, overwrite=False)
        assert reused_doc.metadata.doc_id == doc.metadata.doc_id
        with pytest.raises(FileExistsError):
            write_bundle(doc, bdir, source=one_pdf, dpi=200, workers=4, overwrite=False)

    def test_three_pillars_text_extracted_images_in_json_and_md_plus_page_images(self, tmp_path: Path) -> None:
        import json

        from PIL import Image
        from reportlab.lib.utils import ImageReader

        from datagrunt.pdf.corpus import extract_corpus

        in_dir = tmp_path / "pillar_pdfs"
        in_dir.mkdir()
        for idx in range(2):
            img = Image.new("RGB", (80, 60), color=(20 * idx + 40, 100, 160))
            img_path = tmp_path / f"stamp_{idx}.png"
            img.save(img_path)
            pdf_path = in_dir / f"spec_{idx}.pdf"
            c = canvas.Canvas(str(pdf_path), pagesize=(612, 792))
            c.setFont("Helvetica-Bold", 18)
            c.drawString(72, 720, f"Section {idx + 1} Pump Assembly")
            c.setFont("Helvetica", 11)
            c.drawString(72, 690, f"Operating pressure: {150 + idx} PSI nominal.")
            c.drawImage(ImageReader(str(img_path)), 72, 560, width=80, height=60)
            c.showPage()
            c.save()

        out_dir = tmp_path / "pillar_bundles"
        manifest = extract_corpus(in_dir, out_dir, max_workers=2, ocr="never")
        assert len(manifest.entries) == 2
        for idx, entry in enumerate(manifest.entries):
            bdir = out_dir / entry.bundle_relpath
            doc_json = json.loads((bdir / "json" / "document.json").read_text(encoding="utf-8"))
            doc_md = (bdir / "markdown" / "document.md").read_text(encoding="utf-8")
            # Pillar 1: all text + formatting captured in JSON and Markdown
            assert f"# Section {idx + 1} Pump Assembly" in doc_md
            assert f"**Operating pressure:** {150 + idx} PSI nominal." in doc_md
            # Pillar 2: detected image extracted to disk AND path present in BOTH JSON and Markdown
            figs = [el for p in doc_json["pages"] for el in p["elements"] if el["kind"] == "figure"]
            assert len(figs) == 1
            fig_rel = figs[0]["image_path"]
            assert isinstance(fig_rel, str) and fig_rel.startswith("images/")
            assert (bdir / fig_rel).is_file()
            assert f"../{fig_rel}" in doc_md
            # Pillar 3: page images captured and saved in addition to extracted images
            page_rel = doc_json["pages"][0]["page_image"]["image_path"]
            assert page_rel == "page_images/page_0001.png"
            assert (bdir / page_rel).is_file()

    def test_corpus_proc_pool_runs_distinct_child_pids_and_succeeds_from_stdin(self, tmp_path: Path) -> None:
        import os
        import subprocess
        import sys
        from concurrent.futures import ThreadPoolExecutor

        from datagrunt.pdf import corpus

        assert hasattr(corpus, "_CorpusProcPool"), "Expected _CorpusProcPool in corpus.py"
        in_dir = tmp_path / "stdin_pdfs"
        in_dir.mkdir()
        for idx in range(4):
            p = in_dir / f"doc_{idx}.pdf"
            c = canvas.Canvas(str(p), pagesize=(612, 792))
            c.drawString(72, 700, f"Process worker test {idx}")
            c.showPage()
            c.save()

        items = corpus._resolve_inputs(in_dir)
        opts = corpus.PipelineOptions(ocr="never")
        out_pool = tmp_path / "out_pool"
        out_pool.mkdir()
        with corpus._CorpusProcPool() as proc_pool, ThreadPoolExecutor(max_workers=2) as threads:
            futs = [
                threads.submit(
                    corpus._run_in_corpus_worker,
                    proc_pool,
                    item,
                    out_pool,
                    None,
                    opts,
                    True,
                    include_images=True,
                    render_pages="vision_only",
                )
                for item in items[:2]
            ]
            results = [f.result(timeout=60) for f in futs]
        pids = {pid for pid, _ in results}
        assert os.getpid() not in pids
        assert all(isinstance(entry, corpus.CorpusEntry) for _, entry in results)

        # Must succeed when invoked from `python -` (<stdin>) where multiprocessing spawn fails
        out_stdin = tmp_path / "out_stdin"
        stdin_code = (
            "from pathlib import Path\n"
            "from datagrunt.pdf.corpus import extract_corpus\n"
            f"m = extract_corpus(Path({str(in_dir)!r}), Path({str(out_stdin)!r}), max_workers=2, ocr='never')\n"
            "assert len(m.entries) == 4\n"
        )
        completed = subprocess.run(
            [sys.executable, "-"],
            input=stdin_code,
            text=True,
            capture_output=True,
            check=False,
            timeout=60,
        )
        assert completed.returncode == 0, completed.stderr

    def test_corpus_proc_pool_fault_isolation_and_strict_raise(self, tmp_path: Path) -> None:
        from datagrunt.pdf.corpus import extract_corpus
        from datagrunt.pdf.exceptions import PDFCorruptError

        in_dir = tmp_path / "mixed_pdfs"
        in_dir.mkdir()
        for idx in (0, 2):
            p = in_dir / f"item_{idx}.pdf"
            c = canvas.Canvas(str(p), pagesize=(612, 792))
            c.drawString(72, 700, f"Valid {idx}")
            c.showPage()
            c.save()
        bad = in_dir / "item_1.pdf"
        bad.write_bytes(b"%PDF-1.7\nnot a valid xref table or trailer\n")

        m = extract_corpus(
            in_dir, tmp_path / "out_non_strict", max_workers=2, strict=False, ocr="never", render_pages="vision_only"
        )
        assert len(m.entries) == 2
        assert len(m.failures) == 1
        assert m.failures[0].error_type == "PDFCorruptError"

        with pytest.raises(PDFCorruptError):
            extract_corpus(
                in_dir, tmp_path / "out_strict", max_workers=2, strict=True, ocr="never", render_pages="vision_only"
            )


class TestAuditRegressionFixes:
    """Regression tests for all 4 cross-family gemini-auditor findings."""

    def test_figure_session_state_not_poisoned_on_errors_and_rejects_closed_handle(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from PIL import Image
        from reportlab.lib.utils import ImageReader

        from datagrunt.pdf import pdfium_engine
        from datagrunt.pdf.exceptions import PDFCorruptError
        from datagrunt.pdf.pdfium_engine import FigureSession, ImageLoader, PdfiumFailure, open_document

        img_p = tmp_path / "tile.png"
        Image.new("RGB", (64, 64), color=(80, 120, 200)).save(img_p)
        pdf_path = tmp_path / "two_figs.pdf"
        c = canvas.Canvas(str(pdf_path), pagesize=(612, 792))
        c.drawImage(ImageReader(str(img_p)), 72, 600, width=64, height=64)
        c.drawImage(ImageReader(str(img_p)), 200, 600, width=64, height=64)
        c.showPage()
        c.save()

        handle = open_document(pdf_path)
        raw_page = pdfium_engine.extract_page(handle, 0)
        assert not isinstance(raw_page, pdfium_engine.PageFailure)
        assert len(raw_page.images.images) == 2
        loaders = [ImageLoader(source=pdf_path, page_index=0, image=img) for img in raw_page.images.images]
        assert all(isinstance(ld, ImageLoader) for ld in loaders)

        # (1) _load_page raising PDFCorruptError on fig 0 must NOT poison fig 1 with AssertionError
        with FigureSession(handle) as session:
            with monkeypatch.context() as mp:

                def bad_load(_h: object, _idx: int) -> tuple[object, int]:
                    raise PDFCorruptError("corrupt page", page_number=1)

                mp.setattr(pdfium_engine, "_load_page", bad_load)
                with pytest.raises(PDFCorruptError):
                    session.read(loaders[0], "raw")
                with pytest.raises(PDFCorruptError):
                    session.read(loaders[1], "raw")

        # (2) _page_objects raising PdfiumFailure translates to PDFCorruptError and does not poison state
        with FigureSession(handle) as session:
            with monkeypatch.context() as mp:

                def bad_walk(_raw: object, *, max_objects: int) -> list[tuple[object, int]]:
                    raise PdfiumFailure("walk failed")

                mp.setattr(pdfium_engine, "_page_objects", bad_walk)
                with pytest.raises(PDFCorruptError, match="walk failed"):
                    session.read(loaders[0], "raw")
                with pytest.raises(PDFCorruptError, match="walk failed"):
                    session.read(loaders[1], "raw")

        # (3) Closing handle between two same-page reads raises ValueError instead of C-pointer UAF
        session = FigureSession(handle)
        assert len(session.read(loaders[0], "raw")) > 0
        handle.close()
        with pytest.raises(ValueError, match="document handle is closed"):
            session.read(loaders[1], "raw")
        session.close()

    def test_utm_cancellation_path_kept_and_unpainted_nonfinite_counted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import ctypes

        import pypdfium2 as pdfium
        import pypdfium2.raw as pdfium_c

        from datagrunt.pdf.pdfium_engine import open_document, page_segments

        # (1) Pre-cancellation float32 rounding inside GetBounds under large UTM translation
        utm_pdf = tmp_path / "utm_cancel.pdf"
        doc = pdfium.PdfDocument.new()
        page = pdfium_c.FPDFPage_New(doc.raw, 0, 612.0, 792.0)
        p_utm = pdfium_c.FPDFPageObj_CreateNewPath(536870912.0, 0.0)
        pdfium_c.FPDFPath_LineTo(p_utm, 536870976.0, 0.0)
        pdfium_c.FPDFPath_SetDrawMode(p_utm, pdfium_c.FPDF_FILLMODE_NONE, 1)
        mat = pdfium_c.FS_MATRIX(0.2499999850988388, 0.0, 0.0, 1.0, -134217720.0, 100.0)
        assert pdfium_c.FPDFPageObj_SetMatrix(p_utm, ctypes.byref(mat))
        pdfium_c.FPDFPage_InsertObject(page, p_utm)
        pdfium_c.FPDFPage_GenerateContent(page)
        pdfium_c.FPDF_ClosePage(page)
        doc.save(str(utm_pdf))
        doc.close()

        with open_document(utm_pdf) as h_utm:
            segs = page_segments(h_utm, 0)
        assert len(segs.segments) == 1
        assert segs.segments[0].end - segs.segments[0].start == pytest.approx(16.0, abs=1e-4)

        # (2) Unpainted path with non-finite point increments skipped_nonfinite_paths
        def unpainted_mode(_p: object, fill_out: ctypes.c_int, stroke_out: ctypes.c_int) -> int:
            fill_out.value = pdfium_c.FPDF_FILLMODE_NONE
            stroke_out.value = 0
            return 1

        def nan_point(_seg: object, x_out: ctypes.c_float, y_out: ctypes.c_float) -> int:
            x_out.value = float("nan")
            y_out.value = 10.0
            return 1

        with open_document(utm_pdf) as h_unp:
            monkeypatch.setattr(pdfium_c, "FPDFPath_GetDrawMode", unpainted_mode)
            monkeypatch.setattr(pdfium_c, "FPDFPathSegment_GetPoint", nan_point)
            got = page_segments(h_unp, 0)
        assert got.segments == ()
        assert got.stats.skipped_nonfinite_paths == 1

    def test_corpus_multi_worker_preserves_user_warnings_from_write_bundle(self, tmp_path: Path) -> None:
        import warnings

        from PIL import Image
        from reportlab.lib.utils import ImageReader

        from datagrunt.pdf.corpus import extract_corpus

        in_dir = tmp_path / "scan_pdfs"
        in_dir.mkdir()
        for idx in range(2):
            img_p = tmp_path / f"scan_{idx}.png"
            Image.new("RGB", (400, 400), color=(200, 200, 200)).save(img_p)
            pdf_p = in_dir / f"scan_{idx}.pdf"
            c = canvas.Canvas(str(pdf_p), pagesize=(612, 792))
            c.drawImage(ImageReader(str(img_p)), 36, 100, width=540, height=600)
            c.showPage()
            c.save()

        with warnings.catch_warnings(record=True) as w_single:
            warnings.simplefilter("always", UserWarning)
            extract_corpus(in_dir, tmp_path / "out_w1", max_workers=1, ocr="never", render_pages=False)
        with warnings.catch_warnings(record=True) as w_multi:
            warnings.simplefilter("always", UserWarning)
            extract_corpus(in_dir, tmp_path / "out_w2", max_workers=2, ocr="never", render_pages=False)

        assert len(w_single) == 2
        assert len(w_multi) == 2

    def test_corpus_worker_max_docs_retirement_and_strict_false_message_bounds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datagrunt.pdf import corpus
        from datagrunt.pdf.corpus import extract_corpus

        in_dir = tmp_path / "retire_pdfs"
        in_dir.mkdir()
        for idx in range(4):
            pdf_p = in_dir / f"doc_{idx}.pdf"
            c = canvas.Canvas(str(pdf_p), pagesize=(612, 792))
            c.drawString(72, 700, f"Doc {idx}")
            c.showPage()
            c.save()

        monkeypatch.setattr(corpus, "_MAX_WORKER_DOCS", 1)
        m_ok = extract_corpus(in_dir, tmp_path / "out_retire", max_workers=2, strict=True, ocr="never")
        assert len(m_ok.entries) == 4

        monkeypatch.setattr(corpus, "_MAX_WORKER_DOCS", 100_000)
        monkeypatch.setattr(corpus, "MAX_REQUEST_BYTES", 64)
        m_fail = extract_corpus(in_dir, tmp_path / "out_bound", max_workers=2, strict=False, ocr="never")
        assert len(m_fail.entries) == 0
        assert len(m_fail.failures) == 4
