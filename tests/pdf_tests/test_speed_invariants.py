"""Tests for zero-output-change FFI, trusted-constructor, and single-load speed invariants."""

from __future__ import annotations

import math
import pickle
from pathlib import Path

import pypdfium2.raw as pdfium_c
import pytest
from reportlab.pdfgen import canvas

from datagrunt.pdf import pdfium_engine
from datagrunt.pdf._path_segments import IDENTITY, Matrix, PageSegments, RawSegment, SegmentStats
from datagrunt.pdf._pdfium_text import RawSpan
from datagrunt.pdf.pdfium_engine import (
    ObjectBoxes,
    RawPage,
    extract_page,
    open_document,
    page_geometry,
    page_images,
    page_object_boxes,
    page_segments,
    page_text,
)


def _build_multi_path_pdf(path: Path, path_count: int = 40) -> Path:
    c = canvas.Canvas(str(path), pagesize=(600, 400))
    c.setStrokeColorRGB(0.1, 0.1, 0.1)
    c.setLineWidth(0.6)
    for i in range(path_count):
        x = 20.0 + (i % 10) * 55.0
        y = 30.0 + (i // 10) * 40.0
        span = 18.0 if i % 2 == 0 else 6.0
        c.line(x, y, x + span, y)
    c.showPage()
    c.save()
    return path


class TestMatrixAndTrustedConstructors:
    """Node 1: Matrix.is_finite() and _trusted internal constructors preserve equality and boundary checks."""

    def test_matrix_is_finite_detects_nonfinite_entries(self) -> None:
        assert IDENTITY.is_finite() is True
        assert Matrix(2.0, -0.5, 0.5, 2.0, 100.0, -50.0).is_finite() is True
        for idx in range(6):
            vals = [1.0, 0.0, 0.0, 1.0, 0.0, 0.0]
            vals[idx] = math.nan if idx % 2 == 0 else math.inf
            assert Matrix(*vals).is_finite() is False

    def test_object_boxes_trusted_matches_validated_and_checks_boundaries(self) -> None:
        boxes = ((1.0, 2.0, 10.0, 12.0), (20.0, 30.0, 25.0, 35.0))
        glyphs = ((20.0, 30.0, 25.0, 35.0),)
        trusted = ObjectBoxes._trusted(boxes, 1, glyphs)
        validated = ObjectBoxes(boxes=boxes, unboxed=1, glyph_stroke_boxes=glyphs)
        assert trusted == validated
        assert pickle.loads(pickle.dumps(trusted)) == validated

        with pytest.raises(TypeError):
            ObjectBoxes._trusted(list(boxes), 0, ())  # type: ignore[arg-type]
        with pytest.raises((TypeError, ValueError)):
            ObjectBoxes._trusted(boxes, -1, ())
        with pytest.raises((TypeError, ValueError)):
            ObjectBoxes._trusted(boxes, True, ())  # type: ignore[arg-type]

    def test_raw_segment_and_page_segments_trusted_match_validated(self) -> None:
        seg_trusted = RawSegment._trusted("h", 10.0, 5.0, 25.0, 0.5, "stroke", 3, False, (0, 0, 0, 255))
        seg_valid = RawSegment(
            orientation="h",
            position=10.0,
            start=5.0,
            end=25.0,
            width=0.5,
            kind="stroke",
            object_index=3,
            dashed=False,
            paint_rgba=(0, 0, 0, 255),
        )
        assert seg_trusted == seg_valid
        stats = SegmentStats(skipped_nonfinite_paths=2)
        ps_trusted = PageSegments._trusted((seg_trusted,), stats)
        ps_valid = PageSegments(segments=(seg_valid,), stats=stats)
        assert ps_trusted == ps_valid
        assert pickle.loads(pickle.dumps(ps_trusted)) == ps_valid

        with pytest.raises(ValueError):
            RawSegment._trusted("h", 10.0, 25.0, 5.0, 0.5, "stroke", 3, False, (0, 0, 0, 255))
        with pytest.raises(TypeError):
            PageSegments._trusted([seg_trusted], stats)  # type: ignore[arg-type]

    def test_raw_span_trusted_matches_validated_and_rejects_bad_inputs(self) -> None:
        cboxes = ((10.0, 20.0, 15.0, 30.0), (15.0, 20.0, 20.0, 30.0))
        bbox = (10.0, 20.0, 20.0, 30.0)
        trusted = RawSpan._trusted(
            text="AB",
            bbox=bbox,
            char_boxes=cboxes,
            tight_boxes=cboxes,
            font_size_pt=10.0,
            em_width_pt=10.0,
            base_font_name="Helvetica",
            is_bold=False,
            is_italic=False,
            render_mode=0,
            fill_rgba=(0, 0, 0, 255),
            stroke_rgba=(0, 0, 0, 255),
            object_index=1,
            generated_breaks=(),
            map_errors=(),
        )
        validated = RawSpan(
            text="AB",
            bbox=bbox,
            char_boxes=cboxes,
            tight_boxes=cboxes,
            font_size_pt=10.0,
            em_width_pt=10.0,
            base_font_name="Helvetica",
            is_bold=False,
            is_italic=False,
            render_mode=0,
            fill_rgba=(0, 0, 0, 255),
            stroke_rgba=(0, 0, 0, 255),
            object_index=1,
            generated_breaks=(),
            map_errors=(),
        )
        assert trusted == validated
        assert pickle.loads(pickle.dumps(trusted)) == validated

        with pytest.raises(ValueError):
            RawSpan._trusted(
                text="",
                bbox=bbox,
                char_boxes=(),
                tight_boxes=(),
                font_size_pt=10.0,
                em_width_pt=10.0,
                base_font_name="Helvetica",
                is_bold=False,
                is_italic=False,
                render_mode=0,
                fill_rgba=(0, 0, 0, 255),
                stroke_rgba=(0, 0, 0, 255),
                object_index=1,
                generated_breaks=(),
                map_errors=(),
            )


class TestFusedPathWalkInvariants:
    """Node 2: _read_stages queries FPDFPageObj_GetBounds and FPDFPath_GetDrawMode at most once per path."""

    def test_extract_page_queries_bounds_and_draw_mode_once_per_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pdf_path = _build_multi_path_pdf(tmp_path / "paths.pdf", path_count=40)
        bounds_calls = 0
        draw_mode_calls = 0
        get_type_calls = 0
        real_bounds = pdfium_c.FPDFPageObj_GetBounds
        real_draw_mode = pdfium_c.FPDFPath_GetDrawMode
        real_get_type = pdfium_c.FPDFPageObj_GetType

        def counting_get_type(*args: object) -> int:
            nonlocal get_type_calls
            get_type_calls += 1
            return real_get_type(*args)

        def counting_bounds(*args: object) -> int:
            nonlocal bounds_calls
            bounds_calls += 1
            return real_bounds(*args)

        def counting_draw_mode(*args: object) -> int:
            nonlocal draw_mode_calls
            draw_mode_calls += 1
            return real_draw_mode(*args)

        with open_document(pdf_path) as handle:
            expected = RawPage(
                geometry=page_geometry(handle, 0),
                text=page_text(handle, 0),
                segments=page_segments(handle, 0),
                images=page_images(handle, 0),
                object_boxes=page_object_boxes(handle, 0),
            )
            monkeypatch.setattr(pdfium_c, "FPDFPageObj_GetBounds", counting_bounds)
            monkeypatch.setattr(pdfium_c, "FPDFPath_GetDrawMode", counting_draw_mode)
            monkeypatch.setattr(pdfium_c, "FPDFPageObj_GetType", counting_get_type)
            got = extract_page(handle, 0)

        assert got == expected
        assert bounds_calls <= 45
        assert draw_mode_calls <= 45
        assert get_type_calls <= 45

    def test_segment_overflow_still_collects_all_object_boxes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pdf_path = _build_multi_path_pdf(tmp_path / "overflow.pdf", path_count=30)
        with open_document(pdf_path) as handle:
            expected_boxes = page_object_boxes(handle, 0)
            monkeypatch.setattr(pdfium_engine, "MAX_PAGE_SEGMENTS", 10)
            got = extract_page(handle, 0, allow_segment_overflow=True)

        assert isinstance(got, RawPage)
        assert got.segments.segments == ()
        assert got.segments.stats.segment_bound_hit is not None
        assert got.object_boxes == expected_boxes


def _build_banner_cad_pdf(path: Path, pages: int = 3) -> Path:
    c = canvas.Canvas(str(path), pagesize=(600, 400))
    for p in range(pages):
        c.setFillColorRGB(0.08, 0.16, 0.40)
        c.rect(30.0, 330.0, 540.0, 40.0, stroke=0, fill=1)
        c.setFillColorRGB(1.0, 1.0, 1.0)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(45.0, 344.0, f"CAD BANNER PAGE {p + 1}")
        c.setStrokeColorRGB(0.15, 0.15, 0.15)
        c.setLineWidth(0.5)
        c.rect(30.0, 50.0, 540.0, 240.0, stroke=1, fill=0)
        c.line(30.0, 170.0, 570.0, 170.0)
        c.line(300.0, 50.0, 300.0, 290.0)
        c.showPage()
    c.save()
    return path


class TestSingleLoadAndSingleHashInvariants:
    """Node 3: Single PdfPage load per page (extract + background + bundle render) and single SHA-256 read."""

    def test_extract_document_fuses_background_into_single_page_load(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import pypdfium2 as pdfium

        from datagrunt.pdf.pipeline import extract_document

        pdf_path = _build_banner_cad_pdf(tmp_path / "banner.pdf", pages=3)
        expected_doc = extract_document(pdf_path, ocr="never", sandbox=True)

        get_page_calls = 0
        real_get_page = pdfium.PdfDocument.get_page

        def counting_get_page(self_doc: object, index: int) -> object:
            nonlocal get_page_calls
            get_page_calls += 1
            return real_get_page(self_doc, index)  # type: ignore[arg-type]

        monkeypatch.setattr(pdfium.PdfDocument, "get_page", counting_get_page)
        got_doc = extract_document(pdf_path, ocr="never", sandbox=False)

        assert get_page_calls == 3
        assert got_doc.to_markdown() == expected_doc.to_markdown()

    def test_extract_corpus_loads_vector_pages_once_and_hashes_source_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import pypdfium2 as pdfium

        from datagrunt.pdf import _source
        from datagrunt.pdf.corpus import extract_corpus

        pdf_path = _build_banner_cad_pdf(tmp_path / "cad_corpus.pdf", pages=3)
        get_page_calls = 0
        sha_reads = 0
        real_get_page = pdfium.PdfDocument.get_page
        real_read_chunk = _source._read_chunk

        def counting_get_page(self_doc: object, index: int) -> object:
            nonlocal get_page_calls
            get_page_calls += 1
            return real_get_page(self_doc, index)  # type: ignore[arg-type]

        def counting_read_chunk(stream: object, size: int) -> bytes:
            nonlocal sha_reads
            chunk = real_read_chunk(stream, size)  # type: ignore[arg-type]
            if chunk:
                sha_reads += 1
            return chunk

        monkeypatch.setattr(pdfium.PdfDocument, "get_page", counting_get_page)
        monkeypatch.setattr(_source, "_read_chunk", counting_read_chunk)
        manifest = extract_corpus(
            [pdf_path],
            tmp_path / "out_corpus",
            max_workers=1,
            workers=1,
            ocr="never",
            include_images=True,
            render_pages=True,
            strict=True,
        )

        assert len(manifest.entries) == 1
        assert get_page_calls == 3
        assert sha_reads == 1

    def test_verified_source_toctou_detects_changed_file(self, tmp_path: Path) -> None:
        from datagrunt.pdf._source import SourceIdentity
        from datagrunt.pdf.bundle import _write_bundle_internal
        from datagrunt.pdf.exceptions import PDFSourceChangedError
        from datagrunt.pdf.pipeline import extract_document

        pdf_path = _build_banner_cad_pdf(tmp_path / "toctou.pdf", pages=1)
        doc = extract_document(pdf_path, ocr="never")
        stale_id = SourceIdentity(device=1, inode=1, size=1, mtime_ns=1)

        with pytest.raises(PDFSourceChangedError, match="changed before extraction began"):
            extract_document(pdf_path, ocr="never", _verified_source=(stale_id, doc.metadata.sha256))

        # _write_bundle_internal falls back to _stream_sha256 when identity differs, succeeding if bytes match
        written = _write_bundle_internal(
            doc,
            tmp_path / "out_bun",
            source=pdf_path,
            verified_source=(stale_id, "0" * 64),
        )
        assert written.metadata.sha256 == doc.metadata.sha256

    def test_draw_open_page_and_prerender_refusal_and_type_guards(self, tmp_path: Path) -> None:
        from datagrunt.pdf import _page_worker, _render_worker
        from datagrunt.pdf.bundle import _write_bundle_internal
        from datagrunt.pdf.pipeline import PDFSource, PipelineOptions, extract_document
        from datagrunt.pdf.render import RenderRefused

        pdf_path = _build_banner_cad_pdf(tmp_path / "guards.pdf", pages=1)
        with pytest.raises(TypeError):
            _render_worker.draw_open_page_in_process(
                "not_a_page",  # type: ignore[arg-type]
                "not_geom",  # type: ignore[arg-type]
                [],
                [],
                None,
                pdf_path,
                dpi=72,
            )
        with pytest.raises(TypeError):
            _page_worker._capture_page_prerender(
                "not_a_page",
                "not_info",
                PDFSource(path=pdf_path),
                PipelineOptions(),
            )

        with open_document(pdf_path) as handle:
            geom = page_geometry(handle, 0)
            page = handle._require_open()[0]
            try:
                with pytest.raises(RenderRefused, match="oversized test refusal"):
                    _render_worker.draw_open_page_in_process(
                        page,
                        geom,
                        [],
                        [],
                        "oversized test refusal",
                        pdf_path,
                        dpi=72,
                    )
            finally:
                page.close()

        # Finding 1: Bounded _PrerenderCache enforces byte cap and evicts consumed entries on bundle write
        from datagrunt.pdf.corpus import _MAX_PRERENDER_CACHE_BYTES, _PrerenderCache

        tiny_cache = _PrerenderCache(max_bytes=32)
        tiny_cache.put(0, (200, 10, 10, b"x" * 20))
        assert 0 in tiny_cache
        # Exceeding cap refuses further entries without raising
        tiny_cache.put(1, (200, 10, 10, b"y" * 20))
        assert 1 not in tiny_cache
        assert tiny_cache.is_full is True
        # Consuming pops entry and frees bytes
        popped = tiny_cache.pop_entry(0)
        assert popped == (200, 10, 10, b"x" * 20)
        assert len(tiny_cache) == 0
        assert _MAX_PRERENDER_CACHE_BYTES == 128 * 1024 * 1024

        # Finding 2: _placed_from_entries matches own.then(IDENTITY) IEEE-754 nan propagation on non-finite CTM
        with open_document(pdf_path) as handle:
            page = handle._require_open()[0]
            try:
                objs = pdfium_engine._page_objects(page.raw, max_objects=100)
                inf_fs = pdfium_c.FS_MATRIX(math.inf, 0.0, 0.0, math.inf, 0.0, 0.0)
                monkeypatch_orig = pdfium_c.FPDFPageObj_GetMatrix
                try:

                    def fake_get_mat(_obj: object, out: object) -> int:
                        for k in ("a", "b", "c", "d", "e", "f"):
                            setattr(out, k, getattr(inf_fs, k))
                        return 1

                    pdfium_c.FPDFPageObj_GetMatrix = fake_get_mat
                    ((_idx, _o, placed_mat),) = list(pdfium_engine._placed_from_entries([(0, objs[0][0], IDENTITY)]))
                finally:
                    pdfium_c.FPDFPageObj_GetMatrix = monkeypatch_orig
                assert math.isnan(placed_mat.b) and math.isnan(placed_mat.c)
            finally:
                page.close()

        # Finding 3: Extract at low DPI where narrow page refuses (0 px), then write_bundle at 300 DPI succeeds
        narrow_pdf = tmp_path / "narrow_strip.pdf"
        cn = canvas.Canvas(str(narrow_pdf), pagesize=(1.0, 100.0))
        cn.setFont("Helvetica", 6)
        cn.drawString(0.1, 50.0, "X")
        cn.showPage()
        cn.save()
        pre_narrow = _PrerenderCache()
        doc_narrow = extract_document(narrow_pdf, ocr="never", dpi=36, _prerender_out=pre_narrow)
        # 1.0 pt at 36 DPI rounds to 0 px -> not cached; writing at dpi=300 redraws cleanly at 4 x 417 px!
        assert len(pre_narrow) == 0
        written_narrow = _write_bundle_internal(
            doc_narrow,
            tmp_path / "bun_narrow_300",
            source=narrow_pdf,
            dpi=300,
            prerendered=pre_narrow,
        )
        assert written_narrow.pages[0].page_image is not None
        assert written_narrow.pages[0].page_image.image_path is not None

    def test_trusted_constructors_reject_nan_float_indices_and_mutable_containers(self) -> None:
        # Finding 4a: RawSegment._trusted rejects NaN/Inf floats and non-int object_index
        with pytest.raises((TypeError, ValueError)):
            RawSegment._trusted("h", math.nan, 0.0, 10.0, 1.0, "stroke", 0, False, (0, 0, 0, 255))
        with pytest.raises((TypeError, ValueError)):
            RawSegment._trusted("h", 5.0, -math.inf, math.inf, 1.0, "stroke", 0, False, (0, 0, 0, 255))
        with pytest.raises((TypeError, ValueError)):
            RawSegment._trusted("h", 5.0, 0.0, 10.0, math.nan, "stroke", 0, False, (0, 0, 0, 255))
        with pytest.raises((TypeError, ValueError)):
            RawSegment._trusted("h", 5.0, 0.0, 10.0, 1.0, "stroke", 1.5, False, (0, 0, 0, 255))  # type: ignore[arg-type]
        with pytest.raises((TypeError, ValueError)):
            RawSegment._trusted("h", 5.0, 0.0, 10.0, 1.0, "stroke", 0, 1, (0, 0, 0, 255))  # type: ignore[arg-type]

        # Finding 4b: RawSpan._trusted rejects NaN/negative em_width, list containers, and non-bool flags
        cbox = ((0.0, 0.0, 5.0, 10.0),)
        with pytest.raises((TypeError, ValueError)):
            RawSpan._trusted(
                text="A",
                bbox=cbox[0],
                char_boxes=cbox,
                tight_boxes=list(cbox),  # type: ignore[arg-type]
                font_size_pt=10.0,
                em_width_pt=10.0,
                base_font_name="Helvetica",
                is_bold=False,
                is_italic=False,
                render_mode=0,
                fill_rgba=(0, 0, 0, 255),
                stroke_rgba=(0, 0, 0, 255),
                object_index=0,
                generated_breaks=(),
                map_errors=(),
            )
        with pytest.raises((TypeError, ValueError)):
            RawSpan._trusted(
                text="A",
                bbox=cbox[0],
                char_boxes=cbox,
                tight_boxes=cbox,
                font_size_pt=math.nan,
                em_width_pt=10.0,
                base_font_name="Helvetica",
                is_bold=False,
                is_italic=False,
                render_mode=0,
                fill_rgba=(0, 0, 0, 255),
                stroke_rgba=(0, 0, 0, 255),
                object_index=0,
                generated_breaks=(),
                map_errors=(),
            )
        with pytest.raises((TypeError, ValueError)):
            RawSpan._trusted(
                text="A",
                bbox=cbox[0],
                char_boxes=cbox,
                tight_boxes=cbox,
                font_size_pt=10.0,
                em_width_pt=-1.0,
                base_font_name="Helvetica",
                is_bold=False,
                is_italic=False,
                render_mode=0,
                fill_rgba=(0, 0, 0, 255),
                stroke_rgba=(0, 0, 0, 255),
                object_index=0,
                generated_breaks=(),
                map_errors=(),
            )
        with pytest.raises((TypeError, ValueError)):
            RawSpan._trusted(
                text="A",
                bbox=cbox[0],
                char_boxes=cbox,
                tight_boxes=cbox,
                font_size_pt=10.0,
                em_width_pt=10.0,
                base_font_name="Helvetica",
                is_bold=1,  # type: ignore[arg-type]
                is_italic=False,
                render_mode=0,
                fill_rgba=(0, 0, 0, 255),
                stroke_rgba=(0, 0, 0, 255),
                object_index=0,
                generated_breaks=(),
                map_errors=(),
            )
