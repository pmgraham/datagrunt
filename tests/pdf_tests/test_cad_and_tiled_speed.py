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
