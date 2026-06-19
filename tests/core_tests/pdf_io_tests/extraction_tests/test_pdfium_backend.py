"""Tests for PdfiumBackend."""

import pytest

from datagrunt.core.pdf_io.extraction.pdfium_backend import PdfiumBackend
from datagrunt.core.pdf_io.extraction.shapes import ImageBlock, OcrBlock, PageAnalysis, TextBlock


class TestPdfiumBackend:
    def test_analyze_page(self, sample_pdf):
        a = PdfiumBackend(sample_pdf).analyze_page(0)
        assert isinstance(a, PageAnalysis)
        assert a.has_text_layer is True and a.is_scanned is False and a.image_count >= 1

    def test_text_blocks_classified(self, sample_pdf):
        blocks = PdfiumBackend(sample_pdf).extract_text_blocks(0)
        assert blocks and all(isinstance(b, TextBlock) for b in blocks)
        assert "header" in {b.classification for b in blocks}
        assert "Quarterly Report" in " ".join(b.text for b in blocks)

    def test_images(self, sample_pdf, tmp_path):
        imgs = PdfiumBackend(sample_pdf).extract_images(0, output_dir=str(tmp_path))
        assert imgs and all(isinstance(i, ImageBlock) for i in imgs)
        assert imgs[0].file_path is not None

    def test_ocr(self, scanned_pdf, tesseract_available):
        if not tesseract_available:
            pytest.skip("tesseract not available")
        blocks = PdfiumBackend(scanned_pdf).ocr_page(0, dpi=150)
        assert blocks and all(isinstance(b, OcrBlock) for b in blocks)
        assert "HELLO" in " ".join(b.text for b in blocks).upper()

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PdfiumBackend("nope.pdf")

    def test_pdfium_extract_page_matches_primitives(self, sample_pdf):
        """Single-pass extract_page returns the same analysis/text/images as the
        three primitives called separately."""
        backend = PdfiumBackend(sample_pdf)
        with backend:
            analysis, text_blocks, images = backend.extract_page(0)
            assert analysis == backend.analyze_page(0)
            assert text_blocks == backend.extract_text_blocks(0)
            assert images == backend.extract_images(0)
        assert text_blocks  # sample_pdf has a text layer

    def test_pdfium_extract_page_opens_page_once(self, sample_pdf, monkeypatch):
        """extract_page must open the pdfium page (and textpage) once, not three times."""
        from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument

        count = {"n": 0}
        orig_page = PdfiumDocument.page

        def spy_page(self, page_number):
            count["n"] += 1
            return orig_page(self, page_number)

        monkeypatch.setattr(PdfiumDocument, "page", spy_page)

        backend = PdfiumBackend(sample_pdf)
        with backend:
            backend.extract_page(0)

        assert count["n"] == 1
