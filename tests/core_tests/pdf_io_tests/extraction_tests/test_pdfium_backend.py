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
