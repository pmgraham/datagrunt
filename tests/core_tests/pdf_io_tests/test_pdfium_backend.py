"""Tests for the pdfium extraction backend (extractors-compatible shapes)."""

import os

import pytest

from datagrunt.core.pdf_io import pdfium_backend


class TestAnalyzePage:
    def test_text_pdf_reports_text_layer(self, sample_pdf):
        result = pdfium_backend.analyze_page(sample_pdf, 0)
        assert result["status"] == "success"
        assert result["page_number"] == 0
        assert result["total_pages"] == 1
        assert result["width"] > 0 and result["height"] > 0
        assert result["has_text_layer"] is True
        assert result["is_scanned"] is False
        assert result["image_count"] >= 1
        # extractors.analyze_page key parity
        assert set(result) >= {
            "status", "page_number", "total_pages", "width", "height", "rotation",
            "has_text_layer", "is_scanned", "text_block_count", "image_count",
            "image_block_count", "has_line_drawings", "text_length",
        }

    def test_out_of_range_page(self, sample_pdf):
        result = pdfium_backend.analyze_page(sample_pdf, 99)
        assert result["status"] == "error"


class TestExtractImages:
    def test_metadata_only_without_output_dir(self, sample_pdf):
        result = pdfium_backend.extract_images(sample_pdf, 0)
        assert result["status"] == "success"
        assert len(result["images"]) >= 1
        img = result["images"][0]
        assert set(img) == {"file_path", "bbox", "width_px", "height_px", "format"}
        assert set(img["bbox"]) == {"x", "y", "w", "h"}
        assert img["file_path"] is None
        assert img["width_px"] >= 1 and img["height_px"] >= 1

    def test_writes_files_with_output_dir(self, sample_pdf, tmp_path):
        result = pdfium_backend.extract_images(sample_pdf, 0, output_dir=str(tmp_path))
        img = result["images"][0]
        assert img["file_path"] is not None
        assert os.path.isfile(img["file_path"])

    def test_skips_sub_threshold_images(self, small_image_pdf):
        result = pdfium_backend.extract_images(small_image_pdf, 0)
        assert result["images"] == []


class TestOcrPage:
    def test_ocr_recovers_text(self, scanned_pdf, tesseract_available):
        if not tesseract_available:
            pytest.skip("tesseract binary not available")
        result = pdfium_backend.ocr_page(scanned_pdf, 0, dpi=150)
        assert result["status"] == "success"
        assert result["ocr_engine"] == "tesseract"
        joined = " ".join(b["text"] for b in result["blocks"]).upper()
        assert "HELLO" in joined
        # extractors.ocr_page block shape parity
        b = result["blocks"][0]
        assert set(b) >= {"text", "bbox", "confidence", "word_count", "per_word_confidence"}
        assert set(b["bbox"]) == {"x", "y", "w", "h"}
