"""Tests for the pdfium extraction backend (extractors-compatible shapes)."""

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
