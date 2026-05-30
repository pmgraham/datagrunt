"""Tests for the pure PDF extractor functions."""

import pytest

from datagrunt.core.pdf_io import extractors


class TestAnalyzePage:
    """Test suite for analyze_page."""

    def test_native_text_page(self, sample_pdf):
        result = extractors.analyze_page(sample_pdf, 0)
        assert result["status"] == "success"
        assert result["page_number"] == 0
        assert result["total_pages"] == 1
        assert result["width"] == pytest.approx(612, abs=1)
        assert result["height"] == pytest.approx(792, abs=1)
        assert result["has_text_layer"] is True
        assert result["is_scanned"] is False
        assert result["image_count"] >= 1

    def test_out_of_range_page(self, sample_pdf):
        result = extractors.analyze_page(sample_pdf, 99)
        assert result["status"] == "error"
        assert "out of range" in result["message"]
