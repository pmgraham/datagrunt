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


class TestExtractTextBlocks:
    """Test suite for extract_text_blocks."""

    def test_extracts_header_and_body(self, sample_pdf):
        result = extractors.extract_text_blocks(sample_pdf, 0)
        assert result["status"] == "success"
        texts = [b["text"] for b in result["blocks"]]
        assert any("Quarterly Report" in t for t in texts)
        assert any("body text" in t for t in texts)

        # The 24pt title should classify as a header; 11pt line as body_text.
        classes = {b["text"]: b["classification"] for b in result["blocks"]}
        header_text = next(t for t in texts if "Quarterly Report" in t)
        assert classes[header_text] == "header"

        # Each block carries position + font metadata.
        block = result["blocks"][0]
        assert set(block["bbox"]) == {"x", "y", "w", "h"}
        assert "font" in block and "font_size" in block
        assert "reading_order" in block
