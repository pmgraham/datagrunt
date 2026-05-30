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


class TestExtractTables:
    """Test suite for extract_tables."""

    def test_no_tables_on_plain_page(self, sample_pdf):
        # The sample page has no ruled table; should succeed with empty list.
        result = extractors.extract_tables(sample_pdf, 0)
        assert result["status"] == "success"
        assert result["tables"] == []

    def test_out_of_range_page(self, sample_pdf):
        result = extractors.extract_tables(sample_pdf, 99)
        assert result["status"] == "error"


class TestExtractImages:
    """Test suite for extract_images."""

    def test_metadata_only_when_no_output_dir(self, sample_pdf):
        result = extractors.extract_images(sample_pdf, 0)
        assert result["status"] == "success"
        assert len(result["images"]) >= 1
        img = result["images"][0]
        assert img["file_path"] is None
        assert img["width_px"] >= 40 and img["height_px"] >= 40
        assert "format" in img
        assert set(img["bbox"]) == {"x", "y", "w", "h"}

    def test_writes_files_when_output_dir_given(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        result = extractors.extract_images(
            sample_pdf, 0, output_dir=str(out), name_prefix="report"
        )
        assert result["status"] == "success"
        img = result["images"][0]
        assert img["file_path"] is not None
        import os
        assert os.path.isfile(img["file_path"])
        assert os.path.basename(img["file_path"]).startswith("report_page0_img")
