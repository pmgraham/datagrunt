"""Tests for the shared OCR data->blocks helper."""

from datagrunt.core.pdf_io.extraction.config import _PDFExtractionConfig
from datagrunt.core.pdf_io.extraction.ocr import _data_to_blocks, dpi_for_page
from datagrunt.core.pdf_io.extraction.shapes import OcrBlock


def test_data_to_blocks_groups_line():
    data = {
        "text": ["Hello", "World"],
        "conf": [95, 90],
        "left": [100, 200],
        "top": [50, 50],
        "width": [80, 80],
        "height": [20, 20],
        "block_num": [1, 1],
        "par_num": [1, 1],
        "line_num": [1, 1],
    }
    blocks = _data_to_blocks(data, dpi=300)
    assert len(blocks) == 1
    b = blocks[0]
    assert isinstance(b, OcrBlock)
    assert b.text == "Hello World" and b.word_count == 2 and b.confidence == 92.5
    assert b.bbox.x == 24.0


class TestDpiForPageConfig:
    def test_no_config_preserves_legacy_behavior(self):
        assert dpi_for_page(1000, 1000) == 150
        assert dpi_for_page(1501, 100) == 75
        assert dpi_for_page(100, 1501) == 75
        assert dpi_for_page(1500, 1500) == 150  # boundary: strict >

    def test_config_drives_all_three_knobs(self):
        cfg = _PDFExtractionConfig(ocr_standard_dpi=300, ocr_large_format_dpi=100, ocr_large_format_dimension=2000)
        assert dpi_for_page(1999, 100, cfg) == 300
        assert dpi_for_page(2001, 100, cfg) == 100
        assert dpi_for_page(100, 2001, cfg) == 100

    def test_uniform_dpi_via_equal_knobs(self):
        cfg = _PDFExtractionConfig(ocr_standard_dpi=300, ocr_large_format_dpi=300)
        assert dpi_for_page(100, 100, cfg) == dpi_for_page(9999, 9999, cfg) == 300
