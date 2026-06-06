"""Tests for the shared OCR data->blocks helper."""

from datagrunt.core.pdf_io.extraction.ocr import _data_to_blocks
from datagrunt.core.pdf_io.extraction.shapes import OcrBlock


def test_data_to_blocks_groups_line():
    data = {
        "text": ["Hello", "World"], "conf": [95, 90],
        "left": [100, 200], "top": [50, 50], "width": [80, 80], "height": [20, 20],
        "block_num": [1, 1], "par_num": [1, 1], "line_num": [1, 1],
    }
    blocks = _data_to_blocks(data, dpi=300)
    assert len(blocks) == 1
    b = blocks[0]
    assert isinstance(b, OcrBlock)
    assert b.text == "Hello World" and b.word_count == 2 and b.confidence == 92.5
    assert b.bbox.x == 24.0
