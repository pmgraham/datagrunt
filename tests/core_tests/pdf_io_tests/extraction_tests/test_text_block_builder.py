"""Tests for TextBlockBuilder."""

from datagrunt.core.pdf_io.extraction.shapes import TextItem
from datagrunt.core.pdf_io.extraction.text_block_builder import TextBlockBuilder, classify_font_size


def _item(text, y, size, x0=10.0):
    return TextItem(text=text, x0=x0, x1=x0 + 50, y_top=y, y_bot=y + size, size=size, font="Arial",
                    is_bold=False, is_italic=False)


def test_classify_font_size_header_vs_body():
    sizes = [11.0, 11.0, 11.0, 24.0]
    assert classify_font_size(24.0, False, sizes) == "header"
    assert classify_font_size(11.0, False, sizes) == "body_text"


def test_builder_groups_and_classifies():
    items = [_item("Big Title", 10, 24.0), _item("body one", 50, 11.0), _item("body two", 64, 11.0)]
    blocks = TextBlockBuilder().build(items)
    classes = {b.classification for b in blocks}
    assert "header" in classes
    assert "body_text" in classes
    assert any(b.text == "Big Title" for b in blocks)


def test_builder_empty():
    assert TextBlockBuilder().build([]) == []
