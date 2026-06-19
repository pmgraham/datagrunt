"""Tests for TextBlockBuilder."""

from datagrunt.core.pdf_io.extraction.shapes import TextItem
from datagrunt.core.pdf_io.extraction.text_block_builder import (
    TextBlockBuilder,
    classify_by_median,
    classify_font_size,
    page_median_size,
)


def _item(text, y, size, x0=10.0):
    return TextItem(
        text=text, x0=x0, x1=x0 + 50, y_top=y, y_bot=y + size, size=size, font="Arial", is_bold=False, is_italic=False
    )


def test_classify_font_size_header_vs_body():
    sizes = [11.0, 11.0, 11.0, 24.0]
    assert classify_font_size(24.0, sizes) == "header"
    assert classify_font_size(11.0, sizes) == "body_text"


def test_builder_groups_and_classifies():
    items = [_item("Big Title", 10, 24.0), _item("body one", 50, 11.0), _item("body two", 64, 11.0)]
    blocks = TextBlockBuilder().build(items)
    classes = {b.classification for b in blocks}
    assert "header" in classes
    assert "body_text" in classes
    assert any(b.text == "Big Title" for b in blocks)


def test_builder_empty():
    assert TextBlockBuilder().build([]) == []


def test_builder_column_extraction():
    # Spanning title
    title = TextItem(
        text="Spanning Title",
        x0=50,
        x1=450,
        y_top=50,
        y_bot=74,
        size=24.0,
        font="Arial",
        is_bold=True,
        is_italic=False,
    )
    # Left column items (y=100 and y=120)
    left1 = TextItem(
        text="left 1",
        x0=50,
        x1=150,
        y_top=100,
        y_bot=111,
        size=11.0,
        font="Arial",
        is_bold=False,
        is_italic=False,
    )
    left2 = TextItem(
        text="left 2",
        x0=50,
        x1=150,
        y_top=120,
        y_bot=131,
        size=11.0,
        font="Arial",
        is_bold=False,
        is_italic=False,
    )
    # Right column items (y=100 and y=120)
    right1 = TextItem(
        text="right 1",
        x0=350,
        x1=450,
        y_top=100,
        y_bot=111,
        size=11.0,
        font="Arial",
        is_bold=False,
        is_italic=False,
    )
    right2 = TextItem(
        text="right 2",
        x0=350,
        x1=450,
        y_top=120,
        y_bot=131,
        size=11.0,
        font="Arial",
        is_bold=False,
        is_italic=False,
    )

    # Pass them in non-sequential order
    items = [left2, right1, title, left1, right2]

    blocks = TextBlockBuilder().build(items)

    # We expect 3 blocks in exact reading order: Title -> Left Column (merged) -> Right Column (merged)
    assert len(blocks) == 3
    assert blocks[0].text == "Spanning Title"
    assert blocks[1].text == "left 1 left 2"
    assert blocks[2].text == "right 1 right 2"

    # Check reading order indices
    assert blocks[0].reading_order == 0
    assert blocks[1].reading_order == 1
    assert blocks[2].reading_order == 2


def test_page_median_size_matches_legacy_formula():
    sizes = [10.0, 11.0, 12.0, 24.0]
    # Legacy behavior: sorted(all_sizes)[len // 2] (NOT statistics.median).
    assert page_median_size(sizes) == 12.0
    assert page_median_size([]) is None


def test_classify_by_median_equivalent_to_classify_font_size():
    sizes = [10.0, 11.0, 11.0, 12.0, 24.0]
    median = page_median_size(sizes)
    for fs in (8.0, 11.0, 14.0, 24.0):
        assert classify_by_median(fs, median) == classify_font_size(fs, sizes)


def test_classify_by_median_empty_is_body_text():
    assert classify_by_median(24.0, None) == "body_text"
