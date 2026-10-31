"""Tests for TextBlockBuilder."""

from datagrunt.core.pdf_io.extraction import text_block_builder
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


# --- #389: the dominant value breaks ties by first occurrence, never hash order ---

# Five distinct names, each seen once (a five-way tie). Every rotation puts a
# different name first, so a winner picked by set/hash order cannot match the
# first occurrence in all of them, whatever the process's string-hash seed.
_TIED_FONTS = ["Gamma", "Alpha", "Delta", "Beta", "Epsilon"]


def _rotations(values):
    return [values[i:] + values[:i] for i in range(len(values))]


def test_most_common_breaks_str_ties_by_first_occurrence():
    for values in _rotations(_TIED_FONTS):
        assert text_block_builder.most_common(values) == values[0]


def test_most_common_breaks_float_ties_by_first_occurrence():
    # set({12.0, 8.0}) iterates 8.0 first (float hashes are not seeded), so
    # the hash-order pick is 8.0 for both orders.
    assert text_block_builder.most_common([12.0, 8.0]) == 12.0
    assert text_block_builder.most_common([8.0, 12.0]) == 8.0


def test_most_common_non_tie_returns_most_frequent():
    assert text_block_builder.most_common(["Arial", "Times", "Times"]) == "Times"
    assert text_block_builder.most_common([9.0, 11.0, 9.0, 11.0, 11.0]) == 11.0


def test_most_common_empty_returns_default():
    assert text_block_builder.most_common([]) is None
    assert text_block_builder.most_common([], default="unknown") == "unknown"


def test_dominant_breaks_str_ties_by_first_occurrence():
    for values in _rotations(_TIED_FONTS):
        assert TextBlockBuilder._dominant(values, default="") == values[0]


def test_dominant_breaks_float_ties_by_first_occurrence():
    assert TextBlockBuilder._dominant([12.0, 8.0], default=0.0) == 12.0
    assert TextBlockBuilder._dominant([8.0, 12.0], default=0.0) == 8.0


def test_dominant_non_tie_returns_most_frequent():
    assert TextBlockBuilder._dominant(["Arial", "Times", "Times"], default="") == "Times"
    assert TextBlockBuilder._dominant([9.0, 11.0, 11.0], default=0.0) == 11.0


def test_dominant_ignores_missing_values():
    # Missing fonts/sizes outnumber the real one but never win.
    assert TextBlockBuilder._dominant(["", "", "Arial"], default="") == "Arial"
    assert TextBlockBuilder._dominant([None, None, 10.0], default=0.0) == 10.0


def test_dominant_all_missing_returns_default():
    assert TextBlockBuilder._dominant([], default="") == ""
    assert TextBlockBuilder._dominant(["", None], default=0.0) == 0.0


def test_line_record_tied_fonts_take_the_leftmost_item():
    # _line_record orders a line's items by x0, so the leftmost run wins a tie,
    # whatever order the items arrive in.
    for fonts in _rotations(_TIED_FONTS):
        items = [
            TextItem(
                text=f"w{n}",
                x0=10.0 + 40 * n,
                x1=40.0 + 40 * n,
                y_top=50,
                y_bot=61,
                size=11.0,
                font=font,
                is_bold=False,
                is_italic=False,
            )
            for n, font in enumerate(fonts)
        ]
        assert TextBlockBuilder()._line_record(items[::-1])["font"] == fonts[0]
