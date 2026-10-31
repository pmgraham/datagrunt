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


# --- #389: font ties go to the first occurrence; sizes keep the 4.x pick ---

# Five distinct names, each seen once (a five-way tie). Every rotation puts a
# different name first, so a winner picked by set/hash order cannot match the
# first occurrence in all of them, whatever the process's string-hash seed.
_TIED_FONTS = ["Gamma", "Alpha", "Delta", "Beta", "Epsilon"]

# Tied size lists. Sizes keep the 4.x pick, max(set(sizes), key=sizes.count):
# float hashes are not seeded, so it never varied between processes, but it is
# not the first occurrence (set({12.0, 8.0}) meets 8.0 first in either order).
_TIED_SIZES = [[12.0, 8.0], [8.0, 12.0], [16.0, 8.0], [10.5, 9.96, 11.04], [11.04, 9.96, 10.5, 11.04, 9.96, 10.5]]


def _rotations(values):
    return [values[i:] + values[:i] for i in range(len(values))]


def _legacy_size_pick(sizes):
    """The 4.x expression, verbatim: the oracle for the size rule."""
    return max(set(sizes), key=sizes.count)


def _line_items(fonts, sizes):
    """One line of items, left to right, with the given fonts and sizes."""
    return [
        TextItem(
            text=f"w{n}",
            x0=10.0 + 40 * n,
            x1=40.0 + 40 * n,
            y_top=50,
            y_bot=61,
            size=size,
            font=font,
            is_bold=False,
            is_italic=False,
        )
        for n, (font, size) in enumerate(zip(fonts, sizes))
    ]


def test_tied_size_cases_include_a_non_first_pick():
    # Guard: the size tests below would be vacuous if the 4.x pick always
    # matched the first occurrence.
    assert any(_legacy_size_pick(sizes) != sizes[0] for sizes in _TIED_SIZES)


def test_most_common_breaks_str_ties_by_first_occurrence():
    for values in _rotations(_TIED_FONTS):
        assert text_block_builder.most_common(values) == values[0]


def test_most_common_breaks_float_ties_by_first_occurrence():
    # The helper itself is type-agnostic; sizes just do not use it.
    assert text_block_builder.most_common([12.0, 8.0]) == 12.0
    assert text_block_builder.most_common([8.0, 12.0]) == 8.0


def test_most_common_non_tie_returns_most_frequent():
    assert text_block_builder.most_common(["Arial", "Times", "Times"]) == "Times"
    assert text_block_builder.most_common([9.0, 11.0, 9.0, 11.0, 11.0]) == 11.0


def test_most_common_empty_returns_default():
    assert text_block_builder.most_common([]) is None
    assert text_block_builder.most_common([], default="unknown") == "unknown"


def test_legacy_dominant_size_keeps_the_4x_pick():
    for sizes in _TIED_SIZES:
        assert text_block_builder.legacy_dominant_size(sizes, 0.0) == _legacy_size_pick(sizes)
    assert text_block_builder.legacy_dominant_size([12.0, 8.0], 0.0) == 8.0


def test_legacy_dominant_size_non_tie_returns_most_frequent():
    assert text_block_builder.legacy_dominant_size([9.0, 11.0, 9.0, 11.0, 11.0], 0.0) == 11.0


def test_legacy_dominant_size_empty_returns_default():
    assert text_block_builder.legacy_dominant_size([], 0) == 0


def test_dominant_font_breaks_ties_by_first_occurrence():
    for values in _rotations(_TIED_FONTS):
        assert TextBlockBuilder._dominant_font(values) == values[0]


def test_dominant_font_non_tie_returns_most_frequent():
    assert TextBlockBuilder._dominant_font(["Arial", "Times", "Times"]) == "Times"


def test_dominant_font_ignores_missing_names():
    # Missing names outnumber the real one but never win.
    assert TextBlockBuilder._dominant_font(["", "", None, "Arial"]) == "Arial"


def test_dominant_font_all_missing_is_empty():
    assert TextBlockBuilder._dominant_font([]) == ""
    assert TextBlockBuilder._dominant_font(["", None]) == ""


def test_dominant_size_keeps_the_4x_pick():
    for sizes in _TIED_SIZES:
        assert TextBlockBuilder._dominant_size(sizes) == _legacy_size_pick(sizes)


def test_dominant_size_non_tie_returns_most_frequent():
    assert TextBlockBuilder._dominant_size([9.0, 11.0, 11.0]) == 11.0


def test_dominant_size_ignores_missing_sizes():
    assert TextBlockBuilder._dominant_size([None, None, 10.0]) == 10.0


def test_dominant_size_all_missing_is_zero():
    assert TextBlockBuilder._dominant_size([]) == 0.0
    assert TextBlockBuilder._dominant_size([None]) == 0.0


def test_line_record_tied_fonts_take_the_leftmost_item():
    # _line_record orders a line's items by x0, so the leftmost run wins a tie,
    # whatever order the items arrive in.
    for fonts in _rotations(_TIED_FONTS):
        items = _line_items(fonts, [11.0] * len(fonts))
        assert TextBlockBuilder()._line_record(items[::-1])["font"] == fonts[0]


def test_line_record_tied_sizes_keep_the_4x_pick():
    # Sizes are read left to right (x0 order) and keep the 4.x set-order pick.
    for sizes in _TIED_SIZES:
        items = _line_items(["Arial"] * len(sizes), sizes)
        assert TextBlockBuilder()._line_record(items[::-1])["size"] == _legacy_size_pick(sizes)
    pinned = TextBlockBuilder()._line_record(_line_items(["Arial", "Arial"], [12.0, 8.0]))
    assert pinned["size"] == 8.0
