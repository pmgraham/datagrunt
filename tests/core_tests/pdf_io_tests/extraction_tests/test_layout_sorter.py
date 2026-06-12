"""Tests for layout-aware partitioning (PageLayoutSorter).

These focus on hardening ``partition`` against pathological coordinates that a
corrupt or cropped PDF can produce: off-page (negative) positions, non-finite
values (NaN/inf), and absurdly large finite coordinates. Each must degrade
gracefully (return the items as segments) instead of crashing, hanging, or
allocating gigabytes -- while still partitioning a normal multi-column page
correctly.
"""

import time

from datagrunt.core.pdf_io.extraction.layout_sorter import (
    MAX_HISTOGRAM_BINS,
    PageLayoutSorter,
    TextItemAdapter,
)
from datagrunt.core.pdf_io.extraction.shapes import TextItem


def _text_item(x0: float, x1: float, y: float, text: str = "word") -> TextItem:
    """Build a TextItem at the given horizontal span and y position."""
    return TextItem(
        text=text,
        x0=x0,
        x1=x1,
        y_top=y,
        y_bot=y + 8,
        size=10,
        font="F",
        is_bold=False,
        is_italic=False,
    )


def _flatten(segments: list[list]) -> list:
    """Collapse partition segments into a flat list of items."""
    return [item for segment in segments for item in segment]


class TestPartitionPathologicalCoordinates:
    """partition() must not crash/hang on off-page or corrupt coordinates."""

    def test_off_page_negative_coordinates_do_not_raise(self):
        """Negative (off-page) x-coordinates previously caused IndexError.

        min_x=-90 with page_width=140 drove the gutter search index negative,
        which either wrapped via Python negative indexing or raised IndexError.
        After clamping, partition returns all items intact.
        """
        items = [
            _text_item(-90, -80, 10),
            _text_item(-90, -80, 30),
            _text_item(40, 50, 50),
            _text_item(45, 50, 70),
        ]

        segments = PageLayoutSorter(TextItemAdapter()).partition(items)

        assert _flatten(segments) and len(_flatten(segments)) == len(items)
        assert {id(i) for i in _flatten(segments)} == {id(i) for i in items}

    def test_nan_coordinate_does_not_raise(self):
        """A NaN x-coordinate slipped past the page_width guard and crashed.

        ``NaN < 100`` is False, so the old code proceeded to int(NaN) and raised
        ValueError. The finiteness guard now skips partitioning instead.
        """
        nan = float("nan")
        items = [
            _text_item(nan, 10, 10),
            _text_item(0, 10, 30),
            _text_item(40, 50, 50),
            _text_item(45, 50, 70),
        ]

        segments = PageLayoutSorter(TextItemAdapter()).partition(items)

        assert len(_flatten(segments)) == len(items)

    def test_nan_coordinate_on_non_first_item_does_not_raise(self):
        """A NaN on a NON-first item evades the aggregate finiteness guard.

        ``min(0, nan)`` returns 0 (NaN comparisons are False), so min_x/max_x
        stay finite and partition proceeds to the histogram, where ``int(nan)``
        raised ValueError. Non-finite items must be skipped per item.
        """
        nan = float("nan")
        items = [
            _text_item(0, 10, 10),
            _text_item(nan, nan, 30),
            _text_item(150, 160, 50),
            _text_item(155, 165, 70),
        ]

        segments = PageLayoutSorter(TextItemAdapter()).partition(items)

        assert len(_flatten(segments)) == len(items)

    def test_infinite_coordinate_on_non_first_item_does_not_raise(self):
        """An inf x0 with a finite x1 evades the aggregate guard.

        min_x aggregates x0 (``min(0, inf)`` is 0) and max_x aggregates x1, so
        both stay finite; ``int(inf)`` in the histogram raised OverflowError.
        """
        inf = float("inf")
        items = [
            _text_item(0, 10, 10),
            _text_item(inf, 20, 30),
            _text_item(150, 160, 50),
            _text_item(155, 165, 70),
        ]

        segments = PageLayoutSorter(TextItemAdapter()).partition(items)

        assert len(_flatten(segments)) == len(items)

    def test_infinite_coordinate_does_not_raise(self):
        """An infinite x-coordinate overflowed int() (OverflowError) before.

        The finiteness guard now returns the items as a single segment.
        """
        inf = float("inf")
        items = [
            _text_item(0, inf, 10),
            _text_item(0, 10, 30),
            _text_item(40, 50, 50),
            _text_item(45, 50, 70),
        ]

        segments = PageLayoutSorter(TextItemAdapter()).partition(items)

        assert len(_flatten(segments)) == len(items)

    def test_huge_finite_coordinate_is_fast_and_bounded(self):
        """A single absurd x-coordinate must not allocate a giant histogram.

        Pre-fix, ``[0] * int(1e9)`` allocated ~8 GB and took seconds for this
        tiny input. The histogram cap keeps it instant; we assert it both
        completes quickly and preserves every item.
        """
        items = [
            _text_item(0, 10, 10),
            _text_item(0, 10, 30),
            _text_item(0, 1e9, 50),
            _text_item(45, 50, 70),
        ]

        start = time.perf_counter()
        segments = PageLayoutSorter(TextItemAdapter()).partition(items)
        elapsed = time.perf_counter() - start

        assert len(_flatten(segments)) == len(items)
        # Generous bound: the real fix runs in milliseconds, while the unbounded
        # allocation took several seconds. This fails loudly if the cap regresses.
        assert elapsed < 2.0

    def test_histogram_cap_is_a_sane_bound(self):
        """The bin cap must stay well above any real page width but bounded."""
        assert 10_000 <= MAX_HISTOGRAM_BINS <= 1_000_000


class TestPartitionNormalLayout:
    """Regression guard: clamping must not change correct in-bounds behavior."""

    def test_two_column_page_partitions_left_before_right(self):
        """A clean two-column page yields all left items before all right items.

        Left column spans x 50-240, right column x 360-550, with a ~120pt gutter
        between them on a 500pt-wide page. partition recurses per column, so the
        invariant we pin is reading order: every left-column item precedes every
        right-column item, and no item is dropped or duplicated.
        """
        left_column = [_text_item(50, 240, y) for y in (50, 70, 90, 110)]
        right_column = [_text_item(360, 550, y) for y in (50, 70, 90, 110)]
        items = left_column + right_column

        segments = PageLayoutSorter(TextItemAdapter()).partition(items)
        flattened = _flatten(segments)

        assert len(flattened) == len(items)
        x0_order = [item.x0 for item in flattened]
        # All left-column x0 (50) come before all right-column x0 (360).
        assert x0_order == [50, 50, 50, 50, 360, 360, 360, 360]

    def test_single_column_returns_items_unsplit(self):
        """A narrow single block of text is returned without column splitting."""
        items = [_text_item(50, 90, y) for y in (50, 70)]

        segments = PageLayoutSorter(TextItemAdapter()).partition(items)

        assert segments == [items]


class TestSliceYBandsNegativeY:
    """_slice_y_bands must place negative-Y elements in reading order (#145).

    last_y started at 0.0, so any element with a negative y_top failed every
    band check (above/mid/below) and fell through to the leftover fallback,
    which blindly appends to the end of the first segment - even though those
    elements are the topmost content. Initializing last_y to -inf captures
    them in the first "above"/"below" pass instead.
    """

    def test_negative_y_above_interval_sorts_before_it(self):
        """Negative-Y items above a spanning interval precede it, not appended."""
        sorter = PageLayoutSorter(TextItemAdapter())
        above_negative = [_text_item(50, 90, -20), _text_item(50, 90, -10)]
        below_interval = [_text_item(50, 90, 100), _text_item(50, 90, 110)]
        span = _text_item(0, 500, 50)
        interval = {"y_top": 50.0, "y_bot": 58.0, "items": [span]}

        segments = sorter._slice_y_bands(
            above_negative + below_interval, [interval]
        )
        flattened = _flatten(segments)

        # Every column item plus the spanning item is placed exactly once.
        assert len(flattened) == len(above_negative) + len(below_interval) + 1
        # The topmost (negative-Y) items come before the spanning interval.
        order = [id(it) for it in flattened]
        for negative in above_negative:
            assert order.index(id(negative)) < order.index(id(span))

    def test_no_interval_negative_y_preserves_top_to_bottom_order(self):
        """With no intervals, negative-Y items keep their top-to-bottom order."""
        sorter = PageLayoutSorter(TextItemAdapter())
        # Already in top-to-bottom order; a single narrow column so partition
        # returns them unsplit and order is unambiguous.
        items = [_text_item(50, 90, -30), _text_item(50, 90, -10), _text_item(50, 90, 20)]

        segments = sorter._slice_y_bands(items, [])
        flattened = _flatten(segments)

        assert flattened == items
