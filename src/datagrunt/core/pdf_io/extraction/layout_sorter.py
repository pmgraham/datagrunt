"""Layout-aware sorting and partitioning of positioned elements (text items, blocks, images)."""

from __future__ import annotations


class LayoutAdapter:
    """Interface to abstract coordinate and weight access for different item types."""

    def get_bounds(self, item) -> tuple[float, float, float, float]:
        """Return (x0, y0, x1, y1) bounds for the item."""
        raise NotImplementedError

    def get_weight(self, item) -> int:
        """Return the weight (string length or density weight) of the item."""
        raise NotImplementedError


class TextItemAdapter(LayoutAdapter):
    """Adapter for TextItem data classes."""

    def get_bounds(self, item) -> tuple[float, float, float, float]:
        return item.x0, item.y_top, item.x1, item.y_bot

    def get_weight(self, item) -> int:
        return len(item.text or "")


class ElementAdapter(LayoutAdapter):
    """Adapter for unified-schema element dictionaries."""

    def get_bounds(self, item) -> tuple[float, float, float, float]:
        pos = item.get("position", {})
        x0 = pos.get("x", 0.0)
        y0 = pos.get("y", 0.0)
        return x0, y0, x0 + pos.get("w", 0.0), y0 + pos.get("h", 0.0)

    def get_weight(self, item) -> int:
        content = item.get("content")
        return len(content) if isinstance(content, str) else 10


class PageLayoutSorter:
    """Encapsulates layout-aware column parsing and element sorting using recursive XY-Cut."""

    def __init__(self, adapter: LayoutAdapter, gutter_tolerance: float = 12.0):
        self.adapter = adapter
        self.gutter_tolerance = gutter_tolerance

    def sort(self, items: list) -> list:
        """Sort page items layout-aware by columns and spanning bands."""
        if not items:
            return []
        sorted_elements = []
        for seg in self.partition(items):
            seg.sort(key=lambda it: (self.adapter.get_bounds(it)[1], self.adapter.get_bounds(it)[0]))
            sorted_elements.extend(seg)
        return sorted_elements

    def partition(self, items: list) -> list[list]:
        """Recursively partition items into column-aware segments."""
        if not items:
            return []

        text_items = self._filter_text_items(items)
        if not text_items:
            return [items]

        bounds = self._get_bounding_box(text_items)
        if not bounds:
            return [items]
        min_x, min_y, max_x, max_y = bounds
        page_width = max_x - min_x
        page_height = max_y - min_y

        if page_width < 100 or len(items) < 3:
            return [items]

        bins = self._build_density_histogram(text_items, page_width, max_x)
        gutter_x = self._find_widest_gutter(bins, min_x, page_width)

        if gutter_x is None:
            return [items]

        spanning, columns = self._split_columns_and_spanning(items, gutter_x, page_height)

        if not spanning:
            # Pure two-column split by midpoint
            left = []
            right = []
            for it in columns:
                x0, _, x1, _ = self.adapter.get_bounds(it)
                mid = (x0 + x1) / 2
                if mid < gutter_x:
                    left.append(it)
                else:
                    right.append(it)
            if not left or not right:
                return [items]
            return self.partition(left) + self.partition(right)

        intervals = self._group_spanning_intervals(spanning)
        return self._slice_y_bands(columns, intervals)

    def _filter_text_items(self, items: list) -> list:
        """Filter only text-like elements to identify page layout gutters."""
        text_items = []
        for it in items:
            if isinstance(it, dict):
                if it.get("type") in ("body_text", "caption", "header", "subheader"):
                    text_items.append(it)
            else:
                text_items.append(it)
        return text_items

    def _get_bounding_box(self, items: list) -> tuple[float, float, float, float] | None:
        """Find bounds (min_x, min_y, max_x, max_y) for the given items."""
        try:
            min_x = min(self.adapter.get_bounds(it)[0] for it in items)
            max_x = max(self.adapter.get_bounds(it)[2] for it in items)
            min_y = min(self.adapter.get_bounds(it)[1] for it in items)
            max_y = max(self.adapter.get_bounds(it)[3] for it in items)
            return min_x, min_y, max_x, max_y
        except (ValueError, KeyError, IndexError):
            return None

    def _build_density_histogram(self, text_items: list, page_width: float, max_x: float) -> list[int]:
        """Build a horizontal density histogram from text items."""
        bins = [0] * int(max_x + 2)
        for it in text_items:
            x0, _, x1, _ = self.adapter.get_bounds(it)
            if (x1 - x0) > page_width * 0.7:
                continue
            start = max(0, int(x0))
            end = min(int(max_x), int(x1))
            weight = self.adapter.get_weight(it)
            for x in range(start, end):
                bins[x] += weight
        return bins

    def _find_widest_gutter(self, bins: list[int], min_x: float, page_width: float) -> float | None:
        """Search for the widest vertical gutter in the middle region (25% - 75%)."""
        search_start = int(min_x + page_width * 0.25)
        search_end = int(min_x + page_width * 0.75)
        best_gutter_start = None
        best_gutter_width = 0
        current_start = None

        for x in range(search_start, search_end):
            if bins[x] <= 1:
                if current_start is None:
                    current_start = x
            else:
                if current_start is not None:
                    w = x - current_start
                    if w > best_gutter_width:
                        best_gutter_width = w
                        best_gutter_start = current_start
                    current_start = None

        if current_start is not None:
            w = search_end - current_start
            if w > best_gutter_width:
                best_gutter_width = w
                best_gutter_start = current_start

        if best_gutter_width < self.gutter_tolerance or best_gutter_start is None:
            return None

        return best_gutter_start + best_gutter_width / 2

    def _split_columns_and_spanning(self, items: list, gutter_x: float, page_height: float) -> tuple[list, list]:
        """Identify crossing/spanning items versus column items."""
        spanning = []
        columns = []
        for it in items:
            x0, y0, x1, y1 = self.adapter.get_bounds(it)
            h = y1 - y0
            if x0 < gutter_x - 5 and x1 > gutter_x + 5 and h < page_height * 0.8:
                spanning.append(it)
            else:
                columns.append(it)
        return spanning, columns

    def _group_spanning_intervals(self, spanning: list) -> list[dict]:
        """Group overlapping spanning items into intervals."""
        spanning.sort(key=lambda it: self.adapter.get_bounds(it)[1])
        intervals = []
        for it in spanning:
            x0, y_top, x1, y_bot = self.adapter.get_bounds(it)
            if not intervals or y_top > intervals[-1]["y_bot"]:
                intervals.append({"y_top": y_top, "y_bot": y_bot, "items": [it]})
            else:
                intervals[-1]["y_bot"] = max(intervals[-1]["y_bot"], y_bot)
                intervals[-1]["items"].append(it)
        return intervals

    def _slice_y_bands(self, columns: list, intervals: list[dict]) -> list[list]:
        """Slice Y bands based on spanning intervals."""
        segments = []
        last_y = 0.0
        placed = set()

        for interval in intervals:
            above = []
            for it in columns:
                if id(it) in placed:
                    continue
                _, y_top, _, _ = self.adapter.get_bounds(it)
                if last_y <= y_top < interval["y_top"]:
                    above.append(it)
                    placed.add(id(it))
            if above:
                segments.extend(self.partition(above))

            mid = list(interval["items"])
            for it in columns:
                if id(it) in placed:
                    continue
                _, y_top, _, _ = self.adapter.get_bounds(it)
                if interval["y_top"] <= y_top < interval["y_bot"]:
                    mid.append(it)
                    placed.add(id(it))
            mid.sort(key=lambda it: (self.adapter.get_bounds(it)[1], self.adapter.get_bounds(it)[0]))
            segments.append(mid)

            last_y = interval["y_bot"]

        below = []
        for it in columns:
            if id(it) in placed:
                continue
            _, y_top, _, _ = self.adapter.get_bounds(it)
            if y_top >= last_y:
                below.append(it)
                placed.add(id(it))
        if below:
            segments.extend(self.partition(below))

        # Capture any leftover elements that were not placed in any band
        leftover = [it for it in columns if id(it) not in placed]
        if leftover:
            if segments:
                segments[0].extend(leftover)
            else:
                segments.append(leftover)

        return segments
