"""Group raw text items into classified blocks (engine-agnostic)."""

import re
from bisect import bisect_left

from datagrunt.core.pdf_io.extraction.layout_sorter import PageLayoutSorter, TextItemAdapter
from datagrunt.core.pdf_io.extraction.shapes import BBox, TextBlock

LIST_MARKER_REGEX = re.compile(
    r'^('
    r'[•\-\*]'            # Standard bullet points
    r'|\d+(\.\d+)*\.?'    # Number hierarchies (e.g., 1., 12.5.1)
    r'|[IVXLCDM]{2,}'     # Multi-character Roman numerals (e.g., II, III, IV)
    r'|[IVXLCDM]\.'       # Single Roman numerals followed by dot (e.g., I.)
    r'|[IVXLCDM]\s+[A-Z]' # Single Roman numeral list markers followed by capitalized word (e.g., I Build)
    r'|[a-zA-Z]\.'        # Alphabetical list markers with dot (e.g., a., A.)
    r'|[a-zA-Z]\)'        # Alphabetical list markers with parenthesis (e.g., a), A))
    r')(\s|$)'
)


def page_median_size(all_sizes: list) -> float | None:
    """Median font size for a page (legacy formula), or None if no sizes.

    Uses ``sorted(all_sizes)[len(all_sizes) // 2]`` rather than ``statistics.median`` to
    preserve exact parity with the original classifier (and the Rust port):
    for even-length inputs this picks the upper-middle element, it does NOT
    average the two middle values.
    """
    if not all_sizes:
        return None
    return sorted(all_sizes)[len(all_sizes) // 2]


def classify_by_median(font_size: float, median_size) -> str:
    """Classify a block by font size relative to a precomputed page median.

    Ported from the original ``extractors._classify_block`` (identical thresholds).
    """
    if median_size is None:
        return "body_text"
    if font_size >= median_size * 1.6:
        return "header"
    if font_size >= median_size * 1.2:
        return "subheader"
    if font_size < median_size * 0.85:
        return "caption"
    return "body_text"


def classify_font_size(font_size: float, all_sizes: list) -> str:
    """Classify a block by font size relative to the page's size distribution.

    Wrapper retained for callers that pass the raw size list; the hot
    extraction paths precompute the median once via ``page_median_size``
    and call ``classify_by_median`` directly.
    """
    return classify_by_median(font_size, page_median_size(all_sizes))


class TextBlockBuilder:
    """Cluster text items into lines, merge lines into blocks, then classify."""

    @staticmethod
    def _dominant(values, default=None):
        """Most common value (ties broken by first occurrence)."""
        values = [v for v in values if v not in (None, "")]
        if not values:
            return default
        return max(set(values), key=values.count)

    def build(self, items: list) -> list:
        """Return a list of classified ``TextBlock`` from raw ``TextItem`` list.

        Processes the items in reading order, identifying column layouts to prevent bleed-over.
        """
        if not items:
            return []

        sorter = PageLayoutSorter(TextItemAdapter())
        segments = sorter.partition(items)
        all_blocks = []
        all_sizes = [it.size for it in items if it.text]
        median_size = page_median_size(all_sizes)

        order = 0
        for seg_items in segments:
            if not seg_items:
                continue
            lines = self._cluster_into_lines(seg_items)
            merged = self._merge_lines(lines)
            blocks = self._finalize(merged, median_size)
            for b in blocks:
                b.reading_order = order
                order += 1
            all_blocks.extend(blocks)

        return all_blocks

    def _cluster_into_lines(self, items: list) -> list:
        """Group items whose ``y_top`` are within tolerance into line records.

        Items arrive sorted by ``(round(y_top), x0)``, so each line's anchor
        ``y`` is created in non-decreasing rounded-``y`` order. A new item can
        therefore only match lines whose anchor lies within the tolerance band
        ``[y_top - tol, y_top + tol]``; lines anchored further down can never
        match again. We binary-search the band's lower edge (using the largest
        tolerance any line could have) and scan forward to the first matching
        line, preserving the original "first line in creation order" rule. Each
        line's max item size is cached and updated on append rather than
        recomputed, removing the per-candidate ``max()`` scan.
        """
        if not items:
            return []
        max_item_size = max(it.size for it in items)
        # Widest possible half-band for any (line, item) pair on this page; used
        # only to bound the binary search, never to decide membership.
        max_tol = max(2.0, max_item_size * 0.5)

        lines = []
        line_keys = []  # round(anchor_y, 0) per line, ascending by construction
        for it in sorted(items, key=lambda i: (round(i.y_top, 0), i.x0)):
            lo = bisect_left(line_keys, round(it.y_top - max_tol, 0))
            placed = False
            for ln in lines[lo:]:
                if ln["y"] - it.y_top > max_tol:
                    break  # remaining anchors are even further above the band
                line_max_size = ln["max_size"]
                if abs(ln["y"] - it.y_top) <= max(2.0, max(line_max_size, it.size) * 0.5):
                    ln["items"].append(it)
                    if it.size > line_max_size:
                        ln["max_size"] = it.size
                    placed = True
                    break
            if not placed:
                lines.append({"y": it.y_top, "items": [it], "max_size": it.size})
                line_keys.append(round(it.y_top, 0))
        return [self._line_record(ln["items"]) for ln in lines]

    def _line_record(self, its: list) -> dict:
        """Reduce a line's items to a single record."""
        its = sorted(its, key=lambda i: i.x0)
        sizes = [i.size for i in its]
        return {
            "text": " ".join(i.text for i in its).strip(),
            "x0": min(i.x0 for i in its),
            "x1": max(i.x1 for i in its),
            "y_top": min(i.y_top for i in its),
            "y_bot": max(i.y_bot for i in its),
            "size": self._dominant(sizes, default=0.0),
            "font": self._dominant([i.font for i in its], default=""),
            "bold": any(i.is_bold for i in its),
            "italic": any(i.is_italic for i in its),
        }

    def _merge_lines(self, line_recs: list) -> list:
        """Merge adjacent lines of similar size, small gap, and x-overlap."""
        line_recs = sorted(line_recs, key=lambda r: (r["y_top"], r["x0"]))
        merged = []
        for ln in line_recs:
            if merged and self._should_merge(merged[-1], ln):
                self._absorb(merged[-1], ln)
            else:
                merged.append(dict(ln))
        return merged

    def _should_merge(self, prev: dict, ln: dict) -> bool:
        """Return True if ``ln`` continues the ``prev`` block."""
        if LIST_MARKER_REGEX.match(ln["text"]):
            return False
        gap = ln["y_top"] - prev["y_bot"]
        same_size = abs(ln["size"] - prev["size"]) < 0.6
        close = 0 <= gap <= max(prev["size"], 1.0) * 1.6
        overlap = not (ln["x0"] > prev["x1"] or ln["x1"] < prev["x0"])
        return same_size and close and overlap

    def _absorb(self, prev: dict, ln: dict) -> None:
        """Merge ``ln`` into ``prev`` in place."""
        prev["text"] = (prev["text"] + " " + ln["text"]).strip()
        prev["x0"] = min(prev["x0"], ln["x0"])
        prev["x1"] = max(prev["x1"], ln["x1"])
        prev["y_bot"] = ln["y_bot"]
        prev["bold"] = prev["bold"] or ln["bold"]
        prev["italic"] = prev["italic"] or ln["italic"]

    def _finalize(self, merged: list, median_size) -> list:
        """Convert merged line records into classified ``TextBlock`` objects."""
        blocks = []
        order = 0
        for b in merged:
            if not b["text"]:
                continue
            blocks.append(
                TextBlock(
                    text=b["text"],
                    bbox=BBox(x=b["x0"], y=b["y_top"], w=b["x1"] - b["x0"], h=b["y_bot"] - b["y_top"]),
                    font=b["font"],
                    font_size=round(b["size"], 1),
                    is_bold=b["bold"],
                    is_italic=b["italic"],
                    classification=classify_by_median(round(b["size"], 1), median_size),
                    reading_order=order,
                )
            )
            order += 1
        return blocks
