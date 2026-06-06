"""Group raw text items into classified blocks (engine-agnostic)."""

from datagrunt.core.pdf_io.extraction.shapes import BBox, TextBlock


def classify_font_size(font_size: float, is_bold: bool, all_sizes: list) -> str:
    """Classify a block by font size relative to the page's size distribution.

    Ported from the original ``extractors._classify_block`` (identical thresholds).
    """
    if not all_sizes:
        return "body_text"
    median_size = sorted(all_sizes)[len(all_sizes) // 2]
    if font_size >= median_size * 1.6:
        return "header"
    if font_size >= median_size * 1.2:
        return "subheader"
    if font_size < median_size * 0.85:
        return "caption"
    return "body_text"


def _dominant(values, default=None):
    """Most common value (ties broken by first occurrence)."""
    values = [v for v in values if v not in (None, "")]
    if not values:
        return default
    return max(set(values), key=values.count)


class TextBlockBuilder:
    """Cluster text items into lines, merge lines into blocks, then classify."""

    def build(self, items: list) -> list:
        """Return a list of classified ``TextBlock`` from raw ``TextItem`` list."""
        if not items:
            return []
        lines = self._cluster_into_lines(items)
        merged = self._merge_lines(lines)
        all_sizes = [it.size for it in items if it.text]
        return self._finalize(merged, all_sizes)

    def _cluster_into_lines(self, items: list) -> list:
        """Group items whose ``y_top`` are within tolerance into line records."""
        lines = []
        for it in sorted(items, key=lambda i: (round(i.y_top, 0), i.x0)):
            placed = False
            for ln in lines:
                if abs(ln["y"] - it.y_top) <= max(2.0, it.size * 0.4):
                    ln["items"].append(it)
                    placed = True
                    break
            if not placed:
                lines.append({"y": it.y_top, "items": [it]})
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
            "size": _dominant(sizes, default=0.0),
            "font": _dominant([i.font for i in its], default=""),
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

    def _finalize(self, merged: list, all_sizes: list) -> list:
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
                    classification=classify_font_size(round(b["size"], 1), b["bold"], all_sizes),
                    reading_order=order,
                )
            )
            order += 1
        return blocks
