"""Dataclasses describing extracted PDF page content (engine-agnostic).

These are internal representations. ``pdfcomponents`` serializes them into the
public unified-element dicts, so the public output schema is unchanged.
"""

from dataclasses import dataclass, field


@dataclass
class BBox:
    """A bounding box in datagrunt's top-left origin convention (PDF points)."""

    x: float
    y: float
    w: float
    h: float

    def to_dict(self) -> dict:
        """Return the box as a ``{x, y, w, h}`` dict (rounded to 2dp)."""
        return {"x": round(self.x, 2), "y": round(self.y, 2), "w": round(self.w, 2), "h": round(self.h, 2)}

    @classmethod
    def from_pdfium_bounds(cls, left: float, bottom: float, right: float, top: float, page_height: float) -> "BBox":
        """Build from PDFium's bottom-left ``(left, bottom, right, top)`` bounds."""
        return cls(x=round(left, 2), y=round(page_height - top, 2), w=round(right - left, 2), h=round(top - bottom, 2))


@dataclass
class PageAnalysis:
    """Summary metadata about a single page."""

    width: float
    height: float
    rotation: int
    has_text_layer: bool
    is_scanned: bool
    text_block_count: int
    image_count: int
    image_block_count: int
    has_line_drawings: bool
    text_length: int


@dataclass
class TextItem:
    """One raw text fragment (a single text object) before block grouping."""

    text: str
    x0: float
    x1: float
    y_top: float
    y_bot: float
    size: float
    font: str
    is_bold: bool
    is_italic: bool


@dataclass
class TextBlock:
    """A grouped, classified block of text."""

    text: str
    bbox: BBox
    font: str
    font_size: float
    is_bold: bool
    is_italic: bool
    classification: str
    reading_order: int


@dataclass
class ImageBlock:
    """An embedded image on a page."""

    bbox: BBox
    file_path: str | None
    width_px: int
    height_px: int
    fmt: str


@dataclass
class TableBlock:
    """A detected table."""

    data: list
    bbox: BBox
    rows: int
    columns: int
    has_header_row: bool


@dataclass
class OcrBlock:
    """A line of OCR-recovered text."""

    text: str
    bbox: BBox
    confidence: float
    word_count: int
    per_word_confidence: list = field(default_factory=list)
