"""Engine-independent table extraction via pdfplumber."""

import logging
from pathlib import Path

from datagrunt.core.pdf_io.extraction._doc_session import _ThreadLocalDocSession
from datagrunt.core.pdf_io.extraction.display_frame import display_edges
from datagrunt.core.pdf_io.extraction.shapes import BBox, TableBlock

logger = logging.getLogger(__name__)

# A PDF rectangle ``(left, bottom, right, top)`` in user space.
_Box = tuple[float, float, float, float]


def _import_pdfplumber():
    """Import pdfplumber lazily with a helpful error if the extra is missing."""
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError("PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]") from exc
    return pdfplumber


def _normalized(box) -> _Box:
    """``(left, bottom, right, top)`` with each pair sorted (PDF allows either corner order)."""
    x0, y0, x1, y1 = box
    return min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)


def _visible_box(mediabox: _Box, cropbox: _Box) -> _Box:
    """The CropBox intersected with the MediaBox, or the MediaBox if they do not overlap."""
    left, bottom = max(mediabox[0], cropbox[0]), max(mediabox[1], cropbox[1])
    right, top = min(mediabox[2], cropbox[2]), min(mediabox[3], cropbox[3])
    if left >= right or bottom >= top:
        logger.debug("CropBox %s does not intersect MediaBox %s; measuring tables from the MediaBox", cropbox, mediabox)
        return mediabox
    return left, bottom, right, top


def _display_shift(page) -> tuple[float, float]:
    """``(dx, dy)`` that moves a pdfplumber coordinate into the display frame.

    pdfplumber lays the page out rotated, measured from the displayed
    MediaBox's top-left corner plus its own origin ``page.mediabox[:2]``
    (non-zero when the MediaBox origin is). The display frame (see
    ``display_frame``) measures the same rotated layout from the visible box's
    displayed top-left corner, so the two differ by a translation: where the
    displayed MediaBox's top-left sits in the display frame. pdfplumber's own
    ``page.cropbox`` is not usable for this (it is wrong on /Rotate 90|270
    pages), so the boxes come from pdfminer's page, which inherits them from
    the page tree.
    """
    mediabox = _normalized(page.page_obj.mediabox)
    visible = _visible_box(mediabox, _normalized(page.page_obj.cropbox))
    left, bottom, right, top = mediabox
    media_x, media_y, _, _ = display_edges(left, bottom, right, top, page.rotation, visible)
    return media_x - page.mediabox[0], media_y - page.mediabox[1]


class PdfPlumberTableExtractor(_ThreadLocalDocSession):
    """Detect and extract tables on a page using pdfplumber."""

    _lazy_open = True

    def __init__(self, filepath):
        """Store the path.

        Args:
            filepath (str or Path): Path to the PDF file.
        """
        self.filepath = Path(filepath)
        import threading

        self._local = threading.local()

    def _open_resource(self):
        # Mark a held-open scope but defer the pdfplumber.open() until a page
        # actually needs tables. Text-only documents (no line drawings, has a
        # text layer) never call extract(), so they must never pay for opening
        # pdfplumber. The shared handle is opened lazily on first extract() and
        # closed when the outermost scope exits.
        pdfplumber = _import_pdfplumber()
        try:
            return pdfplumber.open(self.filepath)
        except Exception:  # noqa: BLE001 - pdfplumber raises many types; tables are best-effort
            logger.debug("pdfplumber.open failed for %s; skipping table extraction", self.filepath, exc_info=True)
            return None

    def extract(self, page_number: int) -> list:
        """Return ``TableBlock`` objects on ``page_number`` (0-indexed).

        Boxes are in the page's display frame, where the table renders (see
        ``display_frame``), whatever the CropBox, MediaBox origin or /Rotate.
        Returns an empty list on open/parse failure or out-of-range page (soft).
        """
        pdf, should_close = self._get_doc()
        if not pdf:
            return []
        try:
            if page_number < 0 or page_number >= len(pdf.pages):
                return []
            page = pdf.pages[page_number]
            shift = _display_shift(page)
            blocks = []
            for t in page.find_tables():
                data = t.extract()
                if data:
                    blocks.append(self._to_block(t, data, shift))
            return blocks
        finally:
            if should_close:
                pdf.close()

    def _to_block(self, table, data, shift) -> TableBlock:
        """Convert a pdfplumber table to a ``TableBlock`` (matches legacy shape).

        ``shift`` is ``_display_shift(page)``; it is ``(0, 0)`` on an uncropped,
        zero-origin page, which leaves those boxes unchanged.
        """
        num_rows = len(data)
        num_cols = max((len(r) for r in data), default=0)
        has_header = num_rows > 1 and all(c is not None and c.strip() for c in data[0])
        x0, y0, x1, y1 = table.bbox
        dx, dy = shift
        return TableBlock(
            data=data,
            bbox=BBox(x=round(x0 + dx, 2), y=round(y0 + dy, 2), w=round(x1 - x0, 2), h=round(y1 - y0, 2)),
            rows=num_rows,
            columns=num_cols,
            has_header_row=has_header,
        )
