"""Engine-independent table extraction via pdfplumber."""

import logging
from pathlib import Path

from datagrunt.core.pdf_io.extraction._doc_session import _ThreadLocalDocSession
from datagrunt.core.pdf_io.extraction.shapes import BBox, TableBlock

logger = logging.getLogger(__name__)


def _import_pdfplumber():
    """Import pdfplumber lazily with a helpful error if the extra is missing."""
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError("PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]") from exc
    return pdfplumber


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

        Returns an empty list on open/parse failure or out-of-range page (soft).
        """
        pdf, should_close = self._get_doc()
        if not pdf:
            return []
        try:
            if page_number < 0 or page_number >= len(pdf.pages):
                return []
            blocks = []
            for t in pdf.pages[page_number].find_tables():
                data = t.extract()
                if data:
                    blocks.append(self._to_block(t, data))
            return blocks
        finally:
            if should_close:
                pdf.close()

    def _to_block(self, table, data) -> TableBlock:
        """Convert a pdfplumber table to a ``TableBlock`` (matches legacy shape)."""
        num_rows = len(data)
        num_cols = max((len(r) for r in data), default=0)
        has_header = num_rows > 1 and all(c is not None and c.strip() for c in data[0])
        x0, y0, x1, y1 = table.bbox
        return TableBlock(
            data=data,
            bbox=BBox(x=round(x0, 2), y=round(y0, 2), w=round(x1 - x0, 2), h=round(y1 - y0, 2)),
            rows=num_rows,
            columns=num_cols,
            has_header_row=has_header,
        )
