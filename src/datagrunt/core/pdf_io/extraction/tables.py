"""Engine-independent table extraction via pdfplumber."""

from pathlib import Path

from datagrunt.core.pdf_io.extraction.shapes import BBox, TableBlock


def _import_pdfplumber():
    """Import pdfplumber lazily with a helpful error if the extra is missing."""
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError("PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]") from exc
    return pdfplumber


class PdfPlumberTableExtractor:
    """Detect and extract tables on a page using pdfplumber."""

    def __init__(self, filepath):
        """Store the path.

        Args:
            filepath (str or Path): Path to the PDF file.
        """
        self.filepath = Path(filepath)
        import threading
        self._local = threading.local()

    def __enter__(self):
        if not hasattr(self._local, "depth"):
            self._local.depth = 0
        if self._local.depth == 0:
            pdfplumber = _import_pdfplumber()
            self._local.pdf = pdfplumber.open(self.filepath)
        self._local.depth += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._local.depth -= 1
        if self._local.depth == 0:
            if hasattr(self._local, "pdf") and self._local.pdf:
                self._local.pdf.close()
                self._local.pdf = None

    def _get_pdf(self):
        if hasattr(self._local, "pdf") and self._local.pdf:
            return self._local.pdf, False
        pdfplumber = _import_pdfplumber()
        try:
            return pdfplumber.open(self.filepath), True
        except Exception:
            return None, False

    def extract(self, page_number: int) -> list:
        """Return ``TableBlock`` objects on ``page_number`` (0-indexed).

        Returns an empty list on open/parse failure or out-of-range page (soft).
        """
        pdf, should_close = self._get_pdf()
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
