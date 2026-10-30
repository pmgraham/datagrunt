"""Engine-independent table extraction via pdfplumber."""

import itertools
import logging
from operator import itemgetter
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


# ``(cos, sin)`` of each right-angle /Rotate. Undoing a rotation turns a
# display point (y down) by ``_unrotated_point`` and a matrix's axis vectors
# (PDF device space, y up) by ``_unrotated_axes``; both are exact.
_ROTATION_COS_SIN = {90: (0, 1), 180: (-1, 0), 270: (0, -1)}


def _unrotated_point(x, y, rotation) -> tuple[float, float]:
    """Display point ``(x, y)`` turned back by /Rotate ``rotation``, about the origin.

    Only relative positions matter after the turn, so no translation is added.
    """
    cos, sin = _ROTATION_COS_SIN[rotation]
    return x * cos + y * sin, y * cos - x * sin


def _unrotated_box(box, rotation) -> _Box:
    """Display box ``(x0, top, x1, bottom)`` turned back by /Rotate ``rotation``."""
    x0, top, x1, bottom = box
    (ax, ay), (bx, by) = _unrotated_point(x0, top, rotation), _unrotated_point(x1, bottom, rotation)
    return min(ax, bx), min(ay, by), max(ax, bx), max(ay, by)


def _unrotated_axes(char, rotation) -> tuple[float, float, float, float]:
    """``(a, b, c, d)`` of ``char``'s text-rendering matrix with /Rotate ``rotation`` undone."""
    a, b, c, d = char["matrix"][:4]
    cos, sin = _ROTATION_COS_SIN[rotation]
    return a * cos - b * sin, a * sin + b * cos, c * cos - d * sin, c * sin + d * cos


def _upright_when_unrotated(char, rotation) -> bool:
    """Whether ``char`` reads as upright, left-to-right text on the page without /Rotate.

    pdfminer calls a glyph upright when ``a*d > 0`` and ``b*c <= 0`` (it also
    multiplies in the horizontal scaling, which pdfplumber does not report and
    which is positive unless text is mirrored). That holds for a glyph turned
    180 degrees too, so the baseline must also point rightward (``a > |b|``).
    A glyph that displays upright on a rotated page fails this, which is how
    content turned to display upright is told apart.
    """
    a, b, c, d = _unrotated_axes(char, rotation)
    return a > abs(b) and a * d > 0 and b * c <= 0


def _unrotated_upright_char(char, rotation) -> dict:
    """``char`` as on the page without /Rotate, for one that passed ``_upright_when_unrotated``.

    The geometry pdfplumber's text extraction reads is turned back, and the
    glyph is marked upright, as pdfminer marks it on that unrotated page.
    """
    x0, top, x1, bottom = _unrotated_box((char["x0"], char["top"], char["x1"], char["bottom"]), rotation)
    turned = {"x0": x0, "top": top, "x1": x1, "bottom": bottom, "doctop": top, "width": x1 - x0, "height": bottom - top}
    return {**char, **turned, "upright": True}


def _centre_in(char, box) -> bool:
    """pdfplumber's cell membership test: the char's centre lies in the half-open ``box``."""
    x0, top, x1, bottom = box
    h_mid, v_mid = (char["x0"] + char["x1"]) / 2, (char["top"] + char["bottom"]) / 2
    return x0 <= h_mid < x1 and top <= v_mid < bottom


def _cell_text(cell, chars, extract_text) -> str | None:
    """One cell as ``Table.extract`` reads it: ``None`` for a grid gap, ``""`` if empty."""
    if cell is None:
        return None
    inside = [char for char in chars if _centre_in(char, cell)]
    return extract_text(inside) if inside else ""


def _read_cells(cells, chars) -> list[list[str | None]]:
    """pdfplumber's ``Table.rows`` and ``Table.extract`` over ``cells`` and ``chars`` in one frame.

    Rows group the cells by top edge, top to bottom. Each row has a slot per
    distinct left edge in the table, left to right; a slot with no cell (a
    merged-cell gap) is ``None``. As there, each row first narrows the chars
    to those inside its bounds.
    """
    extract_text = _import_pdfplumber().utils.extract_text
    lefts = sorted({cell[0] for cell in cells})
    grid = []
    for _, group in itertools.groupby(sorted(cells, key=itemgetter(1, 0)), key=itemgetter(1)):
        row_cells = list(group)
        row_box = (
            min(c[0] for c in row_cells),
            min(c[1] for c in row_cells),
            max(c[2] for c in row_cells),
            max(c[3] for c in row_cells),
        )
        row_chars = [char for char in chars if _centre_in(char, row_box)]
        by_left = {cell[0]: cell for cell in row_cells}
        grid.append([_cell_text(by_left.get(left), row_chars, extract_text) for left in lefts])
    return grid


def _rows_as_written(table, rotation) -> list[list[str | None]]:
    """The table's cell text, in the order it was written (#387).

    pdfplumber reads a grid and its cells in display order, /Rotate applied.
    When every glyph in the table stands upright on the page without /Rotate,
    the table was written there and only /Rotate turned it, so its cells and
    glyphs are turned back and read as pdfplumber reads that unrotated page.
    Otherwise (an unrotated page, content turned to display upright, mixed
    orientations, or no glyphs at all) pdfplumber's display reading stands.
    """
    if rotation not in _ROTATION_COS_SIN:
        return table.extract()
    chars = [char for char in table.page.chars if _centre_in(char, table.bbox)]
    if not chars or not all(_upright_when_unrotated(char, rotation) for char in chars):
        return table.extract()
    cells = [_unrotated_box(cell, rotation) for cell in table.cells]
    return _read_cells(cells, [_unrotated_upright_char(char, rotation) for char in chars])


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
        Cells read in the order they were written, even when /Rotate turned
        the table (see ``_rows_as_written``).
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
                data = _rows_as_written(t, page.rotation)
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
