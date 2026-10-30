"""pypdfium2 engine for the v5 PDF document model (#365).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.

pdfium behaviour this module relies on, observed with pypdfium2 5.14.0 /
pdfium 156.0.8076.0 on ReportLab-, PyMuPDF- and hand-built PDFs (#365 T1),
and pinned by ``tests/pdf_tests/test_pdfium_engine.py``:

- Char boxes (``PdfTextPage.get_charbox``) and top-level object bounds
  (``PdfObject.get_bounds``) are ``(left, bottom, right, top)`` in PDF user
  space: bottom-left origin, unrotated, absolute (not relative to the
  CropBox). They are identical on every /Rotate and with or without a CropBox.
- ``PdfPage.get_bbox()`` (``FPDF_GetPageBoundingBox``) is the visible box:
  CropBox intersected with MediaBox, normalised, inherited from the page tree.
  ``get_mediabox()`` / ``get_cropbox()`` do NOT inherit (an inherited
  MediaBox reads as the 612 x 792 fallback), so geometry uses ``get_bbox()``.
- Rendering shows the visible box turned clockwise by ``get_rotation()``;
  ``get_size()`` is that displayed size (width and height swap on 90 / 270).
- An empty MediaBox (``[0 0 0 0]``) reads as (0, 0, 612, 792) in
  ``get_bbox()``, as the renderer draws it.
- ``/UserUnit`` is not supported: pdfium ignores it, so sizes are in user
  units (``/UserUnit 2`` on a 400 x 300 MediaBox reads, and renders, 400 x
  300), and so are this module's points.
- ``FPDF_LoadDocument`` returns NULL on failure and sets
  ``FPDF_GetLastError()``: ``FPDF_ERR_PASSWORD`` for a missing or wrong
  password, ``FPDF_ERR_SECURITY`` for an unknown security handler,
  ``FPDF_ERR_FILE`` when the OS refuses the read, ``FPDF_ERR_FORMAT`` for
  non-PDF, empty and truncated files. A successful load never resets that
  code, so it is read only after a NULL return. A well-formed file with no
  pages loads successfully; pypdfium2 5.14's path open then raises with the
  stale code (an unencrypted file can read as "Incorrect password") and leaks
  the document (``_open_pdf`` never calls ``FPDF_CloseDocument``). So this
  module loads with ``FPDF_LoadDocument`` itself, closes a zero-page
  document, and hands the handle to ``PdfDocument`` (a documented input).

Text (#365 T2): see :mod:`datagrunt.pdf._pdfium_text`.

Paths (#365 T3), pinned by ``tests/pdf_tests/test_pdfium_segments.py``:

- Path points (``FPDFPathSegment_GetPoint``) are in the path's own space.
  ``FPDFPageObj_GetMatrix`` of a path is the CTM at its painting operator
  inside its form, that form's /Matrix included; of a form object, the CTM
  at its ``Do`` in the parent's space. Neither includes an enclosing form's
  matrix, so page space is point x path matrix x each enclosing form's
  matrix, innermost first (``get_bounds()`` is no help: it is form space).
- pdfium writes every close as an edge: ``h``, ``s``, ``b`` and ``re``
  append a LINETO back to the subpath's start, flagged close (``re`` reads
  MOVETO + 4 LINETO). ``n`` and clip-only (``W n``) paths make no object.
- ``FPDFPageObj_GetStrokeWidth`` is ``w`` as written, in the path's space;
  a negative width strokes as a hairline, like 0. A zero-area fill (a
  zero-height ``re f``) paints as a hairline. A dash pattern does not
  change the path's points; ``FPDFPageObj_GetDashArray`` reads it as
  written, and pdfium strokes ``[0 0]`` and ``[4 0]`` solid.
- A number pdfium cannot read as a PDF number (``1e5``, or 39 digits)
  reads as 0, so such a ``cm`` collapses a path rather than overflowing.
- pdfium keeps the CTM in 32-bit floats: five ``1000000000 0 0 1000000000
  0 0 cm`` overflow it, and objects drawn under it read a matrix of
  ``(inf, nan, nan, inf, 0, 0)`` and NaN textpage boxes; pdfium renders the
  rest of the page. The engine skips and counts such objects.

Images (#365 T4), pinned by ``tests/pdf_tests/test_pdfium_images.py``:

- An image object's matrix maps its unit square into the enclosing form's
  space (that form's /Matrix included, an enclosing form's not), as a
  path's does, and so do its bounds; the engine composes form matrices.
  Inline images (``BI`` ... ``EI``) are image objects too, and an object
  under an overflowed CTM reads the same non-finite matrix as a path.
- ``FPDFImageObj_GetImageDataRaw`` is the stream as written, still
  filter-encoded but decrypted (an encrypted PDF's image hashes as the
  unencrypted one's), and an inline image's data between ``ID`` and ``EI``;
  an empty stream reads 0 bytes. Filter names are pdfium's full names (an
  inline ``/AHx`` reads ``ASCIIHexDecode``).
- ``FPDFImageObj_GetImageMetadata`` reads 0 bits per pixel and colour
  space 0 (unknown) for an image it cannot load (a missing or zero /Width,
  which reads as 0 px wide, or an empty stream); an image mask reads 1 bit
  and colour space 0; CMYK reads 24 bits (pdfium decodes it to RGB).
- ``FPDFImageObj_GetBitmap`` decodes at the image's own size (16 x 8
  samples drawn 80 x 40 pt decode 16 x 8; ``GetRenderedBitmap`` would
  resample to 80 x 40) as Gray or BGR, ignoring the matrix and any mask:
  CCITT and 1-bit images arrive as 8-bit gray, CMYK and Indexed as BGR.
  It returns NULL for an image it cannot load (a zero /Width, an empty
  stream).
- pdfium decodes a DCT image at its JPEG frame's (SOF) size whatever /Width
  says (declared 16 x 16 with a 2000 x 2000 frame decodes 2000 x 2000;
  declared 12000 x 12000 with a 16 x 16 frame, 16 x 16), and a JPX image at
  its codestream's SIZ size. ``FPDFImageObj_GetImageMetadata`` decodes a JPX
  image in full (0.14 s, 78 MB at 9000 x 9000) and OpenJPEG allocates ~12
  KB per tile as it reads the header (761 MB for a 2000 x 2000 codestream
  in 16 x 16 tiles); for DCT, Flate and CCITT the call reads no pixels
  (unchanged RSS at a 9000 x 9000 frame and a 9400 x 9400 Flate bomb).
- pdfium decodes no side over 131071 (``/Width 131072`` reads bits per
  pixel 0 and a NULL bitmap) and reads ``/Width -5`` as 4294967291.
- ``FPDFImageObj_GetImageDataRaw``'s length query copies the stream too
  (4 ms against 8 ms for the read of a 200 MB stream).
  ``FPDFImageObj_GetImageDataDecoded`` of ``[/ASCII85Decode /DCTDecode]``
  (what ReportLab writes for every JPEG) is the JPEG as written.

Pages (#365 T5), pinned by ``tests/pdf_tests/test_pdfium_extract_page.py``:

- :func:`extract_page` loads a page once and reads its geometry, text,
  segments and images from that load, in that order. A PDFCorruptError in a
  stage becomes a :class:`PageFailure` naming the stage, with the message
  that stage's own function raises, so one bad page never fails a batch;
  any other error is a bug and propagates. :class:`RawPage` and
  :class:`PageFailure` are picklable: they cross the process pool.
"""

import ctypes
import errno
import hashlib
import os
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from types import MappingProxyType
from typing import Literal, NoReturn

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c

from datagrunt.pdf._path_segments import (
    IDENTITY,
    Matrix,
    NonFiniteGeometry,
    PageSegments,
    PathPaint,
    PathPoint,
    RawSegment,
    SegmentStats,
    _checked_choice,
    path_segments,
)
from datagrunt.pdf._pdfium_common import Box, PdfiumFailure, object_address, read_rgba
from datagrunt.pdf._pdfium_images import (
    DecodeRefused,
    ImageStats,
    OversizedStream,
    PageImages,
    PixelFacts,
    RawImage,
    ascii_decoded_stream,
    collect_images,
    declared_size,
    decoded_png,
    pixel_facts,
    read_filters,
    read_raw_stream,
)
from datagrunt.pdf._pdfium_text import PageText, RawSpan, TextStats, collect_text
from datagrunt.pdf._validate import finite_float, finite_tuple, int_at_least, non_blank_str
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFCorruptError, PDFEncryptedError

__all__ = [
    "DocumentHandle",
    "ImageLoader",
    "ImageStats",
    "PageFailure",
    "PageGeometry",
    "PageImages",
    "PageSegments",
    "PageText",
    "RawImage",
    "RawPage",
    "RawSegment",
    "RawSpan",
    "SegmentStats",
    "TextStats",
    "extract_page",
    "open_document",
    "page_geometry",
    "page_images",
    "page_segments",
    "page_text",
    "to_display_box",
]

# ``get_rotation()`` normalises /Rotate (-90 reads 270, 450 reads 90, 45 reads 0).
_ROTATIONS = (0, 90, 180, 270)


def _checked_rotation(rotation_deg: object) -> int:
    """Return ``rotation_deg`` if it is one of 0, 90, 180, 270 (bool rejected)."""
    if isinstance(rotation_deg, bool) or not isinstance(rotation_deg, int):
        raise TypeError(f"rotation_deg must be an int, got {rotation_deg!r} ({type(rotation_deg).__name__})")
    if rotation_deg not in _ROTATIONS:
        raise ValueError(f"rotation_deg must be one of {_ROTATIONS}, got {rotation_deg!r}")
    return rotation_deg


def _checked_box(name: str, box: object, *, allow_empty: bool) -> Box:
    """Return ``box`` as 4 finite floats ``(left, bottom, right, top)``, edges in order."""
    left, bottom, right, top = finite_tuple(name, box, 4)
    ordered = left <= right and bottom <= top if allow_empty else left < right and bottom < top
    if not ordered:
        relation = "<=" if allow_empty else "<"
        raise ValueError(f"{name} must have left {relation} right and bottom {relation} top, got {box!r}")
    return left, bottom, right, top


def to_display_box(box: Box, *, rotation_deg: int, page_box: Box) -> Box:
    """Map a pdfium page-space box to a displayed, top-left box.

    Args:
        box: ``(left, bottom, right, top)`` in PDF user space, as pdfium
            reports char boxes and object bounds.
        rotation_deg: The page's clockwise /Rotate: 0, 90, 180, or 270.
        page_box: The page's visible box ``(left, bottom, right, top)``, as
            ``PdfPage.get_bbox()`` reports it (CropBox within MediaBox).

    Returns:
        ``(x0, y0, x1, y1)`` in points from the displayed page's top-left
        corner, with ``x0 <= x1`` and ``y0 <= y1``. A box outside the visible
        area keeps its (possibly negative) coordinates.

    Raises:
        TypeError: If ``rotation_deg`` is not an int (bool rejected) or a box
            is not a tuple of numbers.
        ValueError: If ``rotation_deg`` is not a right angle, a box value is
            not finite, ``box`` has reversed edges, or ``page_box`` is empty.
    """
    rotation = _checked_rotation(rotation_deg)
    left, bottom, right, top = _checked_box("box", box, allow_empty=True)
    page_left, page_bottom, page_right, page_top = _checked_box("page_box", page_box, allow_empty=False)
    match rotation:
        case 0:
            return left - page_left, page_top - top, right - page_left, page_top - bottom
        case 90:
            return bottom - page_bottom, left - page_left, top - page_bottom, right - page_left
        case 180:
            return page_right - right, bottom - page_bottom, page_right - left, top - page_bottom
        case _:  # 270: the only value left after _checked_rotation
            return page_top - top, page_right - right, page_top - bottom, page_right - left


def _positive_finite(name: str, value: object) -> float:
    """Return ``value`` as a finite float strictly greater than zero."""
    result = finite_float(name, value)
    if result <= 0.0:
        raise ValueError(f"{name} must be > 0, got {result!r}")
    return result


@dataclass(frozen=True, slots=True, kw_only=True)
class PageGeometry:
    """One page's displayed size, rotation and visible-box offset.

    Attributes:
        page_number: 1-based page number.
        width_pt: Displayed width in points (after /Rotate).
        height_pt: Displayed height in points (after /Rotate).
        rotation_deg: Clockwise /Rotate: 0, 90, 180, or 270.
        crop_offset: ``(left, bottom)`` of the visible box (CropBox within
            MediaBox) in PDF user space; ``(0.0, 0.0)`` when it starts at
            the user-space origin.
    """

    page_number: int
    width_pt: float
    height_pt: float
    rotation_deg: int
    crop_offset: tuple[float, float]

    def __post_init__(self) -> None:
        int_at_least("page_number", self.page_number, 1)
        object.__setattr__(self, "width_pt", _positive_finite("width_pt", self.width_pt))
        object.__setattr__(self, "height_pt", _positive_finite("height_pt", self.height_pt))
        _checked_rotation(self.rotation_deg)
        object.__setattr__(self, "crop_offset", finite_tuple("crop_offset", self.crop_offset, 2))

    @property
    def page_box(self) -> Box:
        """The visible box ``(left, bottom, right, top)`` in PDF user space.

        This is the ``page_box`` :func:`to_display_box` takes: the displayed
        size turned back by the rotation, placed at ``crop_offset``.
        """
        quarter_turn = self.rotation_deg in (90, 270)
        across, up = (self.height_pt, self.width_pt) if quarter_turn else (self.width_pt, self.height_pt)
        left, bottom = self.crop_offset
        return left, bottom, left + across, bottom + up


class DocumentHandle:
    """An open pdfium document, owned by the engine.

    Callers never have to close it. pypdfium2 attaches a ``weakref.finalize``
    to every ``PdfDocument``; this handle holds the only reference and forms
    no reference cycle, so dropping the last reference to the handle closes
    the document at once, by refcounting alone. ``close()`` and ``with``
    release it early.

    Not thread-safe: pdfium is not, so a handle belongs to one thread of one
    process. It cannot be pickled; a worker process reopens the PDF from its
    path (and password) with :func:`open_document`.
    """

    __slots__ = ("_pdf", "_page_count")

    def __init__(self, pdf: pdfium.PdfDocument) -> None:
        if not isinstance(pdf, pdfium.PdfDocument):
            raise TypeError(f"pdf must be a pypdfium2 PdfDocument, got {type(pdf).__name__}")
        self._pdf = pdf
        self._page_count: int = len(pdf)

    @property
    def closed(self) -> bool:
        """True once the pdfium document is closed."""
        return self._pdf.raw is None

    @property
    def page_count(self) -> int:
        """Number of pages; pypdfium2 refuses to open a document with none."""
        self._require_open()
        return self._page_count

    def close(self) -> None:
        """Close the pdfium document now; closing twice is a no-op."""
        self._pdf.close()

    def __enter__(self) -> "DocumentHandle":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __reduce__(self) -> NoReturn:
        raise TypeError(
            "DocumentHandle cannot be pickled: a pdfium document belongs to one process and is not "
            "thread-safe; send the PDF's path (and password) instead and reopen it from its path "
            "with open_document() in the worker"
        )

    def _require_open(self) -> pdfium.PdfDocument:
        if self.closed:
            raise ValueError("document handle is closed")
        return self._pdf


def _checked_path(path: object, name: str = "path") -> Path:
    """Return ``path`` as a Path; bytes are refused (pypdfium2 reads them as PDF data)."""
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError(f"{name} must be a str or os.PathLike, got {type(path).__name__}")
    text = os.fspath(path)
    if not isinstance(text, str):
        raise TypeError(f"{name} must be a str or os.PathLike, got a {type(text).__name__} path")
    return Path(text)


def _checked_password(password: object) -> str | None:
    """Return ``password`` if it is None or a str pdfium can read whole."""
    if password is None:
        return None
    # Name the type only: a password must never reach an error message.
    if not isinstance(password, str):
        raise TypeError(f"password must be a str or None, got {type(password).__name__}")
    # pypdfium2 hands the password to pdfium as a NUL-terminated C string: an
    # embedded NUL would silently truncate it to a different password.
    if "\x00" in password:
        raise ValueError("password must not contain NUL: pdfium would read only the part before it")
    return password


# FPDF_GetLastError() codes after a failed FPDF_LoadDocument, named as pdfium's docs name them.
_LOAD_ERROR_NAMES = MappingProxyType(
    {
        pdfium_c.FPDF_ERR_UNKNOWN: "Unknown error",
        pdfium_c.FPDF_ERR_FILE: "File access error",
        pdfium_c.FPDF_ERR_FORMAT: "Data format error",
        pdfium_c.FPDF_ERR_PASSWORD: "Incorrect password error",
        pdfium_c.FPDF_ERR_SECURITY: "Unsupported security scheme error",
        pdfium_c.FPDF_ERR_PAGE: "Page not found or content error",
    }
)

# pypdfium2's path encoding for FPDF_LoadDocument, so both open the same files.
_PATH_ERRORS = "strict" if sys.platform.startswith("win32") else "surrogateescape"


def _load_error(path: Path, password: str | None, code: int) -> Exception:
    """Translate a failed FPDF_LoadDocument's last-error code into a datagrunt error."""
    if code == pdfium_c.FPDF_ERR_PASSWORD:
        if password is None:
            return PDFEncryptedError(f"{path} is encrypted; pass its password to open it")
        return PDFEncryptedError(f"{path} is encrypted and the password is incorrect")
    if code == pdfium_c.FPDF_ERR_SECURITY:
        return PDFEncryptedError(f"{path} is encrypted with a security handler pdfium does not support")
    reason = _LOAD_ERROR_NAMES.get(code, f"error code {code}")
    if code == pdfium_c.FPDF_ERR_FILE:
        # The path is a file (checked first), so the OS refused the read.
        return PermissionError(errno.EACCES, f"pdfium cannot read the file (pdfium: {reason})", str(path))
    return PDFCorruptError(f"{path} is not a readable PDF (pdfium: {reason})")


def _load_raw_document(path: Path, password: str | None) -> pdfium_c.FPDF_DOCUMENT:
    """Load ``path`` with FPDF_LoadDocument; return a handle to a document with pages."""
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(errno.ENOENT, "not an existing file", str(path))
    encoded_path = (str(resolved) + "\x00").encode("utf-8", errors=_PATH_ERRORS)
    encoded_password = None if password is None else (password + "\x00").encode("utf-8")
    raw = pdfium_c.FPDF_LoadDocument(encoded_path, encoded_password)
    if not raw:
        raise _load_error(path, password, pdfium_c.FPDF_GetLastError())
    if pdfium_c.FPDF_GetPageCount(raw) < 1:
        pdfium_c.FPDF_CloseDocument(raw)
        raise PDFCorruptError(f"{path} has no pages")
    return raw


def open_document(path: str | os.PathLike[str], *, password: str | None = None) -> DocumentHandle:
    """Open the PDF at ``path`` with pdfium.

    Args:
        path: Path to the PDF file.
        password: The user or owner password of an encrypted PDF; ignored by
            pdfium for an unencrypted one.

    Returns:
        A handle that closes the document when it is dropped (or on
        ``close()`` / ``with`` exit, if the caller wants it earlier).

    Raises:
        TypeError: If ``path`` is not a str or os.PathLike of str, or
            ``password`` is not a str or None.
        ValueError: If ``password`` contains NUL.
        FileNotFoundError: If ``path`` is not an existing file.
        PDFEncryptedError: If the password is missing or wrong, or the file
            uses a security handler pdfium does not support.
        PermissionError: If the OS refuses to let pdfium read the file
            (``errno`` is ``EACCES``).
        PDFCorruptError: If the file is not a readable PDF or has no pages
            (``page_number`` is None: the container itself is unreadable).
    """
    source = _checked_path(path)
    raw = _load_raw_document(source, _checked_password(password))
    return DocumentHandle(pdfium.PdfDocument(raw))


def _checked_index(index: object, page_count: int) -> int:
    """Return ``index`` if it is an int in ``0..page_count - 1`` (no wraparound)."""
    if isinstance(index, bool) or not isinstance(index, int):
        raise TypeError(f"index must be an int, got {index!r} ({type(index).__name__})")
    if not 0 <= index < page_count:
        raise IndexError(f"index must be in 0..{page_count - 1}, got {index}")
    return index


def _load_page(handle: object, index: object) -> tuple[pdfium.PdfPage, int]:
    """Validate ``handle`` and ``index``; load the page and return it with its 1-based number."""
    if not isinstance(handle, DocumentHandle):
        raise TypeError(f"handle must be a DocumentHandle, got {type(handle).__name__}")
    pdf = handle._require_open()
    page_number = _checked_index(index, handle.page_count) + 1
    try:
        return pdf[page_number - 1], page_number
    except pdfium.PdfiumError as exc:
        raise PDFCorruptError(f"page {page_number} is unreadable: {exc}", page_number=page_number) from exc


def _read_geometry(page: pdfium.PdfPage, page_number: int) -> PageGeometry:
    """Read a loaded page's displayed geometry."""
    try:
        rotation, visible_box = page.get_rotation(), page.get_bbox()
    except pdfium.PdfiumError as exc:
        raise PDFCorruptError(f"page {page_number} is unreadable: {exc}", page_number=page_number) from exc
    try:
        left, bottom, right, top = _checked_box("visible box", visible_box, allow_empty=False)
    except ValueError as exc:
        message = f"page {page_number} has no visible area (CropBox within MediaBox): {exc}"
        raise PDFCorruptError(message, page_number=page_number) from exc
    across, up = right - left, top - bottom
    width, height = (up, across) if rotation in (90, 270) else (across, up)
    return PageGeometry(
        page_number=page_number, width_pt=width, height_pt=height, rotation_deg=rotation, crop_offset=(left, bottom)
    )


def page_geometry(handle: DocumentHandle, index: int) -> PageGeometry:
    """Return the displayed geometry of the page at 0-based ``index``.

    The size comes from ``get_bbox()`` (the visible box, inherited from the
    page tree) turned by ``get_rotation()``; this equals pdfium's own
    ``get_size()``. The page handle is closed before returning.

    Raises:
        TypeError: If ``handle`` is not a DocumentHandle, or ``index`` is not
            an int (bool rejected).
        ValueError: If ``handle`` is closed.
        IndexError: If ``index`` is outside ``0..page_count - 1``.
        PDFCorruptError: If pdfium cannot load the page, or its CropBox and
            MediaBox leave no visible area (``page_number`` is set).
    """
    page, page_number = _load_page(handle, index)
    try:
        return _read_geometry(page, page_number)
    finally:
        page.close()


# pdfium parses Form XObjects at most 40 levels deep (observed: 80 nested
# forms yield the text of 40), so this bound reaches every object it parsed.
_MAX_FORM_DEPTH = 64


def _children(
    count: int, get_child: Callable[[int], pdfium_c.FPDF_PAGEOBJECT], owner: str
) -> list[pdfium_c.FPDF_PAGEOBJECT]:
    """The ``count`` objects of the page or a form, in paint order; ``owner`` names it."""
    if count < 0:
        raise PdfiumFailure(f"count the objects of {owner}")
    children = []
    for index in range(count):
        child = get_child(index)
        if not child:
            raise PdfiumFailure(f"read object {index} of {owner}")
        children.append(child)
    return children


def _page_objects(page_raw: pdfium_c.FPDF_PAGE) -> list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]]:
    """Every page object in depth-first paint order (a form, then its contents).

    Each comes with the walk index of the form that holds it, -1 for none.
    """
    top = _children(
        pdfium_c.FPDFPage_CountObjects(page_raw), partial(pdfium_c.FPDFPage_GetObject, page_raw), "the page"
    )
    ordered: list[tuple[pdfium_c.FPDF_PAGEOBJECT, int]] = []
    stack = [(iter(top), -1)]
    # Bounded: one pass per object and one per form, each from a pdfium count,
    # and the stack never holds more than _MAX_FORM_DEPTH + 1 levels.
    while stack:
        siblings, parent = stack[-1]
        child = next(siblings, None)
        if child is None:
            stack.pop()
            continue
        ordered.append((child, parent))
        if len(stack) <= _MAX_FORM_DEPTH and pdfium_c.FPDFPageObj_GetType(child) == pdfium_c.FPDF_PAGEOBJ_FORM:
            count, form_index = pdfium_c.FPDFFormObj_CountObjects(child), len(ordered) - 1
            owner = f"form object {form_index}"
            stack.append((iter(_children(count, partial(pdfium_c.FPDFFormObj_GetObject, child), owner)), form_index))
    return ordered


def _object_indices(page_raw: pdfium_c.FPDF_PAGE) -> dict[int, int]:
    """Map each page object's address to its depth-first paint-order index."""
    return {object_address(obj): index for index, (obj, _form) in enumerate(_page_objects(page_raw))}


def _pdfium_error(page_number: int, failure: PdfiumFailure) -> PDFCorruptError:
    """The PDFCorruptError for a pdfium call that failed on page ``page_number``."""
    return PDFCorruptError(f"page {page_number}: pdfium cannot {failure}", page_number=page_number)


def _read_page_text(page: pdfium.PdfPage, geometry: PageGeometry) -> PageText:
    """Read a loaded page's spans, closing its textpage."""
    page_number = geometry.page_number
    try:
        textpage = page.get_textpage()
    except pdfium.PdfiumError as exc:
        message = f"page {page_number}: pdfium cannot load the text page: {exc}"
        raise PDFCorruptError(message, page_number=page_number) from exc
    try:
        return collect_text(textpage.raw, _display_mapping(geometry), _object_indices(page.raw))
    except PdfiumFailure as exc:
        raise _pdfium_error(page_number, exc) from exc
    finally:
        textpage.close()


def page_text(handle: DocumentHandle, index: int) -> PageText:
    """Return the text spans and textpage counts of the page at 0-based ``index``.

    One pass over the textpage: chars pdfium inserted (generated spaces and
    line breaks, and ownerless spaces between right-to-left words) are
    counted and skipped, and become a ``generated_breaks`` offset when they
    fall inside a span; every other char joins the span of its owning text
    object (see :class:`RawSpan`), unless pdfium places that object at a
    non-finite position or size: it is skipped whole and counted
    (``TextStats.skipped_nonfinite_objects``), and the page's other text is
    kept. Boxes are mapped to the displayed page with :func:`to_display_box`.
    The page and textpage handles are closed before returning.

    Raises:
        TypeError: If ``handle`` is not a DocumentHandle, or ``index`` is not
            an int (bool rejected).
        ValueError: If ``handle`` is closed.
        IndexError: If ``index`` is outside ``0..page_count - 1``.
        PDFCorruptError: If pdfium cannot load the page or its textpage, or a
            pdfium call on them fails (``page_number`` is set).
    """
    page, page_number = _load_page(handle, index)
    try:
        return _read_page_text(page, _read_geometry(page, page_number))
    finally:
        page.close()


# pdfium's path segment types, as PathPoint kinds (FPDF_SEGMENT_UNKNOWN, -1, is a failure).
_POINT_KINDS: MappingProxyType[int, Literal["move", "line", "curve"]] = MappingProxyType(
    {
        pdfium_c.FPDF_SEGMENT_MOVETO: "move",
        pdfium_c.FPDF_SEGMENT_LINETO: "line",
        pdfium_c.FPDF_SEGMENT_BEZIERTO: "curve",
    }
)
_FILL_MODES = (pdfium_c.FPDF_FILLMODE_NONE, pdfium_c.FPDF_FILLMODE_ALTERNATE, pdfium_c.FPDF_FILLMODE_WINDING)


def _read_paint(path: pdfium_c.FPDF_PAGEOBJECT, label: str) -> PathPaint:
    """Read how a path object is painted: stroke and fill, line width and dashes (if stroked), colours."""
    fill_mode, stroke = ctypes.c_int(), ctypes.c_int()
    if not pdfium_c.FPDFPath_GetDrawMode(path, fill_mode, stroke) or fill_mode.value not in _FILL_MODES:
        raise PdfiumFailure(f"read the draw mode of {label}")
    width = ctypes.c_float()
    if stroke.value and not pdfium_c.FPDFPageObj_GetStrokeWidth(path, width):
        raise PdfiumFailure(f"read the stroke width of {label}")
    return PathPaint(
        stroked=bool(stroke.value),
        filled=fill_mode.value != pdfium_c.FPDF_FILLMODE_NONE,
        stroke_width=width.value,
        dashed=bool(stroke.value) and _leaves_gaps(_read_dashes(path, label)),
        stroke_rgba=read_rgba(pdfium_c.FPDFPageObj_GetStrokeColor, path, f"stroke colour of {label}"),
        fill_rgba=read_rgba(pdfium_c.FPDFPageObj_GetFillColor, path, f"fill colour of {label}"),
    )


def _read_dashes(path: pdfium_c.FPDF_PAGEOBJECT, label: str) -> tuple[float, ...]:
    """Read a path object's dash array (empty for a solid stroke)."""
    count = pdfium_c.FPDFPageObj_GetDashCount(path)
    if count < 0:
        raise PdfiumFailure(f"read the dash pattern of {label}")
    if count == 0:
        return ()
    dashes = (ctypes.c_float * count)()
    if not pdfium_c.FPDFPageObj_GetDashArray(path, dashes, count):
        raise PdfiumFailure(f"read the dash pattern of {label}")
    return tuple(dashes)


def _leaves_gaps(dashes: tuple[float, ...]) -> bool:
    """Whether a dash array strokes with gaps.

    Its odd entries are the gaps; an odd-length array repeats, so each entry
    is a gap in turn. pdfium strokes an all-zero array (and one whose gaps
    are all 0) solid.
    """
    gaps = dashes[1::2] if len(dashes) % 2 == 0 else dashes
    return any(value > 0 for value in gaps) and any(value != 0 for value in dashes)


def _read_points(path: pdfium_c.FPDF_PAGEOBJECT, label: str) -> list[PathPoint]:
    """Read a path object's points, in its own space."""
    count = pdfium_c.FPDFPath_CountSegments(path)
    if count < 0:
        raise PdfiumFailure(f"count the path segments of {label}")
    points = []
    x, y = ctypes.c_float(), ctypes.c_float()
    for index in range(count):
        segment = pdfium_c.FPDFPath_GetPathSegment(path, index)
        kind = _POINT_KINDS.get(pdfium_c.FPDFPathSegment_GetType(segment)) if segment else None
        if kind is None or not pdfium_c.FPDFPathSegment_GetPoint(segment, x, y):
            raise PdfiumFailure(f"read path segment {index} of {label}")
        points.append(PathPoint(kind, x.value, y.value, bool(pdfium_c.FPDFPathSegment_GetClose(segment))))
    return points


def _read_matrix(obj: pdfium_c.FPDF_PAGEOBJECT, label: str) -> Matrix:
    """Read a page object's matrix (path_segments skips a path whose composed matrix is not finite)."""
    raw = pdfium_c.FS_MATRIX()
    if not pdfium_c.FPDFPageObj_GetMatrix(obj, raw):
        raise PdfiumFailure(f"read the matrix of {label}")
    return Matrix(raw.a, raw.b, raw.c, raw.d, raw.e, raw.f)


def _path_object_segments(
    path: pdfium_c.FPDF_PAGEOBJECT, index: int, matrix: Matrix, to_display: Callable[[Box], Box]
) -> list[RawSegment] | None:
    """The segments of path object ``index`` (matrix to page space ``matrix``), or None if its geometry is not finite.

    pdfium call failures raise; non-finite geometry is a fact of the page
    (pdfium renders the rest of it), so only that path is skipped.
    """
    label = f"object {index}"
    paint = _read_paint(path, label)
    points = _read_points(path, label)
    try:
        return path_segments(points, matrix=matrix, paint=paint, object_index=index, to_display=to_display)
    except NonFiniteGeometry:
        return None


def _placed_objects(page_raw: pdfium_c.FPDF_PAGE, kind: int) -> Iterator[tuple[int, pdfium_c.FPDF_PAGEOBJECT, Matrix]]:
    """Each page object of type ``kind`` (not a form) with its walk index and its matrix to page space.

    pdfium's object matrix maps into the enclosing form's space only, so
    each form's matrix to page space is composed here, in walk order (a form
    precedes its contents).
    """
    form_matrices: dict[int, Matrix] = {}
    for index, (obj, form) in enumerate(_page_objects(page_raw)):
        obj_kind = pdfium_c.FPDFPageObj_GetType(obj)
        if obj_kind not in (kind, pdfium_c.FPDF_PAGEOBJ_FORM):
            continue
        matrix = _read_matrix(obj, f"object {index}").then(form_matrices[form] if form >= 0 else IDENTITY)
        if obj_kind == pdfium_c.FPDF_PAGEOBJ_FORM:
            # A non-finite form matrix is kept: everything inside it is then skipped.
            form_matrices[index] = matrix
        else:
            yield index, obj, matrix


def _display_mapping(geometry: PageGeometry) -> Callable[[Box], Box]:
    """:func:`to_display_box` for the page ``geometry`` describes."""
    return partial(to_display_box, rotation_deg=geometry.rotation_deg, page_box=geometry.page_box)


def _collect_segments(page_raw: pdfium_c.FPDF_PAGE, geometry: PageGeometry) -> PageSegments:
    """The segments of every path object on the page, forms' included, in object-walk order."""
    to_display = _display_mapping(geometry)
    segments: list[RawSegment] = []
    skipped = 0
    for index, path, matrix in _placed_objects(page_raw, pdfium_c.FPDF_PAGEOBJ_PATH):
        found = _path_object_segments(path, index, matrix, to_display)
        if found is None:
            skipped += 1
        else:
            segments.extend(found)
    return PageSegments(segments=tuple(segments), stats=SegmentStats(skipped_nonfinite_paths=skipped))


def _read_segments(page: pdfium.PdfPage, geometry: PageGeometry) -> PageSegments:
    """Read a loaded page's segments."""
    try:
        return _collect_segments(page.raw, geometry)
    except PdfiumFailure as exc:
        raise _pdfium_error(geometry.page_number, exc) from exc


def page_segments(handle: DocumentHandle, index: int) -> PageSegments:
    """Return the axis-aligned segments of the page at 0-based ``index``.

    Every path object is read, those inside Form XObjects included (as deep
    as pdfium parses them), with its full transform to page space composed
    here. A stroked path gives each straight edge at least 12 pt long and
    axis-aligned within 0.5 pt (closing edges included; Bézier curves and
    diagonals are ignored); a filled one gives each rectangle at most 3 pt
    thick and at least 12 pt long as one ``thin_fill`` along its length, and
    each side at least 12 pt long of a thicker rectangle as a ``fill_edge``
    (see :class:`RawSegment`). Segments follow the object walk, and within one
    path its fill's before its stroke's, as PDF paints them. A path whose
    page-space geometry is not finite (matrix, point or stroke width) is
    skipped and counted in :class:`SegmentStats`; pdfium still renders the
    rest of such a page. The page handle is closed before returning.

    Raises:
        TypeError: If ``handle`` is not a DocumentHandle, or ``index`` is not
            an int (bool rejected).
        ValueError: If ``handle`` is closed.
        IndexError: If ``index`` is outside ``0..page_count - 1``.
        PDFCorruptError: If pdfium cannot load the page or a pdfium call on
            it fails (``page_number`` is set).
    """
    page, page_number = _load_page(handle, index)
    try:
        return _read_segments(page, _read_geometry(page, page_number))
    finally:
        page.close()


def _read_images(page: pdfium.PdfPage, geometry: PageGeometry) -> PageImages:
    """Read a loaded page's image objects (no pixel is decoded)."""
    try:
        placed = _placed_objects(page.raw, pdfium_c.FPDF_PAGEOBJ_IMAGE)
        return collect_images(placed, page.raw, geometry.page_box, _display_mapping(geometry))
    except PdfiumFailure as exc:
        raise _pdfium_error(geometry.page_number, exc) from exc


def page_images(handle: DocumentHandle, index: int) -> PageImages:
    """Return the image objects of the page at 0-based ``index``, with their stats.

    Every image object is read: XObjects and inline images (``BI`` ... ``EI``),
    those inside Form XObjects included (as deep as pdfium parses them), with
    the full transform to page space composed here. No size filter applies:
    these are raw facts (A4 applies ``min_image_dimension``). No pixel is
    decoded here: each image's raw stream is copied out once to hash it
    (within a per-page stream budget), JPEG and JPEG 2000 sizes are read
    from their own headers, and pdfium is asked for bits per pixel and
    colour space only for images it may decode and reads them for without
    decoding (not JPX or JBIG2). Pixels are decoded by :class:`ImageLoader`.
    An image whose page-space geometry is not finite is skipped; images too
    big, of unknown size or past the budget are recorded with those facts
    left out; each is counted in :class:`ImageStats`. The page handle is
    closed before returning.

    Raises:
        TypeError: If ``handle`` is not a DocumentHandle, or ``index`` is not
            an int (bool rejected).
        ValueError: If ``handle`` is closed.
        IndexError: If ``index`` is outside ``0..page_count - 1``.
        PDFCorruptError: If pdfium cannot load the page or a pdfium call on
            it fails (``page_number`` is set).
    """
    page, page_number = _load_page(handle, index)
    try:
        return _read_images(page, _read_geometry(page, page_number))
    finally:
        page.close()


@dataclass(frozen=True, slots=True, kw_only=True)
class RawPage:
    """One page's raw facts, read by :func:`extract_page` from one load of the page.

    Picklable, so it crosses the process pool: its images carry no pixels
    (an :class:`ImageLoader` reads them on demand).

    Attributes:
        geometry: The displayed size, rotation and crop offset, as
            :func:`page_geometry` returns them.
        text: The spans and textpage counts, as :func:`page_text` returns them.
        segments: The axis-aligned segments, as :func:`page_segments` returns
            them.
        images: The image objects and their stats, as :func:`page_images`
            returns them.
    """

    geometry: PageGeometry
    text: PageText
    segments: PageSegments
    images: PageImages

    def __post_init__(self) -> None:
        checks = (
            ("geometry", self.geometry, PageGeometry),
            ("text", self.text, PageText),
            ("segments", self.segments, PageSegments),
            ("images", self.images, PageImages),
        )
        for name, value, expected in checks:
            if not isinstance(value, expected):
                raise TypeError(f"{name} must be a {expected.__name__}, got {type(value).__name__}")

    @property
    def page_number(self) -> int:
        """The 1-based page number, read from ``geometry``."""
        return self.geometry.page_number


PageStage = Literal["page", "text", "objects"]
_STAGES: tuple[PageStage, ...] = ("page", "text", "objects")


@dataclass(frozen=True, slots=True, kw_only=True)
class PageFailure:
    """A page :func:`extract_page` could not read, and the stage that failed.

    A raw fact, not an error: the caller decides what it means (A3/A5:
    ``strict`` raises ``PDFCorruptError(message, page_number=page_number)``;
    otherwise the page falls back to vision or becomes an error page).
    Picklable, so it crosses the process pool.

    Attributes:
        page_number: 1-based page number.
        stage: ``"page"`` (loading the page or reading its geometry),
            ``"text"`` (its textpage) or ``"objects"`` (its segments, then its
            images).
        message: The message of the PDFCorruptError that the stage's own
            function (:func:`page_geometry`, :func:`page_text`,
            :func:`page_segments` or :func:`page_images`) raises for the page.
    """

    page_number: int
    stage: PageStage
    message: str

    def __post_init__(self) -> None:
        int_at_least("page_number", self.page_number, 1)
        _checked_choice("stage", self.stage, _STAGES)
        non_blank_str("message", self.message)


def extract_page(handle: DocumentHandle, index: int) -> RawPage | PageFailure:
    """Return every raw fact of the page at 0-based ``index``, read from one load of it.

    The page is loaded once and read in stages: geometry, text, then
    segments and images; the page and its textpage are closed before
    returning. A good page equals the :class:`RawPage` of
    :func:`page_geometry`, :func:`page_text`, :func:`page_segments` and
    :func:`page_images` (each of which loads the page again). A page pdfium
    cannot read is no error here: the PDFCorruptError a stage raises becomes
    a :class:`PageFailure` of that stage with the same message, so one bad
    page never fails a batch. Any other error (a PDFCorruptError naming no
    page included) propagates unchanged: it is a bug, not a page fault.

    Raises:
        TypeError: If ``handle`` is not a DocumentHandle, or ``index`` is not
            an int (bool rejected).
        ValueError: If ``handle`` is closed.
        IndexError: If ``index`` is outside ``0..page_count - 1``.
    """
    try:
        page, page_number = _load_page(handle, index)
    except PDFCorruptError as exc:
        if exc.page_number is None:
            raise
        return PageFailure(page_number=exc.page_number, stage="page", message=str(exc))
    try:
        return _read_stages(page, page_number)
    finally:
        page.close()


def _read_stages(page: pdfium.PdfPage, page_number: int) -> RawPage | PageFailure:
    """Read a loaded page stage by stage; a stage's PDFCorruptError becomes its PageFailure."""
    stage: PageStage = "page"
    try:
        geometry = _read_geometry(page, page_number)
        stage = "text"
        text = _read_page_text(page, geometry)
        stage = "objects"
        segments = _read_segments(page, geometry)
        images = _read_images(page, geometry)
    except PDFCorruptError as exc:
        return PageFailure(page_number=page_number, stage=stage, message=str(exc))
    return RawPage(geometry=geometry, text=text, segments=segments, images=images)


@dataclass(frozen=True, slots=True, kw_only=True)
class ImageLoader:
    """Reads one image object's bytes on demand, from the PDF on disk.

    A picklable stand-in for the image (it crosses the process pool): each
    read reopens ``source`` (with ``password``), finds the object by the same
    object walk that numbered it, and checks it is still ``image`` (its raw
    stream's hash and its pixel facts) before returning anything.

    Attributes:
        source: The PDF's path, made absolute at construction (a worker's
            later ``chdir`` cannot redirect it).
        page_index: The 0-based page holding the image.
        image: The image as :func:`page_images` recorded it; it must have a
            ``raw_sha256`` (its stream was read). ``object_index`` and
            ``raw_sha256`` read from it.
        password: The encrypted PDF's password, or None. Excluded from repr,
            equality and hashing, and never put in an error message; a
            pickle of the loader carries it (the worker needs it).
    """

    source: Path
    page_index: int
    image: RawImage
    password: str | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _checked_path(self.source, "source").absolute())
        int_at_least("page_index", self.page_index, 0)
        if not isinstance(self.image, RawImage):
            raise TypeError(f"image must be a RawImage, got {type(self.image).__name__}")
        if self.image.raw_sha256 is None:
            raise ValueError(
                "image has no raw_sha256 (its stream was not read): there is nothing to verify a read against"
            )
        _checked_password(self.password)

    @property
    def object_index(self) -> int:
        """The image object's index in its page's object walk."""
        return self.image.object_index

    @property
    def raw_sha256(self) -> str:
        """The SHA-256 every read verifies the image's raw stream against."""
        digest = self.image.raw_sha256
        if digest is None:  # refused at construction; this narrows the type
            raise ValueError("image has no raw_sha256")
        return digest

    def raw_bytes(self) -> bytes:
        """Return the image's raw stream, as :attr:`RawImage.raw_sha256` hashed it.

        Raises:
            DatagruntPDFError: If the object at ``page_index`` /
                ``object_index`` is not an image whose raw stream hashes to
                ``raw_sha256`` and whose pixel facts are ``image``'s (the file
                changed), or the stream is over the asset limit.
            PDFCorruptError: If pdfium cannot load the page or a pdfium call
                on it fails (``page_number`` is set).
            FileNotFoundError, PermissionError, PDFEncryptedError: As
                :func:`open_document` raises them.
        """
        return self._read(lambda image, raw, facts, label: raw)

    def png_bytes(self) -> bytes:
        """Return the image decoded by pdfium at its own pixel size, as PNG bytes.

        A derived artifact, not what ``raw_sha256`` hashes: the image is
        verified as :meth:`raw_bytes` verifies it, then decoded with
        ``FPDFImageObj_GetBitmap`` (its samples through its filters and
        colour space; its matrix, soft mask and image mask not applied) and
        encoded with fixed Pillow settings, so the same image gives the same
        bytes on every run with a given Pillow and zlib build.

        Raises:
            DatagruntPDFError: As :meth:`raw_bytes` raises it, or, before
                pdfium decodes anything, if the image would decode to more
                than 89,478,485 pixels (Pillow's default decompression-bomb
                bound: 1 GiB / 4 / 3) or in more than 4096 JPEG 2000 tiles,
                or its decode size cannot be known first (see
                :class:`ImageStats`); or if the bitmap pdfium decodes is over
                the pixel bound after all.
            PDFCorruptError: If pdfium cannot load the page, decode the image,
                or a pdfium call fails (``page_number`` is set).
            FileNotFoundError, PermissionError, PDFEncryptedError: As
                :func:`open_document` raises them.
        """
        return self._read(lambda image, raw, facts, label: decoded_png(image, facts, label))

    def _read(self, finish: Callable[[pdfium_c.FPDF_PAGEOBJECT, bytes, PixelFacts, str], bytes]) -> bytes:
        """Reopen the PDF, verify the image and return ``finish(image, raw stream, pixel facts, label)``.

        The page and the document are closed before returning.
        """
        with open_document(self.source, password=self.password) as handle:
            if self.page_index >= handle.page_count:
                raise self._changed(f"it has {handle.page_count} page(s)")
            page, page_number = _load_page(handle, self.page_index)
            try:
                image, raw, facts = self._verified_image(page.raw)
                return finish(image, raw, facts, f"object {self.object_index}")
            except PdfiumFailure as exc:
                message = f"{self.source}: page {page_number}: pdfium cannot {exc}"
                raise PDFCorruptError(message, page_number=page_number) from exc
            except (OversizedStream, DecodeRefused) as exc:
                raise DatagruntPDFError(f"{self.source}: page {page_number}: {exc}") from exc
            finally:
                page.close()

    def _verified_image(self, page_raw: pdfium_c.FPDF_PAGE) -> tuple[pdfium_c.FPDF_PAGEOBJECT, bytes, PixelFacts]:
        """The image object this loader names on the loaded page, its verified raw stream and pixel facts.

        The stream must hash to ``raw_sha256``, and the facts read from the
        image dictionary and codestream (filters, decode size, declared size,
        and the bits per pixel and colour space when pdfium reads them) must
        be the image's: the same stream under another dictionary would decode
        to another picture. Dictionary changes that keep all of these (a
        /Decode array, a palette's colours) are not detected.
        """
        objects = _page_objects(page_raw)
        if self.object_index >= len(objects):
            raise self._changed(f"the page has {len(objects)} object(s)")
        image, _form = objects[self.object_index]
        kind = pdfium_c.FPDFPageObj_GetType(image)
        if kind != pdfium_c.FPDF_PAGEOBJ_IMAGE:
            name = _OBJECT_KIND_NAMES.get(kind, f"type {kind}")
            raise self._changed(f"object {self.object_index} is a {name} object, not an image")
        label = f"object {self.object_index}"
        raw = read_raw_stream(image, label)
        actual = hashlib.sha256(raw).hexdigest()
        if actual != self.raw_sha256:
            raise self._changed(f"its raw stream hashes to {actual}, not {self.raw_sha256}")
        filters = read_filters(image, label)
        decoded = partial(ascii_decoded_stream, image, len(raw), label)
        facts = pixel_facts(image, page_raw, label, filters, declared_size(image, label), raw, decoded)
        found = _described(filters, facts.px_width, facts.px_height, facts.declared_px, facts, facts.read_metadata)
        expected = _described(
            self.image.filters,
            self.image.px_width,
            self.image.px_height,
            self.image.declared_px,
            self.image,
            facts.read_metadata,
        )
        if found != expected:
            raise self._changed(f"the image now reads {found}; the loader expects {expected}")
        return image, raw, facts

    def _changed(self, detail: str) -> DatagruntPDFError:
        """The error for a source that no longer holds this image (never naming the password)."""
        where = f"{self.source}: page {self.page_index + 1}, object {self.object_index}"
        return DatagruntPDFError(f"{where} is not the image this loader was made for: the file changed ({detail})")


def _described(
    filters: tuple[str, ...],
    width: int,
    height: int,
    declared: tuple[int, int],
    metadata: RawImage | PixelFacts,
    with_metadata: bool,
) -> str:
    """An image's pixel facts in words, its bits per pixel and colour space only ``with_metadata``."""
    words = f"{'/'.join(filters) or 'no filter'}, {width} x {height} px (declared {declared[0]} x {declared[1]})"
    if not with_metadata:
        return words
    return f"{words}, {metadata.bits_per_pixel} bits per pixel, colour space {metadata.colorspace}"


# pdfium's page object types, named for error messages.
_OBJECT_KIND_NAMES = MappingProxyType(
    {
        pdfium_c.FPDF_PAGEOBJ_TEXT: "text",
        pdfium_c.FPDF_PAGEOBJ_PATH: "path",
        pdfium_c.FPDF_PAGEOBJ_IMAGE: "image",
        pdfium_c.FPDF_PAGEOBJ_SHADING: "shading",
        pdfium_c.FPDF_PAGEOBJ_FORM: "form",
    }
)
