"""pypdfium2 engine for the v5 PDF document model (#365).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.

pdfium behaviour this module relies on, observed with pypdfium2 5.14.0 /
pdfium 156.0.8076.0 on ReportLab-, PyMuPDF- and hand-built PDFs (#365 T1),
and pinned by ``tests/pdf_tests/test_pdfium_engine.py``:

- Char boxes (``PdfTextPage.get_charbox``) and object bounds
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
"""

import errno
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c

from datagrunt.pdf._validate import finite_float, int_at_least
from datagrunt.pdf.exceptions import PDFCorruptError, PDFEncryptedError

Box = tuple[float, float, float, float]

# ``get_rotation()`` normalises /Rotate (-90 reads 270, 450 reads 90, 45 reads 0).
_ROTATIONS = (0, 90, 180, 270)


def _checked_rotation(rotation_deg: object) -> int:
    """Return ``rotation_deg`` if it is one of 0, 90, 180, 270 (bool rejected)."""
    if isinstance(rotation_deg, bool) or not isinstance(rotation_deg, int):
        raise TypeError(f"rotation_deg must be an int, got {rotation_deg!r} ({type(rotation_deg).__name__})")
    if rotation_deg not in _ROTATIONS:
        raise ValueError(f"rotation_deg must be one of {_ROTATIONS}, got {rotation_deg!r}")
    return rotation_deg


def _finite_tuple(name: str, value: object, size: int) -> tuple[float, ...]:
    """Return ``value`` as a tuple of ``size`` finite floats."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple, got {type(value).__name__}")
    if len(value) != size:
        raise ValueError(f"{name} must have {size} values, got {len(value)}")
    return tuple(finite_float(f"{name}[{index}]", item) for index, item in enumerate(value))


def _checked_box(name: str, box: object, *, allow_empty: bool) -> Box:
    """Return ``box`` as 4 finite floats ``(left, bottom, right, top)``, edges in order."""
    left, bottom, right, top = _finite_tuple(name, box, 4)
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
        object.__setattr__(self, "crop_offset", _finite_tuple("crop_offset", self.crop_offset, 2))

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


def _checked_path(path: object) -> Path:
    """Return ``path`` as a Path; bytes are refused (pypdfium2 reads them as PDF data)."""
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError(f"path must be a str or os.PathLike, got {type(path).__name__}")
    text = os.fspath(path)
    if not isinstance(text, str):
        raise TypeError(f"path must be a str or os.PathLike, got a {type(text).__name__} path")
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
_LOAD_ERROR_NAMES = {
    pdfium_c.FPDF_ERR_UNKNOWN: "Unknown error",
    pdfium_c.FPDF_ERR_FILE: "File access error",
    pdfium_c.FPDF_ERR_FORMAT: "Data format error",
    pdfium_c.FPDF_ERR_PASSWORD: "Incorrect password error",
    pdfium_c.FPDF_ERR_SECURITY: "Unsupported security scheme error",
    pdfium_c.FPDF_ERR_PAGE: "Page not found or content error",
}

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


def _rotation_and_visible_box(pdf: pdfium.PdfDocument, index: int) -> tuple[int, Box]:
    """Read one page's /Rotate and visible box, closing the page handle."""
    page = pdf[index]
    try:
        return page.get_rotation(), page.get_bbox()
    finally:
        page.close()


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
    if not isinstance(handle, DocumentHandle):
        raise TypeError(f"handle must be a DocumentHandle, got {type(handle).__name__}")
    pdf = handle._require_open()
    page_number = _checked_index(index, handle.page_count) + 1
    try:
        rotation, visible_box = _rotation_and_visible_box(pdf, page_number - 1)
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
