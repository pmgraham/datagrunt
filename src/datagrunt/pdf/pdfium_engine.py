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

Text (#365 T2), pinned by ``tests/pdf_tests/test_pdfium_spans.py``:

- pdfium makes one text object per text-showing operator (``Tj``, ``TJ``,
  ``'``, ``"``), so one object's chars share one baseline. Non-generated
  textpage chars name their owner (``FPDFText_GetTextObject``), except the
  space pdfium inserts between two right-to-left words drawn by separate
  operators: it is not flagged generated and has no owner. A generated
  space inside one object (a wide ``TJ`` gap) names that object.
- Right-to-left text is in logical order: a Hebrew word drawn left to right
  in visual order reads from its rightmost glyph.
- Textpage char boxes and ``FPDFText_GetMatrix`` are page space with every
  enclosing form's matrix applied; ``get_bounds()`` and
  ``FPDFPageObj_GetMatrix`` of an object inside a form are in the form's
  space. pdfium parses forms at most 40 levels deep.
- A non-BMP char is two textpage chars, a UTF-16 surrogate pair with equal
  boxes. An unmapped char code reads as itself, flagged by
  ``FPDFText_HasUnicodeMapError``; without a ToUnicode map a 3- or 4-byte
  CMap code can read beyond U+10FFFF (0x110000, 0xFFFFFFFF).
- The textpage is not every drawn char: pdfium drops text at ``Tf 0``, at
  ``Tz 0`` or under ~0.001 pt, objects of a single space, and a char drawn
  on an identical char at the same place (fake-bold removal); it collapses
  runs of spaces to one, decomposes ligatures (U+FB01 reads "fi") and
  mirrors "(" in right-to-left text (it reads ")"). The engine keeps every
  char the textpage reports.
- Standard-14 fonts have no descriptor: their flags lack ForceBold and their
  weight reads 0, so only the base name says bold. A non-embedded alias
  reads as the standard-14 font pdfium substitutes (``Arial,BoldItalic``
  reads ``Helvetica-BoldOblique``). A pattern fill reads as a colour pdfium
  picks (opaque 75% grey for a tiling pattern).
"""

import ctypes
import errno
import math
import os
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import NamedTuple, NoReturn

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


def _checked_display_box(name: str, box: object) -> Box:
    """Return ``box`` as 4 finite floats ``(x0, y0, x1, y1)`` with ``x0 <= x1`` and ``y0 <= y1``."""
    x0, y0, x1, y1 = _finite_tuple(name, box, 4)
    if x0 > x1 or y0 > y1:
        raise ValueError(f"{name} must have x0 <= x1 and y0 <= y1, got {box!r}")
    return x0, y0, x1, y1


def _union(boxes: tuple[Box, ...]) -> Box:
    """The smallest box holding every one of ``boxes`` (at least one)."""
    return (
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    )


def _checked_bool(name: str, value: object) -> bool:
    """Return ``value`` if it is exactly a bool (not an int standing in for one)."""
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool, got {value!r} ({type(value).__name__})")
    return value


def _checked_rgba(name: str, value: object) -> tuple[int, int, int, int]:
    """Return ``value`` if it is a tuple of 4 ints in 0..255 (bool rejected)."""
    if not isinstance(value, tuple):
        raise TypeError(f"{name} must be a tuple, got {type(value).__name__}")
    if len(value) != 4:
        raise ValueError(f"{name} must have 4 channels, got {len(value)}")
    for index, channel in enumerate(value):
        if isinstance(channel, bool) or not isinstance(channel, int):
            raise TypeError(f"{name}[{index}] must be an int, got {channel!r} ({type(channel).__name__})")
        if not 0 <= channel <= 255:
            raise ValueError(f"{name}[{index}] must be in 0..255, got {channel}")
    red, green, blue, alpha = value
    return red, green, blue, alpha


@dataclass(frozen=True, slots=True, kw_only=True)
class RawSpan:
    """A maximal run of consecutive textpage chars owned by one text object.

    pdfium makes one text object per text-showing operator (``Tj``, ``TJ``,
    ``'``, ``"``), so a span never crosses a line. Boxes are displayed,
    top-left ``(x0, y0, x1, y1)`` points, as :func:`to_display_box` returns.

    Attributes:
        text: The chars pdfium's textpage reports for the object, each
            kept: control chars, U+FFFD and lone surrogates included (A3
            classifies them). A surrogate pair pdfium reports as two chars
            is one char here; a raw code beyond U+10FFFF is U+FFFD. The
            textpage itself drops or rewrites some drawn chars (see the
            module notes: ``Tf 0`` text, space runs, ligatures).
        bbox: The union of ``char_boxes``.
        char_boxes: One box per char of ``text``: pdfium's loose char box
            (advance width by the font's ascent-to-descent height).
        font_size_pt: The ``Tf`` size times the vertical scale of the text's
            page-space matrix (form matrices included); negative for a
            negative ``Tf`` (mirrored glyphs).
        base_font_name: The font's /BaseFont without a subset tag
            (``ABCDEF+``); empty for a font without one (Type3). For a
            non-embedded Type1 font pdfium aliases to a standard-14 font,
            this is pdfium's alias, not the PDF's name: ``Arial-BoldMT``
            reads ``Helvetica-Bold`` and ``TimesNewRomanPS-BoldItalicMT``
            reads ``Times-BoldItalic``.
        is_bold: From a whole style word of the font name (Bold, Black,
            Heavy, Semibold, Bd, Blk, Hv, ...), the ForceBold flag, or
            weight >= 600.
        is_italic: From a whole style word of the font name (Italic,
            Oblique, It, Obl, Slanted) or the Italic flag.
        render_mode: The text render mode, 0 (fill) to 7 (clip).
        fill_rgba: Fill colour and alpha as 0-255 ints, as pdfium reports
            them (a pattern fill reads as a colour pdfium picks).
        stroke_rgba: Stroke colour and alpha as 0-255 ints.
        object_index: The owning text object's position in the page's
            depth-first, paint-order object walk (forms count, then their
            contents), so it also gives the span's z-order.
        generated_breaks: Offsets into ``text``, strictly increasing and
            each in ``1..len(text) - 1``, before which pdfium generated a
            char (a space or line break) inside this span, as at a wide
            ``TJ`` gap: ``[(Name) -12000 (Value)] TJ`` is text "NameValue"
            with a break at 4. The generated chars are not inserted into
            ``text``.
    """

    text: str
    bbox: Box
    char_boxes: tuple[Box, ...]
    font_size_pt: float
    base_font_name: str
    is_bold: bool
    is_italic: bool
    render_mode: int
    fill_rgba: tuple[int, int, int, int]
    stroke_rgba: tuple[int, int, int, int]
    object_index: int
    generated_breaks: tuple[int, ...]

    def __post_init__(self) -> None:
        self._check_text_and_boxes()
        self._check_generated_breaks()
        object.__setattr__(self, "font_size_pt", finite_float("font_size_pt", self.font_size_pt))
        if not isinstance(self.base_font_name, str):
            raise TypeError(f"base_font_name must be a str, got {self.base_font_name!r}")
        _checked_bool("is_bold", self.is_bold)
        _checked_bool("is_italic", self.is_italic)
        if isinstance(self.render_mode, bool) or not isinstance(self.render_mode, int):
            raise TypeError(f"render_mode must be an int, got {self.render_mode!r}")
        if self.render_mode not in range(8):
            raise ValueError(f"render_mode must be in 0..7, got {self.render_mode}")
        _checked_rgba("fill_rgba", self.fill_rgba)
        _checked_rgba("stroke_rgba", self.stroke_rgba)
        int_at_least("object_index", self.object_index, 0)

    def _check_text_and_boxes(self) -> None:
        if not isinstance(self.text, str):
            raise TypeError(f"text must be a str, got {type(self.text).__name__}")
        if not self.text:
            raise ValueError("text must not be empty")
        if not isinstance(self.char_boxes, tuple):
            raise TypeError(f"char_boxes must be a tuple, got {type(self.char_boxes).__name__}")
        if len(self.char_boxes) != len(self.text):
            counts = f"{len(self.char_boxes)} boxes for {len(self.text)} chars"
            raise ValueError(f"char_boxes must hold one box per char of text: {counts}")
        boxes = tuple(_checked_display_box(f"char_boxes[{i}]", box) for i, box in enumerate(self.char_boxes))
        bbox = _checked_display_box("bbox", self.bbox)
        if bbox != _union(boxes):
            raise ValueError(f"bbox must be the union of char_boxes, {_union(boxes)!r}, got {self.bbox!r}")
        object.__setattr__(self, "char_boxes", boxes)
        object.__setattr__(self, "bbox", bbox)

    def _check_generated_breaks(self) -> None:
        breaks = self.generated_breaks
        if not isinstance(breaks, tuple):
            raise TypeError(f"generated_breaks must be a tuple, got {type(breaks).__name__}")
        last = len(self.text) - 1
        for index, offset in enumerate(breaks):
            int_at_least(f"generated_breaks[{index}]", offset, 0)
            if not 1 <= offset <= last:
                raise ValueError(f"generated_breaks[{index}] must be inside text, in 1..{last}, got {offset}")
        if any(later <= earlier for earlier, later in zip(breaks, breaks[1:])):
            raise ValueError(f"generated_breaks must be strictly increasing, got {breaks!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class TextStats:
    """Counts of one page's textpage chars, for A3's text-quality checks.

    The three char counts are disjoint and sum to the textpage's char count.

    Attributes:
        char_count: Non-generated textpage chars a text object owns: the
            chars of the page's spans (a non-BMP char counts twice, as a
            surrogate pair).
        generated_count: Chars pdfium flags as generated (spaces and line
            breaks it inserts); they are in no span.
        ownerless_count: Non-generated chars no text object owns
            (``FPDFText_GetTextObject`` is NULL): pdfium inserts them, as
            the space between two right-to-left words drawn by separate
            operators; they are in no span.
        unicode_map_error_count: Owned chars pdfium could not map to
            Unicode (``FPDFText_HasUnicodeMapError``); each reads as its raw
            char code. A raw code beyond U+10FFFF (a 3- or 4-byte CMap code
            with no ToUnicode entry) reads as U+FFFD and counts here once,
            whether pdfium flags it or not.
    """

    char_count: int
    generated_count: int
    ownerless_count: int
    unicode_map_error_count: int

    def __post_init__(self) -> None:
        int_at_least("char_count", self.char_count, 0)
        int_at_least("generated_count", self.generated_count, 0)
        int_at_least("ownerless_count", self.ownerless_count, 0)
        int_at_least("unicode_map_error_count", self.unicode_map_error_count, 0)
        if self.unicode_map_error_count > self.char_count:
            counts = f"unicode_map_error_count ({self.unicode_map_error_count}) cannot exceed"
            raise ValueError(f"{counts} char_count ({self.char_count})")


@dataclass(frozen=True, slots=True, kw_only=True)
class PageText:
    """One page's spans in textpage order, with its textpage counts.

    Every owned, non-generated textpage char is in exactly one span: one
    span char per textpage char, or one per surrogate pair.
    """

    spans: tuple[RawSpan, ...]
    stats: TextStats

    def __post_init__(self) -> None:
        if not isinstance(self.spans, tuple):
            raise TypeError(f"spans must be a tuple, got {type(self.spans).__name__}")
        for index, item in enumerate(self.spans):
            if not isinstance(item, RawSpan):
                raise TypeError(f"spans[{index}] must be a RawSpan, got {type(item).__name__}")
        if not isinstance(self.stats, TextStats):
            raise TypeError(f"stats must be a TextStats, got {type(self.stats).__name__}")
        span_chars = sum(len(item.text) for item in self.spans)
        if not span_chars <= self.stats.char_count <= 2 * span_chars:
            message = f"spans hold {span_chars} chars, which {self.stats.char_count} textpage chars cannot make"
            raise ValueError(f"{message} (each is 1 textpage char, or 2 for a surrogate pair)")


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


class _PdfiumFailure(Exception):
    """A pdfium call failed; page_text reports it as PDFCorruptError for the page.

    The message completes "pdfium cannot ...".
    """


def _address(pointer: pdfium_c.FPDF_PAGEOBJECT) -> int:
    """The address a pdfium object pointer holds (0 for NULL), to compare objects."""
    return ctypes.cast(pointer, ctypes.c_void_p).value or 0


# pdfium parses Form XObjects at most 40 levels deep (observed: 80 nested
# forms yield the text of 40), so this bound reaches every object it parsed.
_MAX_FORM_DEPTH = 64


def _children(
    count: int, get_child: Callable[[int], pdfium_c.FPDF_PAGEOBJECT], owner: str
) -> list[pdfium_c.FPDF_PAGEOBJECT]:
    """The ``count`` objects of the page or a form, in paint order; ``owner`` names it."""
    if count < 0:
        raise _PdfiumFailure(f"count the objects of {owner}")
    children = []
    for index in range(count):
        child = get_child(index)
        if not child:
            raise _PdfiumFailure(f"read object {index} of {owner}")
        children.append(child)
    return children


def _page_objects(page_raw: pdfium_c.FPDF_PAGE) -> list[pdfium_c.FPDF_PAGEOBJECT]:
    """Every page object in depth-first paint order: a form, then its contents."""
    top = _children(
        pdfium_c.FPDFPage_CountObjects(page_raw), partial(pdfium_c.FPDFPage_GetObject, page_raw), "the page"
    )
    ordered: list[pdfium_c.FPDF_PAGEOBJECT] = []
    stack = [iter(top)]
    # Bounded: one pass per object and one per form, each from a pdfium count,
    # and the stack never holds more than _MAX_FORM_DEPTH + 1 levels.
    while stack:
        child = next(stack[-1], None)
        if child is None:
            stack.pop()
            continue
        ordered.append(child)
        if len(stack) <= _MAX_FORM_DEPTH and pdfium_c.FPDFPageObj_GetType(child) == pdfium_c.FPDF_PAGEOBJ_FORM:
            count = pdfium_c.FPDFFormObj_CountObjects(child)
            owner = f"form object {len(ordered) - 1}"
            stack.append(iter(_children(count, partial(pdfium_c.FPDFFormObj_GetObject, child), owner)))
    return ordered


def _object_indices(page_raw: pdfium_c.FPDF_PAGE) -> dict[int, int]:
    """Map each page object's address to its depth-first paint-order index."""
    return {_address(obj): index for index, obj in enumerate(_page_objects(page_raw))}


class _Style(NamedTuple):
    """The RawSpan fields read from a span's text object."""

    font_size_pt: float
    base_font_name: str
    is_bold: bool
    is_italic: bool
    render_mode: int
    fill_rgba: tuple[int, int, int, int]
    stroke_rgba: tuple[int, int, int, int]


def _rgba(getter: Callable[..., int], text_object: pdfium_c.FPDF_PAGEOBJECT, what: str) -> tuple[int, int, int, int]:
    """Read a page object's fill or stroke colour (``what``) as 0-255 (R, G, B, A)."""
    red, green, blue, alpha = (ctypes.c_uint() for _ in range(4))
    if not getter(text_object, red, green, blue, alpha):
        raise _PdfiumFailure(f"read the {what}")
    return red.value, green.value, blue.value, alpha.value


# PDF 32000-1 font descriptor flags (Table 121) and weights (Table 122).
_FLAG_ITALIC = 0x40
_FLAG_FORCE_BOLD = 0x40000
_BOLD_WEIGHT = 600
# Whole style words, lowercased: SemiBold is "semi" + "bold", BdIt "bd" + "it".
_BOLD_NAME_TOKENS = frozenset(
    {"bold", "black", "heavy", "semibold", "demibold", "extrabold", "ultrabold", "bd", "blk", "hv"}
)
_ITALIC_NAME_TOKENS = frozenset({"italic", "oblique", "it", "obl", "slanted"})
# A font name's words: capitals before a capitalised word (LT in LTStd), a
# capitalised or lowercase word, the rest of a run of capitals, a digit run.
_NAME_WORD = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")
# A font subset's /BaseFont starts with exactly six capitals and "+" (PDF 32000-1, 9.9.2).
_SUBSET_TAG = re.compile(r"[A-Z]{6}\+")
_STYLE_SEPARATOR = re.compile(r"[-,]")
# PDF names are at most 127 bytes in practice (PDF 32000-1, Annex C); this
# bound, NUL included, only stops a hostile length from sizing the buffer.
_MAX_FONT_NAME_BYTES = 4096


def _base_font_name(font: pdfium_c.FPDF_FONT, label: str) -> str:
    """Read a font's /BaseFont without its subset tag.

    pdfium returns the name as UTF-8; bytes that are not UTF-8 are kept as
    visible ``\\xNN`` escapes rather than replaced.
    """
    size = pdfium_c.FPDFFont_GetBaseFontName(font, None, 0)
    if size < 1:
        raise _PdfiumFailure(f"read the font name of {label}")
    if size > _MAX_FONT_NAME_BYTES:
        raise _PdfiumFailure(
            f"read the font name of {label}: {size} bytes is over the {_MAX_FONT_NAME_BYTES}-byte bound"
        )
    buffer = ctypes.create_string_buffer(size)
    if pdfium_c.FPDFFont_GetBaseFontName(font, buffer, size) != size:
        raise _PdfiumFailure(f"read the font name of {label}")
    name = buffer.raw[: size - 1].decode("utf-8", errors="backslashreplace")
    return name[7:] if _SUBSET_TAG.match(name) else name


def _style_words(name: str) -> frozenset[str]:
    """The lowercased words of a font name's style part, split on case and digit boundaries.

    The style part follows the first "-" or "," (Arial-BoldMT: "bold",
    "mt"; Arial,Bold: "bold"). A name without one starts with its family
    word, which is skipped (ArialBlack: "black"; BlackadderITC: "itc").
    Matching whole style words keeps family names such as BlackChancery and
    BlackadderITC-Regular from reading bold.
    """
    separator = _STYLE_SEPARATOR.search(name)
    if separator:
        return frozenset(word.lower() for word in _NAME_WORD.findall(name[separator.end() :]))
    return frozenset(word.lower() for word in _NAME_WORD.findall(name)[1:])


def _bold_and_italic(font: pdfium_c.FPDF_FONT, name: str, label: str) -> tuple[bool, bool]:
    """Read bold and italic from the font name, its descriptor flags and its weight.

    Each source alone suffices: standard-14 fonts have no descriptor (flags
    and weight read 0), so their name is the only one that says bold.
    pdfium keeps the low 19 bits of /Flags, so only a NULL font reads
    negative (-1, every bit set): that is a failure, not bold italic. The
    weight is not a failure signal: pdfium derives it from /StemV (114
    reads 570, so CMBX10 is not bold unless its name says so), and a
    negative /StemV reads negative (-5 reads -25).
    """
    style = _style_words(name)
    flags = pdfium_c.FPDFFont_GetFlags(font)
    if flags < 0:
        raise _PdfiumFailure(f"read the font flags of {label}")
    is_bold = (
        not style.isdisjoint(_BOLD_NAME_TOKENS)
        or bool(flags & _FLAG_FORCE_BOLD)
        or pdfium_c.FPDFFont_GetWeight(font) >= _BOLD_WEIGHT
    )
    is_italic = not style.isdisjoint(_ITALIC_NAME_TOKENS) or bool(flags & _FLAG_ITALIC)
    return is_bold, is_italic


def _font_size(
    textpage: pdfium_c.FPDF_TEXTPAGE, text_object: pdfium_c.FPDF_PAGEOBJECT, char_index: int, label: str
) -> float:
    """``Tf`` size times the vertical scale of the char's page-space matrix.

    The textpage matrix includes every enclosing form's matrix; the object's
    own matrix (``FPDFPageObj_GetMatrix``) is in its form's space.
    """
    nominal = ctypes.c_float()
    if not pdfium_c.FPDFTextObj_GetFontSize(text_object, nominal):
        raise _PdfiumFailure(f"read the font size of {label}")
    matrix = pdfium_c.FS_MATRIX()
    if not pdfium_c.FPDFText_GetMatrix(textpage, char_index, matrix):
        raise _PdfiumFailure(f"read the matrix of char {char_index}")
    size = nominal.value * math.hypot(matrix.c, matrix.d)
    if not math.isfinite(size):
        raise _PdfiumFailure(f"read the font size of {label}: it is not finite ({size})")
    return size


def _read_style(
    textpage: pdfium_c.FPDF_TEXTPAGE, text_object: pdfium_c.FPDF_PAGEOBJECT, char_index: int, object_index: int
) -> _Style:
    """Read the style of text object ``object_index``, which owns textpage char ``char_index``."""
    label = f"object {object_index}"
    font = pdfium_c.FPDFTextObj_GetFont(text_object)
    if not font:
        raise _PdfiumFailure(f"read the font of {label}")
    name = _base_font_name(font, label)
    is_bold, is_italic = _bold_and_italic(font, name, label)
    render_mode = pdfium_c.FPDFTextObj_GetTextRenderMode(text_object)
    if render_mode not in range(8):  # FPDF_TEXTRENDERMODE_UNKNOWN (-1) on failure
        raise _PdfiumFailure(f"read the render mode of {label}")
    return _Style(
        font_size_pt=_font_size(textpage, text_object, char_index, label),
        base_font_name=name,
        is_bold=is_bold,
        is_italic=is_italic,
        render_mode=render_mode,
        fill_rgba=_rgba(pdfium_c.FPDFPageObj_GetFillColor, text_object, f"fill colour of {label}"),
        stroke_rgba=_rgba(pdfium_c.FPDFPageObj_GetStrokeColor, text_object, f"stroke colour of {label}"),
    )


def _loose_box(textpage: pdfium_c.FPDF_TEXTPAGE, char_index: int) -> Box:
    """Read a char's loose box as ``(left, bottom, right, top)`` in PDF user space."""
    rect = pdfium_c.FS_RECTF()
    if not pdfium_c.FPDFText_GetLooseCharBox(textpage, char_index, rect):
        raise _PdfiumFailure(f"read the box of char {char_index}")
    left, bottom, right, top = rect.left, rect.bottom, rect.right, rect.top
    if not all(math.isfinite(edge) for edge in (left, bottom, right, top)):
        raise _PdfiumFailure(f"read the box of char {char_index}: it is not finite {(left, bottom, right, top)}")
    # Edges in order, so a mirrored (negative-size) char still reads as a box.
    return min(left, right), min(bottom, top), max(left, right), max(bottom, top)


_HIGH_SURROGATES = range(0xD800, 0xDC00)
_LOW_SURROGATES = range(0xDC00, 0xE000)
_MAX_CODE_POINT = 0x10FFFF
_REPLACEMENT_CHAR = "\ufffd"


class _SpanBuilder:
    """Groups a page's owned, non-generated textpage chars into RawSpans, in order."""

    def __init__(
        self, textpage: pdfium_c.FPDF_TEXTPAGE, geometry: PageGeometry, object_indices: dict[int, int]
    ) -> None:
        self._textpage = textpage
        self._rotation = geometry.rotation_deg
        self._page_box = geometry.page_box
        self._object_indices = object_indices
        self._styles: dict[int, _Style] = {}
        self._spans: list[RawSpan] = []
        self._owner = 0
        self._chars: list[str] = []
        self._boxes: list[Box] = []
        self._breaks: list[int] = []
        self._break_pending = False

    def mark_inserted(self) -> None:
        """Note that pdfium inserted a char (generated or ownerless) after the last one added."""
        self._break_pending = True

    def add(self, char_index: int, text_object: pdfium_c.FPDF_PAGEOBJECT, code: int) -> None:
        """Append textpage char ``char_index`` (``code``), owned by ``text_object``, to the current or a new span."""
        self._switch_owner(char_index, text_object)
        box = _loose_box(self._textpage, char_index)
        # A char pdfium inserted inside this span becomes a break before this
        # char; one inserted between spans is only counted.
        breaking = self._break_pending and bool(self._chars)
        self._break_pending = False
        if breaking:
            self._breaks.append(len(self._chars))
        elif code in _LOW_SURROGATES and self._chars and ord(self._chars[-1]) in _HIGH_SURROGATES:
            # pdfium reports a non-BMP char as a UTF-16 pair of textpage chars.
            high = ord(self._chars[-1])
            self._chars[-1] = chr(0x10000 + ((high - _HIGH_SURROGATES.start) << 10) + (code - _LOW_SURROGATES.start))
            self._boxes[-1] = _union((self._boxes[-1], box))
            return
        # A code beyond Unicode is a raw char code pdfium could not map; U+FFFD
        # keeps its place (and is no surrogate, so it never pairs).
        self._chars.append(chr(code) if code <= _MAX_CODE_POINT else _REPLACEMENT_CHAR)
        self._boxes.append(box)

    def finish(self) -> tuple[RawSpan, ...]:
        """Close the current span and return every span."""
        self._flush()
        return tuple(self._spans)

    def _switch_owner(self, char_index: int, text_object: pdfium_c.FPDF_PAGEOBJECT) -> None:
        """Start a new span when char ``char_index`` belongs to another text object."""
        owner = _address(text_object)
        if owner == self._owner:
            return
        if owner not in self._object_indices:
            raise _PdfiumFailure(f"read the text object of char {char_index}: it is not in the page's object tree")
        self._flush()
        self._owner = owner
        if owner not in self._styles:
            self._styles[owner] = _read_style(self._textpage, text_object, char_index, self._object_indices[owner])

    def _flush(self) -> None:
        if not self._chars:
            return
        boxes = tuple(to_display_box(box, rotation_deg=self._rotation, page_box=self._page_box) for box in self._boxes)
        self._spans.append(
            RawSpan(
                text="".join(self._chars),
                bbox=_union(boxes),
                char_boxes=boxes,
                object_index=self._object_indices[self._owner],
                generated_breaks=tuple(self._breaks),
                **self._styles[self._owner]._asdict(),
            )
        )
        self._chars, self._boxes, self._breaks = [], [], []


def _textpage_flag(result: int, char_index: int, question: str) -> bool:
    """Return a 0/1 FPDFText_* answer about a char as a bool; anything else is a pdfium failure."""
    if result not in (0, 1):
        raise _PdfiumFailure(f"read whether char {char_index} {question}")
    return result == 1


def _collect_text(textpage: pdfium_c.FPDF_TEXTPAGE, geometry: PageGeometry, object_indices: dict[int, int]) -> PageText:
    """Walk every textpage char once: count them and group the owned, non-generated ones."""
    count = pdfium_c.FPDFText_CountChars(textpage)
    if count < 0:
        raise _PdfiumFailure("count the text page's chars")
    builder = _SpanBuilder(textpage, geometry, object_indices)
    generated = ownerless = map_errors = 0
    for char_index in range(count):
        if _textpage_flag(pdfium_c.FPDFText_IsGenerated(textpage, char_index), char_index, "is generated"):
            generated += 1
            builder.mark_inserted()
            continue
        text_object = pdfium_c.FPDFText_GetTextObject(textpage, char_index)
        if not text_object:
            # pdfium inserted it, though not flagged generated: the space
            # between two right-to-left words drawn by separate operators.
            ownerless += 1
            builder.mark_inserted()
            continue
        map_error = _textpage_flag(
            pdfium_c.FPDFText_HasUnicodeMapError(textpage, char_index), char_index, "maps to Unicode"
        )
        code = pdfium_c.FPDFText_GetUnicode(textpage, char_index)
        # A code beyond U+10FFFF reads as U+FFFD: a map error, counted once.
        map_errors += map_error or code > _MAX_CODE_POINT
        builder.add(char_index, text_object, code)
    stats = TextStats(
        char_count=count - generated - ownerless,
        generated_count=generated,
        ownerless_count=ownerless,
        unicode_map_error_count=map_errors,
    )
    return PageText(spans=builder.finish(), stats=stats)


def _read_page_text(page: pdfium.PdfPage, geometry: PageGeometry) -> PageText:
    """Read a loaded page's spans, closing its textpage."""
    page_number = geometry.page_number
    try:
        textpage = page.get_textpage()
    except pdfium.PdfiumError as exc:
        message = f"page {page_number}: pdfium cannot load the text page: {exc}"
        raise PDFCorruptError(message, page_number=page_number) from exc
    try:
        return _collect_text(textpage.raw, geometry, _object_indices(page.raw))
    except _PdfiumFailure as exc:
        raise PDFCorruptError(f"page {page_number}: pdfium cannot {exc}", page_number=page_number) from exc
    finally:
        textpage.close()


def page_text(handle: DocumentHandle, index: int) -> PageText:
    """Return the text spans and textpage counts of the page at 0-based ``index``.

    One pass over the textpage: chars pdfium inserted (generated spaces and
    line breaks, and ownerless spaces between right-to-left words) are
    counted and skipped, and become a ``generated_breaks`` offset when they
    fall inside a span; every other char joins the span of its owning text
    object (see :class:`RawSpan`). Boxes are mapped to the displayed page
    with :func:`to_display_box`. The page and textpage handles are closed
    before returning.

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
