"""ReportLab builders for v5 PDF test fixtures (#363).

Coordinates are top-left page points: x grows rightward from the left edge,
y grows downward from the top edge (the v5 convention). ReportLab draws in
bottom-left space; ``_flip_y`` is the only place that converts between them.
"""

from __future__ import annotations

import io
import math
import os
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from PIL import Image
from reportlab.lib.pdfencrypt import StandardEncryption
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfgen.canvas import Canvas

# An RGB colour, each channel in [0, 1] (the PDF convention).
RGB = tuple[float, float, float]

_ROTATIONS = (0, 90, 180, 270)
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
# RC4 strengths only. ReportLab's AES-256 needs pyaes (unmaintained since
# 2017), which this toolkit deliberately does not depend on (#363).
_STRENGTHS = (40, 128)
_AES_256_UNSUPPORTED = (
    "strength 256 (AES-256) is not supported: ReportLab needs pyaes for it, "
    "and this toolkit does not install it; use 40 or 128"
)


@dataclass(frozen=True)
class TextRun:
    """One run of text whose origin (x, y) is its top-left baseline point."""

    text: str
    x: float
    y: float
    font: str = "Helvetica"
    size: float = 11.0
    rgb: RGB = (0.0, 0.0, 0.0)
    alpha: float = 1.0
    render_mode: int = 0


@dataclass(frozen=True)
class Line:
    """A stroked segment from (x0, y0) to (x1, y1)."""

    x0: float
    y0: float
    x1: float
    y1: float
    width: float = 0.5


@dataclass(frozen=True)
class Rect:
    """An axis-aligned rectangle whose (x, y) is its top-left corner."""

    x: float
    y: float
    w: float
    h: float
    fill_rgb: RGB | None = None
    stroke: bool = True
    line_width: float = 0.5


@dataclass(frozen=True)
class RasterImage:
    """PNG bytes drawn into the box whose (x, y) is its top-left corner."""

    x: float
    y: float
    w: float
    h: float
    png: bytes


PageItem = TextRun | Line | Rect | RasterImage


@dataclass(frozen=True)
class PageSpec:
    """One page: its size in points, the items drawn on it in order, and its
    /Rotate. Item coordinates are in the unrotated page space."""

    width: float = 612.0
    height: float = 792.0
    items: tuple[PageItem, ...] = ()
    rotate: int = 0


def first_unencodable(font: str, text: str) -> str | None:
    """Return the first character of ``text`` that ``font`` cannot draw as
    itself, or None. ``font`` is one of the standard 14 PDF fonts.

    ReportLab silently substitutes such characters (another font, or a
    missing-glyph box); pdfium would then read back different text. Control
    and format characters are refused too: pdfium rewrites them (U+00AD
    reads back as "-").
    """
    encoding = pdfmetrics.getFont(font).encName
    for char in text:
        if unicodedata.category(char) in ("Cc", "Cf") or not _encodes_as_itself(char, encoding):
            return char
    return None


def _encodes_as_itself(char: str, encoding: str) -> bool:
    # An alias decodes to its canonical character (U+00A0 -> U+0020 in
    # WinAnsi, U+0394 -> U+2206 in Symbol), which is what pdfium reads back.
    try:
        return char.encode(encoding).decode(encoding) == char
    except UnicodeEncodeError:
        return False


def solid_png(width_px: int, height_px: int, rgb: RGB) -> bytes:
    """Return PNG bytes for a ``width_px`` x ``height_px`` image of one colour."""
    _require_positive("solid_png width_px", width_px)
    _require_positive("solid_png height_px", height_px)
    _require_rgb("solid_png rgb", rgb)
    pixel = tuple(round(channel * 255) for channel in rgb)
    buffer = io.BytesIO()
    Image.new("RGB", (width_px, height_px), pixel).save(buffer, format="PNG")
    return buffer.getvalue()


def build_pdf(
    path: str | os.PathLike[str],
    pages: Sequence[PageSpec],
    *,
    user_password: str | None = None,
    owner_password: str | None = None,
    strength: int = 128,
) -> Path:
    """Write ``pages`` to a PDF at ``path`` and return the path.

    Either password encrypts the file with RC4 at ``strength`` bits (40 or 128).
    With only ``owner_password`` the file opens without a password. Every
    spec is checked first: a bad one raises ValueError (TypeError for a wrong
    type) and nothing is written.
    """
    _check_spec(pages, strength)
    out = Path(path)
    # invariant=1 fixes CreationDate, ModDate and /ID, so a spec always
    # builds the same bytes (and RC4 keys, which derive from /ID).
    canvas = Canvas(str(out), invariant=1, encrypt=_encryption(user_password, owner_password, strength))
    for page in pages:
        _draw_page(canvas, page)
    canvas.save()
    return out


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


def _require_int(name: str, value: int) -> None:
    # bool is an int subclass and 3.0 == 3; neither is a real int here.
    if type(value) is not int:
        raise TypeError(f"{name} must be an int, got {value!r}")


def _require_finite(name: str, value: float) -> None:
    _require(math.isfinite(value), f"{name} must be finite, got {value!r}")


def _require_finite_fields(item: PageItem, fields: tuple[str, ...]) -> None:
    for field in fields:
        _require_finite(f"{type(item).__name__}.{field}", getattr(item, field))


def _require_positive(name: str, value: float) -> None:
    _require_finite(name, value)
    _require(value > 0, f"{name} must be > 0, got {value!r}")


def _require_rgb(name: str, rgb: Sequence[float]) -> None:
    ok = len(rgb) == 3 and all(0 <= channel <= 1 for channel in rgb)
    _require(ok, f"{name} must be 3 channels in [0, 1], got {rgb!r}")


def _check_spec(pages: Sequence[PageSpec], strength: int) -> None:
    _require_int("strength", strength)
    _require(strength != 256, _AES_256_UNSUPPORTED)
    _require(strength in _STRENGTHS, f"strength must be one of {_STRENGTHS}, got {strength!r}")
    _require(len(pages) > 0, "pages must not be empty")
    for page in pages:
        _check_page(page)


def _check_page(page: PageSpec) -> None:
    if not isinstance(page, PageSpec):
        raise TypeError(f"pages must hold PageSpec, got {type(page).__name__}")
    _require_positive("PageSpec.width", page.width)
    _require_positive("PageSpec.height", page.height)
    _require_int("PageSpec.rotate", page.rotate)
    _require(page.rotate in _ROTATIONS, f"PageSpec.rotate must be one of {_ROTATIONS}, got {page.rotate!r}")
    for item in page.items:
        _check_item(item)


def _check_item(item: PageItem) -> None:
    match item:
        case TextRun():
            _check_text(item)
        case Line():
            _require_finite_fields(item, ("x0", "y0", "x1", "y1"))
            _require_positive("Line.width", item.width)
        case Rect():
            _check_rect(item)
        case RasterImage():
            _check_image(item)
        case _:
            raise TypeError(f"unsupported page item: {type(item).__name__}")


def _check_text(run: TextRun) -> None:
    _require(run.text != "", "TextRun.text must not be empty")
    _require_finite_fields(run, ("x", "y"))
    fonts = pdfmetrics.standardFonts
    _require(run.font in fonts, f"TextRun.font must be one of the standard 14 PDF fonts, got {run.font!r}")
    _check_text_chars(run)
    _require_positive("TextRun.size", run.size)
    _require_rgb("TextRun.rgb", run.rgb)
    _require(0 <= run.alpha <= 1, f"TextRun.alpha must be in [0, 1], got {run.alpha!r}")
    _require_int("TextRun.render_mode", run.render_mode)
    _require(run.render_mode in range(8), f"TextRun.render_mode must be in 0..7, got {run.render_mode!r}")


def _check_text_chars(run: TextRun) -> None:
    for char in run.text:
        _require(char not in "\n\r", f"TextRun.text must be one line (one run = one line), got U+{ord(char):04X}")
    char = first_unencodable(run.font, run.text)
    if char is not None:
        raise ValueError(f"TextRun.text has U+{ord(char):04X}, which {run.font} cannot draw without glyph substitution")


def _check_image(image: RasterImage) -> None:
    _require_finite_fields(image, ("x", "y"))
    _require_positive("RasterImage.w", image.w)
    _require_positive("RasterImage.h", image.h)
    _require(image.png[:8] == _PNG_SIGNATURE, "RasterImage.png must be PNG data (it lacks the PNG signature)")


def _check_rect(rect: Rect) -> None:
    _require_finite_fields(rect, ("x", "y"))
    _require_positive("Rect.w", rect.w)
    _require_positive("Rect.h", rect.h)
    _require_positive("Rect.line_width", rect.line_width)
    _require(rect.stroke or rect.fill_rgb is not None, "Rect draws nothing: stroke=False and fill_rgb=None")
    if rect.fill_rgb is not None:
        _require_rgb("Rect.fill_rgb", rect.fill_rgb)


def _encryption(user_password: str | None, owner_password: str | None, strength: int) -> StandardEncryption | None:
    if user_password is None and owner_password is None:
        return None
    return StandardEncryption(user_password or "", owner_password, strength=strength)


def _flip_y(page_height: float, y: float) -> float:
    """Convert a top-left y to ReportLab's bottom-left y."""
    return page_height - y


def _draw_page(canvas: Canvas, page: PageSpec) -> None:
    canvas.setPageSize((page.width, page.height))
    canvas.setPageRotation(page.rotate)
    for item in page.items:
        canvas.saveState()
        _draw_item(canvas, item, page.height)
        canvas.restoreState()
    canvas.showPage()


def _draw_item(canvas: Canvas, item: PageItem, page_height: float) -> None:
    """Draw one item; ``_check_item`` has already accepted its type."""
    match item:
        case TextRun():
            _draw_text(canvas, item, page_height)
        case Line():
            _draw_line(canvas, item, page_height)
        case Rect():
            _draw_rect(canvas, item, page_height)
        case RasterImage():
            _draw_image(canvas, item, page_height)
        case _:
            raise TypeError(f"unsupported page item: {type(item).__name__}")


def _draw_text(canvas: Canvas, run: TextRun, page_height: float) -> None:
    # rgb/alpha apply to whatever the render mode paints: fill, stroke, or both.
    # Alpha goes on the canvas: ReportLab 5.1's PDFTextObject.setStrokeAlpha
    # is a silent no-op (it has no _setStrokeAlpha to call).
    canvas.setFillAlpha(run.alpha)
    canvas.setStrokeAlpha(run.alpha)
    text = canvas.beginText(run.x, _flip_y(page_height, run.y))
    text.setFont(run.font, run.size)
    text.setFillColorRGB(*run.rgb)
    text.setStrokeColorRGB(*run.rgb)
    text.setTextRenderMode(run.render_mode)
    text.textOut(run.text)
    canvas.drawText(text)


def _draw_line(canvas: Canvas, line: Line, page_height: float) -> None:
    canvas.setLineWidth(line.width)
    canvas.line(line.x0, _flip_y(page_height, line.y0), line.x1, _flip_y(page_height, line.y1))


def _draw_rect(canvas: Canvas, rect: Rect, page_height: float) -> None:
    canvas.setLineWidth(rect.line_width)
    if rect.fill_rgb is not None:
        canvas.setFillColorRGB(*rect.fill_rgb)
    bottom = _flip_y(page_height, rect.y + rect.h)
    canvas.rect(rect.x, bottom, rect.w, rect.h, stroke=int(rect.stroke), fill=int(rect.fill_rgb is not None))


def _draw_image(canvas: Canvas, image: RasterImage, page_height: float) -> None:
    bottom = _flip_y(page_height, image.y + image.h)
    # mask="auto" keeps a PNG's alpha channel as an /SMask; ReportLab's
    # default (mask=None) silently drops it and paints the pixels opaque.
    reader = ImageReader(io.BytesIO(image.png))
    canvas.drawImage(reader, image.x, bottom, width=image.w, height=image.h, mask="auto")
