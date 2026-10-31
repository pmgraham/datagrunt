"""Guarded page render for the v5 vision route (#369, A5a).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.

A page renders at :func:`effective_dpi`: the spec's
``min(target_dpi, int(72 * sqrt(max_render_pixels / (width_pt * height_pt))))``,
so a large-format sheet lowers its DPI rather than its pixel bound. Each
displayed side renders to ``floor(side_pt * dpi / 72)`` pixels
(:func:`render_size`); rounding down keeps the product within the bound,
which the spec's continuous formula alone guarantees only before rounding.

Before a page renders, the image guard
(:func:`~datagrunt.pdf.pdfium_engine.image_refusal`) checks every image
object pdfium exposes on it, forms' included, against A2's decode bounds,
on the same load of the page the render then draws; a refusal, or the
guard's walk reaching its bound, is a :class:`RenderRefused` naming the
object, and nothing is drawn.
"""

import math
import os
from collections.abc import Iterator
from contextlib import contextmanager

import pypdfium2 as pdfium

from datagrunt.pdf._validate import int_at_least, positive_float
from datagrunt.pdf.exceptions import DatagruntPDFError
from datagrunt.pdf.pdfium_engine import PageGeometry, image_refusal, loaded_page, open_document

# The spec's construction checks (PDFReader / PDFWriter): dpi >= 36, max_render_pixels >= 512 x 512.
_MIN_TARGET_DPI = 36
_MIN_RENDER_PIXELS = 262_144
_POINTS_PER_INCH = 72.0

# The objects (forms' contents included) the guard walks before it refuses a
# page: ~250 times the corpus's busiest page (3,977). The walk costs ~0.4 us
# an object here, so the bound is about half a second of guard.
_MAX_GUARD_OBJECTS = 1_000_000


class RenderRefused(DatagruntPDFError):
    """A page is not rendered: rendering it is refused before pdfium draws anything."""


def render_size(width_pt: float, height_pt: float, dpi: int) -> tuple[int, int]:
    """The ``(width, height)`` in pixels a displayed ``width_pt`` x ``height_pt`` page renders to at ``dpi``.

    Each side is rounded down, so a side under one pixel is 0.

    Raises:
        TypeError: If ``dpi`` is not an int (bool rejected) or a side is not a number.
        ValueError: If ``dpi`` is under 1, or a side is not finite and positive.
    """
    int_at_least("dpi", dpi, 1)
    width = positive_float("width_pt", width_pt)
    height = positive_float("height_pt", height_pt)
    return int(width * dpi / _POINTS_PER_INCH), int(height * dpi / _POINTS_PER_INCH)


def effective_dpi(target_dpi: int, max_render_pixels: int, width_pt: float, height_pt: float) -> int:
    """The DPI a displayed ``width_pt`` x ``height_pt`` page renders at: ``target_dpi``, lowered to fit the pixel bound.

    The spec's ``min(target_dpi, int(72 * sqrt(max_render_pixels / (width_pt * height_pt))))``.
    At the DPI returned, :func:`render_size` is at least 1 x 1 and at most
    ``max_render_pixels`` pixels.

    Raises:
        TypeError: If ``target_dpi`` or ``max_render_pixels`` is not an int
            (bool rejected), or a side is not a number.
        ValueError: If ``target_dpi`` is under 36, ``max_render_pixels`` under
            262,144 (512 x 512), or a side is not finite and positive.
        RenderRefused: If no DPI of 1 or more renders the page within
            ``max_render_pixels`` (a page over about 1,000 inches square at
            the default bound), or a side renders under one pixel at the DPI
            the formula gives (``1e8 x 1`` pt at 28 DPI is 0 px tall): rounding
            it up to 1 px would break the bound, and the page cannot be read.
    """
    int_at_least("target_dpi", target_dpi, _MIN_TARGET_DPI)
    int_at_least("max_render_pixels", max_render_pixels, _MIN_RENDER_PIXELS)
    width = positive_float("width_pt", width_pt)
    height = positive_float("height_pt", height_pt)
    area = width * height
    # A product of tiny sides can underflow to 0: such a page fits at any DPI.
    bound = _POINTS_PER_INCH * math.sqrt(max_render_pixels / area) if area > 0.0 else math.inf
    # Compared before int(): int(inf) would raise, and min(target, int(bound)) is this.
    dpi = target_dpi if bound >= target_dpi else int(bound)
    page = f"a {width} x {height} pt page"
    if dpi < 1:
        message = f"{page} renders over the {max_render_pixels}-pixel bound at 1 DPI (bound: {bound:.2f} DPI)"
        raise RenderRefused(message)
    width_px, height_px = render_size(width, height, dpi)
    if width_px < 1:
        raise RenderRefused(f"{page} renders 0 px wide at {dpi} DPI")
    if height_px < 1:
        raise RenderRefused(f"{page} renders 0 px tall at {dpi} DPI")
    return dpi


@contextmanager
def _guarded_page(
    source: str | os.PathLike[str], page_index: int, password: str | None
) -> Iterator[tuple[pdfium.PdfPage, PageGeometry]]:
    """The loaded page at ``page_index`` with its geometry, once the image guard passes it.

    The guard and the render that follows share this one load of the page.
    The document and page are closed on leaving.

    Raises:
        RenderRefused: If :func:`~datagrunt.pdf.pdfium_engine.image_refusal`
            refuses the page; nothing is yielded.
        FileNotFoundError, PermissionError, PDFEncryptedError, PDFCorruptError,
        IndexError: As :func:`~datagrunt.pdf.pdfium_engine.open_document` and
            :func:`~datagrunt.pdf.pdfium_engine.loaded_page` raise them.
    """
    with open_document(source, password=password) as handle, loaded_page(handle, page_index) as (page, geometry):
        refusal = image_refusal(page, geometry, max_objects=_MAX_GUARD_OBJECTS)
        if refusal is not None:
            raise RenderRefused(f"page {geometry.page_number}: {refusal}; the page is not rendered")
        yield page, geometry
