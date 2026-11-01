"""Page normalization, bands and the tesseract CLI for the v5 vision route (#369, A5a).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Its input is
our own render (:class:`~datagrunt.pdf.render.RenderedPage`): nothing here
decodes PDF content.

:func:`normalized` is the spec's grayscale + 1 % autocontrast (spec §3.3
step 4): Pillow's ``ImageOps.autocontrast(cutoff=1)``, whose histogram runs
in C. It ignores the darkest and the lightest 1 % of pixels and stretches
the grey levels between them to 0..255; a page with nothing left to stretch
(one grey level, or black to white already) comes back unchanged.

:func:`bands` cuts a page into horizontal :class:`Band` s for OCR (spec
§3.3 step 4): 900 px tall, stepping by 840 px, so neighbours share 60 rows
and a line of text up to 60 px tall (14.4 pt at 300 DPI) is whole in one of
them. The last band ends at the page's bottom row, so it may be shorter
than 900 px; every row is in one band or two.
"""

from dataclasses import dataclass, field

from PIL import Image, ImageOps

from datagrunt.pdf._validate import int_at_least
from datagrunt.pdf.render import RenderedPage

# The spec's autocontrast: the percent of pixels ignored at each end of the histogram.
_AUTOCONTRAST_CUTOFF = 1

# The spec's bands: 900 px tall, neighbours sharing 60 rows.
_BAND_HEIGHT = 900
_BAND_OVERLAP = 60


@dataclass(frozen=True, slots=True, kw_only=True)
class Band:
    """Rows ``top_px`` to ``top_px + px_height`` of a page, across its whole width.

    Attributes:
        top_px: The band's first row on the page it was cut from.
        pixels: ``px_width * px_height`` bytes, 8-bit gray, row by row as on
            the page. Not in repr.
        px_width: The page's width in pixels.
        px_height: The band's rows.
    """

    top_px: int
    pixels: bytes = field(repr=False)
    px_width: int
    px_height: int

    def __post_init__(self) -> None:
        int_at_least("top_px", self.top_px, 0)
        int_at_least("px_width", self.px_width, 1)
        int_at_least("px_height", self.px_height, 1)
        if not isinstance(self.pixels, bytes):
            raise TypeError(f"pixels must be bytes, got {type(self.pixels).__name__}")
        expected = self.px_width * self.px_height
        if len(self.pixels) != expected:
            message = f"pixels must hold {self.px_width} x {self.px_height} = {expected} bytes, got {len(self.pixels)}"
            raise ValueError(message)


def normalized(page: RenderedPage) -> RenderedPage:
    """``page`` with its grey levels stretched by a 1 % autocontrast (the spec's normalization).

    Raises:
        TypeError: If ``page`` is not a :class:`~datagrunt.pdf.render.RenderedPage`.
    """
    image = ImageOps.autocontrast(_image(page), cutoff=_AUTOCONTRAST_CUTOFF)
    return RenderedPage(pixels=image.tobytes(), px_width=page.px_width, px_height=page.px_height)


def _image(page: RenderedPage) -> Image.Image:
    """``page`` as a Pillow 8-bit grey (``L``) image."""
    _checked_page(page)
    return Image.frombytes("L", (page.px_width, page.px_height), page.pixels)


def bands(page: RenderedPage, *, height: int = _BAND_HEIGHT, overlap: int = _BAND_OVERLAP) -> tuple[Band, ...]:
    """``page`` cut into bands ``height`` rows tall whose neighbours share ``overlap`` rows.

    Bands start every ``height - overlap`` rows from the top; the last is the
    first to reach the bottom, and ends there (it may be shorter). A page up
    to ``height`` rows tall is one band. Every row is in one band or two, and
    there are at most as many bands as rows.

    Raises:
        TypeError: If ``page`` is not a :class:`~datagrunt.pdf.render.RenderedPage`,
            or ``height`` or ``overlap`` is not an int (bool rejected).
        ValueError: If ``height`` or ``overlap`` is under 1, or ``overlap``
            is over half of ``height`` (a row would be in three bands).
    """
    _checked_page(page)
    int_at_least("height", height, 1)
    int_at_least("overlap", overlap, 1)
    if 2 * overlap > height:
        raise ValueError(f"overlap ({overlap}) must be at most half of height ({height})")
    step = height - overlap
    # The last band is the first whose top is at or past this row: it reaches the bottom.
    last_top = max(page.px_height - height, 0)
    return tuple(_band(page, top, min(height, page.px_height - top)) for top in range(0, last_top + step, step))


def _band(page: RenderedPage, top: int, rows: int) -> Band:
    """Rows ``top`` to ``top + rows`` of ``page``: one contiguous slice of its bytes."""
    start = top * page.px_width
    return Band(
        top_px=top, pixels=page.pixels[start : start + rows * page.px_width], px_width=page.px_width, px_height=rows
    )


def _checked_page(page: RenderedPage) -> RenderedPage:
    """``page``, if it is a RenderedPage."""
    if not isinstance(page, RenderedPage):
        raise TypeError(f"page must be a RenderedPage, got {type(page).__name__}")
    return page
