"""Page normalization, bands and the tesseract CLI for the v5 vision route (#369, A5a).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Its input is
our own render (:class:`~datagrunt.pdf.render.RenderedPage`): nothing here
decodes PDF content.

:func:`normalized` is the spec's grayscale + 1 % autocontrast (spec §3.3
step 4): Pillow's ``ImageOps.autocontrast(cutoff=1)``, whose histogram runs
in C. It ignores the darkest and the lightest 1 % of pixels and stretches
the grey levels between them to 0..255; a page with nothing left to stretch
(one grey level, or black to white already) comes back unchanged.
"""

from PIL import Image, ImageOps

from datagrunt.pdf.render import RenderedPage

# The spec's autocontrast: the percent of pixels ignored at each end of the histogram.
_AUTOCONTRAST_CUTOFF = 1


def normalized(page: RenderedPage) -> RenderedPage:
    """``page`` with its grey levels stretched by a 1 % autocontrast (the spec's normalization).

    Raises:
        TypeError: If ``page`` is not a :class:`~datagrunt.pdf.render.RenderedPage`.
    """
    image = ImageOps.autocontrast(_image(page), cutoff=_AUTOCONTRAST_CUTOFF)
    return RenderedPage(pixels=image.tobytes(), px_width=page.px_width, px_height=page.px_height)


def _image(page: RenderedPage) -> Image.Image:
    """``page`` as a Pillow 8-bit grey (``L``) image."""
    if not isinstance(page, RenderedPage):
        raise TypeError(f"page must be a RenderedPage, got {type(page).__name__}")
    return Image.frombytes("L", (page.px_width, page.px_height), page.pixels)
