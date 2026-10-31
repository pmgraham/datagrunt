"""The pure rules that flag hidden text, shared by the v5 engine and its quality stage (#366, A3).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: no
pdfium. :mod:`datagrunt.pdf._pdfium_text` measures each span's em with
:func:`em_size`; :mod:`datagrunt.pdf._pdfium_recover` measures recovered
text with it and reads :func:`em_reasons` and :func:`off_page` for its
overprint rule; :mod:`datagrunt.pdf.quality` flags textpage spans with the
same two, so a span and recovered text are flagged alike.
"""

import math

from datagrunt.pdf.elements import BBox, HiddenReason

RECOVERY_REASONS: tuple[HiddenReason, ...] = ("tiny_font", "zero_scale")
# A font size (or an em's width) under this many points is too small to read.
TINY_FONT_PT = 1.0


def em_size(size: float, a: float, b: float, c: float, d: float) -> tuple[float, float]:
    """The width and height in points of an em of ``Tf`` ``size`` under a text matrix ``[a b c d]`` to page space.

    The em's sides map to ``(a, b)`` (along the baseline) and ``(c, d)``
    (up the glyphs), and each is measured perpendicular to the other: the
    em is ``|size| x |ad - bc| / hypot(c, d)`` wide and ``|size| x |ad - bc|
    / hypot(a, b)`` tall, the two heights of the parallelogram it maps to.
    Perpendicular sides (any turn, scale or mirror) read their own lengths;
    a shear that lays both sides along almost one line (``1 0 1 0.0001``)
    or a singular matrix (``1 0 1 0``) leaves the em little or no extent
    either way, however long its sides. A side of no length leaves the
    other its own length (``Tz 0``: no width, full height). A mirrored
    (negative) size or scale counts at its magnitude. The one measure for
    textpage spans (``RawSpan``) and for the text objects recovery reads
    back.
    """
    area = abs(a * d - b * c)
    along, up = math.hypot(a, b), math.hypot(c, d)
    width = area / up if up else along
    height = area / along if along else up
    return abs(size) * width, abs(size) * height


def em_reasons(width_pt: float, height_pt: float) -> tuple[HiddenReason, ...]:
    """``tiny_font`` and / or ``zero_scale`` for text whose em is ``width_pt`` by ``height_pt`` (see the module notes).

    The one rule, and :data:`TINY_FONT_PT` the one threshold, for text pdfium
    drops (recovery) and text its textpage keeps (:mod:`datagrunt.pdf.quality`
    flags a span by its ``em_width_pt`` and ``|font_size_pt|``). Both sides
    are magnitudes (:func:`em_size`).
    """
    tiny = height_pt < TINY_FONT_PT
    # A narrower em than a taller one: the matrix (Tz) squeezes it, not the size.
    squeezed = width_pt < height_pt and width_pt < TINY_FONT_PT
    return tuple(reason for reason, flagged in zip(RECOVERY_REASONS, (tiny, squeezed)) if flagged)


def off_page(box: tuple[float, float, float, float], width: float, height: float) -> bool:
    """True if the displayed ``box`` is not within the ``width`` x ``height`` page by A1's ``BBox.within`` rule.

    The one placement rule for recovered copies (the overprint rule) and
    textpage chars (:mod:`datagrunt.pdf.quality` flags ``off_page`` by it).
    """
    x0, y0, x1, y1 = box
    return not BBox(x0=x0, y0=y0, x1=x1, y1=y1).within(width, height)
