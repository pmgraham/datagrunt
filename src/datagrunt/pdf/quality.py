"""Hidden-span flags, raster cover, page quality and routing for the v5 PDF quality stage (#366, A3 parts 1, 3, 4).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads the engine's raw facts (:class:`~datagrunt.pdf.pdfium_engine.RawPage`,
:class:`~datagrunt.pdf.pdfium_engine.RawSpan`,
:class:`~datagrunt.pdf.pdfium_engine.RawImage`) and calls no pdfium function;
it imports the engine for the ``RawPage`` type only (the engine never imports
it). Boxes are displayed, top-left ``(x0, y0, x1, y1)`` points.

Hidden text is flagged, never dropped: :func:`classify_spans` returns every
char of every span, in order, cut into pieces that carry the A1
:data:`~datagrunt.pdf.elements.HiddenReason` values a human reader is subject to:

- ``off_page``: the char's box is not within the page by A1's
  :meth:`~datagrunt.pdf.elements.BBox.within` rule and default tolerance, so
  a char partly past an edge is off the page, and every piece not flagged
  ``off_page`` lies within the page as A1's ``Page`` requires.
- ``invisible``: render mode 3 or 7 (no paint), unless the char lies over
  raster cover: the union cover (:func:`raster_cover_fraction`) of the
  page's scan-like images is over 50 % and the char's centre is inside one
  of their boxes. That is a searchable scan's OCR layer, which a reader sees
  as the scan. An image is scan-like when it is drawn at 50 ppi or more on
  each side: its pixel width and height over its displayed box's sides in
  inches, each pixel side paired with either box side (a quarter-turned page
  or image swaps them; the better pairing counts). An image of unknown pixel
  size is not scan-like, nor is a 1 x 1 px image stretched over the page.
  Clip paths, soft masks and image masks are not applied: a scan clipped
  away still counts. Mode-7 text (glyphs added to the clip) stays invisible
  even where a later paint shows through it (gradient-filled text): pdfium
  does not expose which text clips a later object, nor an image's or a
  shading's alpha, so no paint can be proven to show the glyphs (A3 review
  I6; the evidence is pinned by ``tests/pdf_tests/test_clip_only_text.py``).
- ``tiny_font``: a font size under 1 pt in magnitude (a negative ``Tf``
  draws mirrored glyphs at its magnitude).
- ``transparent``: an alpha of at most 12 of 255 (under 5 %; pdfium rounds
  an ExtGState ``ca`` / ``CA``, so ``0.048`` reads 12 and ``0.05`` reads 13)
  on every channel the render mode paints: fill for modes 0 and 4, stroke
  for 1 and 5, both for 2 and 6. Modes 3 and 7 paint nothing
  (``invisible``), so they are never ``transparent``.
- ``zero_scale``: an em (``RawSpan.em_width_pt``) under 1 pt wide and
  narrower than it is tall: text squeezed flat by its matrix (``Tz 0.05``),
  which pdfium's textpage keeps. Tiny text no narrower than tall is
  ``tiny_font`` only.

``tiny_font`` and ``zero_scale`` are recovery's rule and threshold
(:func:`~datagrunt.pdf._hidden_rules.em_reasons`), so a textpage span and
recovered text are flagged alike.

``matches_background`` (#400) needs the page drawn: a :class:`Backdrop` is
the page drawn without its text, in 24-bit BGR (the render worker's
``colour`` and ``without_text`` modes). :func:`flag_background` checks each
visible piece (no other reason) against it; colour alone never flags (white
text on a dark box is legitimate):

- Each char's background is the median of each channel over the backdrop's
  pixels whose centres its loose box holds, clipped to the page; a box
  holding no pixel centre (under one pixel) samples the pixel under its
  centre. At most :data:`MAX_SAMPLES_PER_AXIS` pixels a side are sampled,
  evenly spaced: a bound on each char's cost. A piece's background is the
  median of each channel over its chars' (the lower median, both times).
- Its paint is the fill for render modes 0, 2, 4 and 6 and the stroke for 1
  and 5. Modes 3 and 7 paint nothing: never matched (a visible one is a
  scan's OCR layer, which a reader sees as the scan). Alpha does not enter
  the colour (a faint paint is ``transparent``).
- The piece is flagged when the Euclidean RGB distance between its paint and
  its background is under :data:`MATCH_DISTANCE` (4).

The backdrop's pixels map onto the displayed page at its own scale on each
axis (it is the page drawn at one DPI, each side rounded down).

The gate (:func:`needs_background`): a page is drawn only when it holds a
visible piece that paints (page text or recovered) whose paint is under
:data:`MATCH_DISTANCE` of white (a page's default background), or whose
box overlaps a fill or an image A2 reads: a thin fill's box, the box of a
filled rectangle's fill edges (the union of each path object's), an image's
box. A fill A2 reads no segment of (a curve, a shape that is no rectangle,
a rectangle under 12 pt each way) does not pass the gate: text the same
colour as such a fill, on a page with no other reason to be drawn, is not
checked.

Order (#400): A3 assesses a page with no background; the pipeline gates it,
draws it, and assesses it again with its :class:`Backdrop`
(``assess_page(..., background=...)``). That assessment flags the page's
pieces, then the recovered pieces (their own reasons first), before the
duplicate check, then routes, measures and warns: flagged text leaves the
routing count, the unmapped regions and the duplicate check's visible
twins, as text with any other reason does. A page that could not be drawn
(its render refused or failed) is assessed with why (a str) and fails
closed: each visible piece the gate selects (see :func:`needs_background`)
is flagged ``unchecked_background``, in the same place in that order, and
the page warns ``background not checked: ...``. Pieces the gate does not
select stay visible.

:func:`assess_page` measures a page and routes it (#366 task 4):

- A char is valid text unless pdfium could not map its code to Unicode
  (``RawSpan.map_errors``: it reads as the raw code, ``\x03`` or ``B``
  alike, so it says nothing), or it is U+FFFD (a code with no Unicode
  mapping), private use (``Co``), unassigned (``Cn``, by the running
  Python's Unicode database) or a lone surrogate (``Cs``). Marks, format
  chars (ZWJ, LRM) and control chars are otherwise valid: nothing is
  stripped.
- ``valid_char_ratio`` is the valid share of every span char, hidden ones
  included. It is recorded, never routed on: a contents page whose dot
  leaders read as U+FFFD keeps its digital text.
- Routing counts the valid, visible (no hidden reason), non-whitespace
  chars. ``ocr="always"`` routes vision. Under 20 routes vision, or digital
  with a warning under ``ocr="never"``. 20 or more on a page whose raster
  cover (of scan-like images, as for ``invisible``) is over 50 % routes
  hybrid (OCR fills the unmapped regions), or digital under
  ``ocr="never"``; else digital.
- Unmapped regions: the box of each maximal run of invalid chars in a
  visible piece, where OCR (A5) reads what the text layer cannot say.

Known limitations (A3 review M7; each needs what this pure stage cannot
see, and an occlusion follow-up is proposed for the last two):

- A glyph with no outline whose ToUnicode map says it is text (a space
  that reads "IGNORE ALL") draws nothing but reads as visible text; alone
  in its object it is an empty object, counted and never read.
- Text a clip path excludes is not flagged: clip paths are not applied.
- Text under a later opaque image or fill is not flagged: occlusion is
  not checked.

Recovered text (#366 task 3,
:func:`~datagrunt.pdf.pdfium_engine.recover_dropped_text`) is kept apart from
the page's pieces, in ``PageAssessment.recovered_pieces``, each flagged with
its recovered reasons and those :func:`classify_spans` computes for it: it is
never page text (A4 lays it out apart). A recovered piece with no such reason
is a drawn copy pdfium dropped as a duplicate (recovery emits a copy painted
or placed unlike the text it copies, or past its window). It is flagged
``duplicate`` (#406) when a visible piece of the page (no reason) has the same
text, exactly, and each edge of that piece's ink box (the union of its tight
boxes; a recovered char box is its object's ink) lies within the recovered
piece's duplicate window of the same edge: recovery's overprint window
(:func:`~datagrunt.pdf._pdfium_recover.duplicate_window`) of its last char's
box, 0.9 of each side, at least 0.5 pt and at most 12 pt. The window is under
the box's side (above the 0.5 pt floor), so such a copy lies on the text it
copies: a shadow or an outline drawn on a title, which a reader reads once.
Every other drawn copy stays visible:

- A copy of hidden text: pdfium kept the hidden copy and dropped the one a
  reader sees.
- A copy with no visible twin of its text, or of part of a visible piece
  (the texts must be equal), or past the window: it is text a reader reads.
- A copy whose text has over :data:`MAX_DUPLICATE_CANDIDATES` (64) visible
  pieces on the page: the visible pieces are indexed by text and no more
  are checked, so it is not checked, and the page warns (fail visible).

A flagged recovered piece (a duplicate too) counts toward
``hidden_spans_flagged``; recovered text never counts toward routing, the
char counts or the unmapped regions. Objects recovery could not read back are
a warning, and so are dropped objects when no recovery is passed (the page's
``TextStats.dropped_text_objects`` is over 0 and ``recovered`` is None).
"""

import math
import unicodedata
from bisect import bisect_left
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from itertools import groupby
from typing import Literal, TypeAlias, get_args

from datagrunt.pdf._hidden_rules import em_reasons, off_page
from datagrunt.pdf._pdfium_common import Box, union_box
from datagrunt.pdf._pdfium_images import RawImage
from datagrunt.pdf._pdfium_recover import RecoveredText, duplicate_window
from datagrunt.pdf._pdfium_text import RawSpan
from datagrunt.pdf._validate import (
    choice_str,
    int_at_least,
    non_blank_str,
    ordered_box,
    positive_float,
    str_tuple,
    typed_tuple,
)
from datagrunt.pdf.elements import (
    HIDDEN_REASONS,
    PAGE_ROUTES,
    HiddenReason,
    PageQuality,
    PageRoute,
    check_hidden_reasons,
)
from datagrunt.pdf.pdfium_engine import RawPage, RawSegment

__all__ = [
    "MATCH_DISTANCE",
    "Backdrop",
    "OcrMode",
    "PageAssessment",
    "SpanPiece",
    "assess_page",
    "classify_spans",
    "flag_background",
    "needs_background",
    "raster_cover_fraction",
    "valid_char",
]

OcrMode: TypeAlias = Literal["auto", "always", "never"]
"""The reader's OCR option: ``"auto"`` routes by the page's text,
``"always"`` sends every page to vision, ``"never"`` routes no page to OCR:
a page without enough text stays digital (with a warning) instead of going
to vision, and one that would be hybrid stays digital."""

_OCR_MODES: tuple[OcrMode, ...] = get_args(OcrMode)

# Text render modes (PDF 32000-1, Table 106): 0 fill, 1 stroke, 2 both, 3
# neither; 4 to 7 do the same and add the glyphs to the clipping path.
_FILL_MODES = frozenset({0, 2, 4, 6})
_STROKE_MODES = frozenset({1, 2, 5, 6})
_NO_PAINT_MODES = frozenset({3, 7})
# An image drawn at this many pixels per inch or more on each side can be a scan; points per inch.
_MIN_SCAN_PPI = 50.0
_POINTS_PER_INCH = 72.0
# A channel painted at this alpha (of 255) or less is too faint to read: under 5 %. Fill and stroke alike.
_FAINT_ALPHA = 12
# Raster cover over this fraction of the page makes no-paint text over it an
# OCR layer, and a page with enough text over it hybrid.
_OCR_LAYER_COVER = 0.5
# Valid, visible, non-whitespace chars a page needs to stay out of vision.
_MIN_TEXT_CHARS = 20
# What a char code with no Unicode mapping reads as.
_REPLACEMENT_CHAR = "\ufffd"
# Private use, unassigned and lone-surrogate chars carry no text.
_INVALID_CATEGORIES = frozenset({"Co", "Cn", "Cs"})
# The visible pieces of a recovered piece's text it is checked against for a drawn duplicate: past this many, it is
# not checked (it stays visible) and the page warns. The corpus (data/pdfs, 95 pages) needs at most 5.
MAX_DUPLICATE_CANDIDATES = 64
_DUPLICATE: tuple[HiddenReason, ...] = ("duplicate",)
# Paint under this Euclidean RGB distance of the background drawn under it matches it (#400): matches_background.
MATCH_DISTANCE = 4.0
# A char's box is sampled at most this many pixels along each axis, evenly spaced: a bound on each char's cost.
MAX_SAMPLES_PER_AXIS = 32
_MATCHES_BACKGROUND: tuple[HiddenReason, ...] = ("matches_background",)
_UNCHECKED_BACKGROUND: tuple[HiddenReason, ...] = ("unchecked_background",)
_BGR_BYTES = 3  # blue, green and red bytes a backdrop pixel
_WHITE_RGB = (255, 255, 255)  # a page's default background


@dataclass(frozen=True, slots=True, kw_only=True)
class SpanPiece:
    """A run of a span's chars, ``span.text[start:end]``, that share their hidden reasons.

    Attributes:
        span: The source span, unchanged.
        start: Offset of the piece's first char in ``span.text``.
        end: Offset just past its last char: ``0 <= start < end <= len(span.text)``.
        hidden_reasons: Why a reader cannot see the piece, in A1's canonical
            ``HIDDEN_REASONS`` order; empty for visible text.
    """

    span: RawSpan
    start: int
    end: int
    hidden_reasons: tuple[HiddenReason, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.span, RawSpan):
            raise TypeError(f"span must be a RawSpan, got {type(self.span).__name__}")
        int_at_least("start", self.start, 0)
        int_at_least("end", self.end, 0)
        count = len(self.span.text)
        if not self.start < self.end <= count:
            bounds = f"start < end <= {count} (the span's char count)"
            raise ValueError(f"SpanPiece must have {bounds}, got start={self.start}, end={self.end}")
        check_hidden_reasons(self.hidden_reasons)

    @property
    def text(self) -> str:
        """The piece's chars: ``span.text[start:end]``."""
        return self.span.text[self.start : self.end]

    @property
    def bbox(self) -> Box:
        """The union of the piece's char boxes."""
        return union_box(self.span.char_boxes[self.start : self.end])


def classify_spans(
    spans: tuple[RawSpan, ...], *, width_pt: float, height_pt: float, images: tuple[RawImage, ...]
) -> tuple[SpanPiece, ...]:
    """Cut each span into maximal runs of chars that share their hidden reasons (see the module notes).

    Every char of every span is in exactly one piece; pieces are in span
    order, then char order. A span with no reason and no off-page char is
    one piece with ``hidden_reasons=()``. Generated breaks never split.

    Cost: O(chars) plus, on a page with raster cover, O(images) per
    no-paint span and O(images under the span) per no-paint char.

    Args:
        spans: The page's spans (``PageText.spans``).
        width_pt: The displayed page width in points (``PageGeometry.width_pt``).
        height_pt: The displayed page height in points.
        images: The page's images (``PageImages.images``).

    Raises:
        TypeError: If ``spans`` or ``images`` is not a tuple of RawSpan /
            RawImage, or a page side is not a number (bool rejected).
        ValueError: If a page side is not finite and greater than zero.
    """
    typed_tuple("spans", spans, RawSpan)
    typed_tuple("images", images, RawImage)
    width, height = positive_float("width_pt", width_pt), positive_float("height_pt", height_pt)
    boxes = _scan_boxes(images)
    return _classify(spans, width, height, boxes, _cover_fraction(boxes, width, height))


def _classify(
    spans: tuple[RawSpan, ...], width: float, height: float, boxes: tuple[Box, ...], cover: float
) -> tuple[SpanPiece, ...]:
    """:func:`classify_spans` of checked input, given the raster ``cover`` of the image ``boxes``."""
    raster = boxes if cover > _OCR_LAYER_COVER else ()
    return tuple(piece for span in spans for piece in _span_pieces(span, width, height, raster))


def _scan_boxes(images: tuple[RawImage, ...]) -> tuple[Box, ...]:
    """The boxes of the scan-like images among ``images``: raster cover (see the module notes)."""
    return tuple(image.bbox for image in images if _scan_like(image))


def _scan_like(image: RawImage) -> bool:
    """True if ``image`` is drawn at 50 ppi or more on each side, its pixel sides paired with either box side."""
    width, height = image.px_width, image.px_height
    if width <= 0 or height <= 0:
        return False
    x0, y0, x1, y1 = image.bbox
    # Pixels per inch at least 50: px >= 50 x (points / 72), kept in multiplication (a side may be 0 pt).
    across, down = _MIN_SCAN_PPI * (x1 - x0) / _POINTS_PER_INCH, _MIN_SCAN_PPI * (y1 - y0) / _POINTS_PER_INCH
    return (width >= across and height >= down) or (width >= down and height >= across)


def _transparent(span: RawSpan) -> bool:
    """True if every channel the span's render mode paints is faint (a mode that paints nothing: False)."""
    mode = span.render_mode
    if mode in _NO_PAINT_MODES:
        return False
    fill_clear = mode not in _FILL_MODES or span.fill_rgba[3] <= _FAINT_ALPHA
    stroke_clear = mode not in _STROKE_MODES or span.stroke_rgba[3] <= _FAINT_ALPHA
    return fill_clear and stroke_clear


def _overlapping(box: Box, raster: tuple[Box, ...]) -> tuple[Box, ...]:
    """The raster boxes that meet ``box`` (edges included)."""
    x0, y0, x1, y1 = box
    return tuple(image for image in raster if image[0] <= x1 and x0 <= image[2] and image[1] <= y1 and y0 <= image[3])


def _over_raster(box: Box, raster: tuple[Box, ...]) -> bool:
    """True if the centre of ``box`` is inside (or on the edge of) some raster box."""
    x, y = (box[0] + box[2]) / 2, (box[1] + box[3]) / 2
    return any(image[0] <= x <= image[2] and image[1] <= y <= image[3] for image in raster)


def _span_pieces(span: RawSpan, width: float, height: float, raster: tuple[Box, ...]) -> Iterator[SpanPiece]:
    """Yield ``span``'s pieces: maximal runs of chars with equal hidden reasons."""
    no_paint = span.render_mode in _NO_PAINT_MODES
    under = _overlapping(span.bbox, raster) if no_paint else ()
    flagged = dict.fromkeys(em_reasons(span.em_width_pt, abs(span.font_size_pt)), True)
    flagged["transparent"] = _transparent(span)
    reasons: list[tuple[HiddenReason, ...]] = []
    for box in span.char_boxes:
        flagged["off_page"] = off_page(box, width, height)
        flagged["invisible"] = no_paint and not _over_raster(box, under)
        reasons.append(tuple(reason for reason in HIDDEN_REASONS if flagged.get(reason, False)))
    start = 0
    for end in range(1, len(reasons) + 1):
        if end == len(reasons) or reasons[end] != reasons[start]:
            yield SpanPiece(span=span, start=start, end=end, hidden_reasons=reasons[start])
            start = end


@dataclass(frozen=True, slots=True, kw_only=True)
class Backdrop:
    """A page drawn without its text, in 24-bit BGR: the background its text is drawn on (#400).

    The render worker draws it (``render_page(..., colour=True,
    without_text=True)``); its pixels are its ``RenderedPage``'s.

    Attributes:
        pixels: ``px_width * px_height * 3`` bytes, row by row from the
            displayed page's top-left corner, unpadded, a pixel's blue, green
            and red bytes in turn. Not in repr.
        px_width: Pixels across the displayed page.
        px_height: Pixels down it.
    """

    pixels: bytes = field(repr=False)
    px_width: int
    px_height: int

    def __post_init__(self) -> None:
        int_at_least("px_width", self.px_width, 1)
        int_at_least("px_height", self.px_height, 1)
        if not isinstance(self.pixels, bytes):
            raise TypeError(f"pixels must be bytes, got {type(self.pixels).__name__}")
        expected = self.px_width * self.px_height * _BGR_BYTES
        if len(self.pixels) != expected:
            size = f"{self.px_width} x {self.px_height} x {_BGR_BYTES}"
            raise ValueError(f"pixels must hold {size} = {expected} bytes, got {len(self.pixels)}")


def flag_background(
    pieces: tuple[SpanPiece, ...], backdrop: Backdrop, *, width_pt: float, height_pt: float
) -> tuple[SpanPiece, ...]:
    """``pieces``, each visible one whose paint matches the background under it flagged ``matches_background``.

    See the module notes. A piece with a reason is returned as it is; so is
    one that paints nothing (render mode 3 or 7). Pieces keep their order.
    Cost: O(chars x MAX_SAMPLES_PER_AXIS) slices of the backdrop, each at
    most MAX_SAMPLES_PER_AXIS pixels.

    Args:
        pieces: The pieces to check, as :func:`classify_spans` cuts them.
        backdrop: The page drawn without its text.
        width_pt: The displayed page width in points (``PageGeometry.width_pt``).
        height_pt: The displayed page height in points.

    Raises:
        TypeError: If ``pieces`` is not a tuple of SpanPiece, ``backdrop`` not
            a Backdrop, or a page side not a number (bool rejected).
        ValueError: If a page side is not finite and greater than zero, or
            the backdrop is not a page of that size drawn at one scale.
    """
    typed_tuple("pieces", pieces, SpanPiece)
    if not isinstance(backdrop, Backdrop):
        raise TypeError(f"backdrop must be a Backdrop, got {type(backdrop).__name__}")
    width, height = positive_float("width_pt", width_pt), positive_float("height_pt", height_pt)
    return _flag_background(pieces, backdrop, _scale(backdrop, width, height))


def _scale(backdrop: Backdrop, width: float, height: float) -> tuple[float, float]:
    """The backdrop's pixels a point across and down, once it is shown to be the page drawn at one scale.

    A page drawn at ``k`` pixels a point is ``floor(width k)`` x
    ``floor(height k)`` px, so some ``k`` lies in both sides' intervals
    ``[px / side, (px + 1) / side)``.

    Raises:
        ValueError: If no single scale draws the page at the backdrop's size.
    """
    across, down = backdrop.px_width / width, backdrop.px_height / height
    ceiling = min((backdrop.px_width + 1) / width, (backdrop.px_height + 1) / height)
    if max(across, down) > ceiling:
        size = f"{backdrop.px_width} x {backdrop.px_height} px"
        raise ValueError(f"backdrop is {size}: not a {width} x {height} pt page drawn at one scale")
    return across, down


def _flag_background(
    pieces: tuple[SpanPiece, ...], backdrop: Backdrop, scale: tuple[float, float]
) -> tuple[SpanPiece, ...]:
    """:func:`flag_background` of checked input, at the backdrop's ``scale``."""
    return tuple(_background_flagged(piece, backdrop, scale) for piece in pieces)


def _background_flagged(piece: SpanPiece, backdrop: Backdrop, scale: tuple[float, float]) -> SpanPiece:
    """``piece``, flagged ``matches_background`` if it is visible and its paint matches its background."""
    if piece.hidden_reasons or not _matches_background(piece, backdrop, scale):
        return piece
    return SpanPiece(span=piece.span, start=piece.start, end=piece.end, hidden_reasons=_MATCHES_BACKGROUND)


def _matches_background(piece: SpanPiece, backdrop: Backdrop, scale: tuple[float, float]) -> bool:
    """True if the piece paints and its paint is under MATCH_DISTANCE of its background."""
    if piece.span.render_mode in _NO_PAINT_MODES:
        return False
    return _near(_paint_rgb(piece.span), _background_rgb(piece, backdrop, scale))


def _near(paint: tuple[int, int, int], background: tuple[int, int, int]) -> bool:
    """True if the Euclidean RGB distance between the two colours is under MATCH_DISTANCE."""
    return math.dist(paint, background) < MATCH_DISTANCE


def _paint_rgb(span: RawSpan) -> tuple[int, int, int]:
    """The colour the span's render mode paints: the fill for modes 0, 2, 4 and 6, else the stroke."""
    red, green, blue, _alpha = span.fill_rgba if span.render_mode in _FILL_MODES else span.stroke_rgba
    return red, green, blue


def _background_rgb(piece: SpanPiece, backdrop: Backdrop, scale: tuple[float, float]) -> tuple[int, int, int]:
    """The piece's background: each channel's median over its chars' backgrounds, as red, green, blue."""
    samples = [_char_bgr(box, backdrop, scale) for box in piece.span.char_boxes[piece.start : piece.end]]
    blue, green, red = (_median([sample[channel] for sample in samples]) for channel in range(_BGR_BYTES))
    return red, green, blue


def _char_bgr(box: Box, backdrop: Backdrop, scale: tuple[float, float]) -> tuple[int, int, int]:
    """A char's background: each channel's median over the backdrop's pixels under its box, as blue, green, red.

    Each row is three strided slices of the backdrop (one a channel): at
    most MAX_SAMPLES_PER_AXIS rows of at most MAX_SAMPLES_PER_AXIS pixels.
    """
    x0, y0, x1, y1 = box
    columns = _pixel_run(x0 * scale[0], x1 * scale[0], backdrop.px_width)
    rows = _pixel_run(y0 * scale[1], y1 * scale[1], backdrop.px_height)
    row_bytes, step = backdrop.px_width * _BGR_BYTES, columns.step * _BGR_BYTES
    first, last = columns.start * _BGR_BYTES, columns[-1] * _BGR_BYTES
    channels: tuple[list[int], list[int], list[int]] = ([], [], [])
    for row in rows:  # at most MAX_SAMPLES_PER_AXIS
        base = row * row_bytes
        for channel, values in enumerate(channels):
            values.extend(backdrop.pixels[base + first + channel : base + last + channel + 1 : step])
    blue, green, red = (_median(values) for values in channels)
    return blue, green, red


def _pixel_run(low: float, high: float, size: int) -> range:
    """The pixels ``0..size - 1`` whose centres lie in ``[low, high]``, at most MAX_SAMPLES_PER_AXIS, evenly spaced.

    When none does (the span is under one pixel, or past an edge), the pixel
    under its centre, clamped to the edge.
    """
    first, last = max(0, math.ceil(low - 0.5)), min(size - 1, math.floor(high - 0.5))
    if first > last:
        nearest = min(size - 1, max(0, math.floor((low + high) / 2)))
        return range(nearest, nearest + 1)
    step = -(-(last - first + 1) // MAX_SAMPLES_PER_AXIS)
    return range(first, last + 1, step)


def _median(values: Sequence[int]) -> int:
    """The lower median of ``values`` (not empty)."""
    return sorted(values)[(len(values) - 1) // 2]


def raster_cover_fraction(images: tuple[RawImage, ...], width_pt: float, height_pt: float) -> float:
    """The fraction, 0 to 1, of the page that the union of the image boxes covers.

    Exact for the boxes: each image's box is clipped to the page
    ``(0, 0, width_pt, height_pt)`` and overlaps count once (a scan drawn
    in strips covers the page though no strip covers half of it). Every
    image counts, an oversized or unsized one included: it is still
    raster (the OCR-layer exemption and the hybrid route count only the
    scan-like ones; see the module notes). A rotated image counts as its axis-aligned box, an
    over-estimate of its painted area. O(n log n) in the image count n: a
    sweep over x with a segment tree over the compressed y edges.

    Raises:
        TypeError: If ``images`` is not a tuple of RawImage, or a page side
            is not a number (bool rejected).
        ValueError: If a page side is not finite and greater than zero.
    """
    typed_tuple("images", images, RawImage)
    width, height = positive_float("width_pt", width_pt), positive_float("height_pt", height_pt)
    return _cover_fraction(tuple(image.bbox for image in images), width, height)


def _cover_fraction(boxes: tuple[Box, ...], width: float, height: float) -> float:
    """The union area of ``boxes`` clipped to the page, as a fraction of the page."""
    clipped = [(max(x0, 0.0), max(y0, 0.0), min(x1, width), min(y1, height)) for x0, y0, x1, y1 in boxes]
    kept = [box for box in clipped if box[0] < box[2] and box[1] < box[3]]
    if not kept:
        return 0.0
    # Float rounding in the sum can put a full-page union a hair over the page.
    return min(1.0, _union_area(kept) / (width * height))


def _union_area(boxes: list[Box]) -> float:
    """The exact area of the union of non-empty ``boxes``: sweep over x, cover tree over y."""
    edges = sorted({y for box in boxes for y in (box[1], box[3])})
    rank = {y: index for index, y in enumerate(edges)}
    opening = [(box[0], 1, rank[box[1]], rank[box[3]]) for box in boxes]
    closing = [(box[2], -1, rank[box[1]], rank[box[3]]) for box in boxes]
    events = sorted(opening + closing)
    tree = _CoverTree(edges)
    area, previous_x = 0.0, events[0][0]
    for x, delta, low, high in events:
        area += tree.covered() * (x - previous_x)
        tree.add(low, high, delta)
        previous_x = x
    return area


class _CoverTree:
    """The covered length of a multiset of y intervals, over compressed edges.

    Leaf ``i`` is the gap ``edges[i]..edges[i + 1]``. A node's count is how
    many intervals cover all of its range through it, and its covered length
    is its whole length when that count is positive, else its children's
    sum. Updates are bottom-up and iterative (no recursion): O(log n) each.
    """

    __slots__ = ("_count", "_covered", "_length", "_size")

    def __init__(self, edges: list[float]) -> None:
        gaps = len(edges) - 1
        size = 1 << max(0, (gaps - 1).bit_length())
        length = [0.0] * (2 * size)
        for index in range(gaps):
            length[size + index] = edges[index + 1] - edges[index]
        for node in range(size - 1, 0, -1):
            length[node] = length[2 * node] + length[2 * node + 1]
        self._size = size
        self._length = length
        self._count = [0] * (2 * size)
        self._covered = [0.0] * (2 * size)

    def covered(self) -> float:
        """The length covered by at least one interval."""
        return self._covered[1]

    def add(self, low: int, high: int, delta: int) -> None:
        """Add ``delta`` (+1 or -1) to the count of gaps ``low..high - 1``.

        Each loop halves its node index, so each runs at most log2(size) + 1 times.
        """
        left, right = low + self._size, high + self._size
        first, last = left, right - 1
        while left < right:
            if left & 1:
                self._count[left] += delta
                self._pull(left)
                left += 1
            if right & 1:
                right -= 1
                self._count[right] += delta
                self._pull(right)
            left, right = left >> 1, right >> 1
        # Every node whose count changed hangs below the paths up from the two end gaps.
        for node in (first, last):
            node >>= 1
            while node:
                self._pull(node)
                node >>= 1

    def _pull(self, node: int) -> None:
        """Recompute a node's covered length from its count and its children."""
        if self._count[node] > 0:
            self._covered[node] = self._length[node]
        elif node >= self._size:
            self._covered[node] = 0.0
        else:
            self._covered[node] = self._covered[2 * node] + self._covered[2 * node + 1]


@dataclass(frozen=True, slots=True, kw_only=True)
class PageAssessment:
    """One page's pieces, quality signals and route, as :func:`assess_page` measures them.

    Picklable, so it crosses the process pool.

    Attributes:
        pieces: Every char of every span of the page's textpage, cut by
            :func:`classify_spans`: the page's text.
        recovered_pieces: The recovered spans' pieces, in recovery order
            (see the module notes): never page text. Empty without a
            recovery.
        quality: The page's quality signals; ``deskew_angle_deg`` and
            ``ocr_mean_confidence`` are None (A5 measures them).
        route: ``"digital"``, ``"hybrid"`` or ``"vision"``.
        unmapped_regions: One box per maximal run of invalid chars in a
            visible piece, in piece order: where OCR (A5) reads what the
            text layer cannot say. Empty when there is none.
        warnings: Non-fatal notes for the page (``Page.warnings``).
    """

    pieces: tuple[SpanPiece, ...]
    recovered_pieces: tuple[SpanPiece, ...]
    quality: PageQuality
    route: PageRoute
    unmapped_regions: tuple[Box, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        typed_tuple("pieces", self.pieces, SpanPiece)
        typed_tuple("recovered_pieces", self.recovered_pieces, SpanPiece)
        if not isinstance(self.quality, PageQuality):
            raise TypeError(f"quality must be a PageQuality, got {type(self.quality).__name__}")
        choice_str("route", self.route, PAGE_ROUTES)
        regions = typed_tuple("unmapped_regions", self.unmapped_regions, tuple)
        boxes = tuple(ordered_box(f"unmapped_regions[{index}]", box) for index, box in enumerate(regions))
        object.__setattr__(self, "unmapped_regions", boxes)
        str_tuple("warnings", self.warnings)


def assess_page(
    page: RawPage,
    *,
    ocr: OcrMode = "auto",
    recovered: RecoveredText | None = None,
    background: Backdrop | str | None = None,
) -> PageAssessment:
    """Classify a page's spans, check them against its background, measure its quality and route it.

    See the module notes, and their order: the background is checked before
    the duplicate check and the route, so flagged text counts as hidden.

    Cost: :func:`classify_spans` of the page's and the recovered spans, one
    raster cover and O(chars); with a backdrop, :func:`flag_background` of
    those pieces; the duplicate check indexes the page's pieces by text and
    makes at most :data:`MAX_DUPLICATE_CANDIDATES` box checks per recovered
    piece.

    Args:
        page: The page's raw facts, as
            :func:`~datagrunt.pdf.pdfium_engine.extract_page` returns them.
        ocr: The reader's OCR option.
        recovered: The page's recovered text, as
            :func:`~datagrunt.pdf.pdfium_engine.recover_dropped_text` returns
            it, or None for none.
        background: The page drawn without its text (a :class:`Backdrop`),
            why it could not be drawn (a str: a warning, and each piece the
            gate selects flagged ``unchecked_background``), or None: not
            checked (the gate did not pass it), no warning.

    Raises:
        TypeError: If ``page`` is not a RawPage, ``ocr`` is not a str,
            ``recovered`` is not a RecoveredText or None, or ``background``
            not a Backdrop, a str or None.
        ValueError: If ``ocr`` is not ``"auto"``, ``"always"`` or ``"never"``,
            or ``recovered`` names another page, or does not account for
            exactly the page's ``dropped_text_objects``, or ``background`` is
            a blank str, or a backdrop not of this page's size.
    """
    if not isinstance(page, RawPage):
        raise TypeError(f"page must be a RawPage, got {type(page).__name__}")
    choice_str("ocr", ocr, _OCR_MODES)
    _check_recovered(recovered, page.page_number, page.text.stats.dropped_text_objects)
    _check_background(background)
    width, height = page.geometry.width_pt, page.geometry.height_pt
    boxes = _scan_boxes(page.images.images)
    cover = _cover_fraction(boxes, width, height)
    pieces = _classify(page.text.spans, width, height, boxes, cover)
    own = _recovered_own(recovered, width, height, boxes, cover)
    if isinstance(background, Backdrop):
        scale = _scale(background, width, height)
        pieces, own = _flag_background(pieces, background, scale), _flag_background(own, background, scale)
    elif background is not None:
        pieces, own = _flag_unchecked(pieces, page), _flag_unchecked(own, page)
    flagged, unchecked = _duplicates_flagged(own, pieces)
    route, warnings = _route(_text_char_count(pieces), cover, ocr)
    warnings += _recovery_warnings(recovered, page.text.stats.dropped_text_objects)
    return PageAssessment(
        pieces=pieces,
        recovered_pieces=flagged,
        quality=_quality(page, pieces + flagged),
        route=route,
        unmapped_regions=_unmapped_regions(pieces),
        warnings=warnings + _duplicate_warnings(unchecked) + _background_warnings(background),
    )


def _check_background(background: object) -> None:
    """Require a Backdrop, a non-blank str (why the page was not drawn) or None."""
    if background is None or isinstance(background, Backdrop):
        return
    if not isinstance(background, str):
        raise TypeError(f"background must be a Backdrop, a str or None, got {type(background).__name__}")
    non_blank_str("background", background)


def _background_warnings(background: Backdrop | str | None) -> tuple[str, ...]:
    """The warning of a page whose background could not be checked (``background`` says why), if so."""
    return (f"background not checked: {background}",) if isinstance(background, str) else ()


def needs_background(page: RawPage, assessment: PageAssessment) -> bool:
    """Whether the page must be drawn to check its text's background: the gate (see the module notes).

    True when a visible piece of ``assessment`` (page or recovered) that
    paints (render mode not 3 or 7) has a paint under MATCH_DISTANCE of
    white, or overlaps (with area) a fill A2 reads or an image of ``page``.
    Cost: O(segments) for the fills, then O(pieces x (fills + images)).

    Raises:
        TypeError: If ``page`` is not a RawPage or ``assessment`` not a PageAssessment.
    """
    if not isinstance(page, RawPage):
        raise TypeError(f"page must be a RawPage, got {type(page).__name__}")
    if not isinstance(assessment, PageAssessment):
        raise TypeError(f"assessment must be a PageAssessment, got {type(assessment).__name__}")
    return any(_gate_selection((*assessment.pieces, *assessment.recovered_pieces), page))


def _gate_selection(pieces: tuple[SpanPiece, ...], page: RawPage) -> tuple[bool, ...]:
    """For each piece, whether the gate selects it: visible, painting, and near white or over paint."""
    covers = (*_fill_boxes(page.segments.segments), *(image.bbox for image in page.images.images))
    return tuple(_gate_selects(piece, covers) for piece in pieces)


def _gate_selects(piece: SpanPiece, covers: tuple[Box, ...]) -> bool:
    """True if the piece is visible, paints (render mode not 3 or 7), and is near white or over a cover."""
    if piece.hidden_reasons or piece.span.render_mode in _NO_PAINT_MODES:
        return False
    return _near(_paint_rgb(piece.span), _WHITE_RGB) or _overlaps_any(piece.bbox, covers)


def _flag_unchecked(pieces: tuple[SpanPiece, ...], page: RawPage) -> tuple[SpanPiece, ...]:
    """``pieces``, each one the gate selects flagged ``unchecked_background``: its background could not be checked."""
    return tuple(
        SpanPiece(span=piece.span, start=piece.start, end=piece.end, hidden_reasons=_UNCHECKED_BACKGROUND)
        if selected
        else piece
        for piece, selected in zip(pieces, _gate_selection(pieces, page), strict=True)
    )


def _fill_boxes(segments: tuple[RawSegment, ...]) -> tuple[Box, ...]:
    """The boxes A2's fill segments say are painted: each thin fill's, and each path object's fill edges' union."""
    thin: list[Box] = []
    edges: dict[int, Box] = {}
    for segment in segments:  # bounded: the page's segments
        if segment.kind == "stroke":
            continue
        box = _segment_box(segment)
        if segment.kind == "thin_fill":
            thin.append(box)
            continue
        known = edges.get(segment.object_index)
        edges[segment.object_index] = box if known is None else union_box((known, box))
    return (*thin, *edges.values())


def _segment_box(segment: RawSegment) -> Box:
    """The displayed box a segment covers: its length along its axis, its width across it (0 for a fill edge)."""
    half = segment.width / 2
    if segment.orientation == "h":
        return segment.start, segment.position - half, segment.end, segment.position + half
    return segment.position - half, segment.start, segment.position + half, segment.end


def _overlaps_any(box: Box, covers: tuple[Box, ...]) -> bool:
    """True if ``box`` and some box of ``covers`` share area (touching edges do not)."""
    x0, y0, x1, y1 = box
    return any(cover[0] < x1 and x0 < cover[2] and cover[1] < y1 and y0 < cover[3] for cover in covers)


def _check_recovered(recovered: object, page_number: int, dropped: int) -> None:
    """Require None, or page ``page_number``'s RecoveredText, accounting for each of its ``dropped`` objects once."""
    if recovered is None:
        return
    if not isinstance(recovered, RecoveredText):
        raise TypeError(f"recovered must be a RecoveredText or None, got {type(recovered).__name__}")
    if recovered.page_number != page_number:
        raise ValueError(f"recovered is the recovery of page {recovered.page_number}, not of page {page_number}")
    stats = recovered.stats
    counted = (
        stats.overprint_duplicates
        + stats.whitespace_objects
        + stats.empty_text_objects
        + stats.unrecovered_text_objects
    )
    accounted = len({item.span.object_index for item in recovered.spans}) + counted
    if accounted != dropped:
        found = f"recovered accounts for {accounted} dropped text object(s), but the page has {dropped}"
        raise ValueError(f"{found}: it is not this page's recovery")


def _recovered_own(
    recovered: RecoveredText | None, width: float, height: float, boxes: tuple[Box, ...], cover: float
) -> tuple[SpanPiece, ...]:
    """The recovered spans' pieces, in recovery order, each with its recovered reasons and the computed ones.

    Reasons are in canonical order. Empty without a recovery.
    """
    if recovered is None:
        return ()
    found: list[SpanPiece] = []
    for item in recovered.spans:
        for piece in _classify((item.span,), width, height, boxes, cover):
            given = set(piece.hidden_reasons) | set(item.hidden_reasons)
            reasons = tuple(reason for reason in HIDDEN_REASONS if reason in given)
            found.append(SpanPiece(span=piece.span, start=piece.start, end=piece.end, hidden_reasons=reasons))
    return tuple(found)


def _duplicates_flagged(own: tuple[SpanPiece, ...], pieces: tuple[SpanPiece, ...]) -> tuple[tuple[SpanPiece, ...], int]:
    """The recovered pieces ``own``, each with no reason that is a drawn copy of a visible piece ``duplicate``.

    Also how many were past the duplicate check's cap (see the module notes).
    Cost: O(pieces) to index them by text, then O(MAX_DUPLICATE_CANDIDATES)
    box checks per recovered piece.
    """
    twins = _visible_by_text(pieces)
    found: list[SpanPiece] = []
    unchecked = 0
    for piece in own:
        candidates = () if piece.hidden_reasons else twins.get(piece.text, ())
        if len(candidates) > MAX_DUPLICATE_CANDIDATES:
            unchecked += 1
            found.append(piece)
        elif any(_drawn_on(piece, twin) for twin in candidates):
            found.append(SpanPiece(span=piece.span, start=piece.start, end=piece.end, hidden_reasons=_DUPLICATE))
        else:
            found.append(piece)
    return tuple(found), unchecked


def _visible_by_text(pieces: tuple[SpanPiece, ...]) -> dict[str, list[SpanPiece]]:
    """The visible pieces (no hidden reason) of ``pieces``, by their text, in piece order."""
    found: dict[str, list[SpanPiece]] = {}
    for piece in pieces:
        if not piece.hidden_reasons:
            found.setdefault(piece.text, []).append(piece)
    return found


def _drawn_on(copy: SpanPiece, twin: SpanPiece) -> bool:
    """True if each edge of ``twin``'s ink box lies within ``copy``'s duplicate window of the same edge of ``copy``'s.

    The window is recovery's (:func:`duplicate_window`) of the copy's last
    char's box, on each displayed axis.
    """
    across, down = duplicate_window(copy.span.char_boxes[copy.end - 1])
    window = (across, down, across, down)
    return all(abs(mine - theirs) <= reach for mine, theirs, reach in zip(_ink(copy), _ink(twin), window))


def _ink(piece: SpanPiece) -> Box:
    """The union of the piece's tight boxes: where its glyphs' ink is (a recovered char box is its object's ink)."""
    return union_box(piece.span.tight_boxes[piece.start : piece.end])


def _duplicate_warnings(unchecked: int) -> tuple[str, ...]:
    """A warning for the ``unchecked`` recovered pieces past the duplicate check's cap, if any."""
    if not unchecked:
        return ()
    each = f"each has over {MAX_DUPLICATE_CANDIDATES} visible pieces of the same text on the page"
    return (f"{unchecked} recovered piece(s) were not checked for a drawn duplicate: {each}; they stay visible",)


def _recovery_warnings(recovered: RecoveredText | None, dropped: int) -> tuple[str, ...]:
    """A warning for the page's ``dropped`` text objects that no recovery read back, if any."""
    objects, missing = "text object(s) that pdfium's textpage drops", "their text is missing from this page"
    if recovered is None:
        return (f"{dropped} {objects} were not recovered (no recovery was passed); {missing}",) if dropped else ()
    lost = recovered.stats.unrecovered_text_objects
    return (f"{lost} {objects} could not be recovered; {missing}",) if lost else ()


def _valid(span: RawSpan, offset: int) -> bool:
    """True unless ``span``'s char at ``offset`` is a map error, U+FFFD, private use, unassigned or a lone surrogate.

    O(log map errors): ``map_errors`` is sorted.
    """
    errors = span.map_errors
    at = bisect_left(errors, offset)
    if at < len(errors) and errors[at] == offset:
        return False
    return _valid_char(span.text[offset])


def valid_char(char: str) -> bool:
    """True unless ``char`` is U+FFFD, private use, unassigned or a lone surrogate: A3's rule for valid text.

    A span's char is also invalid when pdfium could not map it
    (``RawSpan.map_errors``); text with no such map, as OCR reads, is
    measured by this rule alone (A5b's vision route).

    Raises:
        TypeError: If ``char`` is not a str.
        ValueError: If ``char`` is not one char.
    """
    if not isinstance(char, str):
        raise TypeError(f"char must be a str, got {type(char).__name__}")
    if len(char) != 1:
        raise ValueError(f"char must be one char, got {len(char)}: {char!r}")
    return _valid_char(char)


def _valid_char(char: str) -> bool:
    """:func:`valid_char` of a checked char."""
    return char != _REPLACEMENT_CHAR and unicodedata.category(char) not in _INVALID_CATEGORIES


def _quality(page: RawPage, pieces: tuple[SpanPiece, ...]) -> PageQuality:
    """The page's quality signals; every span char, hidden or not, counts toward the ratio."""
    spans = page.text.spans
    count = sum(len(span.text) for span in spans)
    valid = sum(1 for span in spans for offset in range(len(span.text)) if _valid(span, offset))
    return PageQuality(
        valid_char_ratio=valid / count if count else None,
        char_count=count,
        vector_line_count=len(page.segments.segments),
        hidden_spans_flagged=sum(1 for piece in pieces if piece.hidden_reasons),
        rotation_deg=page.geometry.rotation_deg,
        deskew_angle_deg=None,
        ocr_mean_confidence=None,
    )


def _text_char_count(pieces: tuple[SpanPiece, ...]) -> int:
    """The chars routing counts: valid, visible (in a piece with no reason) and not whitespace."""
    visible = (piece for piece in pieces if not piece.hidden_reasons)
    return sum(
        1
        for piece in visible
        for offset in range(piece.start, piece.end)
        if _valid(piece.span, offset) and not piece.span.text[offset].isspace()
    )


def _route(text_chars: int, cover: float, ocr: OcrMode) -> tuple[PageRoute, tuple[str, ...]]:
    """The page's route and warnings, from its routing char count and raster cover (see the module notes)."""
    if ocr == "always":
        return "vision", ()
    if text_chars < _MIN_TEXT_CHARS:
        if ocr == "never":
            count = f"{text_chars} valid, visible, non-whitespace chars (under {_MIN_TEXT_CHARS})"
            return "digital", (f"ocr='never': the page has {count}; it stays digital instead of going to OCR",)
        return "vision", ()
    # hybrid OCRs the unmapped regions, so "never" keeps such a page digital.
    return ("hybrid" if cover > _OCR_LAYER_COVER and ocr != "never" else "digital"), ()


def _unmapped_regions(pieces: tuple[SpanPiece, ...]) -> tuple[Box, ...]:
    """The box of each invalid-char run in a visible piece, in piece order (hidden text is not OCR'd)."""
    return tuple(region for piece in pieces if not piece.hidden_reasons for region in _invalid_runs(piece))


def _invalid_runs(piece: SpanPiece) -> Iterator[Box]:
    """Yield the union box of each maximal run of invalid chars in ``piece``."""
    span, boxes = piece.span, piece.span.char_boxes
    for invalid, run in groupby(range(piece.start, piece.end), key=lambda offset: not _valid(span, offset)):
        if invalid:
            offsets = tuple(run)
            yield union_box(boxes[offsets[0] : offsets[-1] + 1])
