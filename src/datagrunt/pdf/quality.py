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

The gate (:func:`needs_background`) selects each visible piece that paints
(page text or recovered; render mode not 3 or 7) whose paint is under
:data:`MATCH_DISTANCE` of white (a page's default background), or whose box
shares area with the box of any non-text object of the page (A2's
:class:`~datagrunt.pdf.pdfium_engine.ObjectBoxes`: paths of any shape and
strokes, images and shadings, those a form draws included; a form itself
paints nothing and has no box), and every such piece when the page holds
an object A2 could give no box (fail closed). A page is drawn when it
selects a piece: only pages of text alone, none near white, are skipped,
whether the text is drawn through forms or not. The boxes are indexed by :data:`_GATE_BANDS` bands down the
page, and a page spends at most :data:`MAX_GATE_CHECKS` box checks: a piece
whose checks the budget cannot pay for is selected (fail closed). Known
limit: a background drawn with text glyphs (a ``█`` under the text) is
removed with the text from the backdrop, so the text is checked against
what lies under those glyphs, not against them.

Order (#400): A3 assesses a page with no background; the pipeline gates it,
draws it, and assesses it again with its :class:`Backdrop`
(``assess_page(..., background=...)``). That assessment flags the page's
pieces, then the recovered pieces (their own reasons first), before the
duplicate check, then routes, measures and warns: flagged text leaves the
routing count and ratio, the unmapped regions and the duplicate check's
visible twins, as text with any other reason does. A page that could not
be drawn (its render refused or failed) is assessed with why (a str) and
fails closed: each visible piece the gate selects (see
:func:`needs_background`) is flagged ``unchecked_background``, in the same
place in that order, and the page warns ``background not checked: ...``.
Pieces the gate does not select stay visible.

:func:`assess_page` measures a page and routes it (#366 task 4):

- A char is valid text unless pdfium could not map its code to Unicode
  (``RawSpan.map_errors``: it reads as the raw code, ``\x03`` or ``B``
  alike, so it says nothing), or it is U+FFFD (a code with no Unicode
  mapping), private use (``Co``), unassigned (``Cn``, by the running
  Python's Unicode database) or a lone surrogate (``Cs``). Marks, format
  chars (ZWJ, LRM) and control chars are otherwise valid: nothing is
  stripped.
- ``valid_char_ratio`` (recorded) is the valid share of every span char,
  hidden ones and leader runs included, as ``char_count`` counts them.
- Routing (spec §3.3 step 2, #417) reads the visible chars (in a piece with
  no hidden reason) alone, so hidden text can neither send a page to
  vision nor keep it out. The count is of the valid, non-whitespace ones.
  The ratio is the valid share of the rest of them, whitespace included
  (the spec counts spaces valid), with leader runs left out of both its
  numerator and its denominator: a leader run is a maximal run of 3 or
  more identical invalid chars within a piece (dot leaders that read as
  U+FFFD, a private-use rule glyph repeated), which is layout, not text.
  This amends the spec's rule (maintainer, 2026-11-01): a contents page
  whose leaders are most of its chars keeps its route. Garbled text, whose
  invalid chars vary, counts in full; a run of identical valid chars
  (``-----``) is valid and counts. A run is found within one piece: A3
  does not group spans into lines (A4b does), so a run a piece break cuts
  is two runs, each judged on its own length. The routing ratio differs
  from the recorded one on a page with hidden text or a leader run.
- A count under 20 counts only on a page that also holds paint OCR could
  read and its text layer may not say: an image on the visible page, or
  vector paint (a path, a shading, inside a form or not: A2's object
  boxes) with area on it, or an object A2 could give no box (fail
  closed). A page of short, clean text alone (a cover page's title, a
  blank page) has nothing for OCR to read: it stays digital. This amends
  the spec's count rule as #417 amended its ratio rule (maintainer,
  2026-11-02, P2-C). Any area counts: a short word drawn as outlines
  covers under 0.1 % of a letter page, so any larger bound would leave
  such a page's words unread.
- ``ocr="always"`` routes vision. A count under 20 (on a page with paint),
  or a ratio under 0.70, routes vision, or digital under ``ocr="never"``
  with a warning naming each that held; a page with no visible char
  outside leader runs has no ratio (its count, 0, routes it). Otherwise, a page whose raster cover (of
  scan-like images, as for ``invisible``) is over 50 % routes hybrid (read
  from its text layer, as a digital page; its unmapped regions are recorded,
  not OCR'd), or digital under ``ocr="never"``; else digital.
- The image-only marker (#470): ``PageAssessment.image_only_reasons``
  records which of those two held, in every mode (``"always"`` too):
  ``too_little_text`` for the count, ``garbled_text`` for the ratio, both
  when both did (:data:`~datagrunt.pdf.elements.IMAGE_ONLY_REASONS`
  order); ``()`` when the page's text stands. They are the reasons its
  content lives only in its page image unless OCR reads it: A4b's
  :func:`~datagrunt.pdf.assembly.assemble_page` carries them onto the page
  (a page kept digital under ``ocr="never"``, a vision page OCR could not
  read), and a page OCR reads has none
  (:attr:`~datagrunt.pdf.elements.Page.image_only_reasons`). So, of the
  pages A3 assesses, ``ocr="auto"`` routes vision exactly those
  ``ocr="never"`` marks. Routes and warnings are as above: the marker
  changes neither.
- Unmapped regions: the box of each maximal run of invalid chars in a
  visible piece: recorded, for an OCR pass the pipeline does not run yet.

Known limitations (A3 review M7; each needs what this pure stage cannot
see, and an occlusion follow-up is proposed for the last two):

- A glyph with no outline whose ToUnicode map says it is text (a space
  that reads "IGNORE ALL") draws nothing but reads as visible text; alone
  in its object it is an empty object, counted and never read.
- Text a clip path excludes is not flagged: clip paths are not applied.
- Text under a later opaque image or fill is not flagged: occlusion is
  not checked.
- On a ``3 Tr`` CAD line, stem strokes plus an extra visible mark placed at
  the overhanging character pitch have the same bounding boxes as a genuine
  1-pitch SHX shift (and satisfy multi-word corroboration when peer words on
  that span also shift in the same direction), and an interior <= 2-letter
  gap between a backed letter and a backed letter or footnote ``*`` in a
  >= 60 %-backed digit-free word is bridged as narrow-glyph dropout: this
  pure stage inspects glyph boxes rather than glyph outline shapes.

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

``hidden_spans_flagged`` counts each span with a flagged piece once (#456):
a page span cut by both side edges is two ``off_page`` pieces and one span.
A recovered span is a span of its own and always one piece (each of its
char boxes is its object's box), so a flagged recovered piece (a duplicate
too) counts once; recovered text never counts toward routing, the char
counts or the unmapped regions. Objects recovery could not read back are
a warning, and so are dropped objects when no recovery is passed (the page's
``TextStats.dropped_text_objects`` is over 0 and ``recovered`` is None).
"""

import bisect
import math
import unicodedata
from bisect import bisect_left
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from itertools import groupby
from typing import NamedTuple, get_args

from datagrunt.pdf._hidden_rules import em_reasons, off_page
from datagrunt.pdf._options import OcrMode
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
    ImageOnlyReason,
    PageQuality,
    PageRoute,
    check_hidden_reasons,
    check_image_only_reasons,
)
from datagrunt.pdf.pdfium_engine import RawPage

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
# The valid share of its visible chars a page needs to stay out of vision (spec §3.3 step 2, #417).
_MIN_VALID_RATIO = 0.70
# On raster-covered scans (> 50 % cover), valid non-leader non-space text must have at least this lexical share.
_MIN_SCAN_LEXICAL_RATIO = 0.55
# On raster-covered scans with >= 4 tokens, intraword OCR-noise tokens above this share mark garbled_text.
_MAX_SCAN_NOISE_TOKEN_RATIO = 0.40
_MIN_SCAN_TOKENS = 4
_SCAN_LEADER_CHARS = frozenset("._-…")
_INTRAWORD_NOISE_CHARS = frozenset("|~^`\\")
# The image-only reasons routing records (#470): the count's shortfall, then the ratio's, in IMAGE_ONLY_REASONS order.
_SHORTFALL_REASONS: tuple[ImageOnlyReason, ...] = ("too_little_text", "garbled_text")
# A run of this many identical invalid chars or more is a leader or a rule, not text: the routing ratio leaves it out.
_MIN_LEADER_RUN = 3
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
# The box checks the gate may spend on a page: a piece whose checks it cannot pay for is selected (fail closed).
MAX_GATE_CHECKS = 1_000_000
# The gate indexes a page's object boxes by this many horizontal bands of it.
_GATE_BANDS = 32


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


_INVISIBLE_TEXT_MODE = 3
_VECTOR_TOL_PT = 0.5
_MAX_VECTOR_TEXT_CHECKS = 500_000


def classify_spans(
    spans: tuple[RawSpan, ...],
    *,
    width_pt: float,
    height_pt: float,
    images: tuple[RawImage, ...],
    glyph_boxes: tuple[Box, ...] = (),
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
    if not isinstance(glyph_boxes, tuple):
        raise TypeError(f"glyph_boxes must be a tuple, got {type(glyph_boxes).__name__}")
    checked_glyphs = tuple(ordered_box(f"glyph_boxes[{idx}]", box) for idx, box in enumerate(glyph_boxes))
    width, height = positive_float("width_pt", width_pt), positive_float("height_pt", height_pt)
    boxes = _scan_boxes(images)
    v_bands = _glyph_bands(checked_glyphs, height)
    return _classify(spans, width, height, boxes, _cover_fraction(boxes, width, height), v_bands)


_MAX_COMPOUND_WORD_PT = 600.0
_RULE_LIKE_CHARS = frozenset("_-—–=")


class _GlyphBand(NamedTuple):
    """Horizontal page band of glyph stroke boxes sorted by ``x0`` for O(log B) range lookup."""

    boxes: tuple[Box, ...]
    x0s: tuple[float, ...]
    max_w: float


def _glyph_bands(boxes: tuple[Box, ...], height: float) -> tuple[_GlyphBand, ...]:
    """Index ``boxes`` into horizontal bands sorted by ``x0`` in O(G log G)."""
    if not boxes:
        return ()
    buckets: list[list[Box]] = [[] for _ in range(_GATE_BANDS)]
    for box in boxes:
        for band in _bands_of(box[1], box[3], height):
            buckets[band].append(box)
    indexed: list[_GlyphBand] = []
    for bucket in buckets:
        bucket.sort(key=lambda b: b[0])
        max_w = max((b[2] - b[0] for b in bucket), default=0.0)
        indexed.append(_GlyphBand(tuple(bucket), tuple(b[0] for b in bucket), max_w))
    return tuple(indexed)


def _box_backs_char(ch: str, cw: float, ch_h: float, max_side: float, vw: float, vh: float) -> bool:
    """True when stroke dimensions ``(vw, vh)`` match a single glyph or > 36 pt compound outlined word path."""
    if vw > max_side or vh > max_side:
        minor, major = (vw, vh) if vw <= vh else (vh, vw)
        min_word_minor = max(3.0, 0.35 * min(max(cw, 4.0), max(ch_h, 4.0)))
        return (
            ch not in _RULE_LIKE_CHARS and min_word_minor <= minor <= max_side and 36.0 < major <= _MAX_COMPOUND_WORD_PT
        )
    if vh >= max(0.18 * ch_h, 1.2):
        return True
    if ch in "-_.=—–~" and vw >= max(0.35 * cw, 1.0) and vh <= max(0.5 * ch_h, 3.0):
        return True
    return ch in ".,:;'\"\u00b0\u00b7*" and vw <= max(cw * 2.0, 6.0) and vh <= max(ch_h * 2.0, 6.0)


def _x_backs_char(cx0: float, cx1: float, cw: float, vx0: float, vx1: float) -> bool:
    """True when stroke ``[vx0, vx1]`` centres inside ``[cx0, cx1]`` or covers >= 30% of its width."""
    vmid = 0.5 * (vx0 + vx1)
    if cx0 - 0.15 <= vmid <= cx1 + 0.15:
        return True
    return min(cx1, vx1) - max(cx0, vx0) >= max(0.30 * cw, 0.5)


def _char_has_glyph_stroke(ch: str, cbox: Box, bands: tuple[_GlyphBand, ...], height: float, budget: list[int]) -> bool:
    """True when a visible non-white glyph/word stroke box backs ``cbox`` without bleeding into neighbours."""
    cx0, cy0, cx1, cy1 = cbox
    cw, ch_h = max(cx1 - cx0, 0.0), max(cy1 - cy0, 0.0)
    max_side = min(max(2.5 * max(cw, ch_h, 4.0), 18.0), 36.0)
    qx0, qx1 = cx0 - 0.15, cx1 + 0.15
    qy0, qy1 = cy0 - _VECTOR_TOL_PT, cy1 + _VECTOR_TOL_PT
    for band_idx in _bands_of(qy0, qy1, height):
        gband = bands[band_idx]
        lo = bisect.bisect_left(gband.x0s, qx0 - gband.max_w - 1e-6)
        hi = bisect.bisect_right(gband.x0s, qx1)
        for idx in range(lo, hi):
            budget[0] -= 1
            if budget[0] < 0:
                return False
            vx0, vy0, vx1, vy1 = gband.boxes[idx]
            if (
                vy0 <= qy1
                and qy0 <= vy1
                and _x_backs_char(cx0, cx1, cw, vx0, vx1)
                and _box_backs_char(ch, cw, ch_h, max_side, vx1 - vx0, vy1 - vy0)
            ):
                return True
    return False


_MAX_DRIFT_GAP = 2
_SIGN_CHARS = frozenset("+-\u2212\u2010\u2011\u2012\u2013\u2014\u2015\u00b1\uff0b\uff0d~")
_SAFE_TWO_CHAR_WORDS = frozenset(
    {"TO", "IN", "AT", "OF", "ON", "BY", "AS", "IS", "BE", "IT", "AN", "LA", "LE", "DU", "DE", "AU", "EN", "ET"}
)
_UNSAFE_BRIDGE_WORDS = frozenset(
    {
        "NO",
        "NOT",
        "NOR",
        "NONE",
        "NEVER",
        "NEITHER",
        "NAUGHT",
        "CANNOT",
        "CANT",
        "DONT",
        "WONT",
        "SHANT",
        "ISNT",
        "ARENT",
        "WASNT",
        "WERENT",
        "HASNT",
        "HADNT",
        "DOESNT",
        "DIDNT",
        "WOULDNT",
        "COULDNT",
        "SHOULDNT",
        "MUSTNT",
        "NOK",
        "IGNORE",
    }
)
_UNSAFE_TWO_CHAR_PREFIXES = frozenset(
    {"UN", "IN", "IM", "IL", "IR", "NO", "AB", "AN", "BY", "DE", "RE", "EX", "BI", "CO"}
)
_UNSAFE_TWO_CHAR_SUFFIXES = frozenset({"NT"})
_MIN_MULTI_WORD_COUNT = 2
_MIN_MULTI_SPAN_ALNUM = 8
_MIN_MULTI_SPAN_HIT_NUM = 7
_MULTI_SPAN_HIT_DEN = 10


class _SpanDriftCtx(NamedTuple):
    """Span and banded-stroke context for 3 Tr SHX kerning-drift overhang checks."""

    span: RawSpan
    bands: tuple[_GlyphBand, ...]
    tr3_bands: tuple[_GlyphBand, ...]
    height: float
    budget: list[int]
    single_word: bool = True
    right_corroborated: bool = False
    left_corroborated: bool = False


def _is_ascii_alpha(ch: str) -> bool:
    """True strictly for ASCII A-Z / a-z letters (rejecting non-ASCII homoglyphs)."""
    return ch.isascii() and ch.isalpha()


def _is_sign_or_dash(ch: str) -> bool:
    """True for ASCII/Unicode dashes (category Pd) and plus/minus operators or symbols."""
    if not ch:
        return False
    if ch in _SIGN_CHARS:
        return True
    if ord(ch) < 128:
        return False
    if unicodedata.category(ch) == "Pd":
        return True
    name = unicodedata.name(ch, "")
    return "MINUS" in name or "PLUS" in name


def _gap_is_bridgeable(
    text: str, on_page: Sequence[int], pos: int, end: int, alpha_hits: int, word_upper: str = ""
) -> bool:
    """True when interior gap ``pos..end`` is ASCII letters/separators after a backed ASCII letter."""
    if pos <= 0 or end >= len(on_page) or end - pos > _MAX_DRIFT_GAP or alpha_hits < 2:
        return False
    left_ch, right_ch = text[on_page[pos - 1]], text[on_page[end]]
    right_ok = _is_ascii_alpha(right_ch) or (right_ch == "*" and alpha_hits >= 3 and not word_upper.endswith("NT"))
    if not (_is_ascii_alpha(left_ch) and right_ok):
        return False
    for k in range(pos, end):
        ch = text[on_page[k]]
        if not (_is_ascii_alpha(ch) or (alpha_hits >= 4 and ch in "-_")):
            return False
    return True


def _bridge_short_drift_gaps(text: str, on_page: Sequence[int], backed: list[bool], alpha_hits: int = 0) -> None:
    """Bridge strictly interior <= 2-char kerning-drift gaps in O(N) on safe digit-free tokens."""
    n = len(on_page)
    if n < 3 or not text or any(text[i].isdigit() for i in on_page):
        return
    word_upper = "".join(text[i] for i in on_page if text[i].isalnum()).upper()
    if word_upper in _UNSAFE_BRIDGE_WORDS:
        return
    non_sign_left = False
    pos = 0
    while pos < n:
        idx = on_page[pos]
        if backed[idx]:
            non_sign_left = non_sign_left or not _is_sign_or_dash(text[idx])
            pos += 1
            continue
        end = pos + 1
        while end < n and not backed[on_page[end]]:
            end += 1
        if non_sign_left and _gap_is_bridgeable(text, on_page, pos, end, alpha_hits, word_upper):
            for k in range(pos, end):
                backed[on_page[k]] = True
        pos = end


def _adjacent_word_bound(
    edge_box: Box, tr3_bands: tuple[_GlyphBand, ...], height: float, budget: list[int], *, right_side: bool
) -> float | None:
    """Nearest adjacent non-space 3 Tr character edge on the same line across all spans, or None."""
    if not tr3_bands or height <= 0.0:
        return None
    wx0, wy0, wx1, wy1 = edge_box
    cw, ch_h = max(wx1 - wx0, 0.0), max(wy1 - wy0, 0.0)
    reach = max(cw * 2.5, 16.0)
    qy0, qy1 = wy0 + 0.15 * ch_h, wy1 - 0.15 * ch_h
    if qy0 > qy1:
        qy0, qy1 = wy0, wy1
    best: float | None = None
    for band_idx in _bands_of(qy0, qy1, height):
        gband = tr3_bands[band_idx]
        lo = (
            bisect.bisect_right(gband.x0s, wx1 + 0.05)
            if right_side
            else bisect.bisect_left(gband.x0s, wx0 - reach - gband.max_w - 1e-6)
        )
        hi = bisect.bisect_right(gband.x0s, wx1 + reach) if right_side else bisect.bisect_left(gband.x0s, wx0 - 0.05)
        for idx in range(lo, hi):
            budget[0] -= 1
            if budget[0] < 0:
                return best
            bx0, by0, bx1, by1 = gband.boxes[idx]
            if by0 <= qy1 and qy0 <= by1 and right_side and bx0 > wx1 + 0.05:
                best = bx0 if best is None else min(best, bx0)
            elif by0 <= qy1 and qy0 <= by1 and not right_side and bx1 < wx0 - 0.05:
                best = bx1 if best is None else max(best, bx1)
    return best


def _stroke_matches_overhang(
    box: Box,
    wx0: float,
    wx1: float,
    cw: float,
    ch_h: float,
    max_side: float,
    qy0: float,
    qy1: float,
    gap_lo: float,
    gap_hi: float,
    edge_ch: str,
    *,
    right_side: bool,
) -> bool:
    """True when ``box`` is a glyph-scale stroke centred inside ``[gap_lo, gap_hi]`` past the word edge."""
    vx0, vy0, vx1, vy1 = box
    if vy0 > qy1 or qy0 > vy1:
        return False
    vw, vh = vx1 - vx0, vy1 - vy0
    if vw > max_side:
        return False
    vmid = 0.5 * (vx0 + vx1)
    overhangs = (vx1 > wx1 + 0.5 if right_side else vx0 < wx0 - 0.5) and gap_lo <= vmid <= gap_hi
    if not overhangs:
        return False
    if max(0.18 * ch_h, 1.2) <= vh <= max_side:
        return True
    return edge_ch in "*°'\".,:;)]}" and 0.8 <= vh <= max(ch_h * 2.0, 6.0) and vw <= max(cw * 2.0, 6.0)


def _word_has_drift_overhang(
    edge_box: Box,
    edge_ch: str,
    bands: tuple[_GlyphBand, ...],
    tr3_bands: tuple[_GlyphBand, ...],
    height: float,
    budget: list[int],
    *,
    right_side: bool,
    min_chars: int = 1,
) -> bool:
    """True when glyph stroke(s) overhang the token into unclaimed inter-word whitespace."""
    if not bands or not edge_ch or min_chars < 1:
        return False
    wx0, wy0, wx1, wy1 = edge_box
    cw, ch_h = max(wx1 - wx0, 0.0), max(wy1 - wy0, 0.0)
    free_reach = max(cw, 2.0) if min_chars == 1 else max(cw * 2.5, 5.0)
    bound = _adjacent_word_bound(edge_box, tr3_bands, height, budget, right_side=right_side)
    gap_lo = wx1 + 0.2 if right_side else ((bound + 0.2) if bound is not None else wx0 - free_reach)
    gap_hi = ((bound - 0.2) if bound is not None else wx1 + free_reach) if right_side else wx0 - 0.2
    min_gap = 0.0 if min_chars == 1 else max(1.35 * cw, 4.0)
    if gap_hi - gap_lo <= min_gap:
        return False
    max_side = min(max(2.5 * max(cw, ch_h, 4.0), 18.0), 36.0)
    qy0, qy1 = wy0 - _VECTOR_TOL_PT, wy1 + _VECTOR_TOL_PT
    far_step = max(1.05 * cw, 2.0)
    seen: set[Box] = set()
    far_ok = min_chars == 1
    for band_idx in _bands_of(qy0, qy1, height):
        gband = bands[band_idx]
        lo = bisect.bisect_left(gband.x0s, gap_lo - gband.max_w - 1e-6)
        hi = bisect.bisect_right(gband.x0s, gap_hi + 0.5 * max_side)
        for idx in range(lo, hi):
            budget[0] -= 1
            if budget[0] < 0:
                return False
            box = gband.boxes[idx]
            if not _stroke_matches_overhang(
                box, wx0, wx1, cw, ch_h, max_side, qy0, qy1, gap_lo, gap_hi, edge_ch, right_side=right_side
            ):
                continue
            seen.add(box)
            far_ok = far_ok or (box[2] > wx1 + far_step if right_side else box[0] < wx0 - far_step)
            if len(seen) >= min_chars and far_ok:
                return True
    return False


def _overhangs_edge(ctx: _SpanDriftCtx, edge_idx: int, *, right_side: bool, min_chars: int = 1) -> bool:
    """True when ``ctx.span``'s edge char at ``edge_idx`` has SHX drift overhang on ``right_side``."""
    if edge_idx < 0 or edge_idx >= len(ctx.span.text):
        return False
    return _word_has_drift_overhang(
        ctx.span.char_boxes[edge_idx],
        ctx.span.text[edge_idx],
        ctx.bands,
        ctx.tr3_bands,
        ctx.height,
        ctx.budget,
        right_side=right_side,
        min_chars=min_chars,
    )


def _has_sign_prefix(text: str, on_page: Sequence[int], alnum: Sequence[int]) -> bool:
    """True when any on-page character before ``alnum[0]`` is an ASCII or Unicode sign/dash."""
    if not text or not on_page or not alnum:
        return False
    first_alnum = alnum[0]
    return any(_is_sign_or_dash(text[i]) for i in on_page if i < first_alnum)


def _is_eu_directive_lead_digit(
    ch: str,
    text: str,
    on_page: Sequence[int],
    alnum: Sequence[int],
    alpha_hits: int,
    *,
    min_alpha_hits: int = 2,
) -> bool:
    """True only for an unsigned ASCII YYYY/NN/XX directive with 4-digit year ('1'/'2' lead)."""
    if ch not in "12" or len(alnum) < 7 or alpha_hits < min_alpha_hits or _has_sign_prefix(text, on_page, alnum):
        return False
    if any(not text[i].isascii() or _is_sign_or_dash(text[i]) or text[i] in ".,:_-" for i in on_page):
        return False
    slashes = [i for i in on_page if alnum[0] < i < alnum[-1] and text[i] == "/"]
    if len(slashes) != 2 or not (_is_ascii_alpha(text[alnum[-2]]) and _is_ascii_alpha(text[alnum[-1]])):
        return False
    year_digits = [i for i in alnum if i < slashes[0]]
    return len(year_digits) == 4 and all(text[i].isdigit() for i in year_digits)


def _edge_char_ok(
    ch: str,
    text: str,
    on_page: Sequence[int],
    alnum: Sequence[int],
    alpha_hits: int,
    *,
    leading: bool = True,
    single_word: bool = True,
    side_corroborated: bool = False,
) -> bool:
    """True when 1-char ASCII edge ``ch`` is a safe corroborated letter or YYYY/NN/XX directive digit."""
    if len(ch) != 1 or not on_page or not alnum or not all(text[i].isascii() for i in on_page):
        return False
    if _has_sign_prefix(text, on_page, alnum):
        return False
    if ch.isdigit():
        return leading and _is_eu_directive_lead_digit(ch, text, on_page, alnum, alpha_hits)
    if not _is_ascii_alpha(ch):
        return False
    word_upper = "".join(text[i] for i in alnum).upper()
    if word_upper in _UNSAFE_BRIDGE_WORDS:
        return False
    if not single_word and not side_corroborated:
        return False
    if single_word and leading and ch.upper() in "AN":
        return False
    if alpha_hits >= 2:
        return True
    return not leading and _is_eu_directive_lead_digit(
        text[alnum[0]], text, on_page, alnum, alpha_hits, min_alpha_hits=1
    )


def _two_letter_gap_ready(text: str, direct: Sequence[bool], miss0: int, miss1: int, anchor: int) -> bool:
    """True when ``miss0, miss1`` are contiguous unbacked ASCII letters adjacent to backed ``anchor``."""
    if not text or not direct or max(miss0, miss1, anchor) - min(miss0, miss1, anchor) != 2:
        return False
    if direct[miss0] or direct[miss1] or not direct[anchor]:
        return False
    return _is_ascii_alpha(text[miss0]) and _is_ascii_alpha(text[miss1]) and _is_ascii_alpha(text[anchor])


def _bridge_two_char_word(
    ctx: _SpanDriftCtx,
    on_page: Sequence[int],
    alnum: Sequence[int],
    direct: Sequence[bool],
    backed: list[bool],
) -> None:
    """Bridge an allowlisted 2-letter ASCII preposition/article in a corroborated multi-word span."""
    text = ctx.span.text
    if not on_page or len(alnum) != 2 or len(on_page) > 3 or not all(text[i].isascii() for i in on_page):
        return
    if _has_sign_prefix(text, on_page, alnum) or alnum[1] - alnum[0] != 1:
        return
    if not (_is_ascii_alpha(text[alnum[0]]) and _is_ascii_alpha(text[alnum[1]])):
        return
    word_upper = (text[alnum[0]] + text[alnum[1]]).upper()
    if word_upper not in _SAFE_TWO_CHAR_WORDS or direct[alnum[0]] == direct[alnum[1]]:
        return
    right_side = not direct[alnum[0]]
    corroborated = ctx.right_corroborated if right_side else ctx.left_corroborated
    edge_idx = on_page[-1] if right_side else on_page[0]
    if corroborated and _overhangs_edge(ctx, edge_idx, right_side=right_side):
        backed[alnum[0] if right_side else alnum[1]] = True


def _bridge_two_char_edge_drift(
    ctx: _SpanDriftCtx,
    on_page: Sequence[int],
    alnum: Sequence[int],
    direct: Sequence[bool],
    backed: list[bool],
) -> None:
    """Bridge a safe 2-letter ASCII word-edge gap in a corroborated multi-word span with 2-glyph overhang."""
    text = ctx.span.text
    if not on_page or len(alnum) < 4 or not all(text[i].isascii() for i in on_page):
        return
    if _has_sign_prefix(text, on_page, alnum):
        return
    word_upper = "".join(text[i] for i in alnum).upper()
    if word_upper in _UNSAFE_BRIDGE_WORDS:
        return
    alnum_hits = sum(1 for i in alnum if direct[i])
    alpha_hits = sum(1 for i in alnum if direct[i] and _is_ascii_alpha(text[i]))
    if alpha_hits < 2 or alnum_hits * 2 < len(alnum):
        return
    lead_pair = (text[alnum[0]] + text[alnum[1]]).upper()
    lead_ready = (
        ctx.right_corroborated
        and lead_pair not in _UNSAFE_TWO_CHAR_PREFIXES
        and _two_letter_gap_ready(text, direct, alnum[0], alnum[1], alnum[2])
    )
    if lead_ready and _overhangs_edge(ctx, on_page[-1], right_side=True, min_chars=2):
        backed[alnum[0]] = True
        backed[alnum[1]] = True
    trail_pair = (text[alnum[-2]] + text[alnum[-1]]).upper()
    trail_ready = (
        ctx.left_corroborated
        and trail_pair not in _UNSAFE_TWO_CHAR_SUFFIXES
        and _two_letter_gap_ready(text, direct, alnum[-2], alnum[-1], alnum[-3])
    )
    if trail_ready and _overhangs_edge(ctx, on_page[0], right_side=False, min_chars=2):
        backed[alnum[-2]] = True
        backed[alnum[-1]] = True


def _span_word_slices(span: RawSpan, width: float, height: float) -> tuple[list[int], ...]:
    """Non-empty on-page index slices for each whitespace-separated token of ``span`` in O(N)."""
    n = len(span.text)
    if n == 0 or width <= 0.0 or height <= 0.0:
        return ()
    words: list[list[int]] = []
    pos = 0
    while pos < n:
        if span.text[pos].isspace():
            pos += 1
            continue
        end = pos + 1
        while end < n and not span.text[end].isspace():
            end += 1
        on_page = [i for i in range(pos, end) if not off_page(span.char_boxes[i], width, height)]
        if on_page:
            words.append(on_page)
        pos = end
    return tuple(words)


def _multi_word_span_qualifies(text: str, words: Sequence[Sequence[int]], direct: Sequence[bool]) -> bool:
    """True when a multi-word on-page 3 Tr span has >= 8 alnum chars, >= 70% direct hits, and >= 2 hit words."""
    if len(words) < _MIN_MULTI_WORD_COUNT or not text or not direct:
        return False
    alnum_total = 0
    alnum_hits = 0
    words_with_hits = 0
    for word in words:
        word_alnum = [i for i in word if text[i].isalnum()]
        hits = sum(1 for i in word_alnum if direct[i])
        alnum_total += len(word_alnum)
        alnum_hits += hits
        if hits > 0:
            words_with_hits += 1
    if alnum_total < _MIN_MULTI_SPAN_ALNUM or words_with_hits < _MIN_MULTI_WORD_COUNT:
        return False
    return alnum_hits * _MULTI_SPAN_HIT_DEN >= _MIN_MULTI_SPAN_HIT_NUM * alnum_total


def _word_drifts_toward(
    ctx: _SpanDriftCtx, on_page: Sequence[int], direct: Sequence[bool], *, right_side: bool
) -> bool:
    """True when ``on_page`` has direct alnum hits, misses its opposite edge, and overhangs ``right_side``."""
    text = ctx.span.text
    if not on_page or not direct:
        return False
    alnum = [i for i in on_page if text[i].isalnum()]
    if len(alnum) < 2 or not any(direct[i] for i in alnum):
        return False
    missed_edge = not direct[alnum[0]] if right_side else not direct[alnum[-1]]
    if not missed_edge:
        return False
    two_missed = len(alnum) >= 4 and (not direct[alnum[1]] if right_side else not direct[alnum[-2]])
    edge_idx = on_page[-1] if right_side else on_page[0]
    return _overhangs_edge(ctx, edge_idx, right_side=right_side, min_chars=2 if two_missed else 1)


def _with_span_corroboration(
    base_ctx: _SpanDriftCtx, words: Sequence[Sequence[int]], direct: Sequence[bool]
) -> _SpanDriftCtx:
    """Populate multi-word directional drift corroboration (>= 2 words and unidirectional) onto ``base_ctx``."""
    if len(words) < _MIN_MULTI_WORD_COUNT or not direct:
        return base_ctx
    right_n = sum(1 for w in words if _word_drifts_toward(base_ctx, w, direct, right_side=True))
    left_n = sum(1 for w in words if _word_drifts_toward(base_ctx, w, direct, right_side=False))
    right_ok = right_n >= _MIN_MULTI_WORD_COUNT and left_n == 0 and right_n * 2 >= len(words)
    left_ok = left_n >= _MIN_MULTI_WORD_COUNT and right_n == 0 and left_n * 2 >= len(words)
    return base_ctx._replace(single_word=False, right_corroborated=right_ok, left_corroborated=left_ok)


def _bridge_single_word_edges(
    ctx: _SpanDriftCtx,
    on_page: Sequence[int],
    alnum: Sequence[int],
    direct: Sequence[bool],
    backed: list[bool],
) -> None:
    """Bridge interior <= 2-char gaps first, then overhang-proven 1-char word edges on >= 60%-backed tokens."""
    if not on_page or len(alnum) < 3:
        return
    text = ctx.span.text
    alpha_n = sum(1 for i in alnum if direct[i] and _is_ascii_alpha(text[i]))
    ch0, ch1 = text[alnum[0]], text[alnum[-1]]
    lead_cand = (
        not direct[alnum[0]]
        and direct[alnum[1]]
        and _edge_char_ok(
            ch0,
            text,
            on_page,
            alnum,
            alpha_n,
            leading=True,
            single_word=ctx.single_word,
            side_corroborated=ctx.right_corroborated,
        )
    )
    lead_ok = lead_cand and _overhangs_edge(ctx, on_page[-1], right_side=True)
    trail_cand = (
        not direct[alnum[-1]]
        and direct[alnum[-2]]
        and _edge_char_ok(
            ch1,
            text,
            on_page,
            alnum,
            alpha_n,
            leading=False,
            single_word=ctx.single_word,
            side_corroborated=ctx.left_corroborated,
        )
    )
    trail_ok = trail_cand and _overhangs_edge(ctx, on_page[0], right_side=False)
    _bridge_short_drift_gaps(text, on_page, backed, alpha_n)
    if lead_ok:
        backed[alnum[0]] = True
    if trail_ok:
        backed[alnum[-1]] = True


def _mark_word_backing(
    ctx: _SpanDriftCtx,
    on_page: Sequence[int],
    direct: Sequence[bool],
    backed: list[bool],
    *,
    multi_word_ok: bool = False,
) -> None:
    """Bridge single-word (>= 60%) and vetted multi-word-span SHX drift gaps onto ``backed``."""
    if not on_page or not direct:
        return
    alnum = [i for i in on_page if ctx.span.text[i].isalnum()]
    alnum_hits = sum(1 for i in alnum if direct[i])
    if alnum_hits == 0:
        return
    if len(alnum) >= 3 and alnum_hits >= 2 and alnum_hits * 5 >= 3 * len(alnum):
        _bridge_single_word_edges(ctx, on_page, alnum, direct, backed)
    if multi_word_ok:
        _bridge_two_char_word(ctx, on_page, alnum, direct, backed)
        _bridge_two_char_edge_drift(ctx, on_page, alnum, direct, backed)


def _propagate_internal_spaces(span: RawSpan, width: float, height: float, backed: Sequence[bool]) -> tuple[bool, ...]:
    """Mark on-page spaces visible strictly when both immediate non-space neighbours in ``span`` are backed."""
    n = len(span.text)
    if n == 0 or not any(backed):
        return (False,) * n
    left_ok = [False] * n
    last = False
    for idx, ch in enumerate(span.text):
        if not ch.isspace():
            last = backed[idx]
        left_ok[idx] = last
    right_ok = [False] * n
    last = False
    for idx in range(n - 1, -1, -1):
        if not span.text[idx].isspace():
            last = backed[idx]
        right_ok[idx] = last
    return tuple(
        backed[i]
        or (span.text[i].isspace() and not off_page(span.char_boxes[i], width, height) and left_ok[i] and right_ok[i])
        for i in range(n)
    )


def _char_vector_flags(
    span: RawSpan,
    width: float,
    height: float,
    bands: tuple[_GlyphBand, ...],
    budget: list[int],
    tr3_bands: tuple[_GlyphBand, ...] = (),
) -> tuple[bool, ...]:
    """Word-aware vector-stroke backing flags; internal spaces stay visible only between backed neighbours."""
    n = len(span.text)
    if span.render_mode != _INVISIBLE_TEXT_MODE or not bands or n == 0:
        return (False,) * n
    words = _span_word_slices(span, width, height)
    if not words:
        return (False,) * n
    backed = [False] * n
    for on_page in words:
        for idx in on_page:
            backed[idx] = _char_has_glyph_stroke(span.text[idx], span.char_boxes[idx], bands, height, budget)
    direct = tuple(backed)
    if not any(direct):
        return direct
    base_ctx = _SpanDriftCtx(span=span, bands=bands, tr3_bands=tr3_bands, height=height, budget=budget)
    ctx = _with_span_corroboration(base_ctx, words, direct)
    multi_word_ok = _multi_word_span_qualifies(span.text, words, direct)
    for on_page in words:
        _mark_word_backing(ctx, on_page, direct, backed, multi_word_ok=multi_word_ok)
    return _propagate_internal_spaces(span, width, height, backed)


def _classify(
    spans: tuple[RawSpan, ...],
    width: float,
    height: float,
    boxes: tuple[Box, ...],
    cover: float,
    v_bands: tuple[_GlyphBand, ...] = (),
) -> tuple[SpanPiece, ...]:
    """:func:`classify_spans` of checked input, given raster ``cover`` and banded vector glyph boxes."""
    raster = boxes if cover > _OCR_LAYER_COVER else ()
    budget = [_MAX_VECTOR_TEXT_CHECKS]
    tr3_boxes = (
        tuple(
            sp.char_boxes[i]
            for sp in spans
            if sp.render_mode == _INVISIBLE_TEXT_MODE
            for i, ch in enumerate(sp.text)
            if not ch.isspace() and not off_page(sp.char_boxes[i], width, height)
        )
        if v_bands
        else ()
    )
    tr3_bands = _glyph_bands(tr3_boxes, height) if tr3_boxes else ()
    return tuple(
        piece for span in spans for piece in _span_pieces(span, width, height, raster, v_bands, budget, tr3_bands)
    )


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


def _span_pieces(
    span: RawSpan,
    width: float,
    height: float,
    raster: tuple[Box, ...],
    v_bands: tuple[_GlyphBand, ...] = (),
    budget: list[int] | None = None,
    tr3_bands: tuple[_GlyphBand, ...] = (),
) -> Iterator[SpanPiece]:
    """Yield ``span``'s pieces: maximal runs of chars with equal hidden reasons."""
    no_paint = span.render_mode in _NO_PAINT_MODES
    under = _overlapping(span.bbox, raster) if no_paint else ()
    vec_flags = (
        _char_vector_flags(
            span, width, height, v_bands, budget if budget is not None else [_MAX_VECTOR_TEXT_CHECKS], tr3_bands
        )
        if no_paint and v_bands
        else (False,) * len(span.char_boxes)
    )
    flagged = dict.fromkeys(em_reasons(span.em_width_pt, abs(span.font_size_pt)), True)
    flagged["transparent"] = _transparent(span)
    reasons: list[tuple[HiddenReason, ...]] = []
    for box, v_ok in zip(span.char_boxes, vec_flags, strict=True):
        flagged["off_page"] = off_page(box, width, height)
        flagged["invisible"] = no_paint and not _over_raster(box, under) and not v_ok
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
    pieces: tuple[SpanPiece, ...],
    backdrop: Backdrop,
    scale: tuple[float, float],
    *,
    selected: tuple[bool, ...] | None = None,
) -> tuple[SpanPiece, ...]:
    """:func:`flag_background` of checked input, skipping unselected pieces when ``selected`` is given."""
    if selected is None:
        return tuple(_background_flagged(piece, backdrop, scale) for piece in pieces)
    return tuple(
        _background_flagged(piece, backdrop, scale) if is_selected else piece
        for piece, is_selected in zip(pieces, selected, strict=True)
    )


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
    if columns.step == 1 and len(columns) > 1:
        width_bytes = len(columns) * _BGR_BYTES
        row0 = rows[0] * row_bytes + first
        pixel0 = backdrop.pixels[row0 : row0 + _BGR_BYTES]
        solid_row = pixel0 * len(columns)
        if all(backdrop.pixels[r * row_bytes + first : r * row_bytes + first + width_bytes] == solid_row for r in rows):
            return pixel0[0], pixel0[1], pixel0[2]
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
            ``ocr_mean_confidence`` are None. A5's vision route sets
            ``ocr_mean_confidence`` on a page OCR reads; nothing measures
            ``deskew_angle_deg``, which stays None (``PageQuality``: not
            measured).
        route: ``"digital"``, ``"hybrid"`` or ``"vision"``.
        unmapped_regions: One box per maximal run of invalid chars in a
            visible piece, in piece order: where OCR (A5) reads what the
            text layer cannot say. Empty when there is none.
        warnings: Non-fatal notes for the page (``Page.warnings``).
        image_only_reasons: The routing shortfalls the page's visible text
            has, whatever the OCR mode (#470, module notes):
            ``"too_little_text"`` (a count under 20 on a page with paint),
            ``"garbled_text"`` (a ratio under 0.70), both, or ``()`` when its
            text stands. Unique, in ``IMAGE_ONLY_REASONS`` order, and never
            ``"extraction_failed"`` (an assessed page was read). They become
            ``Page.image_only_reasons`` unless OCR reads the page. ``()`` by
            default.
    """

    pieces: tuple[SpanPiece, ...]
    recovered_pieces: tuple[SpanPiece, ...]
    quality: PageQuality
    route: PageRoute
    unmapped_regions: tuple[Box, ...]
    warnings: tuple[str, ...]
    image_only_reasons: tuple[ImageOnlyReason, ...] = ()

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
        check_image_only_reasons(self.image_only_reasons, allowed=_SHORTFALL_REASONS)


def assess_page(
    page: RawPage,
    *,
    ocr: OcrMode = "auto",
    recovered: RecoveredText | None = None,
    background: Backdrop | str | None = None,
    _initial: PageAssessment | None = None,
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
    if _initial is not None and recovered is None:
        pieces: tuple[SpanPiece, ...] = _initial.pieces
        own: tuple[SpanPiece, ...] = ()
    else:
        v_bands = _glyph_bands(page.object_boxes.glyph_stroke_boxes, height)
        pieces = _classify(page.text.spans, width, height, boxes, cover, v_bands)
        own = _recovered_own(recovered, width, height, boxes, cover, v_bands)
    if isinstance(background, Backdrop):
        scale = _scale(background, width, height)
        pieces = _flag_background(pieces, background, scale, selected=_gate_selection(pieces, page))
        own = _flag_background(own, background, scale, selected=_gate_selection(own, page))
    elif background is not None:
        pieces, own = _flag_unchecked(pieces, page), _flag_unchecked(own, page)
    flagged, unchecked = _duplicates_flagged(own, pieces)
    route, warnings, reasons = _route(_visible_text(pieces), cover, _painted(page), ocr, pieces=pieces)
    warnings += _recovery_warnings(recovered, page.text.stats.dropped_text_objects)
    return PageAssessment(
        pieces=pieces,
        recovered_pieces=flagged,
        quality=_quality(page, pieces + flagged, _initial=_initial),
        route=route,
        unmapped_regions=_unmapped_regions(pieces),
        warnings=warnings + _duplicate_warnings(unchecked) + _background_warnings(background),
        image_only_reasons=reasons,
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
    white, or overlaps (with area) any non-text page object of ``page``
    (A2's ``object_boxes``: paths, images, shadings, in forms or not), or when an
    object's box cannot be read (fail closed). Cost: O(objects) to band the
    boxes, then O(pieces x boxes in their bands), within MAX_GATE_CHECKS.

    Raises:
        TypeError: If ``page`` is not a RawPage or ``assessment`` not a PageAssessment.
    """
    if not isinstance(page, RawPage):
        raise TypeError(f"page must be a RawPage, got {type(page).__name__}")
    if not isinstance(assessment, PageAssessment):
        raise TypeError(f"assessment must be a PageAssessment, got {type(assessment).__name__}")
    return any(_gate_selection((*assessment.pieces, *assessment.recovered_pieces), page))


def _gate_selection(pieces: tuple[SpanPiece, ...], page: RawPage) -> tuple[bool, ...]:
    """For each piece, whether the gate selects it: visible, painting, and near white or over paint (module notes)."""
    found, height = page.object_boxes, page.geometry.height_pt
    bands = _banded(found.boxes, height)
    budget = MAX_GATE_CHECKS
    selected: list[bool] = []
    for piece in pieces:  # bounded: the pieces; their box checks, by the budget
        if piece.hidden_reasons or piece.span.render_mode in _NO_PAINT_MODES:
            selected.append(False)
        elif found.unboxed or _near(_paint_rgb(piece.span), _WHITE_RGB):
            selected.append(True)
        else:
            over, budget = _over_paint(piece.bbox, bands, height, budget)
            selected.append(over)
    return tuple(selected)


def _banded(boxes: tuple[Box, ...], height: float) -> tuple[tuple[Box, ...], ...]:
    """``boxes`` by the bands of the page each one meets; a box past an edge is in the edge's band."""
    bands: list[list[Box]] = [[] for _ in range(_GATE_BANDS)]
    for box in boxes:  # bounded: the boxes, each in at most _GATE_BANDS bands
        for band in _bands_of(box[1], box[3], height):
            bands[band].append(box)
    return tuple(tuple(band) for band in bands)


def _bands_of(top: float, bottom: float, height: float) -> range:
    """The bands a span from ``top`` to ``bottom`` down a page ``height`` tall meets, clamped to the page's."""
    first = min(_GATE_BANDS - 1, max(0, int(top / height * _GATE_BANDS)))
    last = min(_GATE_BANDS - 1, max(0, int(bottom / height * _GATE_BANDS)))
    return range(first, last + 1)


def _over_paint(box: Box, bands: tuple[tuple[Box, ...], ...], height: float, budget: int) -> tuple[bool, int]:
    """Whether ``box`` shares area with a box in the bands it meets, and the budget left; past the budget, True."""
    x0, y0, x1, y1 = box
    for band in _bands_of(y0, y1, height):
        for cover in bands[band]:
            budget -= 1
            if budget < 0 or (cover[0] < x1 and x0 < cover[2] and cover[1] < y1 and y0 < cover[3]):
                return True, budget
    return False, budget


def _flag_unchecked(pieces: tuple[SpanPiece, ...], page: RawPage) -> tuple[SpanPiece, ...]:
    """``pieces``, each one the gate selects flagged ``unchecked_background``: its background could not be checked."""
    return tuple(
        SpanPiece(span=piece.span, start=piece.start, end=piece.end, hidden_reasons=_UNCHECKED_BACKGROUND)
        if selected
        else piece
        for piece, selected in zip(pieces, _gate_selection(pieces, page), strict=True)
    )


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
    recovered: RecoveredText | None,
    width: float,
    height: float,
    boxes: tuple[Box, ...],
    cover: float,
    v_bands: tuple[_GlyphBand, ...] = (),
) -> tuple[SpanPiece, ...]:
    """The recovered spans' pieces, in recovery order, each with its recovered reasons and the computed ones.

    Reasons are in canonical order. Empty without a recovery.
    """
    if recovered is None:
        return ()
    found: list[SpanPiece] = []
    for item in recovered.spans:
        for piece in _classify((item.span,), width, height, boxes, cover, v_bands):
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


def _quality(page: RawPage, pieces: tuple[SpanPiece, ...], *, _initial: PageAssessment | None = None) -> PageQuality:
    """The page's quality signals; every span char, hidden or not, counts toward the ratio.

    ``pieces`` are the page's pieces, then the recovered ones: each span with
    a flagged piece counts once toward ``hidden_spans_flagged``.
    """
    if _initial is not None:
        ratio, count = _initial.quality.valid_char_ratio, _initial.quality.char_count
    else:
        spans = page.text.spans
        count = sum(len(span.text) for span in spans)
        valid = sum(1 for span in spans for offset in range(len(span.text)) if _valid(span, offset))
        ratio = valid / count if count else None
    return PageQuality(
        valid_char_ratio=ratio,
        char_count=count,
        vector_line_count=len(page.segments.segments),
        hidden_spans_flagged=_spans_flagged(pieces),
        rotation_deg=page.geometry.rotation_deg,
        deskew_angle_deg=None,
        ocr_mean_confidence=None,
    )


def _spans_flagged(pieces: tuple[SpanPiece, ...]) -> int:
    """How many spans have a flagged piece among ``pieces``, cut as :func:`classify_spans` cuts them (#456).

    A span's pieces are consecutive and its first starts at offset 0, so a
    span cut into several flagged pieces (off both side edges) counts once,
    and two spans of equal text count twice, as ``char_count`` counts them.
    O(pieces).
    """
    count, counted = 0, False
    for piece in pieces:  # bounded: the pieces
        counted = counted and piece.start > 0  # a piece at offset 0 begins the next span
        if piece.hidden_reasons and not counted:
            count, counted = count + 1, True
    return count


class _VisibleText(NamedTuple):
    """What routing reads of a page's visible chars (those in a piece with no reason).

    Attributes:
        chars: The visible chars outside leader runs: the ratio's denominator.
        valid: The valid ones, whitespace included (the spec counts spaces valid): the ratio's numerator.
        counted: The valid ones that are not whitespace: the routing count.
    """

    chars: int
    valid: int
    counted: int


def _visible_text(pieces: tuple[SpanPiece, ...]) -> _VisibleText:
    """The visible chars of ``pieces`` outside leader runs, the valid ones, and the valid non-whitespace ones.

    A leader run is a maximal run, within a piece, of :data:`_MIN_LEADER_RUN`
    or more identical invalid chars (see the module notes). O(chars).
    """
    chars = valid = counted = 0
    for piece in pieces:  # bounded: the pieces, then each one's runs of equal chars
        if piece.hidden_reasons:
            continue
        span = piece.span
        runs = groupby(range(piece.start, piece.end), key=lambda offset: (span.text[offset], _valid(span, offset)))
        for (char, is_valid), run in runs:
            length = sum(1 for _ in run)
            if is_valid:
                chars, valid = chars + length, valid + length
                counted += 0 if char.isspace() else length
            elif length < _MIN_LEADER_RUN:
                chars += length
    return _VisibleText(chars=chars, valid=valid, counted=counted)


def _painted(page: RawPage) -> bool:
    """Whether the page holds paint OCR could read: an image or vector paint with area on the visible page.

    An image or object whose place A2 could not read (a matrix that is not
    finite, bounds pdfium cannot give) counts: fail closed (module notes).
    """
    images, objects = page.images.stats, page.object_boxes
    if images.max_cover_fraction > 0.0 or images.skipped_nonfinite_images or objects.unboxed:
        return True
    width, height = page.geometry.width_pt, page.geometry.height_pt
    return any(max(x0, 0.0) < min(x1, width) and max(y0, 0.0) < min(y1, height) for x0, y0, x1, y1 in objects.boxes)


class _Routing(NamedTuple):
    """A page's route, its warnings, and its image-only reasons: the shortfalls of its visible text (#470)."""

    route: PageRoute
    warnings: tuple[str, ...]
    image_only_reasons: tuple[ImageOnlyReason, ...]


class _Shortfall(NamedTuple):
    """One reason a page's visible text cannot stand, and how its ``ocr='never'`` warning says it."""

    reason: ImageOnlyReason
    note: str


def _route(
    text: _VisibleText,
    cover: float,
    painted: bool,
    ocr: OcrMode,
    *,
    pieces: tuple[SpanPiece, ...] = (),
) -> _Routing:
    """The page's route, warnings and image-only reasons, from its visible text, raster cover and paint."""
    shortfalls = _shortfalls(text, painted, pieces=pieces, cover=cover)
    reasons = tuple(shortfall.reason for shortfall in shortfalls)
    if ocr == "always":
        return _Routing("vision", (), reasons)
    if shortfalls:
        if ocr == "never":
            short = " and ".join(shortfall.note for shortfall in shortfalls)
            warning = f"ocr='never': the page has {short}; it stays digital instead of going to OCR"
            return _Routing("digital", (warning,), reasons)
        return _Routing("vision", (), reasons)
    # hybrid marks a text layer over a scan; under "never" such a page is labelled digital.
    return _Routing("hybrid" if cover > _OCR_LAYER_COVER and ocr != "never" else "digital", (), ())


def _is_lexical_char(char: str) -> bool:
    """True if ``char`` is a letter, number, mark, or format joiner in any Unicode script."""
    cat = unicodedata.category(char)
    return cat[0] in ("L", "N", "M") or cat == "Cf"


def _is_intraword_noise_token(token: str) -> bool:
    """True when ``token`` mixes alphanumeric characters with intraword OCR-noise symbols."""
    if len(token) < 2 or not any(ch.isalnum() for ch in token):
        return False
    return any(ch in _INTRAWORD_NOISE_CHARS for ch in token) or "!" in token[:-1]


def _scan_mojibake_note(pieces: tuple[SpanPiece, ...]) -> str | None:
    """Return a shortfall note when visible text over a >50 % raster scan is ASCII-Mojibake noise."""
    lexical = nonspace = 0
    tokens: list[str] = []
    for piece in pieces:
        if piece.hidden_reasons:
            continue
        span = piece.span
        valid_chars: list[str] = []
        runs = groupby(range(piece.start, piece.end), key=lambda offset: (span.text[offset], _valid(span, offset)))
        for (char, is_valid), run in runs:
            if not is_valid:
                continue
            length = sum(1 for _ in run)
            valid_chars.append(char * length)
            if char.isspace() or (char in _SCAN_LEADER_CHARS and length >= _MIN_LEADER_RUN):
                continue
            nonspace += length
            if _is_lexical_char(char):
                lexical += length
        tokens.extend("".join(valid_chars).split())
    if nonspace >= _MIN_TEXT_CHARS and lexical / nonspace < _MIN_SCAN_LEXICAL_RATIO:
        ratio = f"0.{lexical * 1000 // nonspace:03d}"
        return (
            f"{lexical} lexical chars of {nonspace} non-whitespace scan chars, "
            f"a lexical ratio of {ratio} (under {_MIN_SCAN_LEXICAL_RATIO:.2f})"
        )
    if len(tokens) >= _MIN_SCAN_TOKENS:
        noisy = sum(1 for tok in tokens if _is_intraword_noise_token(tok))
        if noisy / len(tokens) > _MAX_SCAN_NOISE_TOKEN_RATIO:
            return f"{noisy} of {len(tokens)} scan OCR tokens carry intraword symbol noise"
    return None


def _shortfalls(
    text: _VisibleText,
    painted: bool,
    *,
    pieces: tuple[SpanPiece, ...] = (),
    cover: float = 0.0,
) -> tuple[_Shortfall, ...]:
    """Why the page's visible text cannot stand: too few valid chars, garbled text, or both; else empty."""
    found: list[_Shortfall] = []
    if text.counted < _MIN_TEXT_CHARS and painted:
        few = f"{text.counted} valid, visible, non-whitespace chars (under {_MIN_TEXT_CHARS})"
        found.append(_Shortfall("too_little_text", few))
    if text.chars and text.valid / text.chars < _MIN_VALID_RATIO:
        ratio = f"0.{text.valid * 1000 // text.chars:03d}"
        share = f"{text.valid} valid chars of its {text.chars} visible ones (leader runs aside)"
        found.append(_Shortfall("garbled_text", f"{share}, a ratio of {ratio} (under {_MIN_VALID_RATIO:.2f})"))
    elif cover > _OCR_LAYER_COVER and text.counted >= _MIN_TEXT_CHARS and pieces:
        mojibake = _scan_mojibake_note(pieces)
        if mojibake is not None:
            found.append(_Shortfall("garbled_text", mojibake))
    return tuple(found)


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
