"""Recovery of the text pdfium's textpage drops, for the v5 pdfium engine (#366, A3 part 3).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``;
:mod:`datagrunt.pdf.pdfium_engine` re-exports :class:`RecoveredSpan`,
:class:`RecoveryStats` and :class:`RecoveredText`, and runs a recovery on a
page load of its own: it walks the page, reads a textpage
(:func:`~datagrunt.pdf._pdfium_text.collect_text`, which asks
:func:`is_glyphless` of each text object that owns no char), hands the
dropped text objects to :func:`isolate_dropped`, reads a fresh textpage and
hands both reads to :func:`recovered_text`. This module never imports the
engine.

pdfium drops a text object whose box is under 0.01 pt wide (``Tf 0``, ``Tz
0``, a tiny size, a single space) and a copy of identical text drawn within
its duplicate window of one of the five text objects before it (see
:mod:`datagrunt.pdf._pdfium_text`). The object still exists and keeps its
chars, but owns no textpage char, and ``FPDFTextObj_GetText`` (served from
the textpage) reads it as empty.

An object whose box is the point at its origin though its em is 1 unit or
more each way (in its form's space, where pdfium measures its box) draws no
ink: none of its glyphs has an outline. It is an object of a single space,
or of spaces that char spacing draws on one spot (a new size would spread
them apart, still drawing nothing). It is empty, not dropped, and never
recovered. A point box from a tiny or flat em (``Tf 0``, a zero matrix) is
dropped: laid out again, it may draw; if its box is still a point then, it
is empty too.
:func:`isolate_dropped` gives each such object a 12 pt size and an unscaled
matrix (size first: pdfium lays an object out again on a new matrix, not on
a new size) at a place of its own, on a diagonal below the page, so no copy
lies on another text; a fresh textpage then reads its text. The edits live
only in that page load: nothing is saved.

Recovered text keeps where the object was: every char box of its span is the
object's original box (a point for ``Tf 0`` text, a line with no width for
``Tz 0`` text) and its font size and em width are the original ones. Its
hidden reasons (:func:`em_reasons`) are measured on the original object's
text matrix to page space (``[a b c d]``) and ``Tf`` size
(:func:`~datagrunt.pdf._pdfium_text.em_size`), the rule
:mod:`datagrunt.pdf.quality` flags a textpage span with:

- ``tiny_font``: the em's height, measured across its baseline, is under
  1 pt (0 included): a tiny size, or a matrix that lays the em's sides
  along (almost) one line (``0 1 0 1 Tm``), however long they are.
- ``zero_scale``: the em's width, measured across its height side, is under
  1 pt and under its height: the matrix squeezes it, ``hypot(a, b) <
  hypot(c, d)`` (``Tz 0``; the textpage keeps ``Tz 0.05`` text, which this
  rule flags all the same).

Text drawn under 1 pt per em wide is always one or the other. A recovered
object whose em is wider (a copy pdfium dropped as a duplicate, emitted as
below) has neither reason: it is drawn.

A recovered copy of text already on the page (owned, or recovered before it)
is an overprint, counted and not emitted, only when it would show as that
text does and lies on it, so no text a reader sees is counted away and no
hidden copy hides behind a visible one:

- It looks the same: the same render mode, fill and stroke colours and
  alphas, em reasons and placement reasons (``off_page``: both within the
  page, or both not; :func:`off_page`).
- Its origin lies within that text's duplicate window on each displayed
  axis: 0.9 of the size of its last char's box (the glyph's advance by the
  font's height), pdfium's share, at least 0.5 pt and at most 12 pt.

That window holds pdfium's for 12 pt text no matrix shrinks, so drop
shadows and fake-bold copies are counted. Otherwise the copy is emitted:
pdfium keeps the first of two copies whatever their look, so a copy that
paints when the first does not (``3 Tr`` then ``0 Tr``, alpha 0 then
opaque, white then black) is what a reader sees; pdfium's window does not
shrink with the text (Tf 100 under ``0.1 0 0 0.1 0 0 cm`` drops a copy 10
pt from a 2.2 pt "l") and grows with what the font says its glyphs are (a
5 em glyph: 54 pt), so a copy past recovery's window is emitted unflagged.
"""

import ctypes
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from typing import NamedTuple

import pypdfium2.raw as pdfium_c

from datagrunt.pdf._path_segments import Matrix
from datagrunt.pdf._pdfium_common import Box, PdfiumFailure, object_address, union_box
from datagrunt.pdf._pdfium_text import PageText, RawSpan, em_size
from datagrunt.pdf._validate import int_at_least, typed_tuple
from datagrunt.pdf.elements import BBox, HiddenReason, check_hidden_reasons

RECOVERY_REASONS: tuple[HiddenReason, ...] = ("tiny_font", "zero_scale")
# A font size (or an em's width) under this many points is too small to read.
TINY_FONT_PT = 1.0
# The least duplicate window (each axis) of text whose last char's box is a point or a line (Tf 0, Tz 0).
OVERPRINT_TOLERANCE_PT = 0.5
# The most: pdfium's window for 12 pt text ending in Helvetica's widest glyph (W) is 10.2 pt. A copy
# further away is emitted, however wide the font says its glyphs are.
DUPLICATE_REACH_CAP_PT = 12.0
# pdfium's window along the text is this share of the last glyph's advance.
_PDFIUM_SHARE = 0.9
# The size each dropped object is drawn at to be read, and the spacing of the places it is moved to.
_READ_SIZE_PT = 12.0
_PLACE_SPACING = 100.0


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveredSpan:
    """Text pdfium's textpage dropped, read back and placed where its text object is.

    Attributes:
        span: The recovered chars, in textpage order (generated breaks
            kept), with the object's style. Every char box is the object's
            original displayed box (a degenerate one for ``Tf 0`` or ``Tz 0``
            text), and ``font_size_pt`` and ``em_width_pt`` are its
            original size and em width.
        hidden_reasons: ``tiny_font`` and / or ``zero_scale`` (see the module
            notes), in A1's canonical order; empty for a drawn copy.
    """

    span: RawSpan
    hidden_reasons: tuple[HiddenReason, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.span, RawSpan):
            raise TypeError(f"span must be a RawSpan, got {type(self.span).__name__}")
        check_hidden_reasons(self.hidden_reasons)
        if not set(self.hidden_reasons) <= set(RECOVERY_REASONS):
            drawn = f"must be drawn from {RECOVERY_REASONS}, got {self.hidden_reasons}"
            raise ValueError(f"hidden_reasons of a recovered span {drawn}")
        bbox = self.span.bbox
        for index, box in enumerate(self.span.char_boxes):
            if box != bbox:
                where = f"char_boxes[{index}] of a recovered span must be its bbox {bbox!r}"
                raise ValueError(f"{where} (the text object's box), got {box!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveryStats:
    """The dropped text objects a recovery gives no span for.

    Attributes:
        overprint_duplicates: Objects whose recovered text equals an earlier
            text object's (owned or recovered) that looks the same (render
            mode, fill and stroke colours and alphas, em and placement
            reasons), at an origin within its duplicate window (0.9 of its
            last char box's size on each displayed axis, 0.5 to 12 pt; see
            the module notes): a copy drawn on it (a drop shadow, fake
            bold), counted and not emitted. Any other copy is emitted.
        whitespace_objects: Objects whose recovered text is whitespace only.
        empty_text_objects: Objects whose box is still a point once laid out
            again at 12 pt: they draw no glyph with extent (a single space
            at ``Tf 0``). Counted, never a warning.
        unrecovered_text_objects: Objects with extent that still own no
            textpage char, or that pdfium places at a non-finite position:
            their text is lost, and the caller warns.
    """

    overprint_duplicates: int
    whitespace_objects: int
    empty_text_objects: int
    unrecovered_text_objects: int

    def __post_init__(self) -> None:
        int_at_least("overprint_duplicates", self.overprint_duplicates, 0)
        int_at_least("whitespace_objects", self.whitespace_objects, 0)
        int_at_least("empty_text_objects", self.empty_text_objects, 0)
        int_at_least("unrecovered_text_objects", self.unrecovered_text_objects, 0)


@dataclass(frozen=True, slots=True, kw_only=True)
class RecoveredText:
    """One page's recovered spans, in object-walk order, with the objects that gave none.

    Every dropped text object (``TextStats.dropped_text_objects``) is either
    behind one or more spans or counted once in ``stats``. Picklable, so it
    crosses the process pool.
    """

    spans: tuple[RecoveredSpan, ...]
    stats: RecoveryStats

    def __post_init__(self) -> None:
        typed_tuple("spans", self.spans, RecoveredSpan)
        if not isinstance(self.stats, RecoveryStats):
            raise TypeError(f"stats must be a RecoveryStats, got {type(self.stats).__name__}")


class TextObject(NamedTuple):
    """A text object of the page's object walk, as the engine hands it over.

    ``own`` is pdfium's object matrix, into its form's space; ``outer`` maps
    that space to page space (every enclosing form's matrix; identity off a
    form).
    """

    object_index: int
    handle: pdfium_c.FPDF_PAGEOBJECT
    own: Matrix
    outer: Matrix


class Target(NamedTuple):
    """A dropped text object as it was before :func:`isolate_dropped` moved it.

    ``box`` (displayed) and ``origin`` (page space) are None when pdfium
    placed it at a non-finite position or size (its size and em width then
    read 0). ``empty`` is True when its box is still a point once moved and
    laid out again.
    """

    object_index: int
    origin: tuple[float, float] | None
    box: Box | None
    font_size_pt: float
    em_width_pt: float
    hidden_reasons: tuple[HiddenReason, ...]
    empty: bool = False


def is_glyphless(handle: pdfium_c.FPDF_PAGEOBJECT, matrix: Matrix, label: str) -> bool:
    """Whether the text object ``handle`` draws no ink: no glyph with an outline (see the module notes).

    ``matrix`` is its object matrix (into its form's space, where its bounds
    are); its box must be the point at its origin while its em is at least
    :data:`TINY_FONT_PT` each way.

    Raises:
        PdfiumFailure: If a pdfium call fails.
    """
    width, height = em_size(_font_size(handle, label), matrix.a, matrix.b, matrix.c, matrix.d)
    return width >= TINY_FONT_PT and height >= TINY_FONT_PT and _is_point(_bounds(handle, label))


def _font_size(handle: pdfium_c.FPDF_PAGEOBJECT, label: str) -> float:
    """The text object's ``Tf`` size."""
    size = ctypes.c_float()
    if not pdfium_c.FPDFTextObj_GetFontSize(handle, size):
        raise PdfiumFailure(f"read the font size of {label}")
    return size.value


def _bounds(handle: pdfium_c.FPDF_PAGEOBJECT, label: str) -> Box:
    """The object's box ``(left, bottom, right, top)`` in its form's space."""
    left, bottom, right, top = (ctypes.c_float() for _ in range(4))
    if not pdfium_c.FPDFPageObj_GetBounds(handle, left, bottom, right, top):
        raise PdfiumFailure(f"read the bounds of {label}")
    return left.value, bottom.value, right.value, top.value


def _is_point(box: Box) -> bool:
    """True if ``box`` has no width and no height (pdfium's box of an object with no glyph extent)."""
    left, bottom, right, top = box
    return left == right and bottom == top


def isolate_dropped(
    texts: Iterable[TextObject], dropped: frozenset[int], to_display: Callable[[Box], Box]
) -> tuple[Target, ...]:
    """Read each dropped object's original place and size, then move it where a textpage reads it.

    Bounded by the page's text objects; each dropped one gets the next place
    on the diagonal ``(100 k, -100 (k + 1))`` of its form's space.

    Raises:
        PdfiumFailure: If a pdfium call fails.
    """
    targets: list[Target] = []
    for text in texts:
        if object_address(text.handle) not in dropped:
            continue
        target = _original(text, to_display)
        if target.box is not None:
            _move(text, len(targets))
            target = target._replace(empty=_is_point(_bounds(text.handle, f"object {text.object_index}")))
        targets.append(target)
    return tuple(targets)


def _original(text: TextObject, to_display: Callable[[Box], Box]) -> Target:
    """The dropped object ``text`` as pdfium placed it: its displayed box, origin, size, em width and reasons."""
    label = f"object {text.object_index}"
    size = _font_size(text.handle, label)
    left, bottom, right, top = _bounds(text.handle, label)
    matrix = text.own.then(text.outer)
    # Bounds are in the form's space, as the object matrix is.
    corners = [text.outer.apply(x, y) for x in (left, right) for y in (bottom, top)]
    width, height = em_size(size, matrix.a, matrix.b, matrix.c, matrix.d)
    edges = (edge for corner in corners for edge in corner)
    if not all(math.isfinite(value) for value in (*matrix, width, height, *edges)):
        return Target(
            object_index=text.object_index, origin=None, box=None, font_size_pt=0.0, em_width_pt=0.0, hidden_reasons=()
        )
    xs, ys = [corner[0] for corner in corners], [corner[1] for corner in corners]
    return Target(
        object_index=text.object_index,
        origin=(matrix.e, matrix.f),
        box=to_display((min(xs), min(ys), max(xs), max(ys))),
        font_size_pt=math.copysign(height, size),
        em_width_pt=width,
        hidden_reasons=em_reasons(width, height),
    )


def em_reasons(width_pt: float, height_pt: float) -> tuple[HiddenReason, ...]:
    """``tiny_font`` and / or ``zero_scale`` for text whose em is ``width_pt`` by ``height_pt`` (see the module notes).

    The one rule, and :data:`TINY_FONT_PT` the one threshold, for text pdfium
    drops (recovery) and text its textpage keeps (:mod:`datagrunt.pdf.quality`
    flags a span by its ``em_width_pt`` and ``|font_size_pt|``). Both sides
    are magnitudes (:func:`~datagrunt.pdf._pdfium_text.em_size`).
    """
    tiny = height_pt < TINY_FONT_PT
    # A narrower em than a taller one: the matrix (Tz) squeezes it, not the size.
    squeezed = width_pt < height_pt and width_pt < TINY_FONT_PT
    return tuple(reason for reason, flagged in zip(RECOVERY_REASONS, (tiny, squeezed)) if flagged)


def off_page(box: Box, width: float, height: float) -> bool:
    """True if the displayed ``box`` is not within the ``width`` x ``height`` page by A1's ``BBox.within`` rule.

    The one placement rule for recovered copies (the overprint rule) and
    textpage chars (:mod:`datagrunt.pdf.quality` flags ``off_page`` by it).
    """
    x0, y0, x1, y1 = box
    return not BBox(x0=x0, y0=y0, x1=x1, y1=y1).within(width, height)


def _move(text: TextObject, place: int) -> None:
    """Give ``text`` the read size and an unscaled matrix at diagonal place ``place`` of its form's space."""
    label = f"object {text.object_index}"
    if not pdfium_c.FPDFTextObj_SetFontSize(text.handle, ctypes.c_float(_READ_SIZE_PT)):
        raise PdfiumFailure(f"set the font size of {label}")
    # A new matrix makes pdfium lay the object out again (a new size alone does not), so it comes second.
    spot = pdfium_c.FS_MATRIX(1.0, 0.0, 0.0, 1.0, _PLACE_SPACING * place, -_PLACE_SPACING * (place + 1))
    if not pdfium_c.FPDFPageObj_SetMatrix(text.handle, spot):
        raise PdfiumFailure(f"set the matrix of {label}")


class Display(NamedTuple):
    """The displayed page, for the overprint rule: the map of page-space boxes onto it, and its size."""

    to_display: Callable[[Box], Box]
    width_pt: float
    height_pt: float


def recovered_text(
    targets: tuple[Target, ...], texts: Iterable[TextObject], before: PageText, after: PageText, display: Display
) -> RecoveredText:
    """The recovered spans and counts, from the textpages read ``before`` and ``after`` the targets moved.

    ``texts`` (every text object of the walk) gives the owned objects'
    origins, and ``display`` places them, for the overprint rule.
    """
    printed = _owned_copies(before.spans, {text.object_index: _origin(text) for text in texts}, display)
    found = _spans_by_object(after.spans, {target.object_index for target in targets})
    spans: list[RecoveredSpan] = []
    overprints = whitespace = empty = unrecovered = 0
    for target in targets:
        pieces = found.get(target.object_index, [])
        if target.empty:
            empty += 1
            continue
        if target.box is None or target.origin is None or not pieces:
            unrecovered += 1
            continue
        text = "".join(piece.text for piece in pieces)
        look = _look(pieces[0], target.hidden_reasons, target.box, display)
        copy = _Copy(_displayed(target.origin, display), _reach(target.box), look)
        if text.isspace():
            whitespace += 1
        elif _overprints(copy, printed.get(text, ())):
            overprints += 1
        else:
            printed.setdefault(text, []).append(copy)
            spans.extend(_placed(piece, target.box, target) for piece in pieces)
    counts = RecoveryStats(
        overprint_duplicates=overprints,
        whitespace_objects=whitespace,
        empty_text_objects=empty,
        unrecovered_text_objects=unrecovered,
    )
    return RecoveredText(spans=tuple(spans), stats=counts)


# How a copy shows: render mode, fill and stroke colours with their alphas, em reasons, placement reasons.
_Look = tuple[
    int, tuple[int, int, int, int], tuple[int, int, int, int], tuple[HiddenReason, ...], tuple[HiddenReason, ...]
]


class _Copy(NamedTuple):
    """A text object already on the page, for the overprint rule.

    ``origin`` is displayed; ``reach`` is its duplicate window on each
    displayed axis (:func:`_reach`); ``look`` is :func:`_look`'s.
    """

    origin: tuple[float, float]
    reach: tuple[float, float]
    look: _Look


def _look(span: RawSpan, reasons: tuple[HiddenReason, ...], box: Box, display: Display) -> _Look:
    """How ``span``'s object shows: its paint, the em ``reasons`` and the place reasons of its displayed ``box``.

    The place reasons are ``off_page`` when ``box`` is not within the page
    (A1's rule), so a copy past an edge never absorbs one on the page.
    """
    placement: tuple[HiddenReason, ...] = ("off_page",) if off_page(box, display.width_pt, display.height_pt) else ()
    return span.render_mode, span.fill_rgba, span.stroke_rgba, reasons, placement


def _reach(box: Box) -> tuple[float, float]:
    """The duplicate window of text whose last char's displayed box is ``box`` (see the module notes).

    pdfium's share of the box's size on each axis, at least the tolerance
    and at most the cap, so no font's glyph box widens it.
    """
    sides = (box[2] - box[0], box[3] - box[1])
    width, height = (min(DUPLICATE_REACH_CAP_PT, max(OVERPRINT_TOLERANCE_PT, _PDFIUM_SHARE * side)) for side in sides)
    return width, height


def _displayed(point: tuple[float, float], display: Display) -> tuple[float, float]:
    """The page-space ``point`` on the displayed page."""
    x0, y0, _x1, _y1 = display.to_display((*point, *point))
    return x0, y0


def _owned_copies(
    spans: tuple[RawSpan, ...], origins: dict[int, tuple[float, float]], display: Display
) -> dict[str, list[_Copy]]:
    """Each owned object's text (its spans' text joined) to the copies drawing it, in walk order.

    ``origins`` are page space; an object with spans has a finite one (the
    text stage skips an object pdfium places nowhere).
    """
    printed: dict[str, list[_Copy]] = {}
    for index, own in _spans_by_object(spans, set(origins)).items():
        first = own[0]
        em = em_reasons(first.em_width_pt, abs(first.font_size_pt))
        look = _look(first, em, union_box(tuple(span.bbox for span in own)), display)
        copy = _Copy(_displayed(origins[index], display), _reach(own[-1].char_boxes[-1]), look)
        printed.setdefault("".join(span.text for span in own), []).append(copy)
    return printed


def _origin(text: TextObject) -> tuple[float, float]:
    """The page-space origin of ``text`` (its matrix's translation, every form's applied)."""
    return text.outer.apply(text.own.e, text.own.f)


def _spans_by_object(spans: tuple[RawSpan, ...], wanted: set[int]) -> dict[int, list[RawSpan]]:
    """The spans of each object in ``wanted``, in order."""
    found: dict[int, list[RawSpan]] = {}
    for span in spans:
        if span.object_index in wanted:
            found.setdefault(span.object_index, []).append(span)
    return found


def _overprints(copy: _Copy, others: Iterable[_Copy]) -> bool:
    """True if ``copy`` shows as one of ``others`` does and lies within its duplicate window on both axes."""
    x, y = copy.origin
    return any(
        other.look == copy.look
        and abs(x - other.origin[0]) <= other.reach[0]
        and abs(y - other.origin[1]) <= other.reach[1]
        for other in others
    )


def _placed(span: RawSpan, box: Box, target: Target) -> RecoveredSpan:
    """``span`` read from the moved object, put back at the target's original ``box``, size and em width."""
    original = replace(
        span,
        bbox=box,
        char_boxes=(box,) * len(span.text),
        font_size_pt=target.font_size_pt,
        em_width_pt=target.em_width_pt,
    )
    return RecoveredSpan(span=original, hidden_reasons=target.hidden_reasons)
