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
0``, a tiny size, a single space) and a copy drawn on identical text. The
object still exists and keeps its chars, but owns no textpage char, and
``FPDFTextObj_GetText`` (served from the textpage) reads it as empty.

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

- ``tiny_font``: the em's height, ``|Tf| x hypot(c, d)``, is under 1 pt
  (0 included).
- ``zero_scale``: the em's width, ``|Tf| x hypot(a, b)``, is under 1 pt and
  the matrix squeezes it, ``hypot(a, b) < hypot(c, d)`` (``Tz 0``; the
  textpage keeps ``Tz 0.05`` text, which this rule flags all the same).

Text drawn under 1 pt per em wide is always one or the other. A recovered
object whose em is wider (a copy pdfium dropped as a duplicate though it lies
over 0.5 pt from the text it copies) has neither reason: it is drawn.
"""

import ctypes
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from typing import NamedTuple

import pypdfium2.raw as pdfium_c

from datagrunt.pdf._path_segments import Matrix
from datagrunt.pdf._pdfium_common import Box, PdfiumFailure, object_address
from datagrunt.pdf._pdfium_text import PageText, RawSpan, em_size
from datagrunt.pdf._validate import int_at_least, typed_tuple
from datagrunt.pdf.elements import HiddenReason, check_hidden_reasons

RECOVERY_REASONS: tuple[HiddenReason, ...] = ("tiny_font", "zero_scale")
# A font size (or an em's width) under this many points is too small to read.
TINY_FONT_PT = 1.0
# Recovered text equal to another text object's at an origin this close (each axis) is an overprint.
OVERPRINT_TOLERANCE_PT = 0.5
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
            text object's (owned or recovered) at an origin within 0.5 pt on
            each axis: a copy drawn on it, counted and not emitted.
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


def _move(text: TextObject, place: int) -> None:
    """Give ``text`` the read size and an unscaled matrix at diagonal place ``place`` of its form's space."""
    label = f"object {text.object_index}"
    if not pdfium_c.FPDFTextObj_SetFontSize(text.handle, ctypes.c_float(_READ_SIZE_PT)):
        raise PdfiumFailure(f"set the font size of {label}")
    # A new matrix makes pdfium lay the object out again (a new size alone does not), so it comes second.
    spot = pdfium_c.FS_MATRIX(1.0, 0.0, 0.0, 1.0, _PLACE_SPACING * place, -_PLACE_SPACING * (place + 1))
    if not pdfium_c.FPDFPageObj_SetMatrix(text.handle, spot):
        raise PdfiumFailure(f"set the matrix of {label}")


def recovered_text(
    targets: tuple[Target, ...], texts: Iterable[TextObject], before: PageText, after: PageText
) -> RecoveredText:
    """The recovered spans and counts, from the textpages read ``before`` and ``after`` the targets moved.

    ``texts`` (every text object of the walk) gives the owned objects'
    origins, for the overprint rule.
    """
    origins = {text.object_index: _origin(text) for text in texts}
    printed: dict[str, list[tuple[float, float]]] = {}
    for index, text in _texts_by_object(before.spans).items():
        printed.setdefault(text, []).append(origins[index])
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
        if text.isspace():
            whitespace += 1
        elif _overprints(target.origin, printed.get(text, ())):
            overprints += 1
        else:
            printed.setdefault(text, []).append(target.origin)
            spans.extend(_placed(piece, target.box, target) for piece in pieces)
    counts = RecoveryStats(
        overprint_duplicates=overprints,
        whitespace_objects=whitespace,
        empty_text_objects=empty,
        unrecovered_text_objects=unrecovered,
    )
    return RecoveredText(spans=tuple(spans), stats=counts)


def _origin(text: TextObject) -> tuple[float, float]:
    """The page-space origin of ``text`` (its matrix's translation, every form's applied)."""
    return text.outer.apply(text.own.e, text.own.f)


def _texts_by_object(spans: tuple[RawSpan, ...]) -> dict[int, str]:
    """Each object's text: the joined text of its spans, in order."""
    texts: dict[int, str] = {}
    for span in spans:
        texts[span.object_index] = texts.get(span.object_index, "") + span.text
    return texts


def _spans_by_object(spans: tuple[RawSpan, ...], wanted: set[int]) -> dict[int, list[RawSpan]]:
    """The spans of each object in ``wanted``, in order."""
    found: dict[int, list[RawSpan]] = {}
    for span in spans:
        if span.object_index in wanted:
            found.setdefault(span.object_index, []).append(span)
    return found


def _overprints(origin: tuple[float, float], others: Iterable[tuple[float, float]]) -> bool:
    """True if ``origin`` is within the overprint tolerance of one of ``others`` on both axes."""
    x, y = origin
    return any(
        abs(x - other_x) <= OVERPRINT_TOLERANCE_PT and abs(y - other_y) <= OVERPRINT_TOLERANCE_PT
        for other_x, other_y in others
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
