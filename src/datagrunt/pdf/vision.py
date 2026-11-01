"""The v5 vision route (#369, A5b): a vision page's elements from OCR of its render.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. It runs no
child process of its own: renders go through
:func:`~datagrunt.pdf.render.render_page` (the guarded, capped worker) and
tesseract through :mod:`datagrunt.pdf.ocr` (its bounded CLI calls).

OCR words become elements through A4b's layout (spec §3.3 step 4: "populate
word boxes into layout.py"). Each :class:`~datagrunt.pdf.ocr.OCRWord` is one
visible piece of a synthetic span of its own (:func:`ocr_pieces`):

- its text, and its box on the displayed page;
- its chars splitting the box evenly along the word, each as tall as it
  (its loose and tight boxes alike: OCR reads no glyph bounds);
- a font size of the box's height in points (at least :data:`LEAST_SIZE_PT`,
  the least size a layout line holds), its em as wide; not bold, not
  italic, painted (render mode 0, opaque black), with no hidden reason.

:func:`vision_elements` lays the pieces out with
:func:`~datagrunt.pdf.layout.page_layout`, with no lattice (a scan has no
vector rules), so a scan yields headings, paragraphs, key-values, lists
and stream tables; each figure is a slot in the reading order, as on any
page. Digital text kept apart (hidden pieces, every one carrying a reason)
reads as hidden paragraphs placed among them. Pass C
(:func:`~datagrunt.pdf.assembly.conservation`) checks every piece's chars
against the elements: a mismatch is a warning.

:func:`vision_page` makes the A1 page of a page A3 routed to vision (spec
§3.3 steps 2 and 4), with one render and one OCR pass:

1. Render: at :func:`~datagrunt.pdf.render.effective_dpi` of the vision
   DPI and the pixel bound, in the guarded worker.
2. Orientation: tesseract's OSD on the normalized render (grey, 1 %
   autocontrast). Its turn is made only at a confidence of
   :data:`ROTATION_CONFIDENCE` or more: on the corpus (95 pages, each
   rendered upright and turned 90, 180 and 270 degrees) every right turn
   had a confidence of 3.59 or more and the one wrong turn 0.02 (a cover
   with almost no text). Under it, or when OSD cannot tell, the page reads
   as displayed, with a warning.
3. OCR: the righted page's bands (900 px, 60 px shared), each word mapped
   back to the displayed page.
4. Elements: :func:`vision_elements` of the words, the page's figures and
   its digital text kept apart, :func:`superseded`: each visible piece is
   flagged ``"superseded"`` (A1), each hidden one keeps its own reasons, so
   nothing is superseded twice and nothing is dropped.
5. The page: ``status="ok"``, ``route="vision"``, never
   ``integrity_verified`` (A1); A3's quality with the words' mean
   confidence; a lazy :class:`~datagrunt.pdf.elements.PageImage` whose
   loaders (:class:`PageRender`) draw the page again on demand, so no pixels
   are held between pages.

When the page is not rendered (``RenderRefused``, ``RenderFailed``) or
tesseract cannot read it (missing, failed, timed out), it is
``"ocr_unavailable"``: :func:`~datagrunt.pdf.assembly.assemble_page`'s
page of its digital text, the page image when it was rendered, and a
warning naming why. Words read before a band failed are not used: the page
is the digital text's, never half of each.

:func:`fallback_page` makes the A1 page of a page A2 could not read (its
``PageFailure``; spec §3.3 step 2, per-page fault isolation): only its
geometry is read (``page_geometry``), then it is rendered and read as a
vision page is, with no digital text and no figures (none was read). Read,
it is ``"ocr_fallback"`` (route ``"vision"``, never verified): its quality
is the OCR's (the words' chars by A3's rule, their mean confidence; no
vector line, hidden span or skew was measured: 0, 0 and None), and its first
warning says extraction failed and what was not read. When its geometry,
render or OCR fails too, or ``ocr="never"``, it is an ``"error"`` page (A1:
no elements, no quality) whose ``error`` names the extraction failure and
then why it was not read; a page whose geometry cannot be read is pdfium's
default 612 x 792 pt, with a warning.

Known limitation: a word's box runs from its highest to its lowest ink, so
its height is the font's x-height for a word such as "over", and its
ascent to descent for "jumps". Lines whose words mostly reach both read
larger than the page's body, so the layout's heading rule (1.15 times the
body size) takes some body lines for headings: on Root's Law (pages 5, 10
and 20) OCR finds 18 to 22 headings where the digital text has 5 to 7.
Sizing each word by its tesseract line's height instead finds as many (14
to 20) and joins words across their spaces (A5b evidence).
"""

import dataclasses
import io
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

from PIL import Image

from datagrunt.pdf._validate import choice_str, int_at_least, non_blank_str, positive_float, typed_tuple
from datagrunt.pdf.assembly import (
    MIN_IMAGE_DIMENSION,
    FigureDraft,
    assemble_page,
    check_assessment,
    conservation,
    page_elements,
    page_figures,
    raw_page_warnings,
)
from datagrunt.pdf.elements import DocumentElement, HiddenReason, Page, PageImage, PageQuality
from datagrunt.pdf.exceptions import PDFCorruptError
from datagrunt.pdf.layout import LatticeResult, find_lattice_tables, page_layout
from datagrunt.pdf.ocr import Band, OCRWord, Orientation, bands, normalized, ocr_words, orientation, rotated
from datagrunt.pdf.pdfium_engine import (
    DocumentHandle,
    PageFailure,
    PageGeometry,
    RawPage,
    RawSpan,
    _checked_password,
    _checked_path,
    page_geometry,
)
from datagrunt.pdf.quality import OcrMode, PageAssessment, SpanPiece, valid_char
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused, effective_dpi, render_page

__all__ = [
    "LEAST_SIZE_PT",
    "ROTATION_CONFIDENCE",
    "PageRender",
    "VisionElements",
    "VisionOptions",
    "fallback_page",
    "ocr_pieces",
    "superseded",
    "vision_elements",
    "vision_page",
]

# The least font size an OCR piece is given: layout reads line sizes to 0.1 pt, and a line's size is over 0.
LEAST_SIZE_PT = 0.1

# OCR reads ink as it is painted: opaque black, filled (render mode 0).
_PAINTED = (0, 0, 0, 255)
_FILL = 0

# A scan has no vector rules: no lattice tables, no text used by them.
_NO_LATTICE = LatticeResult(tables=(), used_pieces=())

# OSD's turn is made at this confidence or more (see the module notes: right turns 3.59 and up, the wrong one 0.02).
ROTATION_CONFIDENCE = 2.0

_SUPERSEDED: tuple[HiddenReason, ...] = ("superseded",)
_TURNS = (0, 90, 180, 270)
_OCR_MODES: tuple[OcrMode, ...] = ("auto", "always", "never")
# What pdfium reads for a page with no MediaBox: an error page whose own size cannot be read has this one.
_DEFAULT_PAGE_SIZE = (612.0, 792.0)
_NOT_READ = "the page was read by OCR of its render: its text layer, vector rules and images were not read"

# The spec's construction checks (PDFReader / PDFWriter) and defaults: vision_dpi >= 36, 300; max_render_pixels >=
# 512 x 512, 16,000,000.
_MIN_DPI = 36
_MIN_RENDER_PIXELS = 262_144
# The render worker's memory past its start: pdfium can spend ~1.7 GiB decoding an image within A2's bounds (render.py).
_MEMORY_CAP_BYTES = 2 << 30
# A render takes ~0.5 s here (a worker's start included) and a tesseract call ~1 s: each bound is over 100 times that.
_RENDER_TIMEOUT_S = 120.0
_OCR_TIMEOUT_S = 120.0


class VisionElements(NamedTuple):
    """A vision page's elements in reading order, and the warnings laying them out raised.

    Attributes:
        elements: The A1 elements, ids canonical on the page.
        warnings: The layout's warnings, then Pass C's mismatch, if any.
    """

    elements: tuple[DocumentElement, ...]
    warnings: tuple[str, ...]


def ocr_pieces(words: tuple[OCRWord, ...]) -> tuple[SpanPiece, ...]:
    """Each OCR word as one visible piece of a span of its own, in order (see the module notes). O(chars).

    A span's ``object_index`` is its word's position in ``words``.

    Raises:
        TypeError: If ``words`` is not a tuple of OCRWord.
    """
    return tuple(_word_piece(item, index) for index, item in enumerate(typed_tuple("words", words, OCRWord)))


def _word_piece(word: OCRWord, index: int) -> SpanPiece:
    """``word`` as the whole of a visible span: its chars split along its box, its size the box's height."""
    box = word.bbox
    count = len(word.text)
    # The last edge is the word's own, so the chars tile its box exactly whatever the float division.
    edges = [box.x0 + (box.x1 - box.x0) * at / count for at in range(count)] + [box.x1]
    chars = tuple((edges[at], box.y0, edges[at + 1], box.y1) for at in range(count))
    size = max(box.y1 - box.y0, LEAST_SIZE_PT)
    span = RawSpan(
        text=word.text,
        bbox=(box.x0, box.y0, box.x1, box.y1),
        char_boxes=chars,
        tight_boxes=chars,
        font_size_pt=size,
        em_width_pt=size,
        base_font_name="",
        is_bold=False,
        is_italic=False,
        render_mode=_FILL,
        fill_rgba=_PAINTED,
        stroke_rgba=_PAINTED,
        object_index=index,
        generated_breaks=(),
        map_errors=(),
    )
    return SpanPiece(span=span, start=0, end=count, hidden_reasons=())


def vision_elements(
    words: tuple[OCRWord, ...],
    *,
    geometry: PageGeometry,
    apart: tuple[SpanPiece, ...] = (),
    recovered: tuple[SpanPiece, ...] = (),
    figures: tuple[FigureDraft, ...] = (),
) -> VisionElements:
    """The page's elements from its OCR ``words`` (see the module notes), with its digital text kept apart.

    Args:
        words: The page's OCR words (``OCRResult.words``), on the displayed page.
        geometry: The page's geometry: its number and displayed size.
        apart: Digital pieces of the page laid out apart from the words:
            each must carry a hidden reason (the vision page supersedes its
            visible ones first).
        recovered: Recovered digital pieces, likewise all hidden.
        figures: The page's figure drafts; each reads as a figure slot.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: If a piece of ``apart`` or ``recovered`` is visible, or
            as :func:`~datagrunt.pdf.layout.page_layout` raises.
    """
    read = ocr_pieces(words)
    if not isinstance(geometry, PageGeometry):
        raise TypeError(f"geometry must be a PageGeometry, got {type(geometry).__name__}")
    _check_hidden("apart", apart)
    _check_hidden("recovered", recovered)
    typed_tuple("figures", figures, FigureDraft)
    layout = page_layout(
        (*read, *apart),
        recovered,
        _NO_LATTICE,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        figures=tuple(draft.bbox for draft in figures),
    )
    elements = page_elements(layout.items, figures, geometry)
    _, mismatch = conservation((*read, *apart, *recovered), elements)
    return VisionElements(elements=elements, warnings=(*layout.warnings, *mismatch))


def _check_hidden(name: str, pieces: object) -> None:
    """Require a tuple of SpanPiece, each with a hidden reason: a vision page's digital text never mixes with OCR's."""
    for index, piece in enumerate(typed_tuple(name, pieces, SpanPiece)):
        if not piece.hidden_reasons:
            raise ValueError(f"{name}[{index}] is visible ({piece.text!r}): a vision page's digital text reads apart")


def superseded(pieces: tuple[SpanPiece, ...]) -> tuple[SpanPiece, ...]:
    """``pieces``, each visible one (no hidden reason) flagged ``"superseded"``; a hidden one as it is. O(pieces).

    Raises:
        TypeError: If ``pieces`` is not a tuple of SpanPiece.
    """
    return tuple(
        piece
        if piece.hidden_reasons
        else SpanPiece(span=piece.span, start=piece.start, end=piece.end, hidden_reasons=_SUPERSEDED)
        for piece in typed_tuple("pieces", pieces, SpanPiece)
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class VisionOptions:
    """How a vision page is rendered and read. Picklable, so it crosses the process pool.

    Attributes:
        vision_dpi: The render's target DPI (>= 36).
        max_render_pixels: The render's pixel bound (>= 262,144, 512 x 512).
        language: tesseract's language(s), as ``eng`` or ``deu+eng``.
        memory_cap_bytes: The render worker's memory cap past its start (>= 1).
        render_timeout_s: Seconds a render may take, its worker's start included.
        ocr_timeout_s: Seconds each tesseract call may take.

    Raises:
        TypeError, ValueError: If a value is of the wrong type or out of range.
    """

    vision_dpi: int = 300
    max_render_pixels: int = 16_000_000
    language: str = "eng"
    memory_cap_bytes: int = _MEMORY_CAP_BYTES
    render_timeout_s: float = _RENDER_TIMEOUT_S
    ocr_timeout_s: float = _OCR_TIMEOUT_S

    def __post_init__(self) -> None:
        int_at_least("vision_dpi", self.vision_dpi, _MIN_DPI)
        int_at_least("max_render_pixels", self.max_render_pixels, _MIN_RENDER_PIXELS)
        non_blank_str("language", self.language)
        int_at_least("memory_cap_bytes", self.memory_cap_bytes, 1)
        object.__setattr__(self, "render_timeout_s", positive_float("render_timeout_s", self.render_timeout_s))
        object.__setattr__(self, "ocr_timeout_s", positive_float("ocr_timeout_s", self.ocr_timeout_s))


@dataclass(frozen=True, slots=True, kw_only=True)
class PageRender:
    """How to draw a vision page again: a :class:`~datagrunt.pdf.elements.PageImage`'s loaders.

    It holds no pixels: each call renders the page in the guarded worker,
    normalizes it as OCR read it, and encodes PNG (8-bit grey). Picklable,
    so a page crosses the process pool. Each load raises as
    :func:`~datagrunt.pdf.render.render_page` does (``RenderRefused``,
    ``RenderFailed``).

    Attributes:
        source: The PDF's absolute path.
        page_index: The 0-based page.
        dpi: The render's DPI (the page's effective DPI).
        rotation: The clockwise turn OCR read the page at: the bands' turn.
        password: The PDF's password, or None. Not in repr.
        memory_cap_bytes: The render worker's memory cap past its start.
        timeout_s: Seconds a render may take.
    """

    source: Path
    page_index: int
    dpi: int
    rotation: int
    password: str | None = field(default=None, repr=False)
    memory_cap_bytes: int
    timeout_s: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _checked_path(self.source, "source").absolute())
        int_at_least("page_index", self.page_index, 0)
        int_at_least("dpi", self.dpi, 1)
        if type(self.rotation) is not int or self.rotation not in _TURNS:
            raise ValueError(f"rotation must be an int of 0, 90, 180 or 270, got {self.rotation!r}")
        _checked_password(self.password)
        int_at_least("memory_cap_bytes", self.memory_cap_bytes, 1)
        object.__setattr__(self, "timeout_s", positive_float("timeout_s", self.timeout_s))

    def png_bytes(self) -> bytes:
        """The page as displayed (before OCR's turn), normalized, as PNG."""
        return _png(self._drawn())

    def band_png_bytes(self) -> tuple[bytes, ...]:
        """The bands OCR read (of the page turned by ``rotation``, normalized), top to bottom, each as PNG."""
        return tuple(_png(band) for band in bands(rotated(self._drawn(), self.rotation)))

    def _drawn(self) -> RenderedPage:
        page = render_page(
            self.source,
            self.page_index,
            dpi=self.dpi,
            password=self.password,
            memory_cap_bytes=self.memory_cap_bytes,
            timeout_s=self.timeout_s,
        )
        return normalized(page)


def _png(image: RenderedPage | Band) -> bytes:
    """8-bit grey pixels as PNG."""
    buffer = io.BytesIO()
    Image.frombytes("L", (image.px_width, image.px_height), image.pixels).save(buffer, format="PNG")
    return buffer.getvalue()


def vision_page(
    raw_page: RawPage,
    assessment: PageAssessment,
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
    options: VisionOptions = VisionOptions(),
) -> Page:
    """The A1 page of a page A3 routed to vision: OCR's elements, or its digital text's (see the module notes).

    Args:
        raw_page: The page's raw facts (A2's ``RawPage``).
        assessment: A3's assessment of it, routed ``"vision"``.
        source: The PDF the page was read from: it is rendered again from it.
        password: The PDF's password, or None.
        min_image_dimension: The least pixel side of a figure (A4b's).
        options: How the page is rendered and read.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: If ``assessment`` is not of ``raw_page``, or does not
            route it to vision.
    """
    check_assessment(raw_page, assessment)
    if assessment.route != "vision":
        raise ValueError(f"assessment routes the page {assessment.route!r}, not 'vision': assemble_page lays it out")
    if not isinstance(options, VisionOptions):
        raise TypeError(f"options must be a VisionOptions, got {type(options).__name__}")
    geometry = raw_page.geometry
    drawn = _drawn(source, geometry, password, options)
    if drawn.page is None:
        return _unread(raw_page, assessment, drawn.failure, None, source, password, min_image_dimension)
    reading = _read(drawn.page, drawn.dpi, geometry, options)
    image = _page_image(drawn, reading.rotation, geometry, source, password, options)
    if reading.failure:
        return _unread(raw_page, assessment, reading.failure, image, source, password, min_image_dimension)
    figures = page_figures(
        raw_page.images, geometry, source=source, password=password, min_image_dimension=min_image_dimension
    )
    laid = vision_elements(
        reading.words,
        geometry=geometry,
        apart=superseded(assessment.pieces),
        recovered=superseded(assessment.recovered_pieces),
        figures=figures.drafts,
    )
    warnings = (*raw_page_warnings(raw_page), *figures.warnings, *assessment.warnings, *reading.notes, *laid.warnings)
    return Page(
        page_number=geometry.page_number,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        status="ok",
        route="vision",
        quality=dataclasses.replace(assessment.quality, ocr_mean_confidence=_mean_confidence(reading.words)),
        elements=laid.elements,
        page_image=image,
        warnings=warnings,
    )


class _Drawn(NamedTuple):
    """A page's render at its DPI, or why it was not rendered (``page`` None)."""

    page: RenderedPage | None
    dpi: int
    failure: str


def _drawn(
    source: str | os.PathLike[str], geometry: PageGeometry, password: str | None, options: VisionOptions
) -> _Drawn:
    """The page rendered at its effective DPI in the guarded worker; a refusal or failure is its reason."""
    try:
        dpi = effective_dpi(options.vision_dpi, options.max_render_pixels, geometry.width_pt, geometry.height_pt)
        page = render_page(
            source,
            geometry.page_number - 1,
            dpi=dpi,
            password=password,
            memory_cap_bytes=options.memory_cap_bytes,
            timeout_s=options.render_timeout_s,
        )
    except (RenderRefused, RenderFailed) as exc:
        return _Drawn(None, 0, f"the page was not rendered: {exc}")
    return _Drawn(page, dpi, "")


class _Reading(NamedTuple):
    """What OCR read on a page: its words, the turn it read at, notes on the turn, and why it failed ("" if not)."""

    words: tuple[OCRWord, ...]
    rotation: int
    notes: tuple[str, ...]
    failure: str


def _read(page: RenderedPage, dpi: int, geometry: PageGeometry, options: VisionOptions) -> _Reading:
    """The render normalized, oriented, turned, banded and read by tesseract (one OSD call, one call a band)."""
    grey = normalized(page)
    found = orientation(grey, timeout_s=options.ocr_timeout_s)
    if found.status == "unavailable":
        return _Reading((), 0, (), f"tesseract is unavailable: {found.reason}")
    turn, notes = _turn(found)
    result = ocr_words(
        bands(rotated(grey, turn)),
        dpi=dpi,
        rotation=turn,
        page_size_pt=(geometry.width_pt, geometry.height_pt),
        timeout_s=options.ocr_timeout_s,
        language=options.language,
    )
    if result.status == "ok":
        return _Reading(result.words, turn, notes, "")
    why = "is unavailable" if result.status == "unavailable" else result.status
    unused = f"; the {len(result.words)} word(s) it read before that are not used" if result.words else ""
    return _Reading((), turn, notes, f"tesseract {why}: {result.reason}{unused}")


def _turn(found: Orientation) -> tuple[int, tuple[str, ...]]:
    """The turn OCR reads the page at, from OSD's (made at :data:`ROTATION_CONFIDENCE` or more), and a note on it."""
    confidence = found.confidence
    if found.status != "ok" or confidence is None:
        return 0, (f"orientation detection {found.status}: {found.reason}; OCR read the page as displayed",)
    if found.degrees == 0:
        return 0, ()
    if confidence >= ROTATION_CONFIDENCE:
        turned = f"OCR righted it by {found.degrees} degrees clockwise (orientation confidence {confidence:g})"
        return found.degrees, (f"the page reads turned: {turned}",)
    suggests = f"turning the page {found.degrees} degrees clockwise at a confidence of {confidence:g}"
    return 0, (f"orientation detection suggests {suggests}, under {ROTATION_CONFIDENCE:g}; OCR read it as displayed",)


def _page_image(
    drawn: _Drawn,
    rotation: int,
    geometry: PageGeometry,
    source: str | os.PathLike[str],
    password: str | None,
    options: VisionOptions,
) -> PageImage | None:
    """The page's lazy image: the render's size as displayed, its loaders a :class:`PageRender` (no pixels held)."""
    if drawn.page is None:
        return None
    loader = PageRender(
        source=Path(source),
        page_index=geometry.page_number - 1,
        dpi=drawn.dpi,
        rotation=rotation,
        password=password,
        memory_cap_bytes=options.memory_cap_bytes,
        timeout_s=options.render_timeout_s,
    )
    return PageImage(
        page_number=geometry.page_number,
        dpi=float(drawn.dpi),
        width_px=drawn.page.px_width,
        height_px=drawn.page.px_height,
        contrast_normalized=True,
        _loader=loader.png_bytes,
        _band_loader=loader.band_png_bytes,
    )


def _mean_confidence(words: tuple[OCRWord, ...]) -> float | None:
    """The words' mean tesseract confidence; None for no words (no mean to take)."""
    return math.fsum(word.confidence for word in words) / len(words) if words else None


def _unread(
    raw_page: RawPage,
    assessment: PageAssessment,
    reason: str,
    image: PageImage | None,
    source: str | os.PathLike[str],
    password: str | None,
    min_image_dimension: int,
) -> Page:
    """The ``"ocr_unavailable"`` page: assemble_page's of the digital text, the image if rendered, and why."""
    lattice = find_lattice_tables(raw_page.segments.segments, assessment.pieces)
    page = assemble_page(
        raw_page, assessment, lattice, source=source, password=password, min_image_dimension=min_image_dimension
    )
    return dataclasses.replace(
        page, page_image=image, warnings=(*page.warnings, f"OCR could not read the page: {reason}")
    )


def fallback_page(
    handle: DocumentHandle,
    failure: PageFailure,
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    ocr: OcrMode = "auto",
    options: VisionOptions = VisionOptions(),
) -> Page:
    """The A1 page of a page whose extraction failed: read by OCR of its render, or an error page (module notes).

    Args:
        handle: The open document the page failed in; its geometry is read again from it.
        failure: A2's ``PageFailure`` of the page.
        source: The PDF's path: the page is rendered from it.
        password: The PDF's password, or None.
        ocr: The reader's OCR option: ``"never"`` reads nothing by OCR.
        options: How the page is rendered and read.

    Raises:
        TypeError, ValueError: If an argument is of the wrong type or value.
        IndexError: If ``failure`` names a page the document does not have.
    """
    _check_fallback(handle, failure, ocr, options)
    failed = f"extraction failed at stage {failure.stage!r}: {failure.message}"
    try:
        geometry = page_geometry(handle, failure.page_number - 1)
    except PDFCorruptError as exc:
        return _error_page(failure.page_number, None, _unsized(failed, exc))
    if ocr == "never":
        never = f"{failed}; ocr='never', so the page is not read by OCR"
        return _error_page(failure.page_number, geometry, _Unread(never))
    drawn = _drawn(source, geometry, password, options)
    if drawn.page is None:
        return _error_page(failure.page_number, geometry, _Unread(f"{failed}; {drawn.failure}"))
    reading = _read(drawn.page, drawn.dpi, geometry, options)
    image = _page_image(drawn, reading.rotation, geometry, source, password, options)
    if reading.failure:
        why = f"{failed}; OCR could not read the page: {reading.failure}"
        return _error_page(failure.page_number, geometry, _Unread(why, reading.notes, image))
    laid = vision_elements(reading.words, geometry=geometry)
    return Page(
        page_number=geometry.page_number,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        status="ocr_fallback",
        route="vision",
        quality=_ocr_quality(reading.words, geometry),
        elements=laid.elements,
        page_image=image,
        warnings=(f"{failed}; {_NOT_READ}", *reading.notes, *laid.warnings),
    )


def _check_fallback(handle: object, failure: object, ocr: object, options: object) -> None:
    """Require an open document's handle, a PageFailure, an OCR mode and VisionOptions."""
    if not isinstance(handle, DocumentHandle):
        raise TypeError(f"handle must be a DocumentHandle, got {type(handle).__name__}")
    if not isinstance(failure, PageFailure):
        raise TypeError(f"failure must be a PageFailure, got {type(failure).__name__}")
    choice_str("ocr", ocr, _OCR_MODES)
    if not isinstance(options, VisionOptions):
        raise TypeError(f"options must be a VisionOptions, got {type(options).__name__}")


class _Unread(NamedTuple):
    """Why a failed page was not read, its warnings, and its image if it was rendered."""

    error: str
    warnings: tuple[str, ...] = ()
    image: PageImage | None = None


def _unsized(failed: str, exc: PDFCorruptError) -> _Unread:
    """Why a failed page whose geometry cannot be read either is not read, and a warning that its size is a default."""
    size = f"the page's size could not be read ({type(exc).__name__}: {exc})"
    width, height = _DEFAULT_PAGE_SIZE
    default = f"{width:g} x {height:g} pt is pdfium's default page size, not this page's"
    return _Unread(f"{failed}; its geometry cannot be read either: {exc}", (f"{size}; {default}",))


def _error_page(page_number: int, geometry: PageGeometry | None, unread: _Unread) -> Page:
    """The A1 error page: no elements, no quality, routed vision; its size the page's, or pdfium's default."""
    width, height = (geometry.width_pt, geometry.height_pt) if geometry is not None else _DEFAULT_PAGE_SIZE
    return Page(
        page_number=page_number,
        width_pt=width,
        height_pt=height,
        status="error",
        route="vision",
        quality=None,
        page_image=unread.image,
        error=unread.error,
        warnings=unread.warnings,
    )


def _ocr_quality(words: tuple[OCRWord, ...], geometry: PageGeometry) -> PageQuality:
    """A fallback page's quality: its words' chars by A3's rule and their mean confidence; nothing else was read."""
    chars = [char for item in words for char in item.text]
    valid = sum(1 for char in chars if valid_char(char))
    return PageQuality(
        valid_char_ratio=valid / len(chars) if chars else None,
        char_count=len(chars),
        vector_line_count=0,
        hidden_spans_flagged=0,
        rotation_deg=geometry.rotation_deg,
        deskew_angle_deg=None,
        ocr_mean_confidence=_mean_confidence(words),
    )
