"""Page assembly for the v5 PDF document model (#368, A4b group 3): figures, then the page's elements.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads A2's raw facts (:class:`~datagrunt.pdf.pdfium_engine.RawPage`), A3's
assessment and A4a's lattice tables, and calls no pdfium function. A
figure's bytes come from an
:class:`~datagrunt.pdf.pdfium_engine.ImageLoader`, which assembly binds and
never calls: nothing is decoded until a caller reads the figure.

:func:`assemble_page` makes the A1 :class:`~datagrunt.pdf.elements.Page`
of a digital or hybrid page (see its notes): its figures
(:func:`page_figures`), its layout in reading order
(:func:`~datagrunt.pdf.layout.page_layout`), each item an A1 element with
its canonical id, and Pass C (:func:`pass_c`).

:func:`page_figures` drafts the page's figures (:class:`FigureDraft`), in
A2's object-walk order. Each image A2 recorded is, by the first rule it
meets:

1. Skipped when its pixel size is known and either side is under
   ``min_image_dimension`` (v4's rule, ported: ``px_w < min or px_h < min``
   skips, in ``core.pdf_io.extraction.pdfium_document.image_items``; v4's
   default, 40, is ``_DEFAULT_MIN_IMAGE_DIMENSION`` in
   ``core.pdf_io.extraction.config``). Its pixel size is the decode size
   A2 read (a DCT or JPX stream's own header), or, when that cannot be
   known without decoding (a JPEG behind Flate), the image dictionary's
   declared ``/Width`` and ``/Height``.
2. No figure, and a warning, when its stream was not read (no SHA-256: a
   stream over the asset limit, or the page's stream budget spent): A1's
   ``Figure`` is identified by that hash and its loader verifies reads by it.
3. No figure, and a warning, when its pixel size is unknown (neither size
   has both sides): A1's ``Figure`` needs both sides >= 1.
4. Else a figure with the facts A2 recorded: an oversized image too (its
   loader refuses to decode it, so its PNG cannot be read; its raw bytes can).

A figure lies on the page as A1's ``Page`` requires: its box, when within
the page (A1's :meth:`~datagrunt.pdf.elements.BBox.within`); else the part
of it on the page, when that has area (an image bleeding past the page
edge); else its whole box, flagged ``off_page`` (nothing of it shows).

Captions are not read: the corpus (``data/pdfs``) holds no "Figure" or
"Fig." text, so no rule for them can be checked against it (deferred).
"""

import os
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypedDict, get_args

from datagrunt.pdf._pdfium_common import Box
from datagrunt.pdf._validate import int_at_least, sha256_hex, str_tuple, typed_tuple
from datagrunt.pdf.elements import (
    BBox,
    BulletList,
    DocumentElement,
    ExtractionMethod,
    Figure,
    Heading,
    HiddenReason,
    KeyValue,
    Page,
    Paragraph,
    Table,
    check_hidden_reasons,
    make_element_id,
)
from datagrunt.pdf.layout import (
    MAX_CROSSINGS,
    ApartLine,
    FigureSlot,
    LatticeResult,
    LatticeTable,
    PageItem,
    StreamHeading,
    StreamKeyValue,
    StreamList,
    StreamParagraph,
    StreamTable,
    page_layout,
)
from datagrunt.pdf.pdfium_engine import (
    ImageLoader,
    ImageStats,
    PageGeometry,
    PageImages,
    RawImage,
    RawPage,
    _checked_path,
)
from datagrunt.pdf.quality import PageAssessment

__all__ = ["FigureDraft", "PageFigures", "assemble_page", "page_figures", "pass_c"]

# v4's default minimum image side in pixels (core.pdf_io.extraction.config._DEFAULT_MIN_IMAGE_DIMENSION).
MIN_IMAGE_DIMENSION = 40

# The only hidden reason a figure can carry: images have no font, paint mode or alpha A3 reads.
_OFF_PAGE: tuple[HiddenReason, ...] = ("off_page",)

# A warning names at most this many image objects; the rest are counted.
_NAMED_OBJECTS = 5

# The reading turn of an upright table, whose columns run left to right on the page.
_UPRIGHT = 0

# What a vision page's warnings say: assemble_page lays out its digital text, and never pretends to OCR it.
_VISION_NOTE = "route 'vision': OCR (A5) has not run on this page; its digital text is kept as its elements"

_ELEMENT_TYPES: tuple[type, ...] = get_args(DocumentElement)


@dataclass(frozen=True, slots=True, kw_only=True)
class FigureDraft:
    """A page image as an A1 ``Figure`` before page assembly gives it its id and place in reading order.

    Picklable, so it crosses the process pool.

    Attributes:
        bbox: Where it lies on the displayed page (see the module notes).
        hidden_reasons: ``()``, or ``("off_page",)`` when nothing of it is on the page.
        sha256: Its raw stream's SHA-256: the loader's.
        width_px: Its width in pixels (>= 1; see the module notes).
        height_px: Its height in pixels (>= 1).
        loader: Reads its bytes on demand; never called here.
    """

    bbox: BBox
    hidden_reasons: tuple[HiddenReason, ...]
    sha256: str
    width_px: int
    height_px: int
    loader: ImageLoader

    def __post_init__(self) -> None:
        if not isinstance(self.bbox, BBox):
            raise TypeError(f"bbox must be a BBox, got {type(self.bbox).__name__}")
        check_hidden_reasons(self.hidden_reasons)
        if self.hidden_reasons not in ((), _OFF_PAGE):
            raise ValueError(f"hidden_reasons of a figure must be () or {_OFF_PAGE}, got {self.hidden_reasons}")
        sha256_hex("sha256", self.sha256)
        int_at_least("width_px", self.width_px, 1)
        int_at_least("height_px", self.height_px, 1)
        if not isinstance(self.loader, ImageLoader):
            raise TypeError(f"loader must be an ImageLoader, got {type(self.loader).__name__}")
        if self.loader.raw_sha256 != self.sha256:
            raise ValueError(f"sha256 must be the loader's {self.loader.raw_sha256}, got {self.sha256}")

    def figure(self, *, page_number: int, reading_order: int) -> Figure:
        """The A1 ``Figure`` at ``reading_order`` on ``page_number``: its loaders bound, neither called."""
        return Figure(
            element_id=make_element_id(page_number, reading_order),
            page_number=page_number,
            reading_order=reading_order,
            bbox=self.bbox,
            hidden_reasons=self.hidden_reasons,
            sha256=self.sha256,
            width_px=self.width_px,
            height_px=self.height_px,
            _loader=self.loader.png_bytes,
            _raw_loader=self.loader.raw_bytes,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class PageFigures:
    """One page's figure drafts, in A2's object-walk order, and the warnings drafting them raised.

    Attributes:
        drafts: The figures (see the module notes).
        warnings: A2's image counts as warnings, then the images that are
            no figure (``Page.warnings``).
    """

    drafts: tuple[FigureDraft, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        typed_tuple("drafts", self.drafts, FigureDraft)
        str_tuple("warnings", self.warnings)


def page_figures(
    images: PageImages,
    geometry: PageGeometry,
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
) -> PageFigures:
    """Draft the page's figures from the images A2 recorded (see the module notes). O(images).

    Args:
        images: The page's images (``RawPage.images``).
        geometry: The page's geometry (``RawPage.geometry``): its number and size.
        source: The PDF the page was read from; each figure's loader reopens it.
        password: The PDF's password, or None; each loader holds it.
        min_image_dimension: The least pixel side kept (0 keeps every image of known size).

    Raises:
        TypeError: If an argument is of the wrong type (``source`` as for
            :class:`~datagrunt.pdf.pdfium_engine.ImageLoader`).
        ValueError: If ``min_image_dimension`` is negative, or ``password`` holds a NUL.
    """
    if not isinstance(images, PageImages):
        raise TypeError(f"images must be a PageImages, got {type(images).__name__}")
    if not isinstance(geometry, PageGeometry):
        raise TypeError(f"geometry must be a PageGeometry, got {type(geometry).__name__}")
    minimum = int_at_least("min_image_dimension", min_image_dimension, 0)
    path = _checked_path(source, "source")
    drafts: list[FigureDraft] = []
    unread: list[int] = []
    unsized: list[int] = []
    for item in images.images:
        size = _pixel_size(item)
        if size is not None and min(size) < minimum:
            continue
        if item.raw_sha256 is None:
            unread.append(item.object_index)
        elif size is None:
            unsized.append(item.object_index)
        else:
            loader = ImageLoader(source=path, page_index=geometry.page_number - 1, image=item, password=password)
            drafts.append(_draft(item, size, loader, geometry))
    warnings = (
        *_image_count_warnings(images.stats),
        *_not_figures(unread, "their streams were not read"),
        *_not_figures(unsized, "their pixel size is unknown"),
    )
    return PageFigures(drafts=tuple(drafts), warnings=warnings)


def _pixel_size(item: RawImage) -> tuple[int, int] | None:
    """The image's decode size if A2 read both sides, else its declared size if both are known, else None."""
    if item.px_width and item.px_height:
        return item.px_width, item.px_height
    width, height = item.declared_px
    return (width, height) if width and height else None


def _draft(item: RawImage, size: tuple[int, int], loader: ImageLoader, geometry: PageGeometry) -> FigureDraft:
    """The image's draft, placed on the page (see the module notes)."""
    bbox, reasons = _placed(item.bbox, geometry.width_pt, geometry.height_pt)
    return FigureDraft(
        bbox=bbox, hidden_reasons=reasons, sha256=loader.raw_sha256, width_px=size[0], height_px=size[1], loader=loader
    )


def _placed(box: Box, width: float, height: float) -> tuple[BBox, tuple[HiddenReason, ...]]:
    """The image box as A1's page rule takes it: within, clipped to the page, or wholly off it (flagged)."""
    whole = BBox(x0=box[0], y0=box[1], x1=box[2], y1=box[3])
    if whole.within(width, height):
        return whole, ()
    x0, y0, x1, y1 = max(box[0], 0.0), max(box[1], 0.0), min(box[2], width), min(box[3], height)
    if x0 < x1 and y0 < y1:
        return BBox(x0=x0, y0=y0, x1=x1, y1=y1), ()
    return whole, _OFF_PAGE


def _image_count_warnings(stats: ImageStats) -> tuple[str, ...]:
    """A2's image counts that cost the page something, each as a warning."""
    counted = (
        (stats.skipped_nonfinite_images, "image object(s) skipped: their placement on the page is not finite"),
        (stats.skipped_oversized_images, "image(s) too large to decode or read: never decoded"),
        (stats.skipped_unsized_images, "image(s) whose decode size cannot be known without decoding: never decoded"),
        (stats.skipped_over_budget_images, "image(s) not read: the page's image stream budget was spent"),
    )
    return tuple(f"{count} {what}" for count, what in counted if count)


def _not_figures(objects: list[int], why: str) -> tuple[str, ...]:
    """A warning for the image ``objects`` that are no figure, naming at most :data:`_NAMED_OBJECTS` of them."""
    if not objects:
        return ()
    named = ", ".join(str(index) for index in objects[:_NAMED_OBJECTS])
    more = len(objects) - _NAMED_OBJECTS
    which = f"objects {named} and {more} more" if more > 0 else f"object{'s' if len(objects) > 1 else ''} {named}"
    return (f"{len(objects)} image(s) not emitted as figures: {why} ({which})",)


def assemble_page(
    raw_page: RawPage,
    assessment: PageAssessment,
    lattice: LatticeResult,
    *,
    source: str | os.PathLike[str],
    password: str | None = None,
    min_image_dimension: int = MIN_IMAGE_DIMENSION,
) -> Page:
    """The A1 page of a digital or hybrid page: its elements in reading order, its facts and Pass C.

    1. Figures: :func:`page_figures` (loaders bound, never called).
    2. Layout: :func:`~datagrunt.pdf.layout.page_layout` of the page's
       pieces, recovered pieces, lattice tables and figure boxes.
    3. Elements, one per item in order, ``reading_order`` its position and
       ``element_id`` its canonical id; ``scope_path`` is ``()`` (A6 sets
       it), but a heading's is its own text (A1: a heading opens its
       scope). Headings, paragraphs, key-values and lists convert their
       fields (a list keeps its markers); a hidden or recovered line is a
       :class:`~datagrunt.pdf.elements.Paragraph` with its reasons; a table
       is a :class:`~datagrunt.pdf.elements.Table` of its grid, headed by
       its first row when every cell of it is non-blank (A4a's rule), with
       ``column_centres`` when upright (a lattice table's grid column
       spans, a stream table's X-bands), else ``()``: a turned table's run
       along display y, and A6 links only increasing display x. A lattice
       table's box is clamped to the page (its rules may run past it).
    4. Pass C (:func:`pass_c`) sets ``integrity_verified``.

    The page's ``quality`` and ``route`` are A3's. Its warnings are A2's
    (skipped text, path and image objects), then the figures', A3's,
    A4a's (grids past its bounds), the layout's, and Pass C's.

    A vision page (A5's) is not OCR'd here, and not passed off as OCR'd:
    its digital text is laid out as on any page, its status is
    ``"ocr_unavailable"`` (A1: a vision page whose OCR has not run, its
    digital text kept), it is never ``integrity_verified`` (A1) and a
    warning says OCR has not run. A5 replaces it.

    Raises:
        TypeError: If an argument is of the wrong type.
        ValueError: If ``assessment`` is not of ``raw_page`` (its chars or
            rotation differ), or as :func:`page_figures` and ``page_layout``.
    """
    _check_inputs(raw_page, assessment, lattice)
    geometry = raw_page.geometry
    figures = page_figures(
        raw_page.images, geometry, source=source, password=password, min_image_dimension=min_image_dimension
    )
    boxes = tuple(draft.bbox for draft in figures.drafts)
    layout = page_layout(
        assessment.pieces,
        assessment.recovered_pieces,
        lattice,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        figures=boxes,
    )
    elements = tuple(_element(item, order, figures.drafts, geometry) for order, item in enumerate(layout.items))
    verified, mismatch = pass_c(assessment, elements)
    vision = assessment.route == "vision"
    warnings = (*_raw_warnings(raw_page), *figures.warnings, *assessment.warnings, *_lattice_warnings(lattice))
    warnings += (*layout.warnings, *((_VISION_NOTE,) if vision else ()), *mismatch)
    return Page(
        page_number=geometry.page_number,
        width_pt=geometry.width_pt,
        height_pt=geometry.height_pt,
        status="ocr_unavailable" if vision else "ok",
        route=assessment.route,
        quality=assessment.quality,
        elements=elements,
        integrity_verified=verified and not vision,
        warnings=warnings,
    )


def _check_inputs(raw_page: object, assessment: object, lattice: object) -> None:
    """Require a RawPage, a PageAssessment of it (same chars and rotation) and a LatticeResult."""
    if not isinstance(raw_page, RawPage):
        raise TypeError(f"raw_page must be a RawPage, got {type(raw_page).__name__}")
    if not isinstance(assessment, PageAssessment):
        raise TypeError(f"assessment must be a PageAssessment, got {type(assessment).__name__}")
    if not isinstance(lattice, LatticeResult):
        raise TypeError(f"lattice must be a LatticeResult, got {type(lattice).__name__}")
    chars = sum(len(span.text) for span in raw_page.text.spans)
    pieces = sum(piece.end - piece.start for piece in assessment.pieces)
    if assessment.quality.char_count != chars or pieces != chars:
        found = f"assessment counts {assessment.quality.char_count} chars and its pieces hold {pieces}"
        raise ValueError(f"{found}, but the page's spans hold {chars}: the assessment is of another page")
    if assessment.quality.rotation_deg != raw_page.geometry.rotation_deg:
        turned = f"assessment is of a page turned {assessment.quality.rotation_deg}"
        raise ValueError(f"{turned}, not {raw_page.geometry.rotation_deg}: the assessment is of another page")


class _Ids(TypedDict):
    """An element's identity: its canonical id, page and place in reading order."""

    element_id: str
    page_number: int
    reading_order: int


def _element(item: PageItem, order: int, drafts: tuple[FigureDraft, ...], geometry: PageGeometry) -> DocumentElement:
    """The A1 element of one layout ``item`` at ``order`` on the page (:func:`assemble_page`, step 3)."""
    page = geometry.page_number
    ids = _Ids(element_id=make_element_id(page, order), page_number=page, reading_order=order)
    if isinstance(item, FigureSlot):
        return drafts[item.index].figure(page_number=page, reading_order=order)
    if isinstance(item, StreamHeading):
        return Heading(**ids, bbox=item.bbox, level=item.level, text=item.text, scope_path=(item.text,))
    if isinstance(item, StreamParagraph):
        return Paragraph(**ids, bbox=item.bbox, text=item.text)
    if isinstance(item, ApartLine):
        return Paragraph(**ids, bbox=item.bbox, text=item.text, hidden_reasons=item.hidden_reasons)
    if isinstance(item, StreamKeyValue):
        return KeyValue(**ids, bbox=item.bbox, key=item.key, value=item.value)
    if isinstance(item, StreamList):
        items, markers = tuple(entry.text for entry in item.items), tuple(entry.marker for entry in item.items)
        return BulletList(**ids, bbox=item.bbox, items=items, markers=markers, ordered=item.ordered)
    if isinstance(item, StreamTable):
        centres = item.column_centres if item.rows[0].turn == _UPRIGHT else ()
        return _table(ids, item.grid(), item.has_header, "stream", centres, item.bbox)
    bbox = _clamped(item.bbox, geometry.width_pt, geometry.height_pt)
    return _table(ids, item.grid(), item.has_header, "lattice", _lattice_centres(item), bbox)


def _table(
    ids: _Ids,
    grid: tuple[tuple[str, ...], ...],
    has_header: bool,
    method: ExtractionMethod,
    centres: tuple[float, ...],
    bbox: BBox,
) -> Table:
    """An A1 table of ``grid``, its first row the headers when ``has_header`` (A4a's rule)."""
    headers, rows = (grid[0], grid[1:]) if has_header else ((), grid)
    return Table(**ids, bbox=bbox, headers=headers, rows=rows, extraction_method=method, column_centres=centres)


def _lattice_centres(table: LatticeTable) -> tuple[float, ...]:
    """An upright lattice table's column centres: the middle of each grid column's span; ``()`` when turned.

    An upright table's grid columns are its cells' distinct x edges, in
    order (A4a's ``_grid_positions``), so the centres strictly increase.
    """
    if table.turn != _UPRIGHT:
        return ()
    edges = sorted({cell.bbox.x0 for cell in table.cells} | {cell.bbox.x1 for cell in table.cells})
    return tuple((left + right) / 2 for left, right in zip(edges, edges[1:]))


def _clamped(bbox: BBox, width: float, height: float) -> BBox:
    """``bbox`` with each side clamped to the page: within it, as A1's page rule asks of a visible element."""

    def clamp(value: float, limit: float) -> float:
        return min(max(value, 0.0), limit)

    return BBox(
        x0=clamp(bbox.x0, width), y0=clamp(bbox.y0, height), x1=clamp(bbox.x1, width), y1=clamp(bbox.y1, height)
    )


def _raw_warnings(page: RawPage) -> tuple[str, ...]:
    """A2's text and path counts that cost the page something, as warnings (its image counts: :func:`page_figures`)."""
    text, paths = page.text.stats.skipped_nonfinite_objects, page.segments.stats.skipped_nonfinite_paths
    found = []
    if text:
        why = "pdfium places them at a non-finite position or size; their text is missing from this page"
        found.append(f"{text} text object(s) skipped: {why}")
    if paths:
        found.append(
            f"{paths} path object(s) skipped: their geometry is not finite; their rules are missing from this page"
        )
    return tuple(found)


def _lattice_warnings(lattice: LatticeResult) -> tuple[str, ...]:
    """A4a's bounds that left rules unread, as warnings."""
    if lattice.crossing_cap_reached:
        return (
            f"the page's rules cross more than {MAX_CROSSINGS} times: no lattice tables were read; "
            "its text reads as stream text",
        )
    if lattice.skipped_grids:
        grids = f"{lattice.skipped_grids} ruled grid(s) left out of the lattice pass (over its per-page bounds)"
        return (f"{grids}: their text reads as stream text",)
    return ()


def pass_c(assessment: PageAssessment, elements: tuple[DocumentElement, ...]) -> tuple[bool, tuple[str, ...]]:
    """Pass C (spec §3.3 step 3): the page's non-whitespace chars, as a multiset, are its elements' exactly.

    The page's chars are every piece's: its visible pieces (the chars its
    lattice tables used among them), its hidden pieces and its recovered
    pieces, each char once. An element's chars are a heading's or a
    paragraph's text; a key-value's key, ``":"`` and value; a list's
    markers and items; a table's header and body cells; a figure's
    caption. Whitespace (``str.isspace``) never counts. O(chars).

    Returns:
        ``(True, ())`` when the multisets are equal; else ``(False,
        (warning,))``, the warning naming both counts and how many chars
        are missing and extra. A mismatch never raises.

    Raises:
        TypeError: If ``assessment`` is not a PageAssessment or ``elements``
            is not a tuple of A1 elements.
    """
    if not isinstance(assessment, PageAssessment):
        raise TypeError(f"assessment must be a PageAssessment, got {type(assessment).__name__}")
    for index, element in enumerate(typed_tuple("elements", elements, object)):
        if not isinstance(element, _ELEMENT_TYPES):
            raise TypeError(f"elements[{index}] must be an A1 element, got {type(element).__name__}")
    expected = _solid_chars(piece.text for piece in (*assessment.pieces, *assessment.recovered_pieces))
    found = _solid_chars(text for element in elements for text in _element_texts(element))
    if expected == found:
        return True, ()
    missing, extra = (expected - found).total(), (found - expected).total()
    counts = f"the page's text holds {expected.total()} non-whitespace chars and its elements {found.total()}"
    return False, (f"Pass C: {counts}: {missing} missing, {extra} extra",)


def _solid_chars(texts: Iterable[str]) -> Counter[str]:
    """The multiset of the non-whitespace chars of ``texts``."""
    return Counter(char for text in texts for char in text if not char.isspace())


def _element_texts(element: DocumentElement) -> tuple[str, ...]:
    """The texts that make an element's chars for Pass C (see :func:`pass_c`)."""
    if isinstance(element, (Heading, Paragraph)):
        return (element.text,)
    if isinstance(element, KeyValue):
        return (element.key, ":", element.value)
    if isinstance(element, BulletList):
        return (*element.markers, *element.items)
    if isinstance(element, Table):
        return (*element.headers, *(cell for row in element.rows for cell in row))
    return (element.caption or "",)
