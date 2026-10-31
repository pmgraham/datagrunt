"""Page assembly for the v5 PDF document model (#368, A4b group 3): figures, then the page's elements.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. Pure: it
reads A2's raw facts (:class:`~datagrunt.pdf.pdfium_engine.RawImage`,
:class:`~datagrunt.pdf.pdfium_engine.PageImages`) and calls no pdfium
function. A figure's bytes come from an
:class:`~datagrunt.pdf.pdfium_engine.ImageLoader`, which assembly binds and
never calls: nothing is decoded until a caller reads the figure.

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
from dataclasses import dataclass

from datagrunt.pdf._pdfium_common import Box
from datagrunt.pdf._validate import int_at_least, sha256_hex, str_tuple, typed_tuple
from datagrunt.pdf.elements import BBox, Figure, HiddenReason, check_hidden_reasons, make_element_id
from datagrunt.pdf.pdfium_engine import ImageLoader, ImageStats, PageGeometry, PageImages, RawImage, _checked_path

__all__ = ["FigureDraft", "PageFigures", "page_figures"]

# v4's default minimum image side in pixels (core.pdf_io.extraction.config._DEFAULT_MIN_IMAGE_DIMENSION).
MIN_IMAGE_DIMENSION = 40

# The only hidden reason a figure can carry: images have no font, paint mode or alpha A3 reads.
_OFF_PAGE: tuple[HiddenReason, ...] = ("off_page",)

# A warning names at most this many image objects; the rest are counted.
_NAMED_OBJECTS = 5


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
