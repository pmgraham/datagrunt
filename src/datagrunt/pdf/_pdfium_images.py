"""Image objects for the v5 pdfium engine (#365 T4).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``;
:mod:`datagrunt.pdf.pdfium_engine` re-exports :class:`RawImage`,
:class:`ImageStats` and :class:`PageImages`, walks the page's objects and
hands each image object here with its composed page-space matrix. This
module never imports the engine.
"""

import ctypes
import hashlib
import io
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from types import MappingProxyType
from typing import NamedTuple

import pypdfium2.raw as pdfium_c
from PIL import Image

from datagrunt.pdf._path_segments import Matrix, NonFiniteGeometry
from datagrunt.pdf._pdfium_common import Box, PdfiumFailure
from datagrunt.pdf._validate import finite_float, int_at_least, ordered_box, sha256_hex, str_tuple
from datagrunt.pdf.elements import _MAX_ASSET_BYTES

Point = tuple[float, float]

# PDF colour space families, as pdfium's FPDF_COLORSPACE_* values 1..11 name them.
COLORSPACES = (
    "DeviceGray",
    "DeviceRGB",
    "DeviceCMYK",
    "CalGray",
    "CalRGB",
    "Lab",
    "ICCBased",
    "Separation",
    "DeviceN",
    "Indexed",
    "Pattern",
)


@dataclass(frozen=True, slots=True, kw_only=True)
class RawImage:
    """One image object of a page: where it is drawn and what its embedded stream is.

    Attributes:
        bbox: The displayed, top-left ``(x0, y0, x1, y1)`` box (rotation and
            CropBox applied) of the image's unit square under its full matrix
            to page space (form matrices included): its painted area, before
            any clipping path, soft mask or image mask.
        px_width: The image's width in samples, as pdfium reads ``/Width``
            (0 for a missing or zero /Width, which pdfium cannot decode).
        px_height: The image's height in samples (``/Height``), likewise.
        object_index: The image object's position in the page's depth-first,
            paint-order object walk (forms count, then their contents): the
            same index a span or segment uses.
        raw_sha256: SHA-256 of the image's raw embedded stream: its bytes
            as stored in the PDF, still encoded by ``filters`` (decrypted, if
            the PDF is encrypted). Stable across encoder upgrades, and equal
            for every object drawing the same stream.
        filters: The stream's filters in decoding order, as pdfium names them
            (an inline image's abbreviation ``/AHx`` reads ``ASCIIHexDecode``).
        bits_per_pixel: Bits per pixel of the image as pdfium decodes it (24
            for 8-bit RGB, and for CMYK, which pdfium decodes to RGB; 8 for
            8-bit Indexed; 1 for 1-bit gray and image masks), or None when
            pdfium cannot load the image.
        colorspace: The colour space family, one of :data:`COLORSPACES`, or
            None when there is none (an image mask) or pdfium cannot load it.
    """

    bbox: Box
    px_width: int
    px_height: int
    object_index: int
    raw_sha256: str
    filters: tuple[str, ...]
    bits_per_pixel: int | None
    colorspace: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "bbox", ordered_box("bbox", self.bbox))
        int_at_least("px_width", self.px_width, 0)
        int_at_least("px_height", self.px_height, 0)
        int_at_least("object_index", self.object_index, 0)
        sha256_hex("raw_sha256", self.raw_sha256)
        # Names as pdfium reports them, kept verbatim (an empty name included).
        str_tuple("filters", self.filters, allow_blank=True)
        if self.bits_per_pixel is not None:
            int_at_least("bits_per_pixel", self.bits_per_pixel, 1)
        _check_colorspace(self.colorspace)


def _check_colorspace(value: object) -> None:
    """Require ``value`` to be None or one of :data:`COLORSPACES`."""
    if value is None:
        return
    if not isinstance(value, str):
        raise TypeError(f"colorspace must be a str or None, got {value!r} ({type(value).__name__})")
    if value not in COLORSPACES:
        raise ValueError(f"colorspace must be None or one of {COLORSPACES}, got {value!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class ImageStats:
    """Counts and measures of one page's image objects that A3 records or routes on.

    Attributes:
        skipped_nonfinite_images: Image objects skipped because their matrix
            to page space is not finite (pdfium keeps the CTM in 32-bit
            floats, so five ``1e9`` scales overflow it); an image inside a
            form whose matrix overflowed counts here.
        skipped_oversized_images: Image objects skipped because their raw
            stream is over the asset limit (1 GiB), so it is not read or hashed.
        max_cover_fraction: The largest fraction, 0 to 1, of the visible page
            (CropBox within MediaBox) that one image's painted area covers:
            its unit square under its matrix, clipped to the visible page
            (clipping paths and masks are not applied). Oversized images
            count; skipped non-finite ones do not. 0.0 for a page without
            images. A3 routes a page as hybrid when a raster covers over half.
    """

    skipped_nonfinite_images: int
    skipped_oversized_images: int
    max_cover_fraction: float

    def __post_init__(self) -> None:
        int_at_least("skipped_nonfinite_images", self.skipped_nonfinite_images, 0)
        int_at_least("skipped_oversized_images", self.skipped_oversized_images, 0)
        cover = finite_float("max_cover_fraction", self.max_cover_fraction, lo=0.0, hi=1.0)
        object.__setattr__(self, "max_cover_fraction", cover)


@dataclass(frozen=True, slots=True, kw_only=True)
class PageImages:
    """One page's images in object-walk order, with its skip counts and cover fraction."""

    images: tuple[RawImage, ...]
    stats: ImageStats

    def __post_init__(self) -> None:
        if not isinstance(self.images, tuple):
            raise TypeError(f"images must be a tuple, got {type(self.images).__name__}")
        for index, item in enumerate(self.images):
            if not isinstance(item, RawImage):
                raise TypeError(f"images[{index}] must be a RawImage, got {type(item).__name__}")
        if not isinstance(self.stats, ImageStats):
            raise TypeError(f"stats must be an ImageStats, got {type(self.stats).__name__}")


class OversizedStream(ValueError):
    """An image's raw stream is over the asset limit, so it is neither read nor hashed."""


# PDF names are at most 127 bytes in practice (PDF 32000-1, Annex C); this
# bound, NUL included, only stops a hostile length from sizing the buffer.
_MAX_FILTER_NAME_BYTES = 4096

# The most pixels one image is decoded to: 1 GiB / 4 / 3, which is Pillow's
# default Image.MAX_IMAGE_PIXELS (its decompression-bomb bound), pinned here
# so a caller's Pillow setting cannot lift it. pdfium's bitmap is then at
# most a third of the asset limit (4 bytes a pixel).
_MAX_DECODED_PIXELS = _MAX_ASSET_BYTES // 4 // 3

# zlib's default level, pinned: the same pixels encode to the same PNG bytes
# for a given Pillow and zlib build.
_PNG_COMPRESS_LEVEL = 6

# FPDFImageObj_GetBitmap's formats, as (Pillow mode, Pillow raw mode, bytes per pixel).
_BITMAP_FORMATS = MappingProxyType(
    {
        pdfium_c.FPDFBitmap_Gray: ("L", "L", 1),
        pdfium_c.FPDFBitmap_BGR: ("RGB", "BGR", 3),
        pdfium_c.FPDFBitmap_BGRx: ("RGB", "BGRX", 4),
        pdfium_c.FPDFBitmap_BGRA: ("RGBA", "BGRA", 4),
    }
)


class TooManyPixels(ValueError):
    """An image decodes to more than ``_MAX_DECODED_PIXELS`` pixels."""


class Placement(NamedTuple):
    """Where an image is painted: its displayed bbox and the fraction of the visible page it covers."""

    bbox: Box
    cover_fraction: float


def collect_images(
    placed: Iterable[tuple[int, pdfium_c.FPDF_PAGEOBJECT, Matrix]],
    page_raw: pdfium_c.FPDF_PAGE,
    page_box: Box,
    to_display: Callable[[Box], Box],
) -> PageImages:
    """The RawImage of every image object in ``placed``, with the page's image stats.

    Args:
        placed: Each image object with its walk index and its matrix to PDF
            user space, in object-walk order.
        page_raw: The loaded page (pdfium needs it for the colour space).
        page_box: The visible box ``(left, bottom, right, top)`` in user space.
        to_display: Maps a user-space box to a displayed, top-left box.

    pdfium call failures raise PdfiumFailure. An image whose geometry is not
    finite, or whose raw stream is over the asset limit, is skipped and
    counted; the second still counts towards the cover fraction.
    """
    images: list[RawImage] = []
    nonfinite = oversized = 0
    cover = 0.0
    for index, image, matrix in placed:
        try:
            placement = image_placement(matrix, page_box, to_display)
        except NonFiniteGeometry:
            nonfinite += 1
            continue
        cover = max(cover, placement.cover_fraction)
        label = f"object {index}"
        try:
            raw = read_raw_stream(image, label)
        except OversizedStream:
            oversized += 1
            continue
        images.append(_raw_image(image, page_raw, index, placement.bbox, raw))
    stats = ImageStats(skipped_nonfinite_images=nonfinite, skipped_oversized_images=oversized, max_cover_fraction=cover)
    return PageImages(images=tuple(images), stats=stats)


def _raw_image(
    image: pdfium_c.FPDF_PAGEOBJECT, page_raw: pdfium_c.FPDF_PAGE, index: int, bbox: Box, raw: bytes
) -> RawImage:
    """The RawImage of image object ``index``, whose raw stream is ``raw``."""
    label = f"object {index}"
    width, height, bits_per_pixel, colorspace = _pixel_facts(image, page_raw, label)
    return RawImage(
        bbox=bbox,
        px_width=width,
        px_height=height,
        object_index=index,
        raw_sha256=hashlib.sha256(raw).hexdigest(),
        filters=_filters(image, label),
        bits_per_pixel=bits_per_pixel,
        colorspace=colorspace,
    )


def image_placement(matrix: Matrix, page_box: Box, to_display: Callable[[Box], Box]) -> Placement:
    """Where the unit square lands under ``matrix`` (image space to user space).

    Raises:
        NonFiniteGeometry: If the matrix, a corner, the displayed box or the
            covered area is not finite.
    """
    if not all(math.isfinite(value) for value in matrix):
        raise NonFiniteGeometry(f"its matrix to page space is not finite {tuple(matrix)}")
    corners = [matrix.apply(x, y) for x, y in ((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0))]
    for corner in corners:
        if not all(math.isfinite(value) for value in corner):
            raise NonFiniteGeometry(f"its corner {corner} is not finite")
    xs, ys = [x for x, _ in corners], [y for _, y in corners]
    bbox = to_display((min(xs), min(ys), max(xs), max(ys)))
    left, bottom, right, top = page_box
    covered = _area(_clipped(corners, page_box))
    if not all(math.isfinite(value) for value in (*bbox, covered)):
        raise NonFiniteGeometry(f"its painted area is not finite (bbox {bbox}, area {covered})")
    # Float rounding can put a full-page image a hair over the page.
    return Placement(bbox, min(1.0, covered / ((right - left) * (top - bottom))))


def _clipped(polygon: list[Point], box: Box) -> list[Point]:
    """A convex ``polygon`` clipped to ``box`` (Sutherland-Hodgman, one edge of the box at a time).

    Each pass adds at most one vertex, so 4 corners never become more than 8.
    """
    left, bottom, right, top = box
    for axis, bound, keep_above in ((0, left, True), (0, right, False), (1, bottom, True), (1, top, False)):
        polygon = _clipped_to_half_plane(polygon, axis, bound, keep_above)
    return polygon


def _clipped_to_half_plane(polygon: list[Point], axis: int, bound: float, keep_above: bool) -> list[Point]:
    """The part of ``polygon`` whose coordinate ``axis`` is >= ``bound`` (``keep_above``) or <= it."""

    def inside(point: Point) -> bool:
        return point[axis] >= bound if keep_above else point[axis] <= bound

    clipped = []
    for start, end in zip(polygon, polygon[1:] + polygon[:1]):
        if inside(start) != inside(end):
            # On opposite sides, so the coordinates differ: no division by zero.
            t = (bound - start[axis]) / (end[axis] - start[axis])
            clipped.append((start[0] + t * (end[0] - start[0]), start[1] + t * (end[1] - start[1])))
        if inside(end):
            clipped.append(end)
    return clipped


def _area(polygon: list[Point]) -> float:
    """The area of a simple polygon (shoelace formula); 0 for fewer than 3 vertices."""
    ring = zip(polygon, polygon[1:] + polygon[:1])
    return abs(sum(x0 * y1 - x1 * y0 for (x0, y0), (x1, y1) in ring)) / 2


def read_raw_stream(image: pdfium_c.FPDF_PAGEOBJECT, label: str) -> bytes:
    """An image object's raw stream (``FPDFImageObj_GetImageDataRaw``), within the asset limit.

    pdfium reads an empty stream, and a failure to read one, as length 0:
    both are empty here.

    Raises:
        OversizedStream: If the stream is over ``_MAX_ASSET_BYTES``.
        PdfiumFailure: If pdfium copies a different length than it reported.
    """
    size = pdfium_c.FPDFImageObj_GetImageDataRaw(image, None, 0)
    if size > _MAX_ASSET_BYTES:
        raise OversizedStream(
            f"the raw stream of {label} is {size} bytes, over the {_MAX_ASSET_BYTES}-byte asset limit"
        )
    if size == 0:
        return b""
    buffer = ctypes.create_string_buffer(size)
    if pdfium_c.FPDFImageObj_GetImageDataRaw(image, buffer, size) != size:
        raise PdfiumFailure(f"read the raw stream of {label}")
    return buffer.raw


def _filters(image: pdfium_c.FPDF_PAGEOBJECT, label: str) -> tuple[str, ...]:
    """An image object's filter names in decoding order.

    pdfium returns each name as UTF-8; bytes that are not UTF-8 are kept as
    visible ``\\xNN`` escapes rather than replaced.
    """
    count = pdfium_c.FPDFImageObj_GetImageFilterCount(image)
    if count < 0:
        raise PdfiumFailure(f"count the filters of {label}")
    names = []
    for index in range(count):
        size = pdfium_c.FPDFImageObj_GetImageFilter(image, index, None, 0)
        if not 1 <= size <= _MAX_FILTER_NAME_BYTES:
            raise PdfiumFailure(f"read filter {index} of {label} ({size} bytes; the bound is {_MAX_FILTER_NAME_BYTES})")
        buffer = ctypes.create_string_buffer(size)
        if pdfium_c.FPDFImageObj_GetImageFilter(image, index, buffer, size) != size:
            raise PdfiumFailure(f"read filter {index} of {label}")
        names.append(buffer.raw[: size - 1].decode("utf-8", errors="backslashreplace"))
    return tuple(names)


def _pixel_facts(
    image: pdfium_c.FPDF_PAGEOBJECT, page_raw: pdfium_c.FPDF_PAGE, label: str
) -> tuple[int, int, int | None, str | None]:
    """An image object's ``(width, height, bits_per_pixel, colorspace)`` from FPDFImageObj_GetImageMetadata.

    pdfium reads 0 bits per pixel and FPDF_COLORSPACE_UNKNOWN (0) when it
    cannot load the image; an image mask has bits but no colour space.
    """
    metadata = pdfium_c.FPDF_IMAGEOBJ_METADATA()
    if not pdfium_c.FPDFImageObj_GetImageMetadata(image, page_raw, metadata):
        raise PdfiumFailure(f"read the metadata of {label}")
    if not 0 <= metadata.colorspace <= len(COLORSPACES):
        raise PdfiumFailure(f"read the metadata of {label}: unknown colour space {metadata.colorspace}")
    colorspace = COLORSPACES[metadata.colorspace - 1] if metadata.colorspace else None
    return metadata.width, metadata.height, metadata.bits_per_pixel or None, colorspace


def decoded_png(image: pdfium_c.FPDF_PAGEOBJECT, label: str) -> bytes:
    """Decode an image object with pdfium at its own pixel size; return it as PNG.

    ``FPDFImageObj_GetBitmap`` decodes the image's samples through its
    filters and colour space (CMYK, Indexed and 1-bit images arrive as RGB
    or gray) and ignores its matrix and any soft mask or image mask, so the
    PNG has the embedded image's pixels, unscaled. The pixel count is checked
    against ``_MAX_DECODED_PIXELS`` before decoding and again on the bitmap.

    Raises:
        TooManyPixels: If the image (or the bitmap pdfium decodes) has more
            pixels than ``_MAX_DECODED_PIXELS``.
        PdfiumFailure: If pdfium cannot read the pixel size or decode the image.
    """
    width, height = ctypes.c_uint(), ctypes.c_uint()
    if not pdfium_c.FPDFImageObj_GetImagePixelSize(image, width, height):
        raise PdfiumFailure(f"read the pixel size of {label}")
    _check_pixel_count(width.value, height.value, label)
    bitmap = pdfium_c.FPDFImageObj_GetBitmap(image)
    if not bitmap:
        raise PdfiumFailure(f"decode {label}")
    try:
        pixels = _bitmap_image(bitmap, label)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)
    out = io.BytesIO()
    pixels.save(out, format="PNG", compress_level=_PNG_COMPRESS_LEVEL, optimize=False)
    return out.getvalue()


def _check_pixel_count(width: int, height: int, label: str) -> None:
    """Refuse an image of more than ``_MAX_DECODED_PIXELS`` pixels."""
    if width * height > _MAX_DECODED_PIXELS:
        raise TooManyPixels(
            f"{label} decodes to {width} x {height} = {width * height} pixels, "
            f"over the {_MAX_DECODED_PIXELS}-pixel bound"
        )


def _bitmap_image(bitmap: pdfium_c.FPDF_BITMAP, label: str) -> Image.Image:
    """A copy of a pdfium bitmap's pixels as a Pillow image."""
    width, height = pdfium_c.FPDFBitmap_GetWidth(bitmap), pdfium_c.FPDFBitmap_GetHeight(bitmap)
    stride, bitmap_format = pdfium_c.FPDFBitmap_GetStride(bitmap), pdfium_c.FPDFBitmap_GetFormat(bitmap)
    if bitmap_format not in _BITMAP_FORMATS:
        raise PdfiumFailure(f"decode {label}: unknown bitmap format {bitmap_format}")
    mode, raw_mode, pixel_bytes = _BITMAP_FORMATS[bitmap_format]
    if width < 1 or height < 1 or stride < width * pixel_bytes:
        raise PdfiumFailure(f"decode {label}: a {width} x {height} bitmap of stride {stride}")
    _check_pixel_count(width, height, label)
    buffer = pdfium_c.FPDFBitmap_GetBuffer(bitmap)
    if not buffer:
        raise PdfiumFailure(f"read the decoded pixels of {label}")
    data = ctypes.string_at(buffer, stride * height)
    return Image.frombytes(mode, (width, height), data, "raw", raw_mode, stride, 1)
