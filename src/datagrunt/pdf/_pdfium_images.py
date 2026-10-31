"""Image objects for the v5 pdfium engine (#365 T4).

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``;
:mod:`datagrunt.pdf.pdfium_engine` re-exports :class:`RawImage`,
:class:`ImageStats` and :class:`PageImages`, walks the page's objects and
hands each image object here with its composed page-space matrix. This
module never imports the engine.

No pixel is decoded while a page's images are collected: each stream is
copied out once (within a per-page budget) to hash it, JPEG and JPEG 2000
sizes are read from their own headers (:mod:`datagrunt.pdf._image_headers`),
and pdfium's metadata call, which decodes JPX in full, is made only for
codecs it reads without decoding. Pixels are decoded by
:class:`~datagrunt.pdf.pdfium_engine.ImageLoader`, one image at a time, after
the decode bounds (``_MAX_DECODED_PIXELS``, ``_MAX_JPX_TILES``) pass.

Memory for one decode at the pixel bound (``png_bytes`` of a 9400 x 9400
image, 88.4 M pixels, peak RSS over the interpreter's, measured on macOS
arm64 with pypdfium2 5.14 and Pillow 12): Flate RGB +590 MiB (pdfium's
inflated stream and BGR bitmap, then Pillow's 4-byte RGB copy; Pillow reads
gray bitmaps in place), baseline JPEG RGB +590 MiB, progressive JPEG RGB
+844 MiB (libjpeg buffers every coefficient), JPEG 2000 gray +429 MiB.
JPEG 2000 RGB measured +666 MiB at 6000 x 6000 (19 bytes a pixel:
OpenJPEG keeps 4-byte samples per component), so about 1.7 GiB at the
bound: the costliest decode a worker can be asked for.
"""

import ctypes
import hashlib
import io
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, NamedTuple

import pypdfium2.raw as pdfium_c
from PIL import Image

from datagrunt.pdf._image_headers import MAX_DIMENSION, CodestreamSize, jpeg_size, jpx_size
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
        px_width: The width pdfium decodes the image at: for a DCT or JPX
            stream (alone or behind ASCII85 / ASCIIHex) its own header's
            (JPEG frame, JPEG 2000 SIZ), which pdfium uses whatever
            ``/Width`` says; else ``declared_px``'s. 0 when unknown: a side
            outside 1..131071 (pdfium decodes no larger side), a header that
            cannot be read, a codestream behind other filters, or a stream
            not read (see ``raw_sha256``).
        px_height: The height, likewise.
        declared_px: The image dictionary's ``(/Width, /Height)`` as pdfium
            reads them, each 0 when missing or outside 1..131071 (pdfium reads
            a negative /Width modulo 2**32).
        object_index: The image object's position in the page's depth-first,
            paint-order object walk (forms count, then their contents): the
            same index a span or segment uses.
        raw_sha256: SHA-256 of the image's raw embedded stream: its bytes
            as stored in the PDF, still encoded by ``filters`` (decrypted, if
            the PDF is encrypted). Stable across encoder upgrades, and equal
            for every object drawing the same stream. None when the stream
            was not read: it is over the asset limit (1 GiB), or the page's
            stream budget was spent (see :class:`ImageStats`).
        filters: The stream's filters in decoding order, as pdfium names them
            (an inline image's abbreviation ``/AHx`` reads ``ASCIIHexDecode``).
        bits_per_pixel: Bits per pixel of the image as pdfium decodes it (24
            for 8-bit RGB, and for CMYK, which pdfium decodes to RGB; 8 for
            8-bit Indexed; 1 for 1-bit gray and image masks). None when
            pdfium cannot load the image, when it must not be decoded (see
            ``ImageStats``), for JPX and JBIG2 (pdfium decodes those in full
            to report it), and when the stream was not read.
        colorspace: The colour space family, one of :data:`COLORSPACES`, or
            None when there is none (an image mask), or, as for
            ``bits_per_pixel``, it is not read.
    """

    bbox: Box
    px_width: int
    px_height: int
    declared_px: tuple[int, int]
    object_index: int
    raw_sha256: str | None
    filters: tuple[str, ...]
    bits_per_pixel: int | None
    colorspace: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "bbox", ordered_box("bbox", self.bbox))
        _check_dimension("px_width", self.px_width)
        _check_dimension("px_height", self.px_height)
        _check_declared(self.declared_px)
        int_at_least("object_index", self.object_index, 0)
        if self.raw_sha256 is not None:
            sha256_hex("raw_sha256", self.raw_sha256)
        # Names as pdfium reports them, kept verbatim (an empty name included).
        str_tuple("filters", self.filters, allow_blank=True)
        if self.bits_per_pixel is not None:
            int_at_least("bits_per_pixel", self.bits_per_pixel, 1)
        _check_colorspace(self.colorspace)


def _check_dimension(name: str, value: object) -> None:
    """Require ``value`` to be an int in 0..MAX_DIMENSION (0: unknown)."""
    int_at_least(name, value, 0)
    if isinstance(value, int) and value > MAX_DIMENSION:
        raise ValueError(f"{name} must be <= {MAX_DIMENSION}, got {value}")


def _check_declared(value: object) -> None:
    """Require ``value`` to be a (width, height) pair of dimensions."""
    if not isinstance(value, tuple):
        raise TypeError(f"declared_px must be a tuple, got {type(value).__name__}")
    if len(value) != 2:
        raise ValueError(f"declared_px must have 2 values, got {len(value)}")
    for index, item in enumerate(value):
        _check_dimension(f"declared_px[{index}]", item)


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

    Only ``skipped_nonfinite_images`` counts images left out of
    :attr:`PageImages.images`; the other counts are of images recorded with
    some facts skipped (None or 0), and an image adds to at most one of them.

    Attributes:
        skipped_nonfinite_images: Image objects skipped because their matrix
            to page space is not finite (pdfium keeps the CTM in 32-bit
            floats, so five ``1e9`` scales overflow it); an image inside a
            form whose matrix overflowed counts here.
        skipped_oversized_images: Images too big to decode or read, recorded
            without pixel metadata: a raw stream over the asset limit (1 GiB;
            not read, so no hash and no size either), a decode size over
            89,478,485 pixels, or a JPEG 2000 codestream in over 4096 tiles.
            :class:`~datagrunt.pdf.pdfium_engine.ImageLoader` refuses to
            decode them.
        skipped_unsized_images: Images whose decode size cannot be known
            without decoding, recorded without pixel metadata (size 0 where
            unknown): a dimension outside 1..131071, an unreadable JPEG or
            JPEG 2000 header, or such a codestream behind a filter other than
            ASCII85 / ASCIIHex (a Flate one could be a bomb). Never decoded.
        skipped_over_budget_images: Images recorded without hash, size or
            pixel metadata because the page's stream budget (2 GiB of stream
            bytes copied out of pdfium, length queries included) was spent:
            one stream drawn 33,000 times cannot cost 33,000 reads.
        max_cover_fraction: The largest fraction, 0 to 1, of the visible page
            (CropBox within MediaBox) that one image's painted area covers:
            its unit square under its matrix, clipped to the visible page
            (clipping paths and masks are not applied). Oversized images
            count; skipped non-finite ones do not. 0.0 for a page without
            images. A3 routes a page as hybrid when a raster covers over half.
    """

    skipped_nonfinite_images: int
    skipped_oversized_images: int
    skipped_unsized_images: int
    skipped_over_budget_images: int
    max_cover_fraction: float

    def __post_init__(self) -> None:
        int_at_least("skipped_nonfinite_images", self.skipped_nonfinite_images, 0)
        int_at_least("skipped_oversized_images", self.skipped_oversized_images, 0)
        int_at_least("skipped_unsized_images", self.skipped_unsized_images, 0)
        int_at_least("skipped_over_budget_images", self.skipped_over_budget_images, 0)
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


class DecodeRefused(ValueError):
    """pdfium must not decode an image: its decoded size is over a bound, or cannot be known first."""


class TooManyPixels(DecodeRefused):
    """An image decodes to more than ``_MAX_DECODED_PIXELS`` pixels."""


# PDF names are at most 127 bytes in practice (PDF 32000-1, Annex C); this
# bound, NUL included, only stops a hostile length from sizing the buffer.
_MAX_FILTER_NAME_BYTES = 4096

# The most pixels one image is decoded to: 1 GiB / 4 / 3, which is Pillow's
# default Image.MAX_IMAGE_PIXELS (its decompression-bomb bound), pinned here
# so a caller's Pillow setting cannot lift it. pdfium's bitmap is then at
# most a third of the asset limit (4 bytes a pixel).
_MAX_DECODED_PIXELS = _MAX_ASSET_BYTES // 4 // 3

# OpenJPEG allocates ~12 KB per tile while it reads a JPX header (observed:
# a 2000 x 2000 codestream in 15625 tiles of 16 x 16 took 234 MB, and one in
# 62500 tiles 761 MB), before any pixel is decoded. 4096 tiles cost ~50 MB
# and admit tiles of 148 x 148 or larger over a full-bound image.
_MAX_JPX_TILES = 4096
_MAX_JPX_COMPONENTS = 4
_MAX_DECODED_SAMPLES = _MAX_DECODED_PIXELS * 4

# An ASCII-wrapped (ASCII85 / ASCIIHex) DCT or JPX stream at most this long
# is decoded by pdfium to read its header: ASCII85's "z" turns 1 byte into 4,
# so the decoded stream stays within a quarter of the asset limit.
_MAX_ASCII_WRAPPED_BYTES = _MAX_ASSET_BYTES // 16

# The stream bytes one page may copy out of pdfium. A length query copies a
# stream as a read does (observed: 4 ms against 8 ms for a 200 MB stream),
# and reading and hashing runs at ~2.5 GB/s here, so this bounds a page's
# image reads to about a second while admitting a maximal (asset-limit)
# stream and as much again: without it, 33,000 draws of one 20 MB stream
# would re-read 660 GB. pdfium's API names no image stream's object, so two
# draws of one XObject cannot be told apart to read it once.
_MAX_PAGE_STREAM_BYTES = 2 * _MAX_ASSET_BYTES

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

# Codecs that pdfium decodes at their own header's size, and the reader of that header.
_HEADER_READERS: MappingProxyType[str, Callable[[bytes], CodestreamSize | None]] = MappingProxyType(
    {"DCTDecode": jpeg_size, "JPXDecode": jpx_size}
)
# Filters in front of such a codec that pdfium may decode to reach its header:
# ASCII85 and ASCIIHex grow data at most fourfold (Flate or LZW could be a bomb).
_ASCII_FILTERS = frozenset({"ASCII85Decode", "ASCIIHexDecode"})
_SIMPLE_FILTERS = frozenset({*_ASCII_FILTERS, "LZWDecode", "FlateDecode", "RunLengthDecode", "CCITTFaxDecode"})
# Codecs pdfium decodes in full to report an image's metadata (observed for
# JPX: 0.14 s and 78 MB at 9000 x 9000); JBIG2 is treated alike, unobserved
# (no encoder here). Their bits per pixel and colour space are never read.
_DECODED_FOR_METADATA = frozenset({"JPXDecode", "JBIG2Decode"})


class Placement(NamedTuple):
    """Where an image is painted: its displayed bbox and the fraction of the visible page it covers."""

    bbox: Box
    cover_fraction: float


Skip = Literal["oversized", "unsized", "over_budget"]


class PixelFacts(NamedTuple):
    """What an image decodes to, read without decoding it.

    ``refusal`` says why pdfium must not decode it (None: it may), with the
    ImageStats count it falls under; ``read_metadata`` whether the bits per
    pixel and colour space came from pdfium.
    """

    px_width: int
    px_height: int
    declared_px: tuple[int, int]
    bits_per_pixel: int | None
    colorspace: str | None
    refusal: tuple[Skip, str] | None
    read_metadata: bool


class _PageBudget:
    """The stream bytes a page may still copy out of pdfium (``_MAX_PAGE_STREAM_BYTES`` at first)."""

    __slots__ = ("remaining",)

    def __init__(self) -> None:
        self.remaining = _MAX_PAGE_STREAM_BYTES

    def charge(self, size: int) -> bool:
        """Charge ``size`` bytes; if they do not fit, spend all that is left and return False."""
        fits = size <= self.remaining
        self.remaining = self.remaining - size if fits else 0
        return fits


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
    finite is skipped and counted; every other image is recorded, its stream
    hash and pixel facts left out (and counted) where reading them is
    refused (see :class:`ImageStats`). No pixel is decoded.
    """
    images: list[RawImage] = []
    counts = dict.fromkeys(("nonfinite", "oversized", "unsized", "over_budget"), 0)
    cover = 0.0
    budget = _PageBudget()
    for index, image, matrix in placed:
        try:
            placement = image_placement(matrix, page_box, to_display)
        except NonFiniteGeometry:
            counts["nonfinite"] += 1
            continue
        cover = max(cover, placement.cover_fraction)
        recorded, skip = _page_image(image, page_raw, index, placement.bbox, budget)
        images.append(recorded)
        if skip is not None:
            counts[skip] += 1
    stats = ImageStats(
        skipped_nonfinite_images=counts["nonfinite"],
        skipped_oversized_images=counts["oversized"],
        skipped_unsized_images=counts["unsized"],
        skipped_over_budget_images=counts["over_budget"],
        max_cover_fraction=cover,
    )
    return PageImages(images=tuple(images), stats=stats)


def _page_image(
    image: pdfium_c.FPDF_PAGEOBJECT, page_raw: pdfium_c.FPDF_PAGE, index: int, bbox: Box, budget: _PageBudget
) -> tuple[RawImage, Skip | None]:
    """Image object ``index`` as a RawImage, and the ImageStats count it adds to (None for none)."""
    label = f"object {index}"
    filters = read_filters(image, label)
    declared = declared_size(image, label)
    unread = RawImage(
        bbox=bbox,
        px_width=0,
        px_height=0,
        declared_px=declared,
        object_index=index,
        raw_sha256=None,
        filters=filters,
        bits_per_pixel=None,
        colorspace=None,
    )
    if budget.remaining == 0:
        return unread, "over_budget"
    size = pdfium_c.FPDFImageObj_GetImageDataRaw(image, None, 0)  # copies the stream, so it is charged
    fits = budget.charge(size)
    if size > _MAX_ASSET_BYTES:
        return unread, "oversized"
    if not fits:
        return unread, "over_budget"
    raw = _copied_raw_stream(image, size, label)

    def charged_ascii_decoded() -> bytes | None:
        data = ascii_decoded_stream(image, size, label)
        budget.charge(len(data or b""))
        return data

    facts = pixel_facts(image, page_raw, label, filters, declared, raw, charged_ascii_decoded)
    recorded = RawImage(
        bbox=bbox,
        px_width=facts.px_width,
        px_height=facts.px_height,
        declared_px=declared,
        object_index=index,
        raw_sha256=hashlib.sha256(raw).hexdigest(),
        filters=filters,
        bits_per_pixel=facts.bits_per_pixel,
        colorspace=facts.colorspace,
    )
    return recorded, None if facts.refusal is None else facts.refusal[0]


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

    Raises:
        OversizedStream: If the stream is over ``_MAX_ASSET_BYTES``.
        PdfiumFailure: If pdfium copies a different length than it reported.
    """
    size = pdfium_c.FPDFImageObj_GetImageDataRaw(image, None, 0)
    if size > _MAX_ASSET_BYTES:
        raise OversizedStream(
            f"the raw stream of {label} is {size} bytes, over the {_MAX_ASSET_BYTES}-byte asset limit"
        )
    return _copied_raw_stream(image, size, label)


def _copied_raw_stream(image: pdfium_c.FPDF_PAGEOBJECT, size: int, label: str) -> bytes:
    """Copy the ``size``-byte raw stream; pdfium reads an empty stream, and a failure to read one, as 0 bytes."""
    if size == 0:
        return b""
    buffer = ctypes.create_string_buffer(size)
    if pdfium_c.FPDFImageObj_GetImageDataRaw(image, buffer, size) != size:
        raise PdfiumFailure(f"read the raw stream of {label}")
    return buffer.raw


def ascii_decoded_stream(image: pdfium_c.FPDF_PAGEOBJECT, raw_size: int, label: str) -> bytes | None:
    """The stream after pdfium's ASCII85 / ASCIIHex decoding, which a DCT or JPX decoder reads.

    None when the raw stream is over ``_MAX_ASCII_WRAPPED_BYTES``: pdfium is
    then not asked to decode it.
    """
    if raw_size > _MAX_ASCII_WRAPPED_BYTES:
        return None
    size = pdfium_c.FPDFImageObj_GetImageDataDecoded(image, None, 0)
    if size == 0 or size > _MAX_ASSET_BYTES:
        return None
    buffer = ctypes.create_string_buffer(size)
    if pdfium_c.FPDFImageObj_GetImageDataDecoded(image, buffer, size) != size:
        raise PdfiumFailure(f"read the decoded stream of {label}")
    return buffer.raw


def read_filters(image: pdfium_c.FPDF_PAGEOBJECT, label: str) -> tuple[str, ...]:
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


def _dimension(value: int) -> int:
    """``value`` if pdfium can decode a side that long (1..MAX_DIMENSION), else 0: unknown."""
    return value if 1 <= value <= MAX_DIMENSION else 0


def declared_size(image: pdfium_c.FPDF_PAGEOBJECT, label: str) -> tuple[int, int]:
    """The image dictionary's (/Width, /Height), as pdfium reads them without decoding.

    pdfium reads a negative size modulo 2**32 (-5 reads 4294967291) and
    decodes no side over MAX_DIMENSION: such a side, a missing one and 0 are
    0 here.
    """
    width, height = ctypes.c_uint(), ctypes.c_uint()
    if not pdfium_c.FPDFImageObj_GetImagePixelSize(image, width, height):
        raise PdfiumFailure(f"read the pixel size of {label}")
    return _dimension(width.value), _dimension(height.value)


def pixel_facts(
    image: pdfium_c.FPDF_PAGEOBJECT,
    page_raw: pdfium_c.FPDF_PAGE,
    label: str,
    filters: tuple[str, ...],
    declared: tuple[int, int],
    raw: bytes,
    ascii_decoded: Callable[[], bytes | None],
) -> PixelFacts:
    """What image object ``image`` decodes to, without decoding it.

    The size is the codestream header's for a DCT or JPX stream (alone or
    behind ASCII filters, decoded by ``ascii_decoded``), else ``declared``.
    pdfium's metadata (bits per pixel, colour space) is read only when the
    image may be decoded and the codec reads it without decoding.
    """
    width, height, refusal = _decode_size(filters, declared, raw, ascii_decoded, label)
    read_metadata = refusal is None and not _DECODED_FOR_METADATA.intersection(filters)
    bits_per_pixel, colorspace = _metadata(image, page_raw, label) if read_metadata else (None, None)
    return PixelFacts(width, height, declared, bits_per_pixel, colorspace, refusal, read_metadata)


def _decode_size(
    filters: tuple[str, ...],
    declared: tuple[int, int],
    raw: bytes,
    ascii_decoded: Callable[[], bytes | None],
    label: str,
) -> tuple[int, int, tuple[Skip, str] | None]:
    """The (width, height) pdfium decodes an image at, and why it must not (None: it may)."""
    codec = filters[-1] if filters else ""
    reader = _HEADER_READERS.get(codec)
    tiles, components = 1, 1
    if reader is None:
        if not _SIMPLE_FILTERS.issuperset(filters):
            return 0, 0, ("unsized", f"{label} uses filter {codec!r}: its decode size cannot be read without decoding")
        width, height = declared
    else:
        wrappers = filters[:-1]
        if not _ASCII_FILTERS.issuperset(wrappers):
            behind = ", ".join(wrappers)
            message = f"{label} is a {codec} stream behind {behind}: its size cannot be read without decoding"
            return 0, 0, ("unsized", message)
        data = ascii_decoded() if wrappers else raw
        header = None if data is None else reader(data)
        if header is None:
            return 0, 0, ("unsized", f"{label} has a {codec} header that cannot be read")
        if header.components > _MAX_JPX_COMPONENTS:
            message = f"{label} has {header.components} components, over the {_MAX_JPX_COMPONENTS}-component bound"
            return 0, 0, ("oversized", message)
        width, height = _dimension(header.width), _dimension(header.height)
        tiles, components = header.tiles, header.components
    if not (width and height):
        return width, height, ("unsized", f"{label} has no pixel size pdfium can decode ({width} x {height})")
    if width * height > _MAX_DECODED_PIXELS:
        return width, height, ("oversized", _too_many_pixels(width, height, label))
    if width * height * components > _MAX_DECODED_SAMPLES:
        samples = width * height * components
        message = (
            f"{label} is {width} x {height} x {components} "
            f"({samples} decoded samples, over the {_MAX_DECODED_SAMPLES}-sample bound)"
        )
        return 0, 0, ("oversized", message)
    if tiles > _MAX_JPX_TILES:
        return width, height, ("oversized", f"{label} decodes in {tiles} tiles, over the {_MAX_JPX_TILES}-tile bound")
    return width, height, None


def _too_many_pixels(width: int, height: int, label: str) -> str:
    """The refusal of an image of more than ``_MAX_DECODED_PIXELS`` pixels."""
    pixels = width * height
    return f"{label} decodes to {width} x {height} = {pixels} pixels, over the {_MAX_DECODED_PIXELS}-pixel bound"


def _metadata(
    image: pdfium_c.FPDF_PAGEOBJECT, page_raw: pdfium_c.FPDF_PAGE, label: str
) -> tuple[int | None, str | None]:
    """An image object's ``(bits_per_pixel, colorspace)`` from FPDFImageObj_GetImageMetadata.

    pdfium reads 0 bits per pixel and FPDF_COLORSPACE_UNKNOWN (0) when it
    cannot load the image; an image mask has bits but no colour space.
    """
    metadata = pdfium_c.FPDF_IMAGEOBJ_METADATA()
    if not pdfium_c.FPDFImageObj_GetImageMetadata(image, page_raw, metadata):
        raise PdfiumFailure(f"read the metadata of {label}")
    if not 0 <= metadata.colorspace <= len(COLORSPACES):
        raise PdfiumFailure(f"read the metadata of {label}: unknown colour space {metadata.colorspace}")
    colorspace = COLORSPACES[metadata.colorspace - 1] if metadata.colorspace else None
    return metadata.bits_per_pixel or None, colorspace


def decoded_png(image: pdfium_c.FPDF_PAGEOBJECT, facts: PixelFacts, label: str) -> bytes:
    """Decode an image object with pdfium at its own pixel size; return it as PNG.

    ``facts`` (from :func:`pixel_facts`) must allow decoding. pdfium's
    ``FPDFImageObj_GetBitmap`` decodes the image's samples through its filters
    and colour space (CMYK, Indexed and 1-bit images arrive as RGB or gray)
    and ignores its matrix and any soft mask or image mask, so the PNG has
    the embedded image's pixels, unscaled. The bitmap's pixel count is
    checked again, and Pillow reads its pixels in place (no copy).

    Raises:
        DecodeRefused: If ``facts`` refuse decoding, or the bitmap pdfium
            decodes has more pixels than ``_MAX_DECODED_PIXELS``.
        PdfiumFailure: If pdfium cannot decode the image.
    """
    if facts.refusal is not None:
        raise DecodeRefused(facts.refusal[1])
    bitmap = pdfium_c.FPDFImageObj_GetBitmap(image)
    if not bitmap:
        raise PdfiumFailure(f"decode {label}")
    try:
        return _bitmap_png(bitmap, label)
    finally:
        pdfium_c.FPDFBitmap_Destroy(bitmap)


def _bitmap_png(bitmap: pdfium_c.FPDF_BITMAP, label: str) -> bytes:
    """A pdfium bitmap's pixels as PNG bytes; the bitmap must outlive this call (Pillow reads it in place)."""
    width, height = pdfium_c.FPDFBitmap_GetWidth(bitmap), pdfium_c.FPDFBitmap_GetHeight(bitmap)
    stride, bitmap_format = pdfium_c.FPDFBitmap_GetStride(bitmap), pdfium_c.FPDFBitmap_GetFormat(bitmap)
    if bitmap_format not in _BITMAP_FORMATS:
        raise PdfiumFailure(f"decode {label}: unknown bitmap format {bitmap_format}")
    mode, raw_mode, pixel_bytes = _BITMAP_FORMATS[bitmap_format]
    if width < 1 or height < 1:
        raise PdfiumFailure(f"decode {label}: a {width} x {height} bitmap of stride {stride}")
    if width * height > _MAX_DECODED_PIXELS:
        raise TooManyPixels(_too_many_pixels(width, height, label))
    if stride < width * pixel_bytes:
        raise PdfiumFailure(f"decode {label}: a {width} x {height} bitmap of stride {stride}")
    buffer = pdfium_c.FPDFBitmap_GetBuffer(bitmap)
    if not buffer:
        raise PdfiumFailure(f"read the decoded pixels of {label}")
    pixels = (ctypes.c_ubyte * (stride * height)).from_address(buffer)
    out = io.BytesIO()
    Image.frombuffer(mode, (width, height), pixels, "raw", raw_mode, stride, 1).save(
        out, format="PNG", compress_level=_PNG_COMPRESS_LEVEL, optimize=False
    )
    return out.getvalue()
