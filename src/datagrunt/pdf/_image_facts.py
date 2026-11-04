"""The image facts the v5 engine records and a figure's loader carries: no pdfium.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``;
:mod:`datagrunt.pdf._pdfium_images` (which reads them from pdfium) and
:mod:`datagrunt.pdf.pdfium_engine` re-export :class:`RawImage` and
:data:`COLORSPACES`. A figure's loader (:class:`~datagrunt.pdf._loaders.FigureRead`)
holds its image's facts, so the caller unpickles them without pdfium.
"""

from dataclasses import dataclass

from datagrunt.pdf._boxes import Box
from datagrunt.pdf._image_headers import MAX_DIMENSION
from datagrunt.pdf._validate import int_at_least, ordered_box, sha256_hex, str_tuple

__all__ = ["COLORSPACES", "RawImage"]

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
            stream (alone, behind ASCII85 / ASCIIHex, or behind one
            FlateDecode that inflates whole within 1 MiB to exactly the
            stream pdfium decodes) its own header's
            (JPEG frame, JPEG 2000 SIZ), which pdfium uses whatever
            ``/Width`` says; else ``declared_px``'s (a JBIG2 image's too:
            pdfium decodes it at ``/Width``, whatever its page information
            segment says). Kept when the image must not be decoded (over a
            bound: see ``ImageStats``). 0 when unknown: a side outside
            1..131071 (pdfium decodes no larger side), a header that cannot
            be read, a codestream behind other filters, or a stream not read
            (see ``raw_sha256``); the declared size never stands in for it.
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
            pdfium cannot load the image, when it must not be decoded, or
            not in this process (see ``ImageStats``), for JPX and JBIG2
            (pdfium decodes those in full to report it), and when the stream
            was not read.
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
