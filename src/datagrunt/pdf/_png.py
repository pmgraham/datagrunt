"""8-bit grey pixels as PNG, encoded unfiltered for speed: page renders and OCR bands.

Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``. The bundle,
``PDFWriter`` and the page loaders write every page render through it.

Pillow's PNG encoder tries every row filter on every row and keeps the
best: on a large-format page that search costs more than deflating the
pixels. A render is mostly flat paper with thin linework, which deflates
about as well unfiltered (filter type 0 on every row), so the image is
written that way, with zlib at level 1, in about half Pillow's time at
much the same size. It is lossless: a decoder reads back exactly the
pixels given.

The rows are deflated a block at a time (``_BLOCK`` bytes of pixels, at
least one row): no more than one block is held besides the pixels and
zlib's own window, and each block's output is written as one IDAT chunk.
"""

import struct
import zlib
from collections.abc import Callable

from datagrunt.pdf._validate import int_at_least

__all__ = ["grey_png", "write_grey_png"]

_SIGNATURE = b"\x89PNG\r\n\x1a\n"
# IHDR: 8 bits a sample, colour type 0 (grey), deflate, adaptive filtering method, no interlace.
_BIT_DEPTH = 8
_GREY = 0
_UNFILTERED = b"\x00"
_LEVEL = 1
# Pixel bytes deflated (and written as one IDAT chunk) at once.
_BLOCK = 1 << 20


def write_grey_png(write: Callable[[bytes], object], pixels: bytes, width: int, height: int) -> None:
    """Write ``pixels`` (``width`` x ``height``, one byte each, rows top down) as a PNG through ``write``.

    Raises:
        TypeError: If ``pixels`` is not bytes or a size is not an int.
        ValueError: If a size is under 1 or ``pixels`` is not ``width * height`` bytes.
    """
    int_at_least("width", width, 1)
    int_at_least("height", height, 1)
    if not isinstance(pixels, bytes):
        raise TypeError(f"pixels must be bytes, got {type(pixels).__name__}")
    if len(pixels) != width * height:
        raise ValueError(f"pixels must be {width} x {height} = {width * height} bytes, got {len(pixels)}")
    write(_SIGNATURE)
    write(_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, _BIT_DEPTH, _GREY, 0, 0, 0)))
    deflater = zlib.compressobj(_LEVEL)
    rows = max(1, _BLOCK // width)
    view = memoryview(pixels)
    for top in range(0, height, rows):  # bounded: one pass a block of rows
        block = bytearray()
        for row in range(top, min(top + rows, height)):  # bounded: the block's rows
            block += _UNFILTERED
            block += view[row * width : (row + 1) * width]
        data = deflater.compress(block)
        if data:
            write(_chunk(b"IDAT", data))
    write(_chunk(b"IDAT", deflater.flush()))
    write(_chunk(b"IEND", b""))


def grey_png(pixels: bytes, width: int, height: int) -> bytes:
    """``pixels`` (``width`` x ``height``, one byte each) as PNG bytes; raises as :func:`write_grey_png`."""
    parts: list[bytes] = []
    write_grey_png(parts.append, pixels, width, height)
    return b"".join(parts)


def _chunk(kind: bytes, data: bytes) -> bytes:
    """One PNG chunk: length, type, data and the CRC of type and data."""
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
