"""Lossless 8-bit grayscale PNG encoder using filter-type 0 and zlib level 1 for fast page/band renders."""

from __future__ import annotations

import io
import struct
import zlib
from collections.abc import Callable

from datagrunt.pdf._validate import int_at_least

__all__ = ["grey_png_bytes", "write_grey_png"]

_PNG_SIG = b"\x89PNG\r\n\x1a\n"
_TARGET_BLOCK_BYTES = 1 << 20


def _chunk(tag: bytes, body: bytes) -> bytes:
    crc = zlib.crc32(body, zlib.crc32(tag)) & 0xFFFFFFFF
    return struct.pack(">I", len(body)) + tag + body + struct.pack(">I", crc)


_IEND_CHUNK = _chunk(b"IEND", b"")


def write_grey_png(
    write: Callable[[bytes], object],
    width: int,
    height: int,
    pixels: bytes,
) -> None:
    """Write ``width`` x ``height`` 8-bit grayscale ``pixels`` as a valid lossless PNG via ``write``."""
    int_at_least("width", width, 1)
    int_at_least("height", height, 1)
    if not isinstance(pixels, (bytes, bytearray)):
        raise TypeError(f"pixels must be bytes or bytearray, got {type(pixels).__name__}")
    expected = width * height
    if len(pixels) != expected:
        raise ValueError(f"pixels length {len(pixels)} does not match {width} x {height} ({expected})")

    raw_bytes = bytes(pixels) if isinstance(pixels, bytearray) else pixels
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0)
    write(_PNG_SIG)
    write(_chunk(b"IHDR", ihdr))

    rows_per_block = max(1, _TARGET_BLOCK_BYTES // (width + 1))
    compressor = zlib.compressobj(level=1)
    for start_row in range(0, height, rows_per_block):
        end_row = min(height, start_row + rows_per_block)
        scanlines = b"\x00" + b"\x00".join(
            raw_bytes[row * width : (row + 1) * width] for row in range(start_row, end_row)
        )
        compressed = compressor.compress(scanlines)
        if compressed:
            write(_chunk(b"IDAT", compressed))
    tail = compressor.flush()
    if tail:
        write(_chunk(b"IDAT", tail))
    write(_IEND_CHUNK)


def grey_png_bytes(width: int, height: int, pixels: bytes) -> bytes:
    """Return ``width`` x ``height`` 8-bit grayscale ``pixels`` encoded as lossless PNG bytes."""
    buf = io.BytesIO()
    write_grey_png(buf.write, width, height, pixels)
    return buf.getvalue()
