"""Pixel sizes from JPEG and JPEG 2000 headers, for the v5 pdfium engine (#365 T4).

pdfium decodes a DCTDecode image at the size its JPEG frame header (SOFn)
gives and a JPXDecode image at the size its codestream's SIZ marker gives,
whatever the image dictionary's /Width and /Height say (observed: a JPEG
declared 16 x 16 with a 2000 x 2000 frame decodes to 2000 x 2000). The
engine reads those headers here, from the stream bytes, before it lets
pdfium decode anything, so a decode bound applies to the size pdfium would
really allocate.

Pure: no pdfium. Every scan is bounded by the data's length and a marker or
box count. Internal: not exported from ``datagrunt`` or ``datagrunt.pdf``.
"""

import struct
from typing import NamedTuple

# pdfium decodes no image wider or taller than this (kMaxImageDimension;
# observed: /Width 131071 decodes, 131072 does not, and -5 reads 4294967291).
MAX_DIMENSION = 0x1FFFF

# JPEG markers that start a frame: SOF0-SOF15 except DHT (C4), JPG (C8) and DAC (CC).
_SOF_MARKERS = frozenset(range(0xC0, 0xD0)) - {0xC4, 0xC8, 0xCC}
# Markers without a length field: TEM, RST0-RST7, SOI, EOI.
_STANDALONE_MARKERS = frozenset({0x01, *range(0xD0, 0xDA)})
_START_OF_SCAN = 0xDA
# A JPEG holds a few dozen segments before its frame (APPn, DQT, DHT, COM); a
# 64 KB-chunked ICC profile adds one per chunk. Past this the data is refused.
_MAX_JPEG_MARKERS = 4096

# The JP2 signature box (ISO 15444-1 I.5.1); pdfium hands data that starts
# with it to OpenJPEG as a JP2 file, anything else as a raw J2K codestream.
_JP2_SIGNATURE = bytes.fromhex("0000000c6a5020200d0a870a")
_MAX_JP2_BOXES = 4096
# SOC then SIZ: the first two markers of every codestream.
_CODESTREAM_START = b"\xff\x4f\xff\x51"


class CodestreamSize(NamedTuple):
    """The size a codec decodes an image at, and its JPEG 2000 tile count (1 for JPEG)."""

    width: int
    height: int
    tiles: int


def jpeg_size(data: bytes) -> CodestreamSize | None:
    """The frame size of the JPEG in ``data``, or None if its headers cannot be read.

    Like pdfium, the JPEG starts at the first ``FF D8`` (SOI). Segments are
    walked to the first SOFn; a scan (SOS) before it, a malformed or
    truncated segment, or more than ``_MAX_JPEG_MARKERS`` segments, is None.
    The size is as written: a zero height (a DNL frame) reads as 0.
    """
    start = data.find(b"\xff\xd8")
    if start < 0:
        return None
    position = start + 2
    for _ in range(_MAX_JPEG_MARKERS):
        if position >= len(data) or data[position] != 0xFF:
            return None
        while position < len(data) and data[position] == 0xFF:  # fill bytes, bounded by the data
            position += 1
        if position + 3 > len(data):
            return None
        marker = data[position]
        if marker in _STANDALONE_MARKERS:
            position += 1
            continue
        (length,) = struct.unpack_from(">H", data, position + 1)
        if marker in (0x00, _START_OF_SCAN) or length < 2:
            return None
        if marker in _SOF_MARKERS:
            return _frame_size(data, position + 3, length)
        position += 1 + length
    return None


def _frame_size(data: bytes, body: int, length: int) -> CodestreamSize | None:
    """The size in a SOFn segment whose body (after its length) starts at ``body``."""
    # Precision (1), height (2), width (2), component count (1).
    if length < 8 or body + 6 > len(data):
        return None
    _precision, height, width, components = struct.unpack_from(">BHHB", data, body)
    return CodestreamSize(width, height, 1) if components >= 1 else None


def jpx_size(data: bytes) -> CodestreamSize | None:
    """The image size of the JPEG 2000 data, or None if its codestream header cannot be read.

    A JP2 file's first top-level ``jp2c`` box holds the codestream; anything
    else must be a raw codestream. The size is the SIZ marker's
    ``Xsiz - XOsiz`` by ``Ysiz - YOsiz``, with its tile count.
    """
    if data.startswith(_JP2_SIGNATURE):
        start = _jp2_codestream(data)
        return None if start is None else _siz_size(data, start)
    return _siz_size(data, 0)


def _jp2_codestream(data: bytes) -> int | None:
    """Where the first top-level ``jp2c`` box's content starts, or None."""
    position = 0
    for _ in range(_MAX_JP2_BOXES):
        if position + 8 > len(data):
            return None
        length, box_type = struct.unpack_from(">I4s", data, position)
        header = 8
        if length == 1:  # the length is the next 8 bytes (XLBox)
            if position + 16 > len(data):
                return None
            (length,) = struct.unpack_from(">Q", data, position + 8)
            header = 16
        elif length == 0:  # the box runs to the end of the data
            length = len(data) - position
        if box_type == b"jp2c":
            return position + header
        if length < header:
            return None
        position += length
    return None


def _siz_size(data: bytes, start: int) -> CodestreamSize | None:
    """The size in the SIZ marker of the codestream at ``start``."""
    # Lsiz Rsiz Xsiz Ysiz XOsiz YOsiz XTsiz YTsiz XTOsiz YTOsiz Csiz (ISO 15444-1 A.5.1).
    if data[start : start + 4] != _CODESTREAM_START or start + 42 > len(data):
        return None
    lsiz, _rsiz, xsiz, ysiz, xosiz, yosiz, xtsiz, ytsiz, xtosiz, ytosiz, csiz = struct.unpack_from(
        ">HHIIIIIIIIH", data, start + 4
    )
    if lsiz != 38 + 3 * csiz or csiz < 1 or xsiz <= xosiz or ysiz <= yosiz or not (xtsiz and ytsiz):
        return None
    if xtosiz >= xsiz or ytosiz >= ysiz:
        return None
    tiles = -(-(xsiz - xtosiz) // xtsiz) * -(-(ysiz - ytosiz) // ytsiz)
    return CodestreamSize(xsiz - xosiz, ysiz - yosiz, tiles)
