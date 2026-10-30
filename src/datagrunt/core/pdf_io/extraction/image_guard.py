"""Bound image decoding by codestream size, before any engine decodes a page.

A JPEG 2000 (``/JPXDecode``) image may declare a small ``/Width`` ``/Height``
while its codestream's SIZ marker describes a huge image; pdfium and MuPDF
decode the whole codestream at the SIZ size when datagrunt merely reads the
image's metadata, extracts it, or renders a page that contains it (observed:
a ~5 KB PDF reaching 1.36 GB with pdfium and 2.0 GB with pymupdf). JBIG2
(``/JBIG2Decode``) is decoded by MuPDF at its page-information segment size,
likewise ignoring ``/Width`` ``/Height``.

This guard reads those headers from the raw stream bytes and decides, before
any decode, whether an image is safe to touch. It FAILS CLOSED: an image is
safe to touch only when every filter is recognised and no readable header
puts the decode over a documented bound. An unrecognised filter, an
unreadable header, or a header over a bound is skipped (never decoded).

JPEG (``/DCTDecode``) is not affected on any datagrunt v4 path (pdfium and
MuPDF write its stream out undecoded, and a page render scales it within its
placement box), so a DCT image is left to the engine unchanged, as before.

Pure: no pdfium / pymupdf import. Every scan is bounded by the data's length
and a marker, box or segment count.
"""

from typing import NamedTuple

from datagrunt.pdf._image_headers import jpx_size

# pdfium/Pillow decode no image to more than this many pixels: 1 GiB / 4 / 3,
# Pillow's default decompression-bomb bound (Image.MAX_IMAGE_PIXELS). Read from
# the v5 engine (commit 4f21699, ``_pdfium_images._MAX_DECODED_PIXELS``), pinned
# here so a caller's Pillow setting cannot lift it.
_MAX_ASSET_BYTES = 1 << 30  # 1 GiB (v5 ``elements._MAX_ASSET_BYTES``)
_MAX_DECODED_PIXELS = _MAX_ASSET_BYTES // 4 // 3  # 89,478,485

# A JPEG 2000 codestream's memory scales with width x height x components, so
# the sample budget bounds that product. Equivalent to the pixel cap at 4
# components; any codestream with more than 4 components is skipped outright.
_MAX_DECODED_SAMPLES = _MAX_DECODED_PIXELS * 4
_MAX_JPX_COMPONENTS = 4

# OpenJPEG allocates ~12 KB per tile while it reads a JPX header, before any
# pixel is decoded (v5 ``_pdfium_images._MAX_JPX_TILES``). 4096 tiles cost
# ~50 MB and admit tiles of 148 x 148 or larger over a full-bound image.
_MAX_JPX_TILES = 4096

# The most raw stream bytes this guard copies out of one image to read its
# header. A stream longer than this is not a header we can parse cheaply; it
# is skipped (unsized). Bounds a hostile length from forcing a huge copy.
_MAX_RAW_HEADER_BYTES = _MAX_ASSET_BYTES

# JBIG2 segment walk: an embedded stream holds a bounded run of segments; past
# this the data is refused (fail closed).
_MAX_JBIG2_SEGMENTS = 4096
_JBIG2_PAGE_INFO = 48  # the page-information segment type (ISO/IEC 14492 7.4.8)
# Region segments carry a 17-byte region-information field (width x height x
# location x flags) at the start of their data: immediate generic/text/halftone/
# refinement regions and their "intermediate" forms (ISO/IEC 14492 7.4.1).
_JBIG2_REGION_TYPES = frozenset({4, 6, 7, 20, 22, 23, 36, 38, 39, 40, 42, 43})
# Segments that start with a bitmap width x height (region info / page info).
_JBIG2_SIZED_TYPES = _JBIG2_REGION_TYPES | {_JBIG2_PAGE_INFO}
# In a PDF-embedded JBIG2 stream the (single) page is page 1.
_JBIG2_EMBEDDED_PAGE = 1
# A JBIG2 dimension of 0xFFFFFFFF means "unknown" (striped); it is unsized.
_JBIG2_UNKNOWN = 0xFFFFFFFF

# Filter names, normalised, that do not decode an image to a raster bigger than
# its declared /Width x /Height (so pdfium/MuPDF bound them by the declared
# size, not by the stream): the simple stream filters and CCITT fax. DCTDecode
# is treated the same on v4 paths (its stream is written out undecoded, and a
# page render scales it within its box), so it is left to the engine unchanged.
_NON_IMAGE_FILTERS = frozenset(
    {
        "ASCIIHexDecode",
        "ASCII85Decode",
        "LZWDecode",
        "FlateDecode",
        "RunLengthDecode",
        "CCITTFaxDecode",
        "DCTDecode",
    }
)

# Filters that pdfium/MuPDF may decode to reach a codec's header (they grow data
# at most fourfold); any other wrapper in front of a codec means the codec's
# header cannot be read without a full decode, so the image is skipped.
_ASCII_FILTERS = frozenset({"ASCIIHexDecode", "ASCII85Decode"})

# PDF stream-filter abbreviations (used on inline images; normalised here so a
# match is on the full name whichever form the PDF or engine reports).
_FILTER_ABBREVIATIONS = {
    "AHx": "ASCIIHexDecode",
    "A85": "ASCII85Decode",
    "LZW": "LZWDecode",
    "Fl": "FlateDecode",
    "RL": "RunLengthDecode",
    "CCF": "CCITTFaxDecode",
    "DCT": "DCTDecode",
}


class GuardDecision(NamedTuple):
    """Whether an image is safe for an engine to touch, and why not when it is not.

    ``skip`` is True when the image must not be decoded; ``reason`` then names
    the page-level warning to record. A safe image has ``skip=False`` and
    ``reason=None``.
    """

    skip: bool
    reason: str | None


_SAFE = GuardDecision(skip=False, reason=None)


def normalize_filter(name: str) -> str:
    """Return a PDF filter name with its abbreviation expanded, else unchanged."""
    return _FILTER_ABBREVIATIONS.get(name, name)


def inspect_image(
    raw_stream: bytes | None,
    filters,
    label: str = "an image",
    jbig2_globals: bytes | None = None,
    jbig2_globals_missing: bool = False,
) -> GuardDecision:
    """Decide whether an image is safe for an engine to decode, from its raw bytes and filters.

    Args:
        raw_stream: The image's raw (still-encoded) stream bytes, or None when
            they could not be read. None, or a stream over the raw-byte bound,
            is skipped (unsized).
        filters: The image's filter names in decoding order (each a str).
        label: A human-readable name for the image, used in the warning.
        jbig2_globals: For a JBIG2 image, the ``/DecodeParms /JBIG2Globals``
            stream bytes (segments shared across images), or None when there is
            no globals stream.
        jbig2_globals_missing: True when a JBIG2 image references a globals
            stream that could not be read; the image is then skipped (fail
            closed), since its segments cannot be bounded.

    Fails closed: safe only when the last filter is a recognised non-image
    filter or a bounded header-read codec (JPX via SIZ, JBIG2 via its region
    and page-information segments), every earlier filter is a recognised
    non-image filter, and no readable header puts the decode over a bound.
    """
    names = [normalize_filter(str(f)) for f in filters]
    codec = names[-1] if names else ""
    wrappers = names[:-1]

    # No filter at all: a raw raster bounded by its declared /Width x /Height
    # (pdfium/MuPDF never read more than that), so there is nothing to decode
    # beyond what the declared size already bounds. Safe, as before.
    if not names:
        return _SAFE

    # Every wrapper before the codec must be a recognised non-image filter;
    # an image filter (or an unknown name) in front means the codec's own
    # header cannot be trusted without a full decode. Fail closed.
    for wrapper in wrappers:
        if wrapper not in _NON_IMAGE_FILTERS:
            return GuardDecision(True, f"{label}: unsupported filter {wrapper!r} before its image codec; skipped")

    if codec == "JPXDecode":
        return _inspect_jpx(raw_stream, wrappers, label)
    if codec == "JBIG2Decode":
        return _inspect_jbig2(raw_stream, wrappers, label, jbig2_globals, jbig2_globals_missing)
    if codec in _NON_IMAGE_FILTERS:
        # Simple filter or DCT as the image codec: bounded by the declared
        # size; v4 behaviour is unchanged (no decode beyond the declared size).
        return _SAFE
    # Anything else is an unrecognised image codec. Do not assume the declared
    # size (that was the advisory's bypass); skip it.
    return GuardDecision(True, f"{label}: unrecognised image filter {codec!r}; skipped (not decoded)")


def _usable_stream(raw_stream: bytes | None, label: str) -> bytes | GuardDecision:
    """Return the bytes of a readable, bounded stream, or a skip ``GuardDecision``."""
    if raw_stream is None:
        return GuardDecision(True, f"{label}: image stream could not be read; skipped (not decoded)")
    if len(raw_stream) > _MAX_RAW_HEADER_BYTES:
        return GuardDecision(
            True, f"{label}: image stream is {len(raw_stream)} bytes, over the header-read bound; skipped"
        )
    return raw_stream


def _inspect_jpx(raw_stream: bytes | None, wrappers, label: str) -> GuardDecision:
    """Guard a JPEG 2000 image by its codestream SIZ marker (size, components, tiles)."""
    data = _usable_stream(raw_stream, label)
    if isinstance(data, GuardDecision):
        return data
    # A JPX codestream may sit behind ASCII85 / ASCIIHex (which grow data at
    # most fourfold); any other wrapper means its header cannot be read here.
    if not _ASCII_FILTERS.issuperset(wrappers):
        return GuardDecision(True, f"{label}: JPEG 2000 behind {', '.join(wrappers)}; size unreadable, skipped")
    size = jpx_size(data)
    if size is None:
        return GuardDecision(True, f"{label}: JPEG 2000 codestream header cannot be read; skipped (not decoded)")
    if size.components > _MAX_JPX_COMPONENTS:
        return GuardDecision(
            True,
            f"{label}: JPEG 2000 has {size.components} components, "
            f"over the {_MAX_JPX_COMPONENTS}-component bound; skipped",
        )
    pixels = size.width * size.height
    if pixels > _MAX_DECODED_PIXELS:
        return GuardDecision(
            True,
            f"{label}: JPEG 2000 decodes to {size.width} x {size.height} = {pixels} pixels, "
            f"over the {_MAX_DECODED_PIXELS}-pixel bound; skipped",
        )
    samples = pixels * size.components
    if samples > _MAX_DECODED_SAMPLES:
        return GuardDecision(
            True,
            f"{label}: JPEG 2000 decodes to {samples} samples "
            f"({size.width} x {size.height} x {size.components}), "
            f"over the {_MAX_DECODED_SAMPLES}-sample bound; skipped",
        )
    if size.tiles > _MAX_JPX_TILES:
        return GuardDecision(
            True, f"{label}: JPEG 2000 decodes in {size.tiles} tiles, over the {_MAX_JPX_TILES}-tile bound; skipped"
        )
    return _SAFE


def _inspect_jbig2(
    raw_stream: bytes | None,
    wrappers,
    label: str,
    globals_data: bytes | None,
    globals_missing: bool,
) -> GuardDecision:
    """Guard a JBIG2 image by the width x height of EVERY region and page-info segment.

    MuPDF decodes a JBIG2 image at the sizes its segments give, ignoring the
    declared /Width /Height. A bounded walk of the globals stream (shared
    segments) and the image stream reads each region/page bitmap size; the
    largest, at 1 bit per pixel, is checked against the pixel cap. A referenced
    but unreadable globals stream, a malformed/truncated header, a page-info
    segment for another page, or an unknown size, is skipped (fail closed).
    """
    data = _usable_stream(raw_stream, label)
    if isinstance(data, GuardDecision):
        return data
    if wrappers:
        # A JBIG2 stream behind any other filter: its segments cannot be read
        # here without a decode. Skip it.
        return GuardDecision(True, f"{label}: JBIG2 behind {', '.join(wrappers)}; size unreadable, skipped")
    if globals_missing:
        return GuardDecision(True, f"{label}: JBIG2 globals stream cannot be read; skipped (not decoded)")

    saw_page = False
    max_pixels = 0
    for source in (globals_data, data):
        if source is None:
            continue
        result = _jbig2_segment_pixels(source)
        if result is None:
            return GuardDecision(True, f"{label}: JBIG2 segments cannot be read; skipped (not decoded)")
        pixels, had_page = result
        max_pixels = max(max_pixels, pixels)
        saw_page = saw_page or had_page

    if not saw_page:
        # Every embedded JBIG2 image has a page-information segment; its absence
        # means the stream could not be parsed as expected.
        return GuardDecision(True, f"{label}: JBIG2 has no readable page-information segment; skipped")
    if max_pixels > _MAX_DECODED_PIXELS:
        return GuardDecision(
            True,
            f"{label}: JBIG2 decodes to {max_pixels} pixels, over the {_MAX_DECODED_PIXELS}-pixel bound; skipped",
        )
    return _SAFE


def _jbig2_segment_pixels(data: bytes) -> tuple[int, bool] | None:
    """Walk a JBIG2 segment sequence; return ``(max_region_or_page_pixels, saw_page_info)``.

    Returns None (fail closed) on a malformed/truncated header, a page-info
    segment for a page other than the single embedded page, or an unknown
    (0xFFFFFFFF) or zero dimension in a sized segment.
    """
    position = 0
    max_pixels = 0
    saw_page = False
    for _ in range(_MAX_JBIG2_SEGMENTS):
        if position >= len(data):
            return max_pixels, saw_page
        header = _jbig2_segment_header(data, position)
        if header is None:
            return None
        segment_type, page_assoc, data_start, data_length, next_position = header
        if segment_type in _JBIG2_SIZED_TYPES:
            if data_start + 8 > len(data):
                return None
            width = int.from_bytes(data[data_start : data_start + 4], "big")
            height = int.from_bytes(data[data_start + 4 : data_start + 8], "big")
            if width == 0 or height == 0 or _JBIG2_UNKNOWN in (width, height):
                return None
            if segment_type == _JBIG2_PAGE_INFO:
                if page_assoc != _JBIG2_EMBEDDED_PAGE:
                    return None
                saw_page = True
            max_pixels = max(max_pixels, width * height)
        if data_length == _JBIG2_UNKNOWN:
            # An unknown-length segment can only be the last one; we cannot
            # skip past it to reach any later segment.
            return (max_pixels, saw_page) if saw_page else None
        position = next_position
    return None


def jbig2_page_size(data: bytes) -> tuple[int, int] | None:
    """The width x height of a JBIG2 embedded stream's first page-information segment.

    A thin reader kept for inspection/tests; the guard uses
    :func:`_jbig2_segment_pixels` to bound every region as well. Returns None
    if the stream cannot be read, has no page-information segment within the
    segment bound, or declares an unknown dimension.
    """
    position = 0
    for _ in range(_MAX_JBIG2_SEGMENTS):
        header = _jbig2_segment_header(data, position)
        if header is None:
            return None
        segment_type, _page_assoc, data_start, data_length, next_position = header
        if segment_type == _JBIG2_PAGE_INFO:
            if data_start + 8 > len(data):
                return None
            width = int.from_bytes(data[data_start : data_start + 4], "big")
            height = int.from_bytes(data[data_start + 4 : data_start + 8], "big")
            if width == 0 or height == 0 or height == _JBIG2_UNKNOWN:
                return None
            return width, height
        if data_length == _JBIG2_UNKNOWN:
            return None
        position = next_position
    return None


def _jbig2_segment_header(data: bytes, position: int) -> tuple[int, int, int, int, int] | None:
    """Parse one JBIG2 segment header at ``position``.

    Returns ``(segment_type, page_association, data_start, data_length,
    next_position)`` or None if the header runs past the data or is malformed.
    Every field is bounded by the data length.
    """
    # Segment number (4) + header flags (1).
    if position + 5 > len(data):
        return None
    segment_number = int.from_bytes(data[position : position + 4], "big")
    flags = data[position + 4]
    segment_type = flags & 0x3F
    page_assoc_is_4 = bool(flags & 0x40)
    cursor = position + 5

    # Referred-to segment count and retention flags (7.2.4).
    if cursor >= len(data):
        return None
    ref_flags = data[cursor]
    count = ref_flags >> 5
    if count == 7:  # long form: 29-bit count + retention-flag bytes
        if cursor + 4 > len(data):
            return None
        count = int.from_bytes(data[cursor : cursor + 4], "big") & 0x1FFFFFFF
        cursor += 4
        retention_bytes = (count + 8) // 8  # ceil((count + 1) / 8)
        cursor += retention_bytes
    else:
        cursor += 1  # the single short-form flags byte

    # Referred-to segment numbers: width depends on this segment's number.
    if segment_number <= 256:
        ref_size = 1
    elif segment_number <= 65536:
        ref_size = 2
    else:
        ref_size = 4
    cursor += count * ref_size

    # Segment page association (1 or 4 bytes) + segment data length (4 bytes).
    if page_assoc_is_4:
        if cursor + 4 > len(data):
            return None
        page_assoc = int.from_bytes(data[cursor : cursor + 4], "big")
        cursor += 4
    else:
        if cursor + 1 > len(data):
            return None
        page_assoc = data[cursor]
        cursor += 1
    if cursor + 4 > len(data):
        return None
    data_length = int.from_bytes(data[cursor : cursor + 4], "big")
    data_start = cursor + 4
    if data_length != _JBIG2_UNKNOWN and data_start + data_length > len(data):
        return None
    next_position = data_start + data_length if data_length != _JBIG2_UNKNOWN else len(data)
    return segment_type, page_assoc, data_start, data_length, next_position
