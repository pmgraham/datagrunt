"""Tests for the pre-decode image guard (``core.pdf_io.extraction.image_guard``).

The guard decides, from an image's raw stream bytes and filters alone (no
pdfium / pymupdf), whether an engine may decode it. It fails closed: an
unrecognised filter, an unreadable header, or a header over a bound is
skipped. A JPEG 2000 codestream that lies about its size (small /Width with a
huge SIZ marker) is the vulnerability this guards.
"""

import io
import struct

import pytest
from PIL import Image

from datagrunt.core.pdf_io.extraction.image_guard import (
    _MAX_DECODED_PIXELS,
    _MAX_JPX_COMPONENTS,
    _MAX_JPX_TILES,
    inspect_image,
    jbig2_page_size,
    normalize_filter,
)


def j2k(size=(16, 16), *, siz=None, tile=None, jp2=False, mode="L") -> bytes:
    """A Pillow JPEG 2000 codestream; ``siz`` / ``tile`` overwrite its SIZ size and tile size."""
    buffer = io.BytesIO()
    fill = 128 if mode == "L" else (10, 20, 30)
    Image.new(mode, size, fill).save(buffer, "JPEG2000", no_jp2=not jp2)
    data = bytearray(buffer.getvalue())
    at = data.find(b"\xff\x4f\xff\x51")
    if siz is not None:
        data[at + 8 : at + 16] = struct.pack(">II", *siz)
    if tile is not None:
        data[at + 24 : at + 32] = struct.pack(">II", *tile)
    return bytes(data)


def jbig2_segment(number: int, segment_type: int, data: bytes, page: int = 1) -> bytes:
    """A short-form JBIG2 segment header (0 referred segments, 1-byte page) + data."""
    return struct.pack(">IBBBI", number, segment_type, 0x00, page, len(data)) + data


def jbig2_stream(width: int, height: int) -> bytes:
    """A minimal JBIG2 embedded stream: a page-information segment then end-of-page."""
    page_info = struct.pack(">IIIIBH", width, height, 0, 0, 0, 0)
    return jbig2_segment(0, 48, page_info) + jbig2_segment(1, 49, b"")


class TestNormalizeFilter:
    @pytest.mark.parametrize(
        "abbrev,full",
        [("AHx", "ASCIIHexDecode"), ("A85", "ASCII85Decode"), ("Fl", "FlateDecode"), ("DCT", "DCTDecode")],
    )
    def test_expands_abbreviations(self, abbrev, full):
        assert normalize_filter(abbrev) == full

    def test_leaves_full_names_unchanged(self):
        assert normalize_filter("JPXDecode") == "JPXDecode"


class TestSafeImages:
    """Images v4 already handled safely stay safe to touch (no false positives)."""

    def test_no_filter_is_safe(self):
        assert inspect_image(b"\x00" * 16, []) == (False, None)

    @pytest.mark.parametrize("codec", ["FlateDecode", "LZWDecode", "CCITTFaxDecode", "DCTDecode", "RunLengthDecode"])
    def test_non_image_and_dct_codecs_are_safe(self, codec):
        assert inspect_image(b"\x00" * 64, [codec]).skip is False

    def test_a_small_jpx_is_extracted(self):
        assert inspect_image(j2k((16, 8)), ["JPXDecode"]) == (False, None)

    def test_a_small_jpx_behind_asciihex_is_safe(self):
        assert inspect_image(j2k((16, 8)), ["ASCIIHexDecode", "JPXDecode"]).skip is False


class TestJpxSizeBomb:
    """A JPEG 2000 image whose codestream is huge is skipped before any decode."""

    def test_an_oversized_codestream_is_skipped(self):
        decision = inspect_image(j2k((16, 16), siz=(16000, 16000), tile=(16000, 16000)), ["JPXDecode"])
        assert decision.skip is True
        assert "pixel bound" in decision.reason

    def test_the_pixel_bound_value(self):
        assert _MAX_DECODED_PIXELS == 89_478_485

    def test_a_tile_count_bomb_is_skipped(self):
        # 2000 x 2000 with 8 x 8 tiles = 62,500 tiles, over the 4096-tile bound.
        decision = inspect_image(j2k((16, 16), siz=(2000, 2000), tile=(8, 8)), ["JPXDecode"])
        assert decision.skip is True
        assert "tile bound" in decision.reason

    def test_the_tile_bound_value(self):
        assert _MAX_JPX_TILES == 4096


class TestJpxComponentBomb:
    """Memory scales with width x height x components, so a high component count is bounded."""

    def test_the_component_bound_value(self):
        assert _MAX_JPX_COMPONENTS == 4

    def test_more_than_four_components_is_skipped(self):
        data = bytearray(j2k((16, 16)))
        at = data.find(b"\xff\x4f\xff\x51")
        (csiz,) = struct.unpack_from(">H", data, at + 40)
        component = bytes(data[at + 42 : at + 45])
        rebuilt = (
            data[: at + 2]
            + b"\xff\x51"
            + struct.pack(">H", 38 + 3 * 5)
            + data[at + 6 : at + 40]
            + struct.pack(">H", 5)
            + component * 5
            + data[at + 42 + 3 * csiz :]
        )
        decision = inspect_image(bytes(rebuilt), ["JPXDecode"])
        assert decision.skip is True
        assert "component bound" in decision.reason

    def test_four_component_image_within_the_sample_budget_is_safe(self):
        # A small 4-component (RGBA) codestream is under the sample budget.
        assert inspect_image(j2k((16, 16), mode="RGBA"), ["JPXDecode"]).skip is False


class TestFailClosed:
    """Unrecognised filters and unreadable headers are skipped, never assumed safe."""

    def test_an_unrecognised_image_codec_is_skipped(self):
        decision = inspect_image(b"\x00" * 64, ["FooDecode"])
        assert decision.skip is True
        assert "unrecognised" in decision.reason

    def test_an_image_filter_in_front_of_a_codec_is_skipped(self):
        decision = inspect_image(b"\x00" * 64, ["JPXDecode", "FlateDecode"])
        assert decision.skip is True
        assert "before its image codec" in decision.reason

    def test_a_jpx_behind_flate_is_skipped(self):
        decision = inspect_image(j2k((16, 8)), ["FlateDecode", "JPXDecode"])
        assert decision.skip is True
        assert "unreadable" in decision.reason

    def test_an_unreadable_jpx_header_is_skipped(self):
        decision = inspect_image(b"not a codestream", ["JPXDecode"])
        assert decision.skip is True
        assert "cannot be read" in decision.reason

    def test_a_missing_stream_is_skipped(self):
        decision = inspect_image(None, ["JPXDecode"])
        assert decision.skip is True


class TestJbig2:
    """JBIG2 is bounded by its page-information segment size (MuPDF decodes at that size)."""

    def test_reads_a_small_page_size(self):
        assert jbig2_page_size(jbig2_stream(16, 16)) == (16, 16)

    def test_a_small_jbig2_is_safe(self):
        assert inspect_image(jbig2_stream(16, 16), ["JBIG2Decode"]).skip is False

    def test_a_page_info_size_bomb_is_skipped(self):
        decision = inspect_image(jbig2_stream(40000, 40000), ["JBIG2Decode"])
        assert decision.skip is True
        assert "JBIG2 page" in decision.reason

    def test_an_unknown_height_is_skipped(self):
        decision = inspect_image(jbig2_stream(16, 0xFFFFFFFF), ["JBIG2Decode"])
        assert decision.skip is True

    def test_a_stream_without_a_page_info_segment_is_skipped(self):
        only_end = jbig2_segment(0, 49, b"")
        assert jbig2_page_size(only_end) is None
        assert inspect_image(only_end, ["JBIG2Decode"]).skip is True
