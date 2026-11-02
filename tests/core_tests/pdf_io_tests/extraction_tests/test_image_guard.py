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

from datagrunt.core.pdf_io.extraction import image_guard
from datagrunt.core.pdf_io.extraction.image_guard import (
    _MAX_DECODED_PIXELS,
    _MAX_JPX_COMPONENTS,
    _MAX_JPX_TILES,
    inspect_image,
    jbig2_page_size,
    normalize_filter,
)


def jbig2_region(number: int, segment_type: int, width: int, height: int, page: int = 1) -> bytes:
    """A JBIG2 region segment whose 17-byte region-information field declares ``width`` x ``height``."""
    region_info = struct.pack(">IIIIB", width, height, 0, 0, 0)
    return jbig2_segment(number, segment_type, region_info, page=page)


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
        assert "JBIG2" in decision.reason

    def test_an_unknown_height_is_skipped(self):
        decision = inspect_image(jbig2_stream(16, 0xFFFFFFFF), ["JBIG2Decode"])
        assert decision.skip is True

    def test_a_stream_without_a_page_info_segment_is_skipped(self):
        only_end = jbig2_segment(0, 49, b"")
        assert jbig2_page_size(only_end) is None
        assert inspect_image(only_end, ["JBIG2Decode"]).skip is True


class TestJbig2Segments:
    """Every region segment (and page-info) is bounded, not only the page-information segment."""

    SMALL_PAGE = jbig2_segment(0, 48, struct.pack(">IIIIBH", 16, 16, 0, 0, 0, 0))

    def test_the_region_types_bounded(self):
        assert image_guard._JBIG2_GENERIC_REGION_TYPES == frozenset({36, 38, 39})

    @pytest.mark.parametrize("region_type", sorted({4, 6, 7, 20, 22, 23, 36, 38, 39, 40, 42, 43}))
    def test_a_region_segment_bomb_is_skipped(self, region_type):
        stream = self.SMALL_PAGE + jbig2_region(1, region_type, 40000, 40000) + jbig2_segment(2, 49, b"")
        decision = inspect_image(stream, ["JBIG2Decode"])
        assert decision.skip is True
        assert "JBIG2" in decision.reason

    def test_a_small_region_with_a_small_page_is_safe(self):
        stream = self.SMALL_PAGE + jbig2_region(1, 38, 32, 32) + jbig2_segment(2, 49, b"")
        assert inspect_image(stream, ["JBIG2Decode"]).skip is False

    def test_a_region_in_the_globals_stream_is_bounded(self):
        globals_stream = jbig2_region(0, 38, 40000, 40000)
        decision = inspect_image(self.SMALL_PAGE, ["JBIG2Decode"], jbig2_globals=globals_stream)
        assert decision.skip is True

    def test_missing_globals_fails_closed(self):
        decision = inspect_image(self.SMALL_PAGE, ["JBIG2Decode"], jbig2_globals_missing=True)
        assert decision.skip is True
        assert "globals" in decision.reason.lower()

    def test_a_truncated_segment_header_fails_closed(self):
        decision = inspect_image(self.SMALL_PAGE + b"\x00\x00\x00\x09", ["JBIG2Decode"])
        assert decision.skip is True

    def test_a_page_info_for_another_page_fails_closed(self):
        page2 = jbig2_segment(0, 48, struct.pack(">IIIIBH", 16, 16, 0, 0, 0, 0), page=2)
        decision = inspect_image(page2, ["JBIG2Decode"])
        assert decision.skip is True


def generic_region(number: int, width: int, height: int, segment_type: int = 38) -> bytes:
    """A generic region segment: region information, MMR flags, then a few data bytes."""
    data = struct.pack(">IIIIB", width, height, 0, 0, 0) + b"\x01" + b"\xff" * 4
    return jbig2_segment(number, segment_type, data)


def unknown_length(number: int, segment_type: int, data: bytes) -> bytes:
    """A short-form segment header whose data length is 0xFFFFFFFF (unknown), then ``data``."""
    return struct.pack(">IBBBI", number, segment_type, 0x00, 1, 0xFFFFFFFF) + data


PAGE_16 = jbig2_segment(0, 48, struct.pack(">IIIIBH", 16, 16, 0, 0, 0, 0))
END_OF_PAGE = jbig2_segment(9, 49, b"")
NOT_CHECKED = sorted(set(range(64)) - {36, 38, 39, 48, 49, 50, 51, 62})


class TestJbig2SegmentTypes:
    """A JBIG2 image is decoded only when every segment of its stream is of a checked type."""

    def test_the_checked_types(self):
        assert image_guard._JBIG2_CHECKED_TYPES == frozenset({36, 38, 39, 48, 49, 50, 51, 62})

    @pytest.mark.parametrize("segment_type", [36, 38, 39])
    def test_a_generic_region_scan_is_decoded(self, segment_type):
        stream = PAGE_16 + generic_region(1, 16, 16, segment_type) + END_OF_PAGE

        assert inspect_image(stream, ["JBIG2Decode"]) == (False, None)

    def test_end_of_stripe_end_of_file_and_extension_segments_are_decoded(self):
        end_of_stripe = jbig2_segment(2, 50, struct.pack(">I", 15))
        extension = jbig2_segment(3, 62, struct.pack(">I", 0x20000000) + b"note\x00\x00")
        end_of_file = jbig2_segment(4, 51, b"", page=0)
        stream = PAGE_16 + generic_region(1, 16, 16) + end_of_stripe + extension + END_OF_PAGE + end_of_file

        assert inspect_image(stream, ["JBIG2Decode"]) == (False, None)

    @pytest.mark.parametrize("segment_type", NOT_CHECKED, ids=[f"type-{t}" for t in NOT_CHECKED])
    def test_a_segment_of_any_other_type_is_not_decoded(self, segment_type):
        stream = PAGE_16 + jbig2_segment(1, segment_type, bytes(24)) + generic_region(2, 16, 16) + END_OF_PAGE

        decision = inspect_image(stream, ["JBIG2Decode"], label="xref 5")

        assert decision.skip is True
        assert decision.reason == f"xref 5: JBIG2 holds a segment of type {segment_type}, which is not checked; skipped"

    def test_a_second_page_information_segment_is_not_decoded(self):
        stream = PAGE_16 + jbig2_segment(1, 48, struct.pack(">IIIIBH", 16, 16, 0, 0, 0, 0)) + END_OF_PAGE

        assert inspect_image(stream, ["JBIG2Decode"], label="xref 5") == (
            True,
            "xref 5: JBIG2 segments cannot be read; skipped",
        )

    def test_a_page_information_segment_shorter_than_its_size_is_not_decoded(self):
        # Its 8 size bytes would be read from the next segment's header.
        stream = jbig2_segment(0, 48, b"") + generic_region(1, 16, 16) + END_OF_PAGE

        decision = inspect_image(stream, ["JBIG2Decode"], label="xref 5")

        assert decision == (True, "xref 5: JBIG2 segments cannot be read; skipped")

    def test_a_stream_past_the_segment_bound_is_not_decoded(self, monkeypatch):
        monkeypatch.setattr(image_guard, "_MAX_JBIG2_SEGMENTS", 3)
        stream = PAGE_16 + generic_region(1, 16, 16) + END_OF_PAGE

        assert inspect_image(stream, ["JBIG2Decode"]).skip is False
        decision = inspect_image(stream + jbig2_segment(10, 51, b"", page=0), ["JBIG2Decode"], label="xref 5")
        assert decision == (True, "xref 5: JBIG2 has more than 3 segments; skipped")

    def test_a_jbig2_image_with_a_symbol_dictionary_is_not_decoded(self):
        # A symbol dictionary (type 0) before the page, as an embedded stream may carry it.
        stream = jbig2_segment(0, 0, bytes(10)) + PAGE_16 + generic_region(2, 16, 16) + END_OF_PAGE

        assert inspect_image(stream, ["JBIG2Decode"]).skip is True


class TestJbig2SegmentLengths:
    """Every segment's length must be known, so the walk sees every segment the stream holds."""

    @pytest.mark.parametrize("segment_type", [38, 49], ids=["immediate-generic-region", "end-of-page"])
    def test_a_segment_of_unknown_length_is_not_decoded(self, segment_type):
        region = struct.pack(">IIIIB", 16, 16, 0, 0, 0) + b"\x01" + b"\xff" * 4 + b"\x00\x00" + struct.pack(">I", 16)
        stream = PAGE_16 + unknown_length(1, segment_type, region)

        decision = inspect_image(stream, ["JBIG2Decode"], label="xref 5")

        assert decision == (True, "xref 5: JBIG2 holds a segment of unknown length, which is not checked; skipped")

    def test_segments_after_one_of_unknown_length_are_not_trusted(self):
        region = struct.pack(">IIIIB", 16, 16, 0, 0, 0) + b"\x01" + b"\xff" * 4 + b"\x00\x00" + struct.pack(">I", 16)
        stream = PAGE_16 + unknown_length(1, 38, region) + jbig2_segment(2, 0, bytes(10)) + END_OF_PAGE

        assert inspect_image(stream, ["JBIG2Decode"]).skip is True


class TestJbig2RegionsTogether:
    """The generic regions of one image, summed, are within the decode bound."""

    def test_regions_within_the_bound_together_are_decoded(self):
        # 6000 x 7000 = 42M pixels each; two of them sum to 84M, under 89,478,485.
        stream = PAGE_16 + generic_region(1, 6000, 7000, 36) + generic_region(2, 6000, 7000, 36) + END_OF_PAGE

        assert inspect_image(stream, ["JBIG2Decode"]).skip is False

    def test_regions_over_the_bound_together_are_not_decoded(self):
        # Three regions of 42M pixels: each within the bound, 126M together.
        regions = b"".join(generic_region(n, 6000, 7000, 36) for n in (1, 2, 3))

        decision = inspect_image(PAGE_16 + regions + END_OF_PAGE, ["JBIG2Decode"], label="xref 5")

        assert decision == (
            True,
            "xref 5: JBIG2 regions decode to 126000000 pixels together, over the 89478485-pixel bound; skipped",
        )


class TestJbig2Globals:
    """A JBIG2 image with a /JBIG2Globals stream is not decoded, whatever the stream holds."""

    @pytest.mark.parametrize(
        "globals_stream",
        [b"", PAGE_16, generic_region(0, 16, 16)],
        ids=["empty", "page-information", "small-generic-region"],
    )
    def test_a_jbig2_image_with_globals_is_not_decoded(self, globals_stream):
        stream = PAGE_16 + generic_region(1, 16, 16) + END_OF_PAGE

        decision = inspect_image(stream, ["JBIG2Decode"], label="xref 5", jbig2_globals=globals_stream)

        assert decision == (True, "xref 5: JBIG2 with a /JBIG2Globals stream is not checked; skipped (not decoded)")

    def test_decode_parameters_that_cannot_be_read_are_not_decoded(self):
        stream = PAGE_16 + generic_region(1, 16, 16) + END_OF_PAGE

        decision = inspect_image(stream, ["JBIG2Decode"], label="xref 5", jbig2_globals_missing=True)

        assert decision == (
            True,
            "xref 5: JBIG2 decode parameters could not be checked for /JBIG2Globals; skipped (not decoded)",
        )
