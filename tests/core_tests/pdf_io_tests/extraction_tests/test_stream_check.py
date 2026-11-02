"""Tests for the decoded-size check of image streams (``core.pdf_io.extraction.stream_check``).

pdfium decodes every FlateDecode, LZWDecode, RunLengthDecode, ASCII85Decode
and ASCIIHexDecode stage of an image stream whole, whatever the image's
declared ``/Width`` x ``/Height``. The check decodes those stages in bounded
chunks and reports a stream that decodes to more than an image of that size
may hold. pdfium gives the width and height without decoding, but not the
colour space, bits or ``/DecodeParms``, so each is taken at its largest: 8
bytes a pixel, twice that with a predictor's tag bytes, plus a slack. The
fixtures are small: a 10 x 10 image whose stream decodes to a few hundred
kilobytes is already far past that.
"""

import base64
import random
import zlib

import pytest

from datagrunt.core.pdf_io.extraction import stream_check
from datagrunt.core.pdf_io.extraction.stream_check import ByteBudget, has_simple_filter, stream_size_problem
from pdf_io_tests.extraction_tests._stream_fixtures import (
    MIB,
    ascii85,
    ascii_hex,
    flate_zeros,
    image_page_pdf,
    lzw_encode,
    lzw_only_with_early_change_0,
    lzw_zeros,
    png_rows,
    run_length,
    run_length_zeros,
)

OVER = "its stream decodes to more than its declared size; skipped (not decoded)"
SLACK = stream_check._SLACK_BYTES
CHUNK = stream_check._CHUNK_BYTES

SIZE = (10, 10)
LIMIT = 16 * 100 + SLACK  # 8 bytes a pixel, twice that for a predictor, plus the slack
PIXELS = bytes(range(100))  # a 10 x 10 gray image, 8 bits a sample

# The same 100 bytes under each filter, and a stream under each that decodes far past the limit.
EXACT = {
    "FlateDecode": zlib.compress(PIXELS),
    "LZWDecode": lzw_encode(PIXELS),
    "RunLengthDecode": run_length(PIXELS),
    "ASCII85Decode": ascii85(PIXELS),
    "ASCIIHexDecode": ascii_hex(PIXELS),
}
PAST = 300 * 1024
OVERSIZED = {
    "FlateDecode": flate_zeros(8 * MIB),
    "LZWDecode": lzw_zeros(8 * MIB),
    "RunLengthDecode": run_length_zeros(PAST),
    "ASCII85Decode": b"z" * (PAST // 4) + b"~>",
    "ASCIIHexDecode": b"00" * PAST + b">",
}


def check(raw, filters, size=SIZE, budget=None):
    return stream_size_problem(raw, filters, size, budget)


def unchecked(why: str) -> str:
    return f"its stream's decoded size could not be checked ({why}); skipped (not decoded)"


class TestEachFilter:
    @pytest.mark.parametrize("name", sorted(EXACT))
    def test_a_stream_of_its_declared_size_passes(self, name):
        assert check(EXACT[name], [name]) is None

    @pytest.mark.parametrize("name", sorted(OVERSIZED))
    def test_a_stream_that_decodes_past_its_declared_size_is_reported(self, name):
        assert check(OVERSIZED[name], [name]) == OVER

    @pytest.mark.parametrize("name", sorted(OVERSIZED))
    def test_decoding_stops_soon_after_the_limit(self, name):
        budget = ByteBudget(1 << 40)

        assert check(OVERSIZED[name], [name], budget=budget) == OVER
        decoded = (1 << 40) - budget.remaining
        assert decoded <= LIMIT + 2 * CHUNK + 4096, decoded

    @pytest.mark.parametrize(
        "abbreviation,name",
        [
            ("Fl", "FlateDecode"),
            ("LZW", "LZWDecode"),
            ("RL", "RunLengthDecode"),
            ("A85", "ASCII85Decode"),
            ("AHx", "ASCIIHexDecode"),
        ],
    )
    def test_an_abbreviated_filter_name_is_checked_the_same(self, abbreviation, name):
        assert check(EXACT[name], [abbreviation]) is None
        assert check(OVERSIZED[name], [abbreviation]) == OVER

    def test_the_most_a_pixel_may_take_is_allowed_and_no_more(self):
        assert check(zlib.compress(bytes(LIMIT)), ["FlateDecode"]) is None
        assert check(zlib.compress(bytes(LIMIT + 1)), ["FlateDecode"]) == OVER

    def test_the_limit_grows_with_the_declared_size(self):
        assert check(zlib.compress(bytes(LIMIT + 1)), ["FlateDecode"], (20, 10)) is None
        assert check(zlib.compress(bytes(16 * 200 + SLACK + 1)), ["FlateDecode"], (20, 10)) == OVER

    def test_a_flate_stream_that_ends_early_passes(self):
        assert check(zlib.compress(PIXELS)[:-10], ["FlateDecode"]) is None

    def test_data_after_the_end_of_a_flate_stream_is_not_decoded(self):
        assert check(zlib.compress(PIXELS) + flate_zeros(8 * MIB), ["FlateDecode"]) is None

    def test_a_run_length_stream_ends_at_its_end_of_data_byte(self):
        assert check(run_length(PIXELS) + run_length_zeros(PAST), ["RunLengthDecode"]) is None

    def test_ascii_streams_ignore_white_space(self):
        spaced = b"\n".join(ascii_hex(PIXELS)[i : i + 7] for i in range(0, 201, 7))

        assert check(spaced, ["ASCIIHexDecode"]) is None
        assert check(b" \n".join(ascii85(PIXELS)[i : i + 9] for i in range(0, 130, 9)), ["ASCII85Decode"]) is None


class TestPredictors:
    def test_png_predictor_tag_bytes_on_the_largest_pixels_are_allowed(self):
        # One column of 8-byte pixels, 100,000 rows: each row takes a tag byte too.
        raw = zlib.compress(png_rows(bytes(800_000), 8))

        assert check(raw, ["FlateDecode"], (1, 100_000)) is None

    def test_png_predictor_tag_bytes_on_lzw_are_allowed(self):
        raw = lzw_encode(png_rows(bytes(800_000), 8))

        assert check(raw, ["LZWDecode"], (1, 100_000)) is None


class TestLzwReadings:
    """pdfium reads ``/EarlyChange`` from ``/DecodeParms``, which its API does not expose: both readings are checked."""

    @pytest.mark.parametrize("early_change", [0, 1])
    @pytest.mark.parametrize(
        "pixels",
        [
            pytest.param(bytes(range(256)) * 64, id="ramp"),
            pytest.param(random.Random(3).randbytes(16_384), id="noise"),
            pytest.param((b"the quick brown fox jumps over the lazy dog " * 400)[:16_384], id="text"),
            pytest.param(bytes(16_384), id="zeros"),
        ],
    )
    def test_a_stream_of_either_early_change_passes(self, pixels, early_change):
        # The reading with the other early change is cut short where pdfium cuts it.
        assert check(lzw_encode(pixels, early_change), ["LZWDecode"], (128, 128)) is None

    def test_a_stream_past_its_size_only_with_early_change_0_is_reported(self):
        raw = lzw_only_with_early_change_0(PAST)

        assert check(raw, ["LZWDecode"]) == OVER

    def test_a_code_past_the_table_is_read_as_pdfium_reads_it(self):
        # Clear, "A", then code 300 (past the table): pdfium repeats the last string and its first byte.
        raw = int("100000000" + "001000001" + "100101100" + "100000001" + "0000", 2).to_bytes(5, "big")
        budget = ByteBudget(100)

        assert check(raw, ["LZWDecode"], budget=budget) is None
        assert 100 - budget.remaining == 2 * 3  # "A" then "AA", in each reading

    def test_a_first_code_past_the_bytes_ends_the_data(self):
        assert check(b"\x80\x7f\xff", ["LZWDecode"]) is None

    def test_the_code_after_a_code_past_the_table_ends_the_data(self):
        # Clear, "A", 300 (past the table), 258 ("AA"): pdfium stops there; the three "B" codes are not read.
        codes = ["100000000", "001000001", "100101100", "100000010", "001000010", "001000010", "001000010"]
        raw = int("".join(codes) + "100000001" + "00000000", 2).to_bytes(10, "big")
        budget = ByteBudget(100)

        assert check(raw, ["LZWDecode"], budget=budget) is None
        assert 100 - budget.remaining == 2 * 5  # "A", "AA", "AA" in each reading


def _corrupted_lzw(seed: int) -> bytes:
    """A valid LZW stream with a few bits flipped: pdfium reads past-table codes in it its own way."""
    noise = random.Random(seed)
    pixels = noise.randbytes(4000) if seed % 2 else (b"lorem ipsum dolor sit amet " * 200)[:4000]
    data = bytearray(lzw_encode(pixels, seed % 3 % 2))
    for _ in range(noise.randrange(1, 6)):
        data[noise.randrange(2, len(data))] ^= 1 << noise.randrange(8)
    return bytes(data)


def _pdfium_decoded_size(tmp_path, raw: bytes, early_change: int) -> int:
    """The bytes pdfium decodes ``raw`` to as a 10 x 10 LZW image with the given ``/EarlyChange``."""
    import pypdfium2 as pdfium
    import pypdfium2.raw as pdfium_raw

    parms = b"" if early_change else b" /DecodeParms << /EarlyChange 0 >>"
    entries = b"/Width 10 /Height 10 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /LZWDecode" + parms
    pdf = pdfium.PdfDocument(str(image_page_pdf(tmp_path / "lzw.pdf", raw, entries)))
    try:
        page = pdf[0]
        (image,) = page.get_objects(filter=(pdfium_raw.FPDF_PAGEOBJ_IMAGE,))
        size = pdfium_raw.FPDFImageObj_GetImageDataDecoded(image.raw, None, 0)
        page.close()
        return size
    finally:
        pdf.close()


# Seeds 56 to 434 flip bits that leave an entry whose prefix is filled in later (pdfium walks it when used).
@pytest.mark.parametrize("seed", [*range(40), 56, 159, 221, 313, 343, 411, 434])
@pytest.mark.parametrize("early_change", [0, 1])
def test_an_lzw_reading_counts_what_pdfium_decodes(tmp_path, seed, early_change):
    raw = _corrupted_lzw(seed)
    counted = sum(len(chunk) for chunk in stream_check._lzw(iter([raw]), early_change))

    pdfium_size = _pdfium_decoded_size(tmp_path, raw, early_change)

    # pdfium keeps the raw stream when its decode fails; otherwise the counts agree.
    assert counted == pdfium_size or pdfium_size == len(raw)


class TestChains:
    def test_every_stage_of_a_chain_is_decoded(self):
        assert check(ascii85(zlib.compress(PIXELS)), ["ASCII85Decode", "FlateDecode"]) is None
        assert check(ascii_hex(lzw_encode(PIXELS)), ["AHx", "LZW"]) is None

    def test_a_chain_whose_last_stage_decodes_past_the_size_is_reported(self):
        assert check(ascii85(flate_zeros(8 * MIB)), ["ASCII85Decode", "FlateDecode"]) == OVER

    def test_an_earlier_stage_that_decodes_far_past_what_the_next_needs_is_reported(self):
        # The Flate stage inflates to 8 MiB; RunLength stops at its first byte (end of data).
        raw = zlib.compress(b"\x80" + bytes(8 * MIB))

        assert check(raw, ["FlateDecode", "RunLengthDecode"]) == OVER

    def test_an_earlier_stage_may_hold_a_few_times_what_the_next_decodes(self):
        assert check(zlib.compress(ascii_hex(PIXELS)), ["FlateDecode", "ASCIIHexDecode"]) is None

    def test_a_codec_after_the_decoding_filters_bounds_its_input_by_a_multiple(self):
        codec_data = bytes(614)  # a 1 x 1 JPEG's headers alone are larger than its pixels

        assert check(zlib.compress(codec_data), ["FlateDecode", "DCTDecode"], (1, 1)) is None
        assert check(flate_zeros(8 * MIB), ["FlateDecode", "DCTDecode"]) == OVER


def _mixed(size: int) -> bytes:
    """``size`` bytes of noise broken by runs of zeros, so every filter's data spans several chunks."""
    noise = random.Random(5)
    return b"".join(noise.randbytes(97) + bytes(13) for _ in range(size // 110 + 1))[:size]


MIXED = _mixed(200_000)
MULTI_CHUNK = {
    "FlateDecode": zlib.compress(MIXED, 0),
    "RunLengthDecode": run_length(MIXED),
    "ASCII85Decode": ascii85(MIXED),
    "ASCIIHexDecode": b"\n".join(ascii_hex(MIXED)[i : i + 79] for i in range(0, 400_001, 79)),
}


class TestChunkBoundaries:
    @pytest.mark.parametrize("name", sorted(MULTI_CHUNK))
    def test_a_stream_of_many_chunks_decodes_whole(self, name):
        assert len(MULTI_CHUNK[name]) > 3 * CHUNK
        budget = ByteBudget(1 << 40)

        assert check(MULTI_CHUNK[name], [name], (400, 500), budget) is None
        assert (1 << 40) - budget.remaining == len(MIXED)

    def test_an_lzw_stream_of_many_chunks_passes(self):
        assert check(lzw_encode(MIXED), ["LZWDecode"], (400, 500)) is None

    def test_a_chain_of_many_chunks_passes(self):
        assert check(ascii85(zlib.compress(MIXED, 0)), ["ASCII85Decode", "FlateDecode"], (400, 500)) is None

    def test_a_run_cut_short_counts_at_its_declared_length(self):
        budget = ByteBudget(1 << 20)

        assert check(b"\x7fab", ["RunLengthDecode"], budget=budget) is None
        assert (1 << 20) - budget.remaining == 128
        assert check(b"\x81", ["RunLengthDecode"]) is None

    def test_a_last_odd_hex_digit_is_padded(self):
        budget = ByteBudget(100)

        assert check(b"0a1", ["ASCIIHexDecode"], budget=budget) is None
        assert 100 - budget.remaining == 2

    def test_a85_without_its_end_marker_decodes_its_last_group(self):
        budget = ByteBudget(100)

        assert check(base64.a85encode(b"abcdef"), ["ASCII85Decode"], budget=budget) is None
        assert 100 - budget.remaining == 6


class TestFailsClosed:
    @pytest.mark.parametrize(
        "raw,filters,why",
        [
            pytest.param(b"not zlib", ["FlateDecode"], "its FlateDecode data is corrupt", id="corrupt-flate"),
            pytest.param(b"0a0g>", ["ASCIIHexDecode"], "its ASCIIHexDecode data is corrupt", id="bad-hex"),
            pytest.param(b"!!!!{~>", ["ASCII85Decode"], "its ASCII85Decode data is corrupt", id="bad-a85"),
            pytest.param(b"!!z!!!~>", ["ASCII85Decode"], "its ASCII85Decode data is corrupt", id="z-in-a-group"),
            pytest.param(
                EXACT["FlateDecode"], ["FlateDecode", "FooDecode"], "unrecognised filter 'FooDecode'", id="unknown-last"
            ),
            pytest.param(
                EXACT["FlateDecode"],
                ["FooDecode", "FlateDecode"],
                "filter 'FooDecode' is not its last filter",
                id="unknown-first",
            ),
            pytest.param(
                EXACT["FlateDecode"],
                ["DCTDecode", "FlateDecode"],
                "filter 'DCTDecode' is not its last filter",
                id="codec-first",
            ),
            pytest.param(
                EXACT["LZWDecode"],
                ["LZWDecode", "LZWDecode", "LZWDecode"],
                "it has more than 2 LZWDecode filters",
                id="three-lzw",
            ),
            pytest.param(None, ["FlateDecode"], "its stream could not be read", id="unread"),
        ],
    )
    def test_what_cannot_be_decoded_for_certain_is_skipped(self, raw, filters, why):
        assert check(raw, filters) == unchecked(why)

    @pytest.mark.parametrize("size", [None, (0, 10), (10, 0), (-1, 10)])
    def test_a_size_that_cannot_be_read_is_skipped(self, size):
        assert check(EXACT["FlateDecode"], ["FlateDecode"], size) == unchecked(
            "its /Width and /Height could not be read"
        )

    def test_a_check_past_its_budget_is_skipped(self):
        assert check(EXACT["FlateDecode"], ["FlateDecode"], budget=ByteBudget(50)) == (
            "its stream's decoded size could not be checked within the byte budget; skipped (not decoded)"
        )

    def test_a_check_charges_what_it_decodes(self):
        budget = ByteBudget(1000)

        assert check(ascii85(zlib.compress(PIXELS)), ["A85", "Fl"], budget=budget) is None
        assert 1000 - budget.remaining == len(zlib.compress(PIXELS)) + 100


class TestWhatIsChecked:
    @pytest.mark.parametrize(
        "filters,expected",
        [
            ([], False),
            (["DCTDecode"], False),
            (["JPXDecode"], False),
            (["CCITTFaxDecode"], False),
            (["FlateDecode"], True),
            (["Fl", "DCTDecode"], True),
            (["A85"], True),
            (["DCTDecode", "RL"], True),
        ],
    )
    def test_a_stream_with_a_decoding_filter_is_checked(self, filters, expected):
        assert has_simple_filter(filters) is expected

    def test_a_stream_without_a_decoding_filter_is_not_checked(self):
        assert check(b"\xff\xd8 anything", ["DCTDecode"], None) is None
        assert check(b"raw", [], None) is None


class TestByteBudget:
    def test_charges_until_spent(self):
        budget = ByteBudget(10)

        assert budget.charge(4) is True
        assert budget.charge(6) is True
        assert budget.remaining == 0
        assert budget.charge(1) is False

    @pytest.mark.parametrize("bad", [-1, 1.5, "10", True])
    def test_rejects_a_total_that_is_not_a_size(self, bad):
        with pytest.raises(ValueError):
            ByteBudget(bad)

    def test_rejects_a_negative_charge(self):
        with pytest.raises(ValueError):
            ByteBudget(10).charge(-1)
