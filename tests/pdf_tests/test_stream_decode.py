"""Tests for the bounded stream decoders of the v5 image check (``datagrunt.pdf._stream_decode``).

Each decoder reads a simple filter as pdfium does, a chunk at a time, and
stops once its output passes its limit. Agreement with pdfium is pinned by
decoding the same streams with ``FPDFImageObj_GetImageDataDecoded``, which
decodes every simple filter of an image's stream (pdfium reads a stream it
cannot decode, or that decodes to nothing, as its raw bytes).
"""

import base64
import ctypes
import random
import re
import zlib
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest

from datagrunt.pdf import _stream_decode
from datagrunt.pdf._stream_decode import BudgetSpent, OverLimit, Unchecked, decoded_length, decoded_pieces
from pdf_tests.test_pdfium_images import xobject_page

CHUNK = _stream_decode._CHUNK
FLATE = "FlateDecode"
LZW = "LZWDecode"
A85 = "ASCII85Decode"
AHX = "ASCIIHexDecode"
RL = "RunLengthDecode"


def charged(log: list[int] | None = None):
    """A charge that always fits, recording each charge in ``log``."""

    def charge(size: int) -> bool:
        if log is not None:
            log.append(size)
        return True

    return charge


def length(raw: bytes, *filters: str, limit: int = 1 << 30) -> int:
    return decoded_length(raw, filters, limit=limit, charge=charged())


def decoded(raw: bytes, *filters: str, early: int = 1, limit: int = 1 << 30) -> bytes:
    return b"".join(decoded_pieces(raw, filters, early=early, limit=limit, charge=charged()))


def pack(codes: list[tuple[int, int]]) -> bytes:
    """``(code, width)`` pairs packed most significant bit first, as PDF LZW stores them."""
    return from_bits("".join(format(code, f"0{width}b") for code, width in codes))


def from_bits(text: str) -> bytes:
    """A string of 0s and 1s as bytes, zero-padded to a whole byte."""
    text += "0" * (-len(text) % 8)
    return int(text, 2).to_bytes(len(text) // 8, "big") if text else b""


def chain_width(code: int, early: int) -> int:
    """The width pdfium reads ``code`` at when it is the entry about to be added (12 at most)."""
    return min(12, max(9, (code + early).bit_length()))


def lzw_chain_bits(count: int, early: int = 1) -> str:
    """A zero byte, then ``count`` codes each naming the entry about to be added, as a string of bits."""
    codes = [(256, 9), (0, 9)] + [(code, chain_width(code, early)) for code in range(258, 258 + count)]
    return "".join(format(code, f"0{width}b") for code, width in codes)


def lzw_chain(count: int, early: int = 1) -> bytes:
    """``lzw_chain_bits`` as bytes: it decodes to 1 + 2 + ... + (count + 1) zero bytes."""
    return from_bits(lzw_chain_bits(count, early))


def lzw_encode(data: bytes, early: int = 1, *, clear_when_full: bool = True) -> bytes:
    """``data`` LZW-coded with code widths as pdfium reads them under ``/EarlyChange early``."""
    table = {bytes([value]): value for value in range(256)}
    next_code, width, codes = 258, 9, [(256, 9)]
    word = b""
    for value in data:
        longer = word + bytes([value])
        if longer in table:
            word = longer
            continue
        codes.append((table[word], width))
        if next_code < 4096 - early:
            table[longer] = next_code
            next_code += 1
            if width < 12 and next_code + early > (1 << width):
                width += 1
        elif clear_when_full:
            codes.append((256, width))
            table = {bytes([value]): value for value in range(256)}
            next_code, width = 258, 9
        word = bytes([value])
    if word:
        codes.append((table[word], width))
    codes.append((257, width))
    return pack(codes)


def lzw_two_readings(chain: int) -> bytes:
    """An LZW stream that ends after 254 bytes read with /EarlyChange 1, and runs on read with /EarlyChange 0.

    254 literals take both readings to 253 table entries. The next code is
    10 bits wide under /EarlyChange 1 (the end-of-data code, 257:
    0100000001) and 9 bits wide under 0 (010000000: literal 128). Read with
    /EarlyChange 0 the stream goes on from the 257's last bit, a 1: ``chain``
    10- to 12-bit codes from 512, each the entry about to be added, so each
    decodes one byte longer than the one before.
    """
    literals = "".join(format(65, "09b") for _ in range(254))
    codes = "".join(format(code, f"0{chain_width(code, 0)}b") for code in range(512, 512 + chain))
    assert codes.startswith("1")  # the 257's last bit is the first bit of code 512
    return from_bits(literals + "0100000001" + codes[1:])


def pdfium_decoded(tmp_path: Path, raw: bytes, filters: bytes, parms: bytes = b"") -> bytes:
    """What pdfium decodes ``raw`` to through ``filters`` (FPDFImageObj_GetImageDataDecoded)."""
    entries = b"/Width 4 /Height 2 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter " + filters
    if parms:
        entries += b" /DecodeParms " + parms
    document = pdfium.PdfDocument(xobject_page(tmp_path, raw, entries))
    try:
        page = document[0]
        image = pdfium_c.FPDFPage_GetObject(page.raw, 0)
        size = pdfium_c.FPDFImageObj_GetImageDataDecoded(image, None, 0)
        buffer = ctypes.create_string_buffer(size)
        assert pdfium_c.FPDFImageObj_GetImageDataDecoded(image, buffer, size) == size
        page.close()
        return buffer.raw
    finally:
        document.close()


def zeros_flate(size: int) -> bytes:
    return zlib.compress(bytes(size), 9)


class TestFlate:
    def test_the_length_is_the_inflated_length(self):
        assert length(zlib.compress(bytes(range(256)) * 40), FLATE) == 10240

    def test_a_stream_of_exactly_the_limit_passes(self):
        assert length(zeros_flate(810), FLATE, limit=810) == 810

    def test_one_byte_past_the_limit_is_over_it(self):
        with pytest.raises(OverLimit, match=re.escape("FlateDecode decodes past 810 bytes")):
            length(zeros_flate(811), FLATE, limit=810)

    def test_inflating_stops_one_chunk_past_the_limit(self):
        # 8 MiB of zeros in ~8 KB: the check inflates at most the limit and one chunk of it.
        log: list[int] = []
        with pytest.raises(OverLimit):
            decoded_length(zeros_flate(8 << 20), (FLATE,), limit=810, charge=charged(log))
        assert sum(log) <= 810 + CHUNK and max(log) <= CHUNK

    def test_data_after_the_end_of_the_stream_is_not_read(self):
        assert length(zlib.compress(b"abc") + zeros_flate(1 << 20), FLATE, limit=3) == 3

    def test_a_truncated_stream_counts_what_it_inflates(self):
        whole = zlib.compress(bytes(range(200)) * 50)
        assert 0 < length(whole[: len(whole) // 2], FLATE) < 10000

    def test_invalid_data_is_unchecked(self):
        with pytest.raises(Unchecked, match=re.escape("FlateDecode data cannot be inflated")):
            length(b"not a zlib stream", FLATE)


class TestLzw:
    @pytest.mark.parametrize("early", [1, 0])
    @pytest.mark.parametrize("clear_when_full", [True, False], ids=["clears", "full-table"])
    def test_a_stream_decodes_to_what_was_coded(self, early, clear_when_full):
        data = bytes(random.Random(early).randrange(256) for _ in range(30000)) + bytes(30000)

        assert decoded(lzw_encode(data, early, clear_when_full=clear_when_full), LZW, early=early) == data

    @pytest.mark.parametrize("early", [1, 0])
    def test_the_length_is_the_most_either_early_change_decodes(self, early):
        data = bytes(range(256)) * 20
        coded = lzw_encode(data, early)

        assert length(coded, LZW) == max(len(decoded(coded, LZW, early=1)), len(decoded(coded, LZW, early=0)))
        assert length(coded, LZW) >= len(data)

    def test_a_stream_within_its_limit_under_one_early_change_and_past_it_under_the_other_is_over_it(self, tmp_path):
        coded = lzw_two_readings(200)

        assert pdfium_decoded(tmp_path, coded, b"/LZWDecode", b"<< /EarlyChange 1 >>") == b"A" * 254
        assert len(pdfium_decoded(tmp_path, coded, b"/LZWDecode", b"<< /EarlyChange 0 >>")) > 20000
        with pytest.raises(OverLimit, match=re.escape("LZWDecode decodes past 810 bytes")):
            length(coded, LZW, limit=810)

    def test_a_stream_of_exactly_the_limit_passes(self):
        coded = lzw_encode(bytes(810))

        assert length(coded, LZW, limit=810) == 810
        with pytest.raises(OverLimit):
            length(coded, LZW, limit=809)

    def test_decoding_stops_one_chunk_past_the_limit(self):
        coded = lzw_chain(2000)  # 2 MB in 3 KB
        log: list[int] = []
        with pytest.raises(OverLimit):
            decoded_length(coded, (LZW,), limit=810, charge=charged(log))
        assert sum(log) <= 810 + CHUNK and max(log) <= CHUNK

    def test_a_chain_of_codes_decodes_to_its_lengths(self):
        assert decoded(lzw_chain(3), LZW) == bytes(1 + 2 + 3 + 4)

    def test_two_lzw_filters_are_unchecked(self):
        with pytest.raises(Unchecked, match=re.escape("2 LZWDecode filters")):
            length(lzw_encode(lzw_encode(b"abc")), LZW, LZW)


def lzw_streams() -> list:
    rng = random.Random(7)
    texts = bytes(rng.choice(b"abcdefgh ") for _ in range(20000))
    streams = [pytest.param(lzw_encode(texts, early), early, id=f"text-early{early}") for early in (1, 0)]
    streams += [pytest.param(lzw_encode(texts, 1 - early), early, id=f"other-early{early}") for early in (1, 0)]
    streams += [
        pytest.param(bytes(rng.randrange(256) for _ in range(size)), early, id=f"random-{size}-early{early}")
        for size in (2, 40, 700)
        for early in (1, 0)
    ]
    # Codes naming the entry about to be added, past pdfium's 4000-byte decode stack, then codes past the table.
    long_strings = from_bits(lzw_chain_bits(3800) + "".join(format(code, "012b") for code in (4095,) * 3 + (300, 301)))
    streams.append(pytest.param(long_strings, 1, id="long-strings"))
    streams.append(pytest.param(pack([(65, 9), (300, 9), (259, 9), (66, 9)]), 1, id="code-past-the-table"))
    streams.append(pytest.param(pack([(258, 9), (65, 9)]), 1, id="code-before-any-entry"))
    return streams


@pytest.mark.parametrize("raw,early", lzw_streams())
def test_lzw_decodes_as_pdfium_does(tmp_path, raw, early):
    mine = decoded(raw, LZW, early=early)
    theirs = pdfium_decoded(tmp_path, raw, b"/LZWDecode", b"<< /EarlyChange %d >>" % early)

    # pdfium reads a stream it cannot decode, or that decodes to nothing, as its raw bytes.
    assert theirs in (mine, raw)


def test_lzw_stops_where_pdfium_cannot_decode(tmp_path):
    # A code past the literals before any entry: pdfium stops decoding and reads the raw stream.
    raw = pack([(258, 9), (65, 9)])

    assert (decoded(raw, LZW), pdfium_decoded(tmp_path, raw, b"/LZWDecode")) == (b"", raw)


class TestAscii85:
    def test_the_length_is_the_decoded_length(self):
        assert length(base64.a85encode(bytes(range(256))) + b"~>", A85) == 256

    def test_z_is_four_zero_bytes(self):
        assert decoded(b"zz!!!!!~>", A85) == bytes(12)

    def test_z_past_the_limit_is_over_it(self):
        with pytest.raises(OverLimit, match=re.escape("ASCII85Decode decodes past 810 bytes")):
            length(b"z" * 203 + b"~>", A85, limit=810)

    def test_whitespace_is_skipped_and_the_end_marker_ends_it(self):
        assert decoded(b'87cU\nRD]i, "Ebo80~>87cUR', A85) == b"Hello World!"

    @pytest.mark.parametrize(
        "raw,why",
        [
            pytest.param(b"87cU\x00RD]i~>", "byte 0x00", id="nul"),
            pytest.param(b"87cUvRD]i~>", "byte 0x76", id="past-u"),
            pytest.param(b"!!z!!!~>", "a z inside a group", id="z-inside"),
            pytest.param(b's8W-"~>', "a group over 2**32 - 1", id="overflow"),
            pytest.param(b"87cURD~>", "a final group of one character", id="one-char"),
        ],
    )
    def test_data_outside_the_format_is_unchecked(self, raw, why):
        with pytest.raises(Unchecked, match=re.escape(why)):
            length(raw, A85)

    @pytest.mark.parametrize(
        "raw",
        [
            base64.a85encode(bytes(range(256)) * 3) + b"~>",
            base64.a85encode(bytes(100), foldspaces=False) + b"~>",
            base64.a85encode(b"Hello World", wrapcol=4) + b"~>trailing",
            base64.a85encode(b"Hello Worl"),
        ],
    )
    def test_ascii85_decodes_as_pdfium_does(self, tmp_path, raw):
        assert decoded(raw, A85) == pdfium_decoded(tmp_path, raw, b"/ASCII85Decode")

    @pytest.mark.parametrize("size", [0, 4, 8, 400, 4 * 5000 + 4])
    def test_groups_decode_as_the_standard_library_does(self, size):
        data = random.Random(size).randbytes(size)
        text = base64.a85encode(data).replace(b"z", b"!!!!!")
        assert _stream_decode._groups(text) == base64.a85decode(text) == data

    @pytest.mark.parametrize("text", [b"s8W-!", b"!!!!!s8W-!", b"s8W,u"])
    def test_groups_up_to_2_32_minus_1_decode(self, text):
        assert _stream_decode._groups(text) == base64.a85decode(text)

    @pytest.mark.parametrize("text", [b's8W-"', b"!!!!!uuuuu", b"s8W.!!!!!!"])
    def test_a_group_over_2_32_minus_1_is_unchecked(self, text):
        with pytest.raises(Unchecked, match=re.escape("a group over 2**32 - 1")):
            _stream_decode._groups(text)

    def test_a_long_stream_decodes_whole_across_chunks(self):
        data = random.Random(5).randbytes(3 * CHUNK + 12)
        raw = base64.a85encode(data, wrapcol=75) + b"~>"
        assert decoded(raw, A85, limit=len(data)) == data


class TestAsciiHex:
    def test_the_length_is_the_decoded_length(self):
        assert length(bytes(range(256)).hex().encode() + b">", AHX) == 256

    def test_an_odd_final_digit_is_padded(self):
        assert decoded(b"486>", AHX) == b"H`"

    def test_past_the_limit_is_over_it(self):
        with pytest.raises(OverLimit, match=re.escape("ASCIIHexDecode decodes past 4 bytes")):
            length(b"0102030405>", AHX, limit=4)

    @pytest.mark.parametrize(
        "raw",
        [
            b"48656c6c6f>",
            b"48656c6c6f>4142",
            b"4865 6c\n6c6f",
            b"486>",
            b"48\x0065\x0c6c zz6c6f>",
        ],
    )
    def test_ascii_hex_decodes_as_pdfium_does(self, tmp_path, raw):
        assert decoded(raw, AHX) == pdfium_decoded(tmp_path, raw, b"/ASCIIHexDecode")


class TestRunLength:
    def test_runs_decode(self):
        assert decoded(bytes([2]) + b"abc" + bytes([254]) + b"x" + bytes([128]) + b"ignored", RL) == b"abcxxx"

    def test_past_the_limit_is_over_it(self):
        with pytest.raises(OverLimit, match=re.escape("RunLengthDecode decodes past 810 bytes")):
            length(bytes([129, 0]) * 7, RL, limit=810)

    @pytest.mark.parametrize(
        "raw",
        [
            bytes([2]) + b"abc" + bytes([254]) + b"x",
            bytes([10]) + b"ab",
            bytes([2]) + b"abc" + bytes([200]),
            bytes([0]) + b"a" + bytes([129]) + b"y" + bytes([128]),
        ],
        ids=["runs", "short-literal", "repeat-without-its-byte", "end-marker"],
    )
    def test_run_length_decodes_as_pdfium_does(self, tmp_path, raw):
        assert decoded(raw, RL) == pdfium_decoded(tmp_path, raw, b"/RunLengthDecode")


def flate_then_zeros(groups: int) -> bytes:
    """ASCII85 of a Flate stream of b"abc" (zero-padded to whole groups), then ``groups`` z: 4 zero bytes each."""
    data = zlib.compress(b"abc")
    return base64.a85encode(data + bytes(-len(data) % 4)) + b"z" * groups + b"~>"


class TestChains:
    def test_each_filter_decodes_in_turn(self):
        raw = base64.a85encode(zlib.compress(bytes(range(256)) * 4)) + b"~>"

        assert decoded(raw, A85, FLATE) == bytes(range(256)) * 4

    def test_a_chain_decodes_as_pdfium_does(self, tmp_path):
        raw = base64.a85encode(lzw_encode(bytes(range(256)) * 9)) + b"~>"

        assert decoded(raw, A85, LZW) == pdfium_decoded(tmp_path, raw, b"[/ASCII85Decode /LZWDecode]")

    def test_the_last_filter_is_bounded_by_the_limit(self):
        raw = base64.a85encode(zeros_flate(811)) + b"~>"

        with pytest.raises(OverLimit, match=re.escape("FlateDecode decodes past 810 bytes")):
            length(raw, A85, FLATE, limit=810)

    def test_an_earlier_filter_is_bounded_by_the_encoded_limit(self):
        # pdfium decodes every filter but the last in full: z after the Flate data still decodes.
        raw = flate_then_zeros(2000)

        assert _stream_decode.encoded_limit(810) == 2 * 810 + 4096
        with pytest.raises(OverLimit, match=re.escape("ASCII85Decode decodes past 5716 bytes")):
            length(raw, A85, FLATE, limit=810)

    def test_an_earlier_filter_within_the_encoded_limit_passes(self):
        raw = flate_then_zeros(1000)

        assert length(raw, A85, FLATE, limit=810) == 3

    def test_an_earlier_filter_is_read_to_its_end_after_the_next_one_ends(self):
        # 200,000 zero bytes after the Flate data: Flate ends in the first chunk, within the encoded
        # limit (144,096 bytes), and so does the second; only the third passes it.
        raw = flate_then_zeros(50000)

        with pytest.raises(OverLimit, match=re.escape("ASCII85Decode decodes past 144096 bytes")):
            length(raw, A85, FLATE, limit=70000)

    def test_an_unknown_filter_is_unchecked(self):
        with pytest.raises(Unchecked, match=re.escape("filter 'Crypt' is not one the check decodes")):
            length(b"abc", A85, "Crypt")


class TestBudget:
    def test_each_decoded_piece_is_charged(self):
        log: list[int] = []

        decoded_length(zeros_flate(3 * CHUNK + 5), (FLATE,), limit=1 << 30, charge=charged(log))

        assert log == [CHUNK, CHUNK, CHUNK, 5]

    def test_a_charge_that_does_not_fit_stops_the_check(self):
        with pytest.raises(BudgetSpent):
            decoded_length(zeros_flate(3 * CHUNK), (FLATE,), limit=1 << 30, charge=lambda size: False)


@pytest.mark.parametrize(
    "make,error,message",
    [
        pytest.param(lambda: length(b"", FLATE, limit=0), ValueError, "limit must be >= 1, got 0", id="limit"),
        pytest.param(lambda: length("abc", FLATE), TypeError, "raw must be bytes, got str", id="raw"),
        pytest.param(lambda: length(b"abc"), ValueError, "filters must name at least one filter", id="no-filter"),
    ],
)
def test_invalid_arguments_raise(make, error, message):
    with pytest.raises(error, match=re.escape(message)):
        make()
