"""The v5 engine checks an image's stream against its declared size before pdfium may decode it in this process.

pdfium decodes an image behind LZW, ASCII85 or ASCIIHex (or a Flate or
RunLength filter before another) in full to read its metadata or pixels,
whatever its /Width and /Height say. A2 decodes such a stream first, with
the bounded decoders of ``datagrunt.pdf._stream_decode``, and lets pdfium
decode the image in this process only when the stream decodes within the
most its declared size takes: /Width x /Height at 64 bits a pixel (4
components of 16 bits, the most any colour space but DeviceN declares),
plus a predictor byte a row. An image past it, or whose stream cannot be
checked, is recorded at its declared size without pixel metadata, counted,
and never decoded here; a page render, in the memory-capped render worker,
still draws it.

Fixtures are 10 x 10 images (a limit of 10 x (10 x 8 + 1) = 810 bytes) on
the hand-written page of ``test_pdfium_images``.
"""

import base64
import re
import zlib

import pytest
from PIL import Image

from datagrunt.pdf import DatagruntPDFError, _pdfium_images
from datagrunt.pdf.assembly import page_figures
from datagrunt.pdf.pdfium_engine import RawPage, extract_page, open_document
from pdf_tests.test_pdfium_images import (
    DRAWN,
    ccitt_g4,
    decoded,
    gray,
    images_of,
    only_loader,
    run_memory_limited,
    sha,
    stats,
    xobject_page,
)
from pdf_tests.test_render import refusal_of
from pdf_tests.test_stream_decode import from_bits, lzw_chain, lzw_chain_bits, lzw_encode, lzw_two_readings

LIMIT = 810  # 10 rows of 10 pixels at 8 bytes, and a predictor byte each


def ten(filters: bytes) -> bytes:
    """A 10 x 10 8-bit gray image's entries, behind ``filters``."""
    return gray(10, 10, filters)


def flate_past(size: int) -> bytes:
    return zlib.compress(bytes(size), 9)


# Streams of a 10 x 10 image that decode past 810 bytes, each behind its filters.
PAST_THE_LIMIT = [
    pytest.param(flate_past(8 << 20), b"/FlateDecode", id="flate-8MiB"),
    pytest.param(flate_past(LIMIT + 1), b"/FlateDecode", id="flate-one-byte-over"),
    pytest.param(lzw_chain(2000), b"/LZWDecode", id="lzw"),
    pytest.param(lzw_two_readings(200), b"/LZWDecode", id="lzw-either-early-change"),
    pytest.param(b"z" * 203 + b"~>", b"/ASCII85Decode", id="ascii85"),
    pytest.param(bytes(LIMIT + 1).hex().encode() + b">", b"/ASCIIHexDecode", id="asciihex"),
    pytest.param(bytes([129, 0]) * 7, b"/RunLengthDecode", id="runlength"),
    pytest.param(base64.a85encode(flate_past(8 << 20)) + b"~>", b"[/ASCII85Decode /FlateDecode]", id="ascii85-flate"),
    pytest.param((bytes([129, 0]) * 7).hex().encode() + b">", b"[/ASCIIHexDecode /RunLengthDecode]", id="asciihex-rl"),
    pytest.param(b"z" * 1500 + b"~>", b"[/ASCII85Decode /CCITTFaxDecode]", id="ascii85-ccitt"),
]


def predicted_rows(row: bytes, rows: int = 10) -> bytes:
    """``rows`` rows of ``row``, each after a PNG predictor tag of 0 (None)."""
    return (b"\x00" + row) * rows


# A 10 x 10 16-bit CMYK image (8 bytes a pixel) whose PNG-predicted rows inflate to exactly the limit.
CMYK16 = b"/Width 10 /Height 10 /ColorSpace /DeviceCMYK /BitsPerComponent 16 /Filter /FlateDecode "
CMYK16 += b"/DecodeParms << /Predictor 15 /Colors 4 /BitsPerComponent 16 /Columns 10 >>"
CMYK16_ROWS = predicted_rows(bytes(range(80)))


@pytest.fixture
def decode_calls(monkeypatch):
    """Record every pdfium call that can decode an image's pixels; none of them reaches pdfium."""
    calls = []
    for name in ("FPDFImageObj_GetImageMetadata", "FPDFImageObj_GetBitmap", "FPDFImageObj_GetRenderedBitmap"):
        monkeypatch.setattr(_pdfium_images.pdfium_c, name, lambda *args, _name=name: calls.append(_name))
    return calls


def test_the_limit_is_64_bits_a_pixel_and_a_predictor_byte_a_row():
    assert _pdfium_images._MAX_PIXEL_BITS == 4 * 16
    assert _pdfium_images.stream_limit(10, 10) == LIMIT
    assert _pdfium_images.stream_limit(4, 2) == 2 * (4 * 8 + 1)


class TestPastTheDeclaredSize:
    @pytest.mark.parametrize("data,filters", PAST_THE_LIMIT)
    def test_it_is_recorded_at_its_declared_size_without_pdfium_reading_it(self, tmp_path, decode_calls, data, filters):
        got = images_of(xobject_page(tmp_path, data, ten(filters)))

        (image,) = got.images
        assert (image.px_width, image.px_height, image.declared_px) == (10, 10, (10, 10))
        assert (image.bits_per_pixel, image.colorspace, image.raw_sha256) == (None, None, sha(data))
        assert got.stats == stats(overlong=1, cover=DRAWN)
        assert decode_calls == []

    @pytest.mark.parametrize("data,filters", PAST_THE_LIMIT)
    def test_png_bytes_refuses_it_before_pdfium_decodes_it(self, tmp_path, decode_calls, data, filters):
        image_loader = only_loader(xobject_page(tmp_path, data, ten(filters)))

        with pytest.raises(DatagruntPDFError, match=r"page 1: object 0's stream does not decode within its declared"):
            image_loader.png_bytes()
        assert image_loader.raw_bytes() == data
        assert decode_calls == []

    def test_the_refusal_names_the_filter_and_the_limit(self, tmp_path):
        image_loader = only_loader(xobject_page(tmp_path, flate_past(8 << 20), ten(b"/FlateDecode")))

        message = (
            "page 1: object 0's stream does not decode within its declared size: FlateDecode decodes past 810 "
            "bytes (10 x 10 pixels at 64 bits a pixel, and a predictor byte a row); it is decoded only when its "
            "page is rendered, in the render worker"
        )
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            image_loader.png_bytes()

    def test_a_figure_refuses_to_read_its_png(self, tmp_path):
        data = flate_past(8 << 20)
        path = xobject_page(tmp_path, data, ten(b"/FlateDecode"))
        with open_document(path) as handle:
            page = extract_page(handle, 0)
        assert isinstance(page, RawPage)

        (draft,) = page_figures(page.images, page.geometry, source=path, min_image_dimension=0).drafts
        figure = draft.figure(page_number=1, reading_order=0)

        assert figure.read_raw_bytes() == data
        with pytest.raises(DatagruntPDFError, match=re.escape("decodes past 810 bytes")):
            figure.read_bytes()

    def test_the_render_guard_passes_it_to_the_capped_worker(self, tmp_path):
        assert refusal_of(xobject_page(tmp_path, flate_past(8 << 20), ten(b"/FlateDecode"))) is None

    def test_a_long_lzw_stream_stays_small_in_a_memory_limited_process(self, tmp_path):
        # 1.2 GiB in 0.5 MB: codes naming the entry about to be added, then the longest entry again and again.
        repeats = (1200 << 20) // 3801
        data = from_bits(lzw_chain_bits(3800) + format(4057, "012b") * repeats)
        done = run_memory_limited(xobject_page(tmp_path, data, ten(b"/LZWDecode")))

        assert done.returncode == 0, done.stdout + done.stderr
        assert "refused: " in done.stdout and "LZWDecode decodes past 810 bytes" in done.stdout
        assert int(re.search(r"peak (\d+) MiB", done.stdout).group(1)) < 400, done.stdout


class TestWithinTheDeclaredSize:
    @pytest.mark.parametrize(
        "data,filters",
        [
            pytest.param(flate_past(LIMIT), b"/FlateDecode", id="flate"),
            pytest.param(lzw_encode(bytes(LIMIT)), b"/LZWDecode", id="lzw"),
            pytest.param(b"z" * 202 + base64.a85encode(bytes(2)) + b"~>", b"/ASCII85Decode", id="ascii85"),
            pytest.param(bytes(LIMIT).hex().encode() + b">", b"/ASCIIHexDecode", id="asciihex"),
            pytest.param(bytes([129, 0]) * 6 + bytes([215, 0]), b"/RunLengthDecode", id="runlength"),
            pytest.param(
                base64.a85encode(flate_past(LIMIT)) + b"~>", b"[/ASCII85Decode /FlateDecode]", id="ascii85-flate"
            ),
        ],
    )
    def test_a_stream_of_exactly_the_limit_is_decoded(self, tmp_path, data, filters):
        path = xobject_page(tmp_path, data, ten(filters))

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height, image.bits_per_pixel, image.colorspace) == (10, 10, 8, "DeviceGray")
        assert got.stats == stats(cover=DRAWN)
        assert decoded(only_loader(path).png_bytes()).size == (10, 10)

    def test_a_16_bit_cmyk_image_with_a_predictor_fills_the_limit_and_is_decoded(self, tmp_path):
        assert len(CMYK16_ROWS) == LIMIT
        path = xobject_page(tmp_path, zlib.compress(CMYK16_ROWS), CMYK16)

        got = images_of(path)

        (image,) = got.images
        assert (image.bits_per_pixel, image.colorspace) == (24, "DeviceCMYK")
        assert got.stats == stats(cover=DRAWN)
        assert decoded(only_loader(path).png_bytes()).size == (10, 10)

    def test_one_byte_more_than_the_predictor_allows_is_past_it(self, tmp_path):
        got = images_of(xobject_page(tmp_path, zlib.compress(CMYK16_ROWS + b"\x00"), CMYK16))

        assert got.stats == stats(overlong=1, cover=DRAWN)

    def test_a_ccitt_image_behind_ascii85_is_decoded(self, tmp_path):
        bilevel = Image.new("1", (10, 10), 1)
        data = base64.a85encode(ccitt_g4(bilevel)) + b"~>"
        entries = b"/Width 10 /Height 10 /ColorSpace /DeviceGray /BitsPerComponent 1 "
        entries += b"/Filter [/ASCII85Decode /CCITTFaxDecode] /DecodeParms [null << /K -1 /Columns 10 /Rows 10 >>]"
        path = xobject_page(tmp_path, data, entries)

        got = images_of(path)

        assert got.stats == stats(cover=DRAWN)
        assert decoded(only_loader(path).png_bytes()).size == (10, 10)


class TestUnchecked:
    @pytest.mark.parametrize(
        "data,filters,why",
        [
            pytest.param(b"not a zlib stream", b"/FlateDecode", "FlateDecode data cannot be inflated", id="flate"),
            pytest.param(b"87cU\x00RD]i~>", b"/ASCII85Decode", "ASCII85Decode data holds byte 0x00", id="ascii85"),
        ],
    )
    def test_a_stream_that_cannot_be_checked_is_not_decoded(self, tmp_path, decode_calls, data, filters, why):
        path = xobject_page(tmp_path, data, ten(filters))

        got = images_of(path)

        assert got.stats == stats(overlong=1, cover=DRAWN)
        message = f"page 1: object 0's stream cannot be checked against its declared size: {why}"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            only_loader(path).png_bytes()
        assert decode_calls == []


class TestBudget:
    def test_the_check_charges_what_it_decodes_to_the_page_budget(self, tmp_path, monkeypatch):
        data = flate_past(800)
        content = b"q 20 0 0 10 10 20 cm /Im Do Q q 20 0 0 10 35 20 cm /Im Do Q"
        # Room for both raw streams and one image's 800 decoded bytes, not two.
        monkeypatch.setattr(_pdfium_images, "_MAX_PAGE_STREAM_BYTES", 2 * len(data) + 800 + 100)

        got = images_of(xobject_page(tmp_path, data, ten(b"/FlateDecode"), content))

        assert [item.raw_sha256 for item in got.images] == [sha(data), None]
        assert (got.stats.skipped_over_budget_images, got.stats.skipped_overlong_images) == (1, 0)
