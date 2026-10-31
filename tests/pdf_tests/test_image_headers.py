"""Tests for the JPEG and JPEG 2000 header readers (``datagrunt.pdf._image_headers``, #365 T4).

pdfium decodes a DCT image at its SOF size and a JPX image at its SIZ size,
whatever the image dictionary declares, so the engine reads those headers
itself before it lets pdfium decode anything. Fixtures are Pillow-written
JPEG and JPEG 2000 data, patched where a header must lie.
"""

import io
import struct

import pytest
from PIL import Image

from datagrunt.pdf._image_headers import CodestreamSize, jpeg_size, jpx_size


def jpeg(size=(16, 8), mode="L", **options) -> bytes:
    buffer = io.BytesIO()
    Image.new(mode, size, 128 if mode == "L" else (10, 20, 30)).save(buffer, "JPEG", **options)
    return buffer.getvalue()


def jpeg_2000(size=(16, 8), *, jp2: bool = True, **options) -> bytes:
    buffer = io.BytesIO()
    Image.new("L", size, 128).save(buffer, "JPEG2000", no_jp2=not jp2, **options)
    return buffer.getvalue()


SOI = b"\xff\xd8"


class TestJpegSize:
    """jpeg_size reads the first SOFn after the first SOI, as libjpeg does."""

    @pytest.mark.parametrize(
        "data,expected",
        [
            pytest.param(jpeg(), CodestreamSize(16, 8, 1), id="baseline-gray"),
            pytest.param(jpeg((33, 17), "RGB", progressive=True), CodestreamSize(33, 17, 1), id="progressive-rgb"),
            pytest.param(jpeg((9, 5), "RGB", icc_profile=b"p" * 200_000), CodestreamSize(9, 5, 1), id="icc-app2"),
            pytest.param(b"junk" + jpeg(), CodestreamSize(16, 8, 1), id="junk-before-soi"),
        ],
    )
    def test_reads_the_frame_size(self, data, expected):
        assert jpeg_size(data) == expected

    def test_fill_bytes_before_a_marker_are_skipped(self):
        data = jpeg()
        filled = data[:2] + b"\xff\xff" + data[2:]

        assert jpeg_size(filled) == CodestreamSize(16, 8, 1)

    def test_a_zero_height_frame_reads_as_zero(self):
        # A DNL frame: libjpeg refuses it; the caller treats 0 as no size.
        data = bytearray(jpeg())
        sof = data.find(b"\xff\xc0")
        data[sof + 5 : sof + 7] = b"\x00\x00"

        assert jpeg_size(bytes(data)) == CodestreamSize(16, 0, 1)

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(b"", id="empty"),
            pytest.param(b"\x89PNG\r\n\x1a\n" + bytes(40), id="not-jpeg"),
            pytest.param(SOI + b"\xff\xda\x00\x08" + bytes(20), id="scan-before-frame"),
            pytest.param(SOI + b"\xff\xe0\x00\x01" + bytes(20), id="length-below-2"),
            pytest.param(SOI + b"\xff\xe0\x00\x40" + bytes(4), id="segment-past-the-end"),
            pytest.param(jpeg()[:-200][: jpeg().find(b"\xff\xc0") + 6], id="frame-truncated"),
            pytest.param(SOI + b"\x00\xff\xc0" + bytes(20), id="no-marker-after-soi"),
            pytest.param(SOI + b"\xff\x00\x00\x08" + bytes(20), id="stuffed-zero-marker"),
            pytest.param(SOI + b"\xff\xfe\x00\x02" * 5000 + jpeg()[2:], id="over-the-marker-bound"),
        ],
    )
    def test_unreadable_data_has_no_size(self, data):
        assert jpeg_size(data) is None


def siz_offset(data: bytes) -> int:
    return data.find(b"\xff\x4f\xff\x51")


class TestJpxSize:
    """jpx_size reads the codestream's SIZ: a raw J2K codestream's, or a JP2 file's first jp2c box's."""

    @pytest.mark.parametrize("jp2", [True, False], ids=["jp2", "j2k"])
    def test_reads_the_image_size(self, jp2):
        assert jpx_size(jpeg_2000((33, 17), jp2=jp2)) == CodestreamSize(33, 17, 1)

    def test_the_image_offset_is_subtracted(self):
        data = jpeg_2000((33, 17), offset=(5, 3), tile_size=(64, 64))
        (xsiz, ysiz, xo, yo) = struct.unpack_from(">IIII", data, siz_offset(data) + 8)

        assert (xsiz, ysiz, xo, yo) == (38, 20, 5, 3)
        assert jpx_size(data) == CodestreamSize(33, 17, 1)

    def test_counts_the_tiles(self):
        # ceil(33 / 8) x ceil(17 / 8) tiles.
        assert jpx_size(jpeg_2000((33, 17), tile_size=(8, 8))) == CodestreamSize(33, 17, 15)

    def test_reads_a_jp2c_box_after_an_extended_length_box(self):
        data = jpeg_2000()
        extended = struct.pack(">I4sQ", 1, b"uuid", 16 + 4) + b"abcd"

        assert jpx_size(data[:12] + extended + data[12:]) == CodestreamSize(16, 8, 1)

    def test_reads_a_jp2c_box_that_runs_to_the_end(self):
        data = bytearray(jpeg_2000())
        box = data.find(b"jp2c") - 4
        data[box : box + 4] = b"\x00\x00\x00\x00"

        assert jpx_size(bytes(data)) == CodestreamSize(16, 8, 1)

    def test_a_patched_size_is_read_as_written(self):
        data = bytearray(jpeg_2000(jp2=False))
        data[siz_offset(data) + 8 : siz_offset(data) + 16] = struct.pack(">II", 16000, 16000)

        # Pillow's one 16 x 8 tile now repeats 1000 x 2000 times.
        assert jpx_size(bytes(data)) == CodestreamSize(16000, 16000, 2_000_000)

    @pytest.mark.parametrize(
        "patch",
        [
            pytest.param(lambda data: data[: siz_offset(data) + 30], id="siz-truncated"),
            pytest.param(lambda data: data.replace(b"\xff\x4f\xff\x51", b"\xff\x4f\xff\x52", 1), id="no-siz"),
            pytest.param(lambda data: data.replace(b"jp2c", b"jp2x", 1), id="no-jp2c-box"),
            pytest.param(
                lambda data: data[: siz_offset(data) + 4] + b"\x00\x30" + data[siz_offset(data) + 6 :], id="bad-lsiz"
            ),
            pytest.param(
                lambda data: data[: siz_offset(data) + 16] + struct.pack(">I", 99) + data[siz_offset(data) + 20 :],
                id="offset-past-width",
            ),
            pytest.param(
                lambda data: data[: siz_offset(data) + 24] + bytes(4) + data[siz_offset(data) + 28 :], id="zero-tile"
            ),
            pytest.param(lambda data: data[:12] + struct.pack(">I4s", 4, b"free") + data[12:], id="box-below-header"),
            pytest.param(lambda data: data[:12] + struct.pack(">I4s", 8, b"free") * 5000 + data[12:], id="box-bound"),
            pytest.param(lambda data: b"", id="empty"),
        ],
    )
    def test_unreadable_data_has_no_size(self, patch):
        assert jpx_size(patch(jpeg_2000())) is None


class TestJpxComponents:
    """jpx_size reports the SIZ component count (Csiz): decoded memory scales with it."""

    @pytest.mark.parametrize("jp2", [True, False], ids=["jp2", "j2k"])
    @pytest.mark.parametrize("mode,components", [("L", 1), ("RGB", 3), ("RGBA", 4)])
    def test_reads_the_component_count(self, jp2, mode, components):
        buffer = io.BytesIO()
        Image.new(mode, (16, 8)).save(buffer, "JPEG2000", no_jp2=not jp2)

        assert jpx_size(buffer.getvalue()).components == components

    def test_a_count_written_into_siz_is_read_as_written(self):
        # Csiz 40: Lsiz grows by three bytes (Ssiz, XRsiz, YRsiz) per component.
        data = jpeg_2000(jp2=False)
        at = siz_offset(data)
        (csiz,) = struct.unpack_from(">H", data, at + 40)
        component = data[at + 42 : at + 45]
        siz = data[at + 2 : at + 40] + struct.pack(">H", 40) + component * 40
        patched = data[: at + 2] + b"\xff\x51" + struct.pack(">H", 38 + 3 * 40) + siz[4:] + data[at + 42 + 3 * csiz :]

        assert jpx_size(patched).components == 40

    def test_jpeg_and_existing_callers_read_one_component(self):
        # The field defaults to 1, so a three-field CodestreamSize still compares equal.
        assert CodestreamSize(16, 8, 1) == CodestreamSize(16, 8, 1, 1)
        assert jpeg_size(jpeg((9, 5), "RGB")).components == 1
