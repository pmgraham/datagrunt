"""Tests for the unfiltered grey PNG encoder (``datagrunt.pdf._png``).

The encoder writes page renders and OCR bands. It must be lossless: Pillow
reads back exactly the pixels written, whatever the size, including images
of more than one deflate block.
"""

import io
import random
import struct
import zlib

import pytest
from PIL import Image

from datagrunt.pdf import _png
from datagrunt.pdf._png import grey_png, write_grey_png


def decoded(data: bytes) -> Image.Image:
    with Image.open(io.BytesIO(data)) as image:
        image.load()
        return image.copy()


def chunks(data: bytes) -> list[tuple[bytes, bytes]]:
    """``data``'s chunks as ``(type, data)``, each CRC checked."""
    assert data.startswith(b"\x89PNG\r\n\x1a\n")
    at, found = 8, []
    while at < len(data):
        (size,) = struct.unpack(">I", data[at : at + 4])
        kind, body = data[at + 4 : at + 8], data[at + 8 : at + 8 + size]
        (crc,) = struct.unpack(">I", data[at + 8 + size : at + 12 + size])
        assert crc == zlib.crc32(kind + body)
        found.append((kind, body))
        at += 12 + size
    return found


@pytest.mark.parametrize(("width", "height"), [(1, 1), (7, 3), (1, 300), (513, 257)])
def test_round_trips_exactly(width: int, height: int) -> None:
    pixels = random.Random(width * height).randbytes(width * height)
    image = decoded(grey_png(pixels, width, height))
    assert (image.mode, image.size) == ("L", (width, height))
    assert image.tobytes() == pixels


def test_round_trips_across_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    # Random pixels do not deflate, so zlib hands back output as the blocks come.
    monkeypatch.setattr(_png, "_BLOCK", 2000)
    width, height = 201, 400
    pixels = random.Random(1).randbytes(width * height)
    data = grey_png(pixels, width, height)
    kinds = [kind for kind, _ in chunks(data)]
    assert kinds[0] == b"IHDR" and kinds[-1] == b"IEND"
    assert kinds.count(b"IDAT") > 2
    assert decoded(data).tobytes() == pixels


def test_a_row_wider_than_a_block_is_one_block(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_png, "_BLOCK", 4)
    pixels = bytes(range(10)) * 3
    assert decoded(grey_png(pixels, 10, 3)).tobytes() == pixels


def test_header_is_8_bit_grey_and_rows_unfiltered() -> None:
    pixels = bytes([0, 255, 128, 7, 9, 11])
    found = chunks(grey_png(pixels, 3, 2))
    assert found[0] == (b"IHDR", struct.pack(">IIBBBBB", 3, 2, 8, 0, 0, 0, 0))
    raw = zlib.decompress(b"".join(body for kind, body in found if kind == b"IDAT"))
    assert raw == b"\x00" + pixels[:3] + b"\x00" + pixels[3:]


def test_write_streams_the_same_bytes() -> None:
    pixels = random.Random(2).randbytes(40 * 30)
    sink = io.BytesIO()
    write_grey_png(sink.write, pixels, 40, 30)
    assert sink.getvalue() == grey_png(pixels, 40, 30)


def test_a_flat_page_stays_small() -> None:
    width, height = 2000, 1500
    data = grey_png(b"\xff" * (width * height), width, height)
    assert len(data) < width * height // 100


@pytest.mark.parametrize(
    ("pixels", "width", "height", "error", "match"),
    [
        (b"\x00" * 6, 0, 6, ValueError, "width must be >= 1"),
        (b"\x00" * 6, 6, 0, ValueError, "height must be >= 1"),
        (b"\x00" * 6, 2.0, 3, TypeError, "width must be an int"),
        (b"\x00" * 6, True, 6, TypeError, "width must be an int"),
        (bytearray(6), 2, 3, TypeError, "pixels must be bytes"),
        (b"\x00" * 5, 2, 3, ValueError, r"pixels must be 2 x 3 = 6 bytes, got 5"),
    ],
)
def test_rejects_bad_arguments(pixels: object, width: object, height: object, error: type, match: str) -> None:
    with pytest.raises(error, match=match):
        grey_png(pixels, width, height)  # type: ignore[arg-type]
