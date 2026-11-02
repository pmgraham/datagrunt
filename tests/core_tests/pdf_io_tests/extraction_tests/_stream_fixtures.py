"""Small generated image streams and PDFs for the decoded-size check tests.

Every encoder here is small and exact. A stream that decodes to far more than
its image declares is a few kilobytes on disk: zeros compress well under every
filter. Nothing large is written; the decoded sizes exist only while decoding.
"""

import base64
import binascii
import zlib
from pathlib import Path

MIB = 1 << 20

_LZW_CLEAR = 256
_LZW_END = 257
_LZW_FIRST = 258
# The test encoder starts a new table well before the 4096-code limit.
_LZW_TABLE_RESTART = 4000


def hand_written_pdf(path: Path, objects: list[bytes]) -> Path:
    """Write ``objects`` (object 1 is the catalog) as a PDF with a correct xref."""
    data = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(data))
        data += b"%d 0 obj\n%s\nendobj\n" % (number, body)
    xref = len(data)
    data += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1)
    data += b"".join(b"%010d 00000 n \n" % offset for offset in offsets)
    data += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref)
    path.write_bytes(bytes(data))
    return path


def flate_zeros(size: int) -> bytes:
    """A Flate stream that inflates to ``size`` zero bytes."""
    compressor = zlib.compressobj(9)
    block = bytes(min(size, MIB))
    parts = []
    for start in range(0, size, MIB):
        parts.append(compressor.compress(block[: min(MIB, size - start)]))
    parts.append(compressor.flush())
    return b"".join(parts)


class _LzwWriter:
    """Writes LZW codes most significant bit first, at the width the decoder reads them with."""

    def __init__(self, early_change: int) -> None:
        self.early_change = early_change
        self.bits = 0
        self.count = 0
        self.out = bytearray()
        self.decoder_size = _LZW_FIRST
        self.first = True

    def width(self) -> int:
        width = 9
        while width < 12 and self.decoder_size + self.early_change >= (1 << width):
            width += 1
        return width

    def emit(self, code: int) -> None:
        width = self.width()
        self.bits = (self.bits << width) | code
        self.count += width
        while self.count >= 8:
            self.count -= 8
            self.out.append((self.bits >> self.count) & 0xFF)
        self.bits &= (1 << self.count) - 1

    def data_code(self, code: int) -> None:
        """Emit a data code; the decoder adds a table entry for every data code but the first after a clear."""
        self.emit(code)
        if not self.first:
            self.decoder_size = min(self.decoder_size + 1, 4096)
        self.first = False

    def clear(self) -> None:
        self.emit(_LZW_CLEAR)
        self.decoder_size = _LZW_FIRST
        self.first = True

    def finish(self) -> bytes:
        self.emit(_LZW_END)
        if self.count:
            self.out.append((self.bits << (8 - self.count)) & 0xFF)
        return bytes(self.out)


def lzw_encode(data: bytes, early_change: int = 1) -> bytes:
    """``data`` LZW-encoded as a PDF ``/LZWDecode`` stream with the given ``/EarlyChange``."""
    writer = _LzwWriter(early_change)
    writer.clear()
    table = {bytes((value,)): value for value in range(256)}
    next_code = _LZW_FIRST
    current = b""
    for value in data:
        candidate = current + bytes((value,))
        if candidate in table:
            current = candidate
            continue
        writer.data_code(table[current])
        table[candidate] = next_code
        next_code += 1
        if next_code >= _LZW_TABLE_RESTART:
            writer.clear()
            table = {bytes((byte,)): byte for byte in range(256)}
            next_code = _LZW_FIRST
        current = bytes((value,))
    if current:
        writer.data_code(table[current])
    return writer.finish()


def lzw_zeros(size: int, early_change: int = 1) -> bytes:
    """An LZW stream that decodes to at least ``size`` zero bytes: codes 0, 258, 259, ... (each one zero longer)."""
    writer = _LzwWriter(early_change)
    writer.clear()
    total, next_code = 0, _LZW_FIRST
    while total < size:  # every code adds at least one byte
        if next_code >= _LZW_TABLE_RESTART:
            writer.clear()
            next_code = _LZW_FIRST
        if writer.first:
            writer.data_code(0)
            total += 1
            continue
        writer.data_code(next_code)
        total += next_code - 256
        next_code += 1
    return writer.finish()


def lzw_only_with_early_change_0(size: int) -> bytes:
    """An LZW stream for ``/EarlyChange 0`` that decodes to at least ``size`` bytes, but ends early read with 1.

    Both readings agree until the table holds 511 codes, where a reader with
    ``/EarlyChange 1`` widens to 10 bits one code early: the next 9-bit code
    (128) and the first bit of the code after it read, at 10 bits, as 257 (end
    of data). The ``/EarlyChange 0`` reader goes on through codes for 254 zeros.
    """
    writer = _LzwWriter(early_change=0)
    writer.clear()
    writer.data_code(0)
    total, next_code = 1, _LZW_FIRST
    while writer.decoder_size < 511:  # one table entry a code
        writer.data_code(next_code)
        total += next_code - 256
        next_code += 1
    writer.data_code(128)
    writer.data_code(writer.decoder_size)  # the code just made (a 1 bit first, at 10 bits)
    total += 3
    while total < size:  # each code adds 254 bytes
        writer.data_code(510)
        total += 254
    return writer.finish()


def run_length(data: bytes) -> bytes:
    """``data`` as RunLengthDecode literal runs of up to 128 bytes, then the end-of-data byte."""
    runs = [bytes((len(data[i : i + 128]) - 1,)) + data[i : i + 128] for i in range(0, len(data), 128)]
    return b"".join(runs) + b"\x80"


def run_length_zeros(size: int) -> bytes:
    """A RunLengthDecode stream of 128-byte zero runs that decodes to at least ``size`` bytes."""
    return b"\x81\x00" * -(-size // 128) + b"\x80"


def ascii85(data: bytes) -> bytes:
    """``data`` as an ASCII85Decode stream (``z`` for four zero bytes), ended by ``~>``."""
    return base64.a85encode(data) + b"~>"


def ascii_hex(data: bytes) -> bytes:
    """``data`` as an ASCIIHexDecode stream, ended by ``>``."""
    return binascii.hexlify(data) + b">"


def png_rows(data: bytes, row: int) -> bytes:
    """``data`` split into rows of ``row`` bytes, each led by PNG filter type 0 (None)."""
    return b"".join(b"\x00" + data[i : i + row] for i in range(0, len(data), row))


def text_content(extra: bytes = b"") -> bytes:
    """A page content stream drawing one line of text (so no page is scanned and OCR'd), then ``extra``."""
    return b"BT /F1 12 Tf 72 720 Td (Image stream) Tj ET " + extra


_FONT = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"


def _stream(body: bytes, entries: bytes = b"") -> bytes:
    return b"<< %s /Length %d >>\nstream\n%s\nendstream" % (entries, len(body), body)


def image_page_pdf(
    path: Path, stream: bytes, entries: bytes, *, text: bool = True, extra_objects: tuple[bytes, ...] = ()
) -> Path:
    """A one-page PDF drawing image XObject 5 (``entries`` + ``stream``); with a text line unless ``text`` is False.

    ``extra_objects`` are written from object 7 on (object 6 is the font).
    """
    draw = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    content = text_content(draw) if text else draw
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 6 0 R >> /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        _stream(content),
        _stream(stream, b"/Type /XObject /Subtype /Image " + entries),
        _FONT,
        *extra_objects,
    ]
    return hand_written_pdf(path, objects)


def image_object(stream: bytes, entries: bytes) -> bytes:
    """An image XObject of ``entries`` and ``stream``, to pass as one of ``extra_objects``."""
    return _stream(stream, b"/Type /XObject /Subtype /Image " + entries)


def inline_image(image_dict: bytes, data: bytes) -> bytes:
    """A content-stream fragment drawing one inline image."""
    assert b"EI" not in data, "inline data must not hold EI"
    return b"q 100 0 0 100 50 50 cm BI " + image_dict + b" ID " + data + b"\nEI Q "


def pages_pdf(path: Path, contents: list[bytes], extra_objects: tuple[bytes, ...] = ()) -> Path:
    """A PDF with one page per content stream (each also drawing a text line); objects 4.. are the contents.

    The font is the object after the contents; ``extra_objects`` follow it, and
    each page's resources name ``/Fm0`` as the first of them when given.
    """
    count = len(contents)
    font = 3 + 2 * count
    kids = b" ".join(b"%d 0 R" % (3 + i) for i in range(count))
    form = b" /XObject << /Fm0 %d 0 R >>" % (font + 1) if extra_objects else b""
    pages = [
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 %d 0 R >>%s >> "
        b"/Contents %d 0 R >>" % (font, form, 3 + count + i)
        for i in range(count)
    ]
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [%s] /Count %d >>" % (kids, count),
        *pages,
        *(_stream(text_content(content)) for content in contents),
        _FONT,
        *extra_objects,
    ]
    return hand_written_pdf(path, objects)


def form_object(content: bytes) -> bytes:
    """A Form XObject whose content stream is ``content``."""
    return _stream(content, b"/Type /XObject /Subtype /Form /BBox [0 0 612 792]")
