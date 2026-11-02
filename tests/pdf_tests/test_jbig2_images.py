"""JBIG2 images in the v5 pdfium engine: recorded at their declared size, decoded only in the render worker.

pdfium decodes a JBIG2 image at its image dictionary's ``/Width`` x
``/Height``, whatever its page information segment says (pinned below), so
A2 records that size. Its pixels are decoded only when its page is drawn by
the render worker, whose memory is capped: ImageLoader never decodes one in
the calling process.

No JBIG2 encoder is installed, so fixtures are written by hand: an embedded
stream (no file header) of a page information segment, one immediate
generic region whose data is MMR (CCITT Group 4, from Pillow's TIFF writer,
of the inverted image: in JBIG2 a 1 bit is black), and an end of page.
"""

import ctypes
import io
import re
import struct
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest
from PIL import Image

from datagrunt.pdf import DatagruntPDFError
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.render import RenderFailed, render_page
from pdf_tests.builders import PageSpec, build_pdf, hand_written_pdf
from pdf_tests.test_ocr import needs_tesseract
from pdf_tests.test_ocr_scans import LINES, TOKENS, rendered
from pdf_tests.test_pdfium_images import (
    images_of,
    only_loader,
    stats,
    xobject_page,
)
from pdf_tests.test_pipeline import element_texts
from pdf_tests.test_render import refusal_of

DRAWN = 80 * 40 / 120000  # the cover of an image drawn by xobject_page
ASCII85_JBIG2 = b"[/ASCII85Decode /JBIG2Decode]"


@pytest.fixture
def decode_calls(monkeypatch):
    """Record every pdfium call that can decode an image's pixels; none of them reaches pdfium."""
    calls = []
    for name in ("FPDFImageObj_GetImageMetadata", "FPDFImageObj_GetBitmap", "FPDFImageObj_GetRenderedBitmap"):
        monkeypatch.setattr(pdfium_c, name, lambda *args, _name=name: calls.append(_name))
    return calls


def segment(number: int, segment_type: int, data: bytes) -> bytes:
    """A segment header (no referred-to segments, page 1, 1-byte page association) and its data."""
    return struct.pack(">IBBBI", number, segment_type, 0, 1, len(data)) + data


def mmr(image: Image.Image) -> bytes:
    """``image`` (Pillow mode "1", 0 black) MMR-coded as a JBIG2 generic region reads it."""
    inverted = Image.eval(image.convert("L"), lambda value: 255 - value).convert("1")
    buffer = io.BytesIO()
    inverted.save(buffer, "TIFF", compression="group4", tiffinfo={278: image.height})  # one strip (RowsPerStrip)
    tiff = Image.open(io.BytesIO(buffer.getvalue()))
    (offset,), (count,) = tiff.tag_v2[273], tiff.tag_v2[279]
    return buffer.getvalue()[offset : offset + count]


def jbig2(image: Image.Image, page_size: tuple[int, int] | None = None) -> bytes:
    """An embedded JBIG2 stream of ``image``: page information (``page_size``, default the image's), region, end."""
    width, height = image.size
    page = struct.pack(">IIIIBH", *(page_size or image.size), 0, 0, 0, 0)
    region = struct.pack(">IIIIB", width, height, 0, 0, 0) + b"\x01" + mmr(image)
    return segment(0, 48, page) + segment(1, 38, region) + segment(2, 49, b"")


def pattern(width: int = 64, height: int = 48) -> Image.Image:
    """A bilevel pattern of 4-pixel squares."""
    image = Image.new("1", (width, height), 1)
    for x in range(width):
        for y in range(height):
            if (x // 4 + y // 4) % 3 == 0:
                image.putpixel((x, y), 0)
    return image


def entries(width: int, height: int) -> bytes:
    return b"/Width %d /Height %d /ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /JBIG2Decode" % (width, height)


def pdfium_bitmap_size(path: Path) -> tuple[int, int]:
    """The size of the bitmap pdfium decodes page 1's first image object to."""
    document = pdfium.PdfDocument(str(path))
    try:
        page = document[0]
        image = pdfium_c.FPDFPage_GetObject(page.raw, 0)
        bitmap = pdfium_c.FPDFImageObj_GetBitmap(image)
        assert bitmap
        try:
            return pdfium_c.FPDFBitmap_GetWidth(bitmap), pdfium_c.FPDFBitmap_GetHeight(bitmap)
        finally:
            pdfium_c.FPDFBitmap_Destroy(bitmap)
    finally:
        document.close()


class TestRecord:
    """A2 records a JBIG2 image at the size pdfium decodes it at: its dictionary's."""

    @pytest.mark.parametrize("declared", [(64, 48), (32, 16), (128, 96)])
    def test_pdfium_decodes_at_the_declared_size_whatever_the_page_information_says(self, tmp_path, declared):
        path = xobject_page(tmp_path, jbig2(pattern()), entries(*declared))

        assert pdfium_bitmap_size(path) == declared

    def test_a_jbig2_image_is_recorded_at_its_declared_size(self, tmp_path, decode_calls):
        data = jbig2(pattern(), page_size=(640, 480))
        path = xobject_page(tmp_path, data, entries(64, 48))

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height, image.declared_px) == (64, 48, (64, 48))
        assert (image.filters, image.bits_per_pixel, image.colorspace) == (("JBIG2Decode",), None, None)
        assert got.stats == stats(cover=DRAWN)
        assert decode_calls == []

    def test_a_jbig2_image_behind_ascii85_is_recorded_at_its_declared_size(self, tmp_path):
        import base64

        wrapped = base64.a85encode(jbig2(pattern())) + b"~>"
        path = xobject_page(tmp_path, wrapped, entries(64, 48).replace(b"/JBIG2Decode", ASCII85_JBIG2))

        (image,) = images_of(path).images

        assert (image.px_width, image.px_height) == (64, 48)

    def test_a_jbig2_image_behind_flatedecode_is_unsized(self, tmp_path, decode_calls):
        import zlib

        filters = entries(64, 48).replace(b"/JBIG2Decode", b"[/FlateDecode /JBIG2Decode]")
        path = xobject_page(tmp_path, zlib.compress(jbig2(pattern())), filters)

        got = images_of(path)

        assert [(image.px_width, image.px_height) for image in got.images] == [(0, 0)]
        assert got.stats == stats(unsized=1, cover=DRAWN)
        message = "object 0 is a JBIG2Decode stream behind FlateDecode: its size cannot be read without decoding"
        assert refusal_of(path) == message
        assert decode_calls == []

    def test_a_jbig2_image_over_the_pixel_bound_is_oversized(self, tmp_path, decode_calls):
        path = xobject_page(tmp_path, jbig2(pattern()), entries(10000, 10000))

        got = images_of(path)

        assert [(image.px_width, image.px_height) for image in got.images] == [(10000, 10000)]
        assert got.stats == stats(oversized=1, cover=DRAWN)
        assert decode_calls == []


class TestLoader:
    """ImageLoader never decodes a JBIG2 image in the calling process."""

    def test_png_bytes_refuses_a_jbig2_image(self, tmp_path, decode_calls):
        path = xobject_page(tmp_path, jbig2(pattern()), entries(64, 48))

        message = "object 0 is a JBIG2 image: it is decoded only when its page is rendered, in the render worker"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            only_loader(path).png_bytes()
        assert decode_calls == []

    def test_raw_bytes_returns_the_jbig2_stream(self, tmp_path):
        data = jbig2(pattern())

        assert only_loader(xobject_page(tmp_path, data, entries(64, 48))).raw_bytes() == data


class TestRender:
    """The render guard passes a JBIG2 image to the render worker, whose memory cap bounds its decode."""

    def test_the_guard_passes_a_jbig2_image(self, tmp_path):
        assert refusal_of(xobject_page(tmp_path, jbig2(pattern()), entries(64, 48))) is None

    def test_the_guard_refuses_a_jbig2_image_over_the_pixel_bound(self, tmp_path):
        path = xobject_page(tmp_path, jbig2(pattern()), entries(10000, 10000))

        assert refusal_of(path) == "object 0 decodes to 10000 x 10000 = 100000000 pixels, over the 89478485-pixel bound"

    def test_the_worker_draws_a_jbig2_image(self, tmp_path):
        # The 64 x 48 pattern drawn 64 x 48 pt at 72 DPI on a page of its size: pixel for pixel.
        content = b"q 64 0 0 48 0 0 cm /Im Do Q"
        path = xobject_page(tmp_path, jbig2(pattern()), entries(64, 48), content=content)

        page = render_page(path, 0, dpi=72, memory_cap_bytes=1 << 30, timeout_s=120.0)

        drawn = Image.frombytes("L", (page.px_width, page.px_height), page.pixels).crop((0, 252, 64, 300))
        assert drawn.point(lambda value: 255 if value > 127 else 0).convert("1").tobytes() == pattern().tobytes()

    def test_a_jbig2_decode_over_the_worker_cap_is_a_render_failure(self, tmp_path):
        # An all-white 8000 x 8000 region (one MMR bit a row) decodes to an 8 MB 1-bit bitmap in the worker:
        # observed on macOS, its page renders under a 24 MiB cap and not under 16 MiB.
        side = 8000
        region = struct.pack(">IIIIB", side, side, 0, 0, 0) + b"\x01" + b"\xff" * (side // 8 + 1)
        page_info = struct.pack(">IIIIBH", side, side, 0, 0, 0, 0)
        data = segment(0, 48, page_info) + segment(1, 38, region) + segment(2, 49, b"")
        path = xobject_page(tmp_path, data, entries(side, side))

        assert refusal_of(path) is None
        # The page itself is 400 x 300 px at 72 DPI: only the decode is over the cap.
        with pytest.raises(RenderFailed):
            render_page(path, 0, dpi=72, memory_cap_bytes=4 << 20, timeout_s=120.0)
        assert render_page(path, 0, dpi=72, memory_cap_bytes=1 << 30, timeout_s=120.0).px_width == 400


class TestDecodedBeforeTheRender:
    """The render worker decodes each JBIG2 image on its own before drawing the page.

    pdfium drops an image it cannot decode and draws the rest of the page: a
    decode that fails for memory (Linux and Windows cap the worker's address
    space or commit, so the allocation fails) would leave a blank where the
    image was. Decoded first, its failure is the page's: a RenderFailed.
    """

    def test_a_jbig2_image_that_cannot_be_decoded_fails_the_page(self, tmp_path, monkeypatch):
        from datagrunt.pdf import render

        path = xobject_page(tmp_path, jbig2(pattern()), entries(64, 48))
        decoded = []

        def cannot_decode(image):
            decoded.append(image)
            return None

        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetBitmap", cannot_decode)

        message = "page 1: pdfium cannot decode JBIG2 image object 0 in the render worker; the page is not rendered"
        with pytest.raises(RenderFailed, match=re.escape(message)):
            render._render_guarded(path, 0, 72, None)
        assert len(decoded) == 1

    def test_a_page_whose_jbig2_images_decode_is_drawn(self, tmp_path, monkeypatch):
        from datagrunt.pdf import render

        content = b"q 64 0 0 48 0 0 cm /Im Do Q"
        path = xobject_page(tmp_path, jbig2(pattern()), entries(64, 48), content=content)
        real = pdfium_c.FPDFImageObj_GetBitmap
        decoded = []

        def counted(image):
            decoded.append(image)
            return real(image)

        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetBitmap", counted)

        drawn = render._render_guarded(path, 0, 72, None)

        assert (drawn.width, drawn.height, len(decoded)) == (400, 300, 1)

    def test_a_page_without_jbig2_images_decodes_none_first(self, tmp_path, monkeypatch):
        from datagrunt.pdf import render

        path = xobject_page(tmp_path, b"\x80" * 64, b"/Width 8 /Height 8 /ColorSpace /DeviceGray /BitsPerComponent 8")
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetBitmap", lambda image: pytest.fail("decoded before the render"))

        assert render._render_guarded(path, 0, 72, None).width == 400


@pytest.fixture(scope="module")
def jbig2_scan_pdf(tmp_path_factory) -> Path:
    """A letter page of four sentences rendered at 300 DPI, made bilevel and embedded as one JBIG2 image."""
    folder = tmp_path_factory.mktemp("jbig2-scan")
    digital = build_pdf(folder / "digital.pdf", [PageSpec(items=LINES)])
    page = rendered(digital)
    gray = Image.frombytes("L", (page.px_width, page.px_height), page.pixels)
    bilevel = gray.point(lambda value: 255 if value > 160 else 0).convert("1")
    content = b"q 612 0 0 792 0 0 cm /Im Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /XObject << /Im 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
    ]
    data = jbig2(bilevel)
    objects.append(
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n" % (entries(*bilevel.size), len(data))
        + data
        + b"\nendstream"
    )
    return hand_written_pdf(folder / "jbig2-scan.pdf", objects)


@needs_tesseract
def test_a_jbig2_scan_is_read_by_ocr_through_extract_document(jbig2_scan_pdf):
    (page,) = extract_document(jbig2_scan_pdf).pages

    assert (page.status, page.route) == ("ok", "vision")
    read = set(" ".join(element_texts(page)).split())
    assert len(set(TOKENS) & read) >= 0.9 * len(set(TOKENS))


def test_the_bitmap_of_a_jbig2_image_is_its_pattern(tmp_path):
    # The fixture decodes as drawn: pdfium reads the MMR region at the declared size, 1 bits black.
    path = xobject_page(tmp_path, jbig2(pattern()), entries(64, 48))
    document = pdfium.PdfDocument(str(path))
    try:
        page = document[0]
        image = pdfium_c.FPDFPage_GetObject(page.raw, 0)
        bitmap = pdfium_c.FPDFImageObj_GetBitmap(image)
        try:
            stride = pdfium_c.FPDFBitmap_GetStride(bitmap)
            pixels = (ctypes.c_ubyte * (stride * 48)).from_address(pdfium_c.FPDFBitmap_GetBuffer(bitmap))
            got = Image.frombuffer("L", (64, 48), bytes(pixels), "raw", "L", stride, 1).convert("1")
        finally:
            pdfium_c.FPDFBitmap_Destroy(bitmap)
    finally:
        document.close()

    assert got.tobytes() == pattern().tobytes()
