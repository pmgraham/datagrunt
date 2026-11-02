"""A JPEG 2000 / JBIG2 image must not be decoded at its codestream size.

A ``/JPXDecode`` image may declare a small ``/Width`` ``/Height`` while its
codestream's SIZ marker describes a huge image; pdfium and MuPDF decode the
whole codestream at the SIZ size when datagrunt reads the image's size, extracts
it, or renders a page that contains it. These tests build real JPEG 2000
fixtures (Pillow) embedded in hand-written PDFs and prove that every v4 path
(both engines, ``to_dicts`` / ``extract_images`` / ``render_pages_as_images`` /
the OCR path) records a warning and does not decode the image.

Two kinds of proof:
* a skip-with-warning test with the caps patched small and a spy on every
  decode entry point (no gigabytes allocated); and
* a subprocess per engine with a moderate codestream, asserting peak RSS stays
  under a small bound (it is far over the bound before the guard).
"""

import io
import struct
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from PIL import Image

from datagrunt.core.pdf_io.extraction import image_guard
from datagrunt.pdf_api import PDFReader, PDFWriter

# ---------------------------------------------------------------------------
# Fixtures: hand-written PDFs embedding real JPEG 2000 / JBIG2 image streams.
# ---------------------------------------------------------------------------


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


def j2k(size=(16, 16), *, siz=None, tile=None, mode="L") -> bytes:
    """A Pillow raw J2K codestream; ``siz`` / ``tile`` overwrite its SIZ size and tile size."""
    buffer = io.BytesIO()
    fill = 128 if mode == "L" else (10, 20, 30)
    Image.new(mode, size, fill).save(buffer, "JPEG2000", no_jp2=True)
    data = bytearray(buffer.getvalue())
    at = data.find(b"\xff\x4f\xff\x51")
    if siz is not None:
        data[at + 8 : at + 16] = struct.pack(">II", *siz)
    if tile is not None:
        data[at + 24 : at + 32] = struct.pack(">II", *tile)
    return bytes(data)


def gray_entries(width: int, height: int, codec: bytes, extra: bytes = b"") -> bytes:
    return b"/Width %d /Height %d /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter %s %s" % (
        width,
        height,
        codec,
        extra,
    )


def image_page_pdf(path: Path, stream: bytes, entries: bytes) -> Path:
    """A one-page PDF drawing a single image XObject with ``entries`` and ``stream``.

    The page has no text layer, so the scanned/OCR route runs when parsed.
    """
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream" % (entries, len(stream), stream),
    ]
    return hand_written_pdf(path, objects)


def jbig2_segment(number: int, segment_type: int, data: bytes, page: int = 1) -> bytes:
    return struct.pack(">IBBBI", number, segment_type, 0x00, page, len(data)) + data


def jbig2_stream(width: int, height: int) -> bytes:
    page_info = struct.pack(">IIIIBH", width, height, 0, 0, 0, 0)
    return jbig2_segment(0, 48, page_info) + jbig2_segment(1, 49, b"")


# SIZ 16 x 16 declared, 100 x 100 codestream: decodes to 10,000 px (over a
# 1,000-px patched cap) but allocates almost nothing if the guard misfires.
JPX_SMALL_BOMB = j2k((16, 16), siz=(100, 100), tile=(100, 100))
# 10000 x 10000 gray codestream (100M px, over the 89.4M-px cap): decodes to
# 487-692 MiB (measured, both engines), far over the 400 MiB bound; the guard
# skips it, so after the fix the decode never happens.
JPX_RSS_BOMB = j2k((16, 16), siz=(10000, 10000), tile=(10000, 10000))
NORMAL_JPX = j2k((48, 48))
BOMB_ENTRIES = gray_entries(16, 16, b"/JPXDecode")

ENGINES = ["pdfium", "pymupdf"]


def smask_bomb_pdf(path: Path) -> Path:
    """A one-page PDF: a good Flate base image carrying a JPEG 2000 bomb as its /SMask.

    The base image is tiny and legitimate; the soft mask is a separate stream a
    render decodes at its codestream size.
    """
    import zlib

    base = zlib.compress(b"\x80" * 64 * 64)
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image /Width 64 /Height 64 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter /FlateDecode /SMask 6 0 R /Length %d >>\nstream\n%s\nendstream"
        % (len(base), base),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (BOMB_ENTRIES, len(JPX_RSS_BOMB), JPX_RSS_BOMB),
    ]
    return hand_written_pdf(path, objects)


def _bomb_image_obj() -> bytes:
    return b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream" % (
        BOMB_ENTRIES,
        len(JPX_RSS_BOMB),
        JPX_RSS_BOMB,
    )


def annotation_ap_bomb_pdf(path: Path) -> Path:
    """A page with an empty content stream whose annotation appearance draws a JPX bomb."""
    appearance = b"q 100 0 0 100 0 0 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << >> /Contents 4 0 R /Annots [5 0 R] >>",
        b"<< /Length 0 >>\nstream\n\nendstream",
        b"<< /Type /Annot /Subtype /Stamp /Rect [50 50 150 150] /AP << /N 6 0 R >> >>",
        b"<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] "
        b"/Resources << /XObject << /Im0 7 0 R >> >> /Length %d >>\nstream\n%s\nendstream"
        % (len(appearance), appearance),
        _bomb_image_obj(),
    ]
    return hand_written_pdf(path, objects)


def tiling_pattern_bomb_pdf(path: Path) -> Path:
    """A page filled with a tiling pattern whose cell draws a JPX bomb."""
    content = b"q /Pattern cs /P0 scn 0 0 612 792 re f Q"
    cell = b"q 100 0 0 100 0 0 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Pattern << /P0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 100 100] "
        b"/XStep 100 /YStep 100 /Resources << /XObject << /Im0 6 0 R >> >> /Length %d >>\nstream\n%s\nendstream"
        % (len(cell), cell),
        _bomb_image_obj(),
    ]
    return hand_written_pdf(path, objects)


def deep_form_bomb_pdf(path: Path, depth: int = 70) -> Path:
    """A page whose image is nested ``depth`` Form XObjects deep."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Fm0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length 8 >>\nstream\n/Fm0 Do\nendstream",
    ]
    for i in range(depth):
        n = len(objects) + 1
        last = i == depth - 1
        body = b"q 200 0 0 200 50 50 cm /Im0 Do Q" if last else b"/Fm0 Do"
        member = n + 1
        res = (b"/XObject << /Im0 %d 0 R >>" % member) if last else (b"/XObject << /Fm0 %d 0 R >>" % member)
        objects.append(
            b"<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] /Resources << %s >> /Length %d >>"
            b"\nstream\n%s\nendstream" % (res, len(body), body)
        )
    objects.append(_bomb_image_obj())
    return hand_written_pdf(path, objects)


def _type3_pdf(path: Path, glyph: bytes) -> Path:
    """A one-page PDF drawing a Type3 font character whose glyph procedure is ``glyph``."""
    content = b"BT /F0 100 Tf 100 100 Td (A) Tj ET"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /Font /Subtype /Type3 /FontBBox [0 0 100 100] /FontMatrix [0.01 0 0 0.01 0 0] "
        b"/CharProcs << /ch 6 0 R >> /Encoding << /Differences [65 /ch] >> "
        b"/FirstChar 65 /LastChar 65 /Widths [100] >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(glyph), glyph),
    ]
    return hand_written_pdf(path, objects)


def type3_inline_bomb_pdf(path: Path) -> Path:
    """A Type3 glyph procedure carrying an inline (BI/ID/EI) JPEG 2000 bomb."""
    assert b"EI" not in JPX_RSS_BOMB
    glyph = b"d0 100 0 q 100 0 0 100 0 0 cm BI /W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode ID " + JPX_RSS_BOMB + b"\nEI Q"
    return _type3_pdf(path, glyph)


def inline_in_pattern_bomb_pdf(path: Path) -> Path:
    """A page whose tiling pattern's cell content carries an inline JPEG 2000 bomb."""
    assert b"EI" not in JPX_RSS_BOMB
    page_content = b"q /Pattern cs /P0 scn 0 0 612 792 re f Q"
    cell = b"q 100 0 0 100 0 0 cm BI /W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode ID " + JPX_RSS_BOMB + b"\nEI Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Pattern << /P0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(page_content), page_content),
        b"<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 100 100] "
        b"/XStep 100 /YStep 100 /Resources << >> /Length %d >>\nstream\n%s\nendstream" % (len(cell), cell),
    ]
    return hand_written_pdf(path, objects)


def inline_bomb_pdf(path: Path) -> Path:
    """A page whose content stream carries an inline (BI/ID/EI) JPEG 2000 bomb."""
    assert b"EI" not in JPX_RSS_BOMB
    content = b"q 100 0 0 100 50 50 cm BI /W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode ID " + JPX_RSS_BOMB + b"\nEI Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
    ]
    return hand_written_pdf(path, objects)


# Builders for a JPX-bomb reachable only through a given container; each must
# make every render of the document skip (document-wide guard).
BOMB_CONTAINERS = {
    "smask": smask_bomb_pdf,
    "annotation": annotation_ap_bomb_pdf,
    "pattern": tiling_pattern_bomb_pdf,
    "deep70": deep_form_bomb_pdf,
    "inline": inline_bomb_pdf,
    "inline_pattern": inline_in_pattern_bomb_pdf,
    "type3_inline": type3_inline_bomb_pdf,
}


def normal_rich_pdf(path: Path) -> Path:
    """A page with a legitimate image + small /SMask, an annotation image, and a tiling pattern image.

    Every image is small and valid, so the document renders normally (no false
    positive from the document-wide guard).
    """
    import zlib

    base = zlib.compress(b"\x80" * 64 * 64)
    mask = zlib.compress(b"\xff" * 64 * 64)
    small_jpx = NORMAL_JPX
    appearance = b"q 40 0 0 40 0 0 cm /Ia Do Q"
    cell = b"q 40 0 0 40 0 0 cm /Ip Do Q"
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q /Pattern cs /P0 scn 0 400 200 200 re f"
    gray = b"/Width 64 /Height 64 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /FlateDecode"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> /Pattern << /P0 8 0 R >> >> "
        b"/Contents 4 0 R /Annots [10 0 R] >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /SMask 6 0 R /Length %d >>\nstream\n%s\nendstream"
        % (gray, len(base), base),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream" % (gray, len(mask), mask),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (gray_entries(48, 48, b"/JPXDecode"), len(small_jpx), small_jpx),  # obj 7: small JPX in the pattern
        b"<< /Type /Pattern /PatternType 1 /PaintType 1 /TilingType 1 /BBox [0 0 40 40] "
        b"/XStep 40 /YStep 40 /Resources << /XObject << /Ip 7 0 R >> >> /Length %d >>\nstream\n%s\nendstream"
        % (len(cell), cell),
        b"<< /Type /XObject /Subtype /Form /BBox [0 0 40 40] "
        b"/Resources << /XObject << /Ia 7 0 R >> >> /Length %d >>\nstream\n%s\nendstream"
        % (len(appearance), appearance),
        b"<< /Type /Annot /Subtype /Stamp /Rect [400 50 440 90] /AP << /N 9 0 R >> >>",
    ]
    return hand_written_pdf(path, objects)


# ---------------------------------------------------------------------------
# Skip-with-warning, no decode: caps patched small, a spy on decode entry points.
# ---------------------------------------------------------------------------


@pytest.fixture
def tiny_cap(monkeypatch):
    """Shrink the pixel cap so a 100 x 100 codestream trips it (no gigabytes needed)."""
    monkeypatch.setattr(image_guard, "_MAX_DECODED_PIXELS", 1_000)
    monkeypatch.setattr(image_guard, "_MAX_DECODED_SAMPLES", 4_000)


@pytest.fixture
def no_decode(monkeypatch):
    """Spy on every pdfium / pymupdf call that can decode an image; fail if any runs.

    Each spy raises, so a decode that the guard should have prevented turns
    into a loud failure instead of a silent gigabyte allocation.
    """
    import pymupdf
    import pypdfium2.raw as pdfium_raw

    from datagrunt.core.pdf_io.extraction import pdfium_document

    calls: list[str] = []

    def forbid(name):
        def spy(*args, **kwargs):
            calls.append(name)
            raise AssertionError(f"decode entry point {name} was called on a guarded image")

        return spy

    # pdfium: metadata (decodes JPX), bitmap, and page render.
    monkeypatch.setattr(pdfium_raw, "FPDFImageObj_GetImageMetadata", forbid("FPDFImageObj_GetImageMetadata"))
    monkeypatch.setattr(pdfium_raw, "FPDFImageObj_GetBitmap", forbid("FPDFImageObj_GetBitmap"))
    monkeypatch.setattr(pdfium_document.PdfiumPage, "render_pil", forbid("PdfiumPage.render_pil"))
    # pymupdf: image extraction, Pixmap construction, per-xref rect lookup, page render.
    monkeypatch.setattr(pymupdf.Document, "extract_image", forbid("Document.extract_image"))
    monkeypatch.setattr(pymupdf, "Pixmap", forbid("pymupdf.Pixmap"))
    monkeypatch.setattr(pymupdf.Page, "get_image_rects", forbid("Page.get_image_rects"))
    monkeypatch.setattr(pymupdf.Page, "get_pixmap", forbid("Page.get_pixmap"))
    return calls


def _warnings_of(document: dict) -> list[str]:
    return [w for page in document["document"]["pages"] for w in (page.get("warnings") or [])]


def _image_elements(document: dict) -> list:
    out = []
    for page in document["document"]["pages"]:
        for element in page.get("elements", []):
            if element.get("type") == "image":
                out.append(element)
        for image in page.get("images", []):  # native pdfium schema
            out.append(image)
    return out


@pytest.mark.parametrize("engine", ENGINES)
def test_to_dicts_skips_the_bomb_without_decoding(tmp_path, engine, tiny_cap, no_decode):
    path = image_page_pdf(tmp_path / "bomb.pdf", JPX_SMALL_BOMB, BOMB_ENTRIES)
    document = PDFReader(path, engine=engine).to_dicts()

    warnings = _warnings_of(document)
    assert warnings, "a skipped image must record a page warning"
    assert any("JPEG 2000" in w for w in warnings)
    assert _image_elements(document) == []
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_extract_images_skips_the_bomb_without_decoding(tmp_path, engine, tiny_cap, no_decode):
    path = image_page_pdf(tmp_path / "bomb.pdf", JPX_SMALL_BOMB, BOMB_ENTRIES)
    out = tmp_path / "images"
    paths = PDFWriter(path, engine=engine).extract_images(str(out))

    assert paths == []
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_render_pages_skips_a_page_with_a_bomb(tmp_path, engine, tiny_cap, no_decode):
    path = image_page_pdf(tmp_path / "bomb.pdf", JPX_SMALL_BOMB, BOMB_ENTRIES)
    out = tmp_path / "pages"
    paths = PDFWriter(path, engine=engine).render_pages_as_images(str(out), dpi=72)

    assert paths == [], "a page whose only image is a bomb is not rendered"
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_tile_count_bomb_is_skipped(tmp_path, engine, no_decode):
    # 2000 x 2000 is well under the real 89M-px cap, but 8 x 8 tiles over it is
    # 62,500 tiles, over the 4,096-tile bound (OpenJPEG allocates ~12 KB/tile).
    stream = j2k((16, 16), siz=(2000, 2000), tile=(8, 8))
    path = image_page_pdf(tmp_path / "tiles.pdf", stream, BOMB_ENTRIES)
    document = PDFReader(path, engine=engine).to_dicts()

    assert any("tile bound" in w for w in _warnings_of(document))
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_jbig2_page_size_bomb_is_skipped(tmp_path, engine, tiny_cap, no_decode):
    path = image_page_pdf(
        tmp_path / "jbig2.pdf",
        jbig2_stream(40000, 40000),
        b"/Width 16 /Height 16 /ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /JBIG2Decode",
    )
    document = PDFReader(path, engine=engine).to_dicts()

    assert any("JBIG2" in w for w in _warnings_of(document))
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("container", sorted(BOMB_CONTAINERS))
def test_render_skips_a_bomb_reachable_through_any_container(tmp_path, engine, container, no_decode):
    # The bomb is reachable only via a soft mask, an annotation appearance, a
    # tiling pattern, 70 nested forms, or an inline image. A render decodes it
    # however it is drawn, so the document-wide gate must skip every render.
    path = BOMB_CONTAINERS[container](tmp_path / f"{container}.pdf")
    out = tmp_path / "pages"
    paths = PDFWriter(path, engine=engine).render_pages_as_images(str(out), dpi=72)

    assert paths == [], f"{engine}/{container}: a page drawing a bomb must not be rendered"
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_ocr_path_skips_a_bomb_on_a_scanned_page(tmp_path, engine, no_decode):
    # A scanned page (no text, its own image) whose image is a JPX bomb: the
    # OCR route would render it. The document-wide gate stops the render.
    path = image_page_pdf(tmp_path / "scan.pdf", JPX_RSS_BOMB, BOMB_ENTRIES)
    document = PDFReader(path, engine=engine).to_dicts()

    assert any("OCR skipped" in w or "JPEG 2000" in w for w in _warnings_of(document))
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_normal_rich_document_still_renders(tmp_path, engine):
    # Legitimate base image + small /SMask + annotation image + tiling-pattern
    # image: no false positive, the document renders.
    path = normal_rich_pdf(tmp_path / "rich.pdf")
    out = tmp_path / "pages"
    paths = PDFWriter(path, engine=engine).render_pages_as_images(str(out), dpi=72)

    assert len(paths) == 1, f"{engine}: a normal rich document must still render"
    assert Path(paths[0]).exists()


@pytest.mark.parametrize("engine", ENGINES)
def test_a_normal_type3_document_still_renders(tmp_path, engine):
    # A Type3 glyph that draws vector art (no bomb) must not block the render.
    path = _type3_pdf(tmp_path / "t3.pdf", b"d0 100 0 10 10 80 80 re f")
    out = tmp_path / "pages"
    paths = PDFWriter(path, engine=engine).render_pages_as_images(str(out), dpi=72)

    assert len(paths) == 1, f"{engine}: a normal Type3 document must still render"


def test_a_jpx_behind_asciihex_is_still_guarded(tmp_path, tiny_cap, no_decode):
    import binascii

    wrapped = binascii.hexlify(JPX_SMALL_BOMB) + b">"
    path = image_page_pdf(
        tmp_path / "ahx.pdf",
        wrapped,
        gray_entries(16, 16, b"[/ASCIIHexDecode /JPXDecode]"),
    )
    document = PDFReader(path, engine="pdfium").to_dicts()

    assert any("JPEG 2000" in w for w in _warnings_of(document))
    assert no_decode == []


# ---------------------------------------------------------------------------
# No false positive: a normal small JPEG 2000 image is still extracted.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_a_normal_small_jpx_is_still_extracted(tmp_path, engine):
    path = image_page_pdf(tmp_path / "ok.pdf", NORMAL_JPX, gray_entries(48, 48, b"/JPXDecode"))
    out = tmp_path / "images"
    paths = PDFWriter(path, engine=engine).extract_images(str(out))

    assert len(paths) == 1, "a legitimate small JPX must still be extracted"
    assert Path(paths[0]).exists()


# ---------------------------------------------------------------------------
# Peak RSS: a real moderate codestream, measured in a watchdog subprocess.
# ---------------------------------------------------------------------------

_RSS_BOUND_MIB = 400
_WATCHDOG_LIMIT_MIB = 1200

_SUBPROCESS = textwrap.dedent(
    """
    import os, resource, sys, tempfile, threading, time

    LIMIT = {limit} << 20

    def peak():
        # Linux keeps ru_maxrss across fork + exec, so a child of a large process
        # (pytest) reports the parent's peak; VmHWM is this process's own, reset at exec.
        if sys.platform.startswith("linux"):
            with open("/proc/self/status") as status:
                for line in status:
                    if line.startswith("VmHWM:"):
                        return int(line.split()[1]) * 1024
            raise RuntimeError("no VmHWM line in /proc/self/status")
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # macOS reports bytes

    def watch():
        while True:
            if peak() > LIMIT:
                sys.stdout.write("KILLED watchdog\\n"); sys.stdout.flush(); os._exit(99)
            time.sleep(0.005)

    threading.Thread(target=watch, daemon=True).start()
    from datagrunt.pdf_api import PDFReader, PDFWriter

    path, engine, op = sys.argv[1], sys.argv[2], sys.argv[3]
    base = peak()
    out = tempfile.mkdtemp()
    if op == "to_dicts":
        PDFReader(path, engine=engine).to_dicts()
    elif op == "extract_images":
        PDFWriter(path, engine=engine).extract_images(out)
    elif op == "render":
        PDFWriter(path, engine=engine).render_pages_as_images(out, dpi=72)
    print(f"PEAK {{(peak() - base) >> 20}}", flush=True)
    """
).format(limit=_WATCHDOG_LIMIT_MIB)


def _run_peak(path: Path, engine: str, op: str) -> int:
    result = subprocess.run(
        [sys.executable, "-c", _SUBPROCESS, str(path), engine, op],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, f"subprocess failed/killed: {result.stdout}\n{result.stderr[-600:]}"
    line = next((ln for ln in result.stdout.splitlines() if ln.startswith("PEAK ")), None)
    assert line is not None, f"no PEAK line: {result.stdout}\n{result.stderr[-600:]}"
    return int(line.split()[1])


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("op", ["to_dicts", "extract_images", "render"])
def test_peak_rss_stays_bounded(tmp_path, engine, op):
    path = image_page_pdf(tmp_path / "rss.pdf", JPX_RSS_BOMB, BOMB_ENTRIES)
    peak = _run_peak(path, engine, op)
    assert peak < _RSS_BOUND_MIB, f"{engine}/{op} peaked at {peak} MiB (bound {_RSS_BOUND_MIB})"


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("container", sorted(BOMB_CONTAINERS))
def test_peak_rss_bounded_rendering_any_container(tmp_path, engine, container):
    # Rendering a document whose bomb is reachable only through a soft mask,
    # annotation, pattern, deep nesting or inline image stays bounded.
    path = BOMB_CONTAINERS[container](tmp_path / f"{container}.pdf")
    peak = _run_peak(path, engine, "render")
    assert peak < _RSS_BOUND_MIB, f"{engine}/{container} render peaked at {peak} MiB (bound {_RSS_BOUND_MIB})"


# ---------------------------------------------------------------------------
# Guard bypass regressions: indirect /Filter, #xx hex escapes, inline BI/ID/EI
# token boundaries, and fail-closed on unreadable content streams.
# ---------------------------------------------------------------------------


def _jpx_tile_bomb() -> bytes:
    """A J2K codestream whose SIZ/tile headers exceed the 4,096-tile guard cap."""
    return j2k((16, 16), siz=(2000, 2000), tile=(8, 8))


def _pdf_with_indirect_filter_image(data: bytes, filter_obj: bytes = b"/JPXDecode") -> bytes:
    """Build a 1-page PDF bytes buffer where Image XObject 5 has ``/Filter 6 0 R``."""
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image /Width 16 /Height 16 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter 6 0 R /Length %d >>\nstream\n%s\nendstream" % (len(data), data),
        filter_obj,
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n%s\nendobj\n" % (number, body)
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1)
    out += b"".join(b"%010d 00000 n \n" % offset for offset in offsets)
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objects) + 1, xref)
    return bytes(out)


@pytest.fixture
def no_pymupdf_image_decode(monkeypatch):
    """Fail if PyMuPDF image decode/rect/render entry points are called."""
    import pymupdf

    calls: list[str] = []

    def forbid(name):
        def spy(*args, **kwargs):
            calls.append(name)
            pytest.fail(f"PyMuPDF image decode entry point {name} was called on a guarded image")

        return spy

    monkeypatch.setattr(pymupdf.Document, "extract_image", forbid("Document.extract_image"))
    monkeypatch.setattr(pymupdf, "Pixmap", forbid("pymupdf.Pixmap"))
    monkeypatch.setattr(pymupdf.Page, "get_image_rects", forbid("Page.get_image_rects"))
    monkeypatch.setattr(pymupdf.Page, "get_pixmap", forbid("Page.get_pixmap"))
    return calls


@pytest.fixture
def no_pymupdf_page_render(monkeypatch):
    """Fail if PyMuPDF page rendering (Page.get_pixmap) is called."""
    import pymupdf

    calls: list[str] = []

    def forbid(name):
        def spy(*args, **kwargs):
            calls.append(name)
            pytest.fail(f"PyMuPDF page render entry point {name} was called on a guarded page")

        return spy

    monkeypatch.setattr(pymupdf.Page, "get_pixmap", forbid("Page.get_pixmap"))
    return calls


def test_indirect_filter_reference_is_guarded_in_to_dicts(tmp_path, no_pymupdf_image_decode):
    path = tmp_path / "indirect_filter.pdf"
    path.write_bytes(_pdf_with_indirect_filter_image(_jpx_tile_bomb(), b"/JPXDecode"))
    document = PDFReader(path, engine="pymupdf").to_dicts()

    assert any("JPEG 2000" in w for w in _warnings_of(document))
    assert _image_elements(document) == []
    assert no_pymupdf_image_decode == []


def test_indirect_filter_array_reference_is_guarded_in_to_dicts(tmp_path, no_pymupdf_image_decode):
    import binascii

    wrapped = binascii.hexlify(_jpx_tile_bomb()) + b">"
    path = tmp_path / "indirect_filter_array.pdf"
    path.write_bytes(_pdf_with_indirect_filter_image(wrapped, b"[/ASCIIHexDecode /JPXDecode]"))
    document = PDFReader(path, engine="pymupdf").to_dicts()

    assert any("JPEG 2000" in w for w in _warnings_of(document))
    assert _image_elements(document) == []
    assert no_pymupdf_image_decode == []


def test_indirect_filter_reference_is_guarded_in_render_pages(tmp_path, no_pymupdf_page_render):
    path = tmp_path / "indirect_filter_render.pdf"
    path.write_bytes(_pdf_with_indirect_filter_image(_jpx_tile_bomb(), b"/JPXDecode"))
    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)

    assert paths == []
    assert no_pymupdf_page_render == []


def test_hex_escaped_subtype_and_filter_xobject_is_guarded(tmp_path, no_pymupdf_image_decode, no_pymupdf_page_render):
    bomb = _jpx_tile_bomb()
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /#49mage /Width 16 /Height 16 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter /#4aPXDecode /Length %d >>\nstream\n%s\nendstream" % (len(bomb), bomb),
    ]
    path = hand_written_pdf(tmp_path / "hex_xobj.pdf", objects)

    document = PDFReader(path, engine="pymupdf").to_dicts()
    assert any("JPEG 2000" in w for w in _warnings_of(document))
    assert _image_elements(document) == []
    assert no_pymupdf_image_decode == []

    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)
    assert paths == []
    assert no_pymupdf_page_render == []

    indirect_hex_path = tmp_path / "indirect_hex_xobj.pdf"
    indirect_hex_path.write_bytes(_pdf_with_indirect_filter_image(bomb, b"/#4aPXDecode"))
    indirect_doc = PDFReader(indirect_hex_path, engine="pymupdf").to_dicts()
    assert any("JPEG 2000" in w for w in _warnings_of(indirect_doc))
    assert _image_elements(indirect_doc) == []
    assert no_pymupdf_image_decode == []

    indirect_paths = PDFWriter(indirect_hex_path, engine="pymupdf").render_pages_as_images(
        str(tmp_path / "indirect_pages"), dpi=72
    )
    assert indirect_paths == []
    assert no_pymupdf_page_render == []


def test_hex_escaped_inline_image_filter_is_guarded_in_render_pages(tmp_path, no_pymupdf_page_render):
    bomb = _jpx_tile_bomb()
    assert b"EI" not in bomb
    content = b"q 100 0 0 100 50 50 cm BI /W 16 /H 16 /CS /G /BPC 8 /#46 /#4aPXDecode ID " + bomb + b"\nEI Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
    ]
    path = hand_written_pdf(tmp_path / "hex_inline.pdf", objects)
    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)

    assert paths == []
    assert no_pymupdf_page_render == []


def test_inline_image_with_slash_id_key_before_filter_is_guarded(tmp_path, no_pymupdf_page_render):
    bomb = _jpx_tile_bomb()
    assert b"EI" not in bomb
    content = b"q 100 0 0 100 50 50 cm BI /W 16 /H 16 /CS /G /BPC 8 /X /ID /F /JPXDecode ID " + bomb + b"\nEI Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
    ]
    path = hand_written_pdf(tmp_path / "slash_id_inline.pdf", objects)
    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)

    assert paths == []
    assert no_pymupdf_page_render == []


def test_inline_image_without_matching_id_operator_fails_closed(tmp_path, no_pymupdf_page_render):
    content = b"q 100 0 0 100 50 50 cm BI /W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
    ]
    path = hand_written_pdf(tmp_path / "unterminated_bi.pdf", objects)
    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)

    assert paths == []
    assert no_pymupdf_page_render == []


def test_document_block_reason_fails_closed_when_read_contents_raises(tmp_path, monkeypatch):
    import pymupdf

    from datagrunt.core.pdf_io.extraction.render_guard import document_block_reason

    path = normal_rich_pdf(tmp_path / "normal.pdf")
    doc = pymupdf.open(path)
    try:

        def _boom(self):
            raise RuntimeError("corrupt content stream")

        monkeypatch.setattr(pymupdf.Page, "read_contents", _boom)
        reason = document_block_reason(doc)
    finally:
        doc.close()

    assert reason is not None
    assert "unreadable" in reason.lower()


def test_indirect_subtype_image_xobject_is_guarded_in_render_pages(tmp_path, no_pymupdf_page_render):
    bomb = _jpx_tile_bomb()
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype 6 0 R /Width 16 /Height 16 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter /JPXDecode /Length %d >>\nstream\n%s\nendstream" % (len(bomb), bomb),
        b"/Image",
    ]
    path = hand_written_pdf(tmp_path / "indirect_subtype.pdf", objects)
    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)

    assert paths == []
    assert no_pymupdf_page_render == []


@pytest.mark.parametrize(
    "extra_objects",
    [
        [b"7 0 R", b"8 0 R", b"9 0 R", b"/Image"],
        [b"123"],
    ],
    ids=["hop_limit_exceeded", "non_name_target"],
)
def test_unresolvable_indirect_subtype_fails_closed_in_render_pages(tmp_path, extra_objects, no_pymupdf_page_render):
    bomb = _jpx_tile_bomb()
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype 6 0 R /Width 16 /Height 16 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter /JPXDecode /Length %d >>\nstream\n%s\nendstream" % (len(bomb), bomb),
        *extra_objects,
    ]
    path = hand_written_pdf(tmp_path / "unresolvable_subtype.pdf", objects)
    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)

    assert paths == []
    assert no_pymupdf_page_render == []


def _jbig2_globals_bomb_pdf(path: Path, decode_parms_entry: bytes, extra_objects: list[bytes]) -> Path:
    """Build a 1-page PDF with a 16x16 JBIG2 image whose object 6 globals stream holds a 40000x40000 region bomb."""
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    local_stream = jbig2_stream(16, 16)
    region_bomb = jbig2_segment(0, 4, struct.pack(">IIIIB", 40000, 40000, 0, 0, 0), page=0)
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image /Width 16 /Height 16 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 1 /Filter [/JBIG2Decode] %s /Length %d >>\nstream\n%s\nendstream"
        % (decode_parms_entry, len(local_stream), local_stream),
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(region_bomb), region_bomb),
        *extra_objects,
    ]
    return hand_written_pdf(path, objects)


@pytest.mark.parametrize(
    ("decode_parms_entry", "extra_objects"),
    [
        (b"/DecodeParms [<< /JBIG2Globals 6 0 R >>]", []),
        (b"/DecodeParms 7 0 R", [b"<< /JBIG2Globals 6 0 R >>"]),
        (b"/DecodeParms 7 0 R", [b"[<< /JBIG2Globals 6 0 R >>]"]),
        (b"/DecodeParms [7 0 R]", [b"<< /JBIG2Globals 6 0 R >>"]),
    ],
    ids=["array_of_dict", "indirect_dict", "indirect_array", "array_of_indirect_dict"],
)
def test_jbig2_globals_in_array_or_indirect_decode_parms_is_guarded(
    tmp_path, decode_parms_entry, extra_objects, no_pymupdf_image_decode, no_pymupdf_page_render
):
    path = _jbig2_globals_bomb_pdf(tmp_path / "jbig2_globals.pdf", decode_parms_entry, extra_objects)

    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)
    assert paths == []
    assert no_pymupdf_page_render == []

    document = PDFReader(path, engine="pymupdf").to_dicts()
    assert any("JBIG2" in w for w in _warnings_of(document))
    assert _image_elements(document) == []
    assert no_pymupdf_image_decode == []


def test_filter_array_with_indirect_element_is_guarded(tmp_path, no_pymupdf_image_decode, no_pymupdf_page_render):
    import binascii

    wrapped = binascii.hexlify(_jpx_tile_bomb()) + b">"
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image /Width 16 /Height 16 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter [/ASCIIHexDecode 6 0 R] /Length %d >>\nstream\n%s\nendstream"
        % (len(wrapped), wrapped),
        b"/JPXDecode",
    ]
    path = hand_written_pdf(tmp_path / "filter_array_indirect_elem.pdf", objects)

    document = PDFReader(path, engine="pymupdf").to_dicts()
    assert any("JPEG 2000" in w for w in _warnings_of(document))
    assert _image_elements(document) == []
    assert no_pymupdf_image_decode == []

    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)
    assert paths == []
    assert no_pymupdf_page_render == []


def test_filter_array_with_unresolvable_indirect_element_fails_closed(
    tmp_path, no_pymupdf_image_decode, no_pymupdf_page_render
):
    import binascii

    wrapped = binascii.hexlify(b"\x80" * 256) + b">"
    content = b"q 200 0 0 200 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image /Width 16 /Height 16 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter [/ASCIIHexDecode 99 0 R] /Length %d >>\nstream\n%s\nendstream"
        % (len(wrapped), wrapped),
    ]
    path = hand_written_pdf(tmp_path / "filter_array_bad_ref.pdf", objects)

    document = PDFReader(path, engine="pymupdf").to_dicts()
    assert _warnings_of(document)
    assert _image_elements(document) == []
    assert no_pymupdf_image_decode == []

    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "pages"), dpi=72)
    assert paths == []
    assert no_pymupdf_page_render == []


# ---------------------------------------------------------------------------
# JBIG2: decoded only when every segment of its stream is checked.
# ---------------------------------------------------------------------------

SCAN_SIZE = (64, 48)
SCAN_ENTRIES = b"/Width 64 /Height 48 /ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /JBIG2Decode"


def scan_pattern() -> Image.Image:
    """A 64 x 48 bilevel pattern (0 is black, as Pillow's mode "1" stores it)."""
    image = Image.new("1", SCAN_SIZE, 1)
    for x in range(SCAN_SIZE[0]):
        for y in range(SCAN_SIZE[1]):
            if (x // 4 + y // 4) % 3 == 0:
                image.putpixel((x, y), 0)
    return image


def generic_region_mmr(image: Image.Image) -> bytes:
    """``image`` MMR-coded as a JBIG2 generic region decodes it (1 is black there, so it is inverted first)."""
    inverted = Image.eval(image.convert("L"), lambda value: 255 - value).convert("1")
    buffer = io.BytesIO()
    inverted.save(buffer, "TIFF", compression="group4")
    tiff = Image.open(io.BytesIO(buffer.getvalue()))
    (offset,), (count,) = tiff.tag_v2[273], tiff.tag_v2[279]
    return buffer.getvalue()[offset : offset + count]


def region_information(width: int, height: int) -> bytes:
    return struct.pack(">IIIIB", width, height, 0, 0, 0)


def jbig2_scan(*, extra: bytes = b"", region_length: int | None = None) -> bytes:
    """A 64 x 48 generic-region JBIG2 scan: page information, ``extra``, one MMR region, end of page."""
    page_info = struct.pack(">IIIIBH", *SCAN_SIZE, 0, 0, 0, 0)
    region = region_information(*SCAN_SIZE) + b"\x01" + generic_region_mmr(scan_pattern())
    if region_length is None:
        region_segment = jbig2_segment(1, 38, region)
    else:  # an end sequence (00 00) and the row count follow MMR data of unknown length
        region += b"\x00\x00" + struct.pack(">I", SCAN_SIZE[1])
        region_segment = struct.pack(">IBBBI", 1, 38, 0x00, 1, region_length) + region
    return jbig2_segment(0, 48, page_info) + extra + region_segment + jbig2_segment(2, 49, b"")


def jbig2_scan_pdf(path: Path, stream: bytes, decode_parms: bytes = b"", extra_objects: tuple = ()) -> Path:
    """A one-page PDF (no text layer) drawing one JBIG2 image XObject; extra objects are numbered from 6."""
    content = b"q 200 0 0 150 50 50 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, decode_parms, len(stream), stream),
        *extra_objects,
    ]
    return hand_written_pdf(path, objects)


def globals_object(data: bytes) -> bytes:
    return b"<< /Length %d >>\nstream\n%s\nendstream" % (len(data), data)


@pytest.mark.parametrize("engine", ENGINES)
def test_a_jbig2_generic_region_scan_is_decoded_and_extracted(tmp_path, engine):
    path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
    out = tmp_path / "images"

    document = PDFReader(path, engine=engine).to_dicts(image_output_dir=str(out))

    (image,) = _image_elements(document)
    assert (image["metadata"]["width_px"], image["metadata"]["height_px"]) == SCAN_SIZE
    with Image.open(image["metadata"]["file_path"]) as written:
        assert written.convert("1").tobytes() == scan_pattern().tobytes()
    assert not any("JBIG2" in warning for warning in _warnings_of(document))
    paths = PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72)
    assert len(paths) == 1


def _one_segment(segment_type: int) -> bytes:
    """A segment of ``segment_type`` with a few bytes of data, associated with the page."""
    return jbig2_segment(7, segment_type, region_information(16, 16) + bytes(8))


NOT_CHECKED_STREAMS = {
    "symbol-dictionary": jbig2_scan(extra=_one_segment(0)),
    "text-region": jbig2_scan(extra=_one_segment(6)),
    "pattern-dictionary": jbig2_scan(extra=_one_segment(16)),
    "halftone-region": jbig2_scan(extra=_one_segment(22)),
    "refinement-region": jbig2_scan(extra=_one_segment(42)),
    "tables": jbig2_scan(extra=_one_segment(53)),
    "unknown-length": jbig2_scan(region_length=0xFFFFFFFF),
    "oversized-region": jbig2_scan(extra=jbig2_segment(7, 36, region_information(100, 100) + b"\x01")),
    "regions-over-the-bound-together": jbig2_scan(
        extra=jbig2_segment(7, 36, region_information(50, 50) + b"\x01")
        + jbig2_segment(8, 36, region_information(50, 50) + b"\x01")
    ),
}


@pytest.fixture
def scan_cap(monkeypatch):
    """A 4,000-pixel bound: the 64 x 48 scan (3,072 px) fits it, a 50 x 50 region beside it does not."""
    monkeypatch.setattr(image_guard, "_MAX_DECODED_PIXELS", 4_000)


def _assert_not_decoded(path: Path, engine: str, tmp_path: Path, calls: list) -> None:
    """``path``'s JBIG2 image is skipped with a warning, never decoded, and its page is not rendered."""
    document = PDFReader(path, engine=engine).to_dicts(image_output_dir=str(tmp_path / "images"))
    assert _image_elements(document) == []
    assert any("JBIG2" in warning for warning in _warnings_of(document))
    paths = PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72)
    assert paths == []
    assert calls == []


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("case", sorted(NOT_CHECKED_STREAMS))
def test_a_jbig2_image_whose_segments_are_not_all_checked_is_not_decoded(tmp_path, engine, case, scan_cap, no_decode):
    path = jbig2_scan_pdf(tmp_path / "scan.pdf", NOT_CHECKED_STREAMS[case])

    _assert_not_decoded(path, engine, tmp_path, no_decode)


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(
    ("decode_parms", "extra_objects"),
    [
        (b"/DecodeParms << /JBIG2Globals 6 0 R >>", (globals_object(b""),)),
        (b"/DecodeParms [<< /JBIG2Globals 6 0 R >>]", (globals_object(b""),)),
        (b"/DecodeParms 7 0 R", (globals_object(b""), b"<< /JBIG2Globals 6 0 R >>")),
        (b"/DecodeParms << /JBIG2Globals 99 0 R >>", ()),
    ],
    ids=["dict", "array", "indirect-dict", "unresolvable"],
)
def test_a_jbig2_image_with_globals_is_not_decoded(tmp_path, engine, decode_parms, extra_objects, no_decode):
    path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan(), decode_parms, extra_objects)

    _assert_not_decoded(path, engine, tmp_path, no_decode)


def test_a_jbig2_stream_shared_with_an_image_with_globals_is_not_decoded_by_pdfium(tmp_path, no_decode):
    # pdfium's API names no image's decode parameters: an image is matched to its
    # object by its raw stream, so a stream one image draws with globals is never trusted.
    stream = jbig2_scan()
    content = b"q 200 0 0 150 50 50 cm /Im0 Do Q q 200 0 0 150 300 50 cm /Im1 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R /Im1 6 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, len(stream), stream),
        b"<< /Type /XObject /Subtype /Image %s /DecodeParms << /JBIG2Globals 7 0 R >> /Length %d >>\n"
        b"stream\n%s\nendstream" % (SCAN_ENTRIES, len(stream), stream),
        globals_object(b""),
    ]
    path = hand_written_pdf(tmp_path / "shared.pdf", objects)

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "images"))

    assert _image_elements(document) == []
    assert sum("JBIG2" in warning for warning in _warnings_of(document)) >= 2
    assert no_decode == []


def test_an_inline_jbig2_image_with_globals_blocks_rendering(tmp_path, no_decode):
    stream = jbig2_scan()
    inline = (
        b"q 200 0 0 150 50 50 cm BI /W 64 /H 48 /CS /G /BPC 1 /F /JBIG2Decode "
        b"/DP << /JBIG2Globals 6 0 R >> ID " + stream + b"\nEI Q"
    )
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(inline), inline),
        globals_object(b""),
        globals_object(b""),
    ]
    path = hand_written_pdf(tmp_path / "inline.pdf", objects)

    for engine in ENGINES:
        paths = PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / engine), dpi=72)
        assert paths == [], engine
    assert no_decode == []


class TestJbig2Agreement:
    """What PyMuPDF agrees the pdfium engine may decode: nothing whenever the scan cannot finish."""

    @staticmethod
    def scan(path: Path):
        import pymupdf

        from datagrunt.core.pdf_io.extraction.render_guard import jbig2_agreement

        with pymupdf.open(path) as doc:
            return jbig2_agreement(doc)

    def test_a_clean_scan_is_agreed_with_its_page(self, tmp_path):
        import hashlib

        stream = jbig2_scan()
        path = jbig2_scan_pdf(tmp_path / "scan.pdf", stream)
        digest = hashlib.sha256(stream).hexdigest()

        assert tuple(self.scan(path)) == (frozenset({digest}), (frozenset({digest}),))

    def test_over_the_object_bound_nothing_is_agreed(self, tmp_path, monkeypatch):
        from datagrunt.core.pdf_io.extraction import render_guard

        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
        monkeypatch.setattr(render_guard, "_MAX_SCAN_OBJECTS", 5)

        assert tuple(self.scan(path)) == (frozenset(), ())

    @pytest.mark.parametrize("bound", [10, 200], ids=["streams", "content"])
    def test_over_the_byte_bound_nothing_is_agreed(self, tmp_path, monkeypatch, bound):
        # 200 bytes holds the image stream (about 140) but not the page content as well.
        from datagrunt.core.pdf_io.extraction import render_guard

        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
        monkeypatch.setattr(render_guard, "_MAX_SCAN_BYTES", bound)

        assert tuple(self.scan(path)) == (frozenset(), ())

    def test_an_unreadable_subtype_agrees_nothing(self, tmp_path):
        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan(), extra_objects=(b"<< /Subtype 99 0 R >>",))

        assert tuple(self.scan(path)) == (frozenset(), ())

    def test_an_unreadable_type3_glyph_list_agrees_nothing(self, tmp_path, monkeypatch):
        import pymupdf

        font = b"<< /Type /Font /Subtype /Type3 /CharProcs 99 0 R >>"
        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan(), extra_objects=(font,))
        monkeypatch.setattr(pymupdf.Document, "xref_get_keys", lambda document, xref: 1 / 0)

        assert tuple(self.scan(path)) == (frozenset(), ())

    @pytest.mark.parametrize("method", ["xref_is_stream", "xref_stream_raw"])
    @pytest.mark.parametrize("fails", ["returns-none", "raises"])
    def test_an_unreadable_stream_agrees_nothing(self, tmp_path, monkeypatch, method, fails):
        import pymupdf

        def unreadable(document, xref):
            if fails == "raises":
                raise RuntimeError("stream unreadable")
            return None if method == "xref_stream_raw" else True

        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
        if method == "xref_is_stream" and fails == "returns-none":
            monkeypatch.setattr(pymupdf.Document, "xref_stream_raw", lambda document, xref: None)
        monkeypatch.setattr(pymupdf.Document, method, unreadable)

        assert tuple(self.scan(path)) == (frozenset(), ())

    def test_a_page_whose_images_cannot_be_listed_agrees_nothing(self, tmp_path, monkeypatch):
        import pymupdf

        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
        monkeypatch.setattr(pymupdf.Page, "get_images", lambda page, full=False: 1 / 0)

        assert tuple(self.scan(path)) == (frozenset(), ())

    def test_an_unreadable_content_stream_agrees_nothing(self, tmp_path, monkeypatch):
        import pymupdf

        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
        monkeypatch.setattr(pymupdf.Page, "read_contents", lambda page: 1 / 0)

        assert tuple(self.scan(path)) == (frozenset(), ())

    def test_an_inline_image_whose_filter_cannot_be_read_agrees_nothing(self, tmp_path):
        stream = jbig2_scan()
        content = b"q 200 0 0 150 50 50 cm /Im0 Do Q BI /W 4 /H 4 /F [/Fl ID x EI"
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
            b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
            b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
            % (SCAN_ENTRIES, len(stream), stream),
        ]

        assert tuple(self.scan(hand_written_pdf(tmp_path / "inline.pdf", objects))) == (frozenset(), ())

    def test_an_inline_image_of_another_codec_agrees_still(self, tmp_path):
        import hashlib

        stream = jbig2_scan()
        content = (
            b"q 200 0 0 150 50 50 cm /Im0 Do Q q 4 0 0 4 0 0 cm BI /W 2 /H 2 /CS /G /BPC 8 /F /AHx ID 00ff00ff> EI Q"
        )
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
            b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
            b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
            % (SCAN_ENTRIES, len(stream), stream),
        ]

        agreed = self.scan(hand_written_pdf(tmp_path / "inline.pdf", objects))

        assert agreed.streams == frozenset({hashlib.sha256(stream).hexdigest()})

    def test_a_type3_font_without_glyphs_agrees_still(self, tmp_path):
        font = b"<< /Type /Font /Subtype /Type3 >>"
        path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan(), extra_objects=(font,))

        assert len(self.scan(path).streams) == 1

    def test_a_document_that_cannot_tell_its_revisions_agrees_nothing(self):
        from datagrunt.core.pdf_io.extraction.render_guard import jbig2_agreement

        class Unknown:
            @property
            def is_repaired(self):
                raise RuntimeError("cannot tell")

        assert tuple(jbig2_agreement(Unknown())) == (frozenset(), ())

    def test_an_unreadable_object_table_agrees_nothing(self):
        from datagrunt.core.pdf_io.extraction.render_guard import jbig2_agreement

        class NoTable:
            is_repaired = False
            version_count = 1

            def xref_length(self):
                raise RuntimeError("no object table")

        assert tuple(jbig2_agreement(NoTable())) == (frozenset(), ())

    def test_a_file_pymupdf_cannot_open_agrees_nothing_and_warns(self, tmp_path, caplog):
        from datagrunt.core.pdf_io.extraction.render_guard import jbig2_agreement_for_path

        path = tmp_path / "not.pdf"
        path.write_bytes(b"not a pdf")

        assert tuple(jbig2_agreement_for_path(path)) == (frozenset(), ())
        assert "JBIG2 decode parameters of" in caplog.text


def test_a_pdfium_page_without_the_documents_jbig2_list_decodes_no_jbig2_image(tmp_path, no_decode):
    from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument, PdfiumPage

    path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
    warnings: list[str] = []
    with PdfiumDocument(path) as doc:
        page = PdfiumPage(doc._pdf[0], doc._raw)
        try:
            assert list(page.image_items(warnings=warnings)) == []
        finally:
            page.close()

    assert warnings == [
        "page 1 image 0: JBIG2 decode parameters could not be checked for /JBIG2Globals; skipped (not decoded)"
    ]
    assert no_decode == []


# ---------------------------------------------------------------------------
# Reads the guard depends on: a read that fails skips the image or the render.
# ---------------------------------------------------------------------------

FLATE_GRAY_64 = __import__("zlib").compress(b"\x80" * 64 * 64)
FLATE_GRAY_ENTRIES = b"/Width 64 /Height 64 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /FlateDecode"


def _failing_filter_count(original):
    return lambda image: -1


def _failing_filter_size(original):
    return lambda image, index, buffer, length: 0


def _short_filter_copy(original):
    def stand_in(image, index, buffer, length):
        size = original(image, index, None, 0)
        return size if buffer is None else size - 1

    return stand_in


FILTER_READ_FAILURES = {
    "count": ("FPDFImageObj_GetImageFilterCount", _failing_filter_count),
    "name-size": ("FPDFImageObj_GetImageFilter", _failing_filter_size),
    "name-copy": ("FPDFImageObj_GetImageFilter", _short_filter_copy),
}


def _assert_pdfium_skips_and_renders_nothing(path: Path, tmp_path: Path, calls: list, warning: str) -> None:
    """Both pdfium schemas skip the page's image with ``warning`` and OCR nothing; its render is skipped."""
    for native in (False, True):
        document = PDFReader(path, engine="pdfium", native=native).to_dicts(image_output_dir=str(tmp_path / "out"))
        assert _image_elements(document) == []
        assert any(warning in w for w in _warnings_of(document)), (native, _warnings_of(document))
    assert PDFWriter(path, engine="pdfium").render_pages_as_images(str(tmp_path / "pages"), dpi=72) == []
    assert calls == []


@pytest.mark.parametrize("failure", sorted(FILTER_READ_FAILURES))
def test_an_image_whose_filters_cannot_be_read_is_not_decoded_by_pdfium(tmp_path, monkeypatch, failure, no_decode):
    import pypdfium2.raw as pdfium_raw

    path = image_page_pdf(tmp_path / "flate.pdf", FLATE_GRAY_64, FLATE_GRAY_ENTRIES)
    name, stand_in = FILTER_READ_FAILURES[failure]
    monkeypatch.setattr(pdfium_raw, name, stand_in(getattr(pdfium_raw, name)))

    _assert_pdfium_skips_and_renders_nothing(
        path, tmp_path, no_decode, "page 1 image 0: image filters could not be read"
    )


def test_an_image_whose_filter_name_is_not_utf8_is_not_decoded_by_pdfium(tmp_path, no_decode):
    entries = b"/Width 64 /Height 64 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter [/FlateDecode /X#E9]"
    path = image_page_pdf(tmp_path / "name.pdf", FLATE_GRAY_64, entries)

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert _image_elements(document) == []
    assert "page 1 image 0: image filters could not be read; skipped (not decoded)" in _warnings_of(document)
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_flate_image_is_still_decoded_and_rendered(tmp_path, engine):
    path = image_page_pdf(tmp_path / "flate.pdf", FLATE_GRAY_64, FLATE_GRAY_ENTRIES)

    natives = (False, True) if engine == "pdfium" else (False,)
    for native in natives:
        document = PDFReader(path, engine=engine, native=native).to_dicts(image_output_dir=str(tmp_path / "out"))
        assert len(_image_elements(document)) == 1, (engine, native)
        assert not _warnings_of(document)
    assert len(PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72)) == 1


def _type3_indirect_charprocs_pdf(path: Path) -> Path:
    """A Type3 font whose /CharProcs is an indirect dictionary (object 7)."""
    glyph = b"d0 100 0 10 10 80 80 re f"
    content = b"BT /F0 100 Tf 100 100 Td (A) Tj ET"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /Font /Subtype /Type3 /FontBBox [0 0 100 100] /FontMatrix [0.01 0 0 0.01 0 0] "
        b"/CharProcs 7 0 R /Encoding << /Differences [65 /ch] >> /FirstChar 65 /LastChar 65 /Widths [100] >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(glyph), glyph),
        b"<< /ch 6 0 R >>",
    ]
    return hand_written_pdf(path, objects)


KEY_READ_FAILURES = {
    "subtype": ("xref_get_key", "Subtype", lambda path: image_page_pdf(path, FLATE_GRAY_64, FLATE_GRAY_ENTRIES)),
    "pattern-type": (
        "xref_get_key",
        "PatternType",
        lambda path: image_page_pdf(path, FLATE_GRAY_64, FLATE_GRAY_ENTRIES),
    ),
    "char-procs": ("xref_get_key", "CharProcs", lambda path: _type3_pdf(path, b"d0 100 0 10 10 80 80 re f")),
    "char-procs-keys": ("xref_get_keys", None, _type3_indirect_charprocs_pdf),
}


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("failure", sorted(KEY_READ_FAILURES))
def test_a_render_is_skipped_when_a_key_the_scan_reads_cannot_be_read(
    tmp_path, monkeypatch, engine, failure, no_decode
):
    import pymupdf

    method, key, make = KEY_READ_FAILURES[failure]
    original = getattr(pymupdf.Document, method)

    def failing(document, xref, *args):
        if key is None or args[0] == key:
            raise RuntimeError("unreadable")
        return original(document, xref, *args)

    path = make(tmp_path / "doc.pdf")
    monkeypatch.setattr(pymupdf.Document, method, failing)

    assert PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72) == []
    assert no_decode == []


# ---------------------------------------------------------------------------
# The pdfium engine decodes a JBIG2 image only when PyMuPDF and pdfium agree on it.
# ---------------------------------------------------------------------------

JBIG2_UNCHECKED = "JBIG2 decode parameters could not be checked for /JBIG2Globals; skipped (not decoded)"


def other_jbig2_scan() -> bytes:
    """A second clean scan whose stream differs from ``jbig2_scan()`` (an extension segment added)."""
    return jbig2_scan(extra=jbig2_segment(7, 62, struct.pack(">I", 0x20000000) + b"other\x00"))


def append_revision(path: Path, objects: dict, size: int) -> Path:
    """Append an incremental update to ``path``: ``objects`` (number -> body), a new xref section and trailer."""
    data = path.read_bytes()
    previous = int(data.rsplit(b"startxref", 1)[1].split()[0])
    out = bytearray(data)
    offsets = {}
    for number, body in sorted(objects.items()):
        offsets[number] = len(out)
        out += b"%d 0 obj\n%s\nendobj\n" % (number, body)
    xref = len(out)
    out += b"xref\n" + b"".join(b"%d 1\n%010d 00000 n \n" % (n, offsets[n]) for n in sorted(offsets))
    out += b"trailer\n<< /Size %d /Root 1 0 R /Prev %d >>\nstartxref\n%d\n%%%%EOF\n" % (size, previous, xref)
    path.write_bytes(bytes(out))
    return path


def two_image_pdf(path: Path, first: bytes, second: bytes, *, second_entries: bytes = b"", draw_second=True) -> Path:
    """A page whose resources hold two JBIG2 XObjects (5 and 6); it draws the second only if ``draw_second``."""
    content = b"q 200 0 0 150 50 50 cm /Im0 Do Q" + (b" q 200 0 0 150 300 50 cm /Im1 Do Q" if draw_second else b"")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R /Im1 6 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, len(first), first),
        b"<< /Type /XObject /Subtype /Image %s %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, second_entries, len(second), second),
        globals_object(b""),
    ]
    return hand_written_pdf(path, objects)


def _assert_pdfium_decodes_no_jbig2(path: Path, tmp_path: Path, calls: list) -> None:
    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))
    assert _image_elements(document) == []
    assert any(JBIG2_UNCHECKED in warning for warning in _warnings_of(document)), _warnings_of(document)
    assert PDFWriter(path, engine="pdfium").render_pages_as_images(str(tmp_path / "pages"), dpi=72) == []
    assert calls == []


def test_a_repaired_file_decodes_no_jbig2_in_pdfium(tmp_path, no_decode):
    path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
    data = path.read_bytes()
    catalog = data.index(b"1 0 obj")
    path.write_bytes(data.replace(b"%010d 00000 n" % catalog, b"%010d 00000 n" % (catalog + 3), 1))

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)


def test_an_updated_file_decodes_no_jbig2_in_pdfium(tmp_path, no_decode):
    path = append_revision(
        jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan()), {1: b"<< /Type /Catalog /Pages 2 0 R /Lang (en) >>"}, 6
    )

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)


def test_an_update_that_adds_globals_decodes_no_jbig2_in_pdfium(tmp_path, no_decode):
    # Only the first revision's image has no /JBIG2Globals.
    stream = jbig2_scan()
    image = b"<< /Type /XObject /Subtype /Image %s /DecodeParms << /JBIG2Globals 6 0 R >> /Length %d >>\n" % (
        SCAN_ENTRIES,
        len(stream),
    )
    image += b"stream\n" + stream + b"\nendstream"
    path = append_revision(jbig2_scan_pdf(tmp_path / "scan.pdf", stream), {5: image, 6: globals_object(b"")}, 7)

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)


def test_an_inline_jbig2_image_with_a_clean_images_data_decodes_no_jbig2_in_pdfium(tmp_path, no_decode):
    stream = jbig2_scan()
    content = (
        b"q 200 0 0 150 50 50 cm /Im0 Do Q q 200 0 0 150 300 50 cm BI /W 64 /H 48 /CS /G /BPC 1 /F /JBIG2Decode "
        b"/DP << /JBIG2Globals 6 0 R >> ID " + stream + b"\nEI Q"
    )
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, len(stream), stream),
        globals_object(b""),
    ]
    path = hand_written_pdf(tmp_path / "inline.pdf", objects)

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert _image_elements(document) == []
    assert any(JBIG2_UNCHECKED in warning for warning in _warnings_of(document)), _warnings_of(document)
    assert no_decode == []


def test_two_images_with_one_stream_one_with_globals_decode_no_jbig2_in_pdfium(tmp_path, no_decode):
    stream = jbig2_scan()
    path = two_image_pdf(tmp_path / "two.pdf", stream, stream, second_entries=b"/DecodeParms << /JBIG2Globals 7 0 R >>")

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)


def test_two_clean_images_with_one_stream_decode_no_jbig2_in_pdfium(tmp_path, no_decode):
    stream = jbig2_scan()
    path = two_image_pdf(tmp_path / "two.pdf", stream, stream)

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)


def test_a_page_whose_jbig2_images_the_readers_count_differently_decodes_none_in_pdfium(tmp_path, no_decode):
    # PyMuPDF finds both images in the page's resources; pdfium enumerates the one drawn.
    path = two_image_pdf(tmp_path / "two.pdf", jbig2_scan(), other_jbig2_scan(), draw_second=False)

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)


def test_two_different_clean_images_drawn_on_a_page_are_decoded_in_pdfium(tmp_path):
    path = two_image_pdf(tmp_path / "two.pdf", jbig2_scan(), other_jbig2_scan())

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert len(_image_elements(document)) == 2
    assert not any("JBIG2" in warning for warning in _warnings_of(document))


def test_a_jbig2_image_drawn_twice_through_a_form_is_decoded_in_pdfium(tmp_path):
    # pdfium enumerates two drawings of one image; PyMuPDF lists the image once: one image either way.
    stream = jbig2_scan()
    content = b"q /Fm0 Do Q"
    form = b"q 200 0 0 150 50 50 cm /Im0 Do Q q 200 0 0 150 50 300 cm /Im0 Do Q"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Fm0 6 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, len(stream), stream),
        b"<< /Type /XObject /Subtype /Form /BBox [0 0 612 792] /Resources << /XObject << /Im0 5 0 R >> >> "
        b"/Length %d >>\nstream\n%s\nendstream" % (len(form), form),
    ]
    path = hand_written_pdf(tmp_path / "form.pdf", objects)

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert len(_image_elements(document)) == 2
    assert not any("JBIG2" in warning for warning in _warnings_of(document))


@pytest.mark.parametrize("engine", ENGINES)
def test_a_type3_font_with_indirect_glyphs_still_renders(tmp_path, engine):
    path = _type3_indirect_charprocs_pdf(tmp_path / "t3.pdf")

    assert len(PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72)) == 1


@pytest.mark.parametrize("unreadable", ["filters", "stream"])
def test_a_jbig2_image_is_not_decoded_when_another_on_its_page_cannot_be_read(
    tmp_path, monkeypatch, unreadable, no_decode
):
    from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumPage

    other = other_jbig2_scan()
    method = "_image_filters" if unreadable == "filters" else "_raw_image_stream"
    original_raw, original = PdfiumPage._raw_image_stream, getattr(PdfiumPage, method)

    def fails_for_the_other(page, obj):
        return None if original_raw(page, obj) == other else original(page, obj)

    monkeypatch.setattr(PdfiumPage, method, fails_for_the_other)
    path = two_image_pdf(tmp_path / "two.pdf", jbig2_scan(), other)

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert _image_elements(document) == []
    assert any(w.startswith("page 1 image 0: " + JBIG2_UNCHECKED[:20]) for w in _warnings_of(document)), _warnings_of(
        document
    )
    assert no_decode == []


def test_a_non_image_object_with_the_stream_and_globals_decodes_no_jbig2_in_pdfium(tmp_path, no_decode):
    stream = jbig2_scan()
    other = b"<< /DecodeParms << /JBIG2Globals 7 0 R >> /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream"
    path = jbig2_scan_pdf(tmp_path / "scan.pdf", stream, extra_objects=(other, globals_object(b"")))

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)


# ---------------------------------------------------------------------------
# Inline images are read by a lexer: every dictionary entry, any separator, no byte window.
# ---------------------------------------------------------------------------

JPX_100_SIZED_INLINE = j2k((16, 16), siz=(100, 100), tile=(100, 100))
assert b"EI" not in JPX_100_SIZED_INLINE


def content_pdf(path: Path, content: bytes, resources: bytes = b"") -> Path:
    """A one-page PDF (no image XObject) whose content stream is ``content``."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << %s >> /Contents 4 0 R >>" % resources,
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
    ]
    return hand_written_pdf(path, objects)


def _jpx_inline(dictionary: bytes, sep: bytes = b" ") -> bytes:
    """``q ... cm BI <dictionary> ID <JPX> EI Q``, every separator ``sep``."""
    parts = [b"q", b"100 0 0 100 50 50 cm", b"BI", dictionary, b"ID", JPX_100_SIZED_INLINE]
    return sep.join(parts) + sep + b"EI" + sep + b"Q"


INLINE_CASES = {
    "repeated-filter": _jpx_inline(b"/W 16 /H 16 /CS /G /BPC 8 /F /Fl /F /JPXDecode"),
    "filter-and-f": _jpx_inline(b"/W 16 /H 16 /CS /G /BPC 8 /Filter /Fl /F /JPXDecode"),
    "repeated-parms": _jpx_inline(b"/W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode /DP null /DecodeParms null"),
    "filter-not-a-name": _jpx_inline(b"/W 16 /H 16 /CS /G /BPC 8 /F (JPXDecode)"),
    "filter-past-4-kb": _jpx_inline(b"/W 16 /H 16 /CS /G /BPC 8 /Pad (" + b"a" * 5000 + b") /F /JPXDecode"),
    "nul-separators": _jpx_inline(b"/W\x0016\x00/H\x0016\x00/CS\x00/G\x00/BPC\x008\x00/F\x00/JPXDecode", sep=b"\x00"),
    "form-feed-separators": _jpx_inline(b"/W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode", sep=b"\x0c"),
    "no-id": b"q BI /W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode Q",
    "ei-before-a-delimiter": b"q 100 0 0 100 50 50 cm BI /W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode ID "
    + JPX_100_SIZED_INLINE
    + b"\nEI/Q",
}


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("case", sorted(INLINE_CASES))
def test_an_inline_image_the_lexer_reads_or_doubts_blocks_the_render(tmp_path, engine, case, tiny_cap, no_decode):
    path = content_pdf(tmp_path / "inline.pdf", INLINE_CASES[case])

    assert PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72) == []
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_bi_in_a_text_string_does_not_block_the_render(tmp_path, engine):
    content = b"BT /F1 12 Tf 72 700 Td (the BI team) Tj ET"
    font = b"/Font << /F1 << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> >>"
    path = content_pdf(tmp_path / "text.pdf", content, font)

    assert len(PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72)) == 1


def test_an_inline_jbig2_image_behind_a_repeated_filter_key_decodes_no_jbig2_in_pdfium(tmp_path, no_decode):
    stream = jbig2_scan()
    content = (
        b"q 200 0 0 150 50 50 cm /Im0 Do Q q 200 0 0 150 300 50 cm BI /W 64 /H 48 /CS /G /BPC 1 "
        b"/F /Fl /F /JBIG2Decode ID " + stream + b"\nEI Q"
    )
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, len(stream), stream),
    ]
    path = hand_written_pdf(tmp_path / "inline.pdf", objects)

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert _image_elements(document) == []
    assert any(JBIG2_UNCHECKED in warning for warning in _warnings_of(document)), _warnings_of(document)
    assert no_decode == []


# ---------------------------------------------------------------------------
# Both readers must find one revision and a sound cross-reference table.
# ---------------------------------------------------------------------------


def _nineteen_byte_xref_entries(path: Path) -> Path:
    """``path`` with each xref entry ending in a bare line feed: pdfium rebuilds the table, MuPDF reads it."""
    import re

    path.write_bytes(re.sub(rb"(\d{10} \d{5} [nf]) \n", rb"\1\n", path.read_bytes()))
    return path


def _without_eof(path: Path) -> Path:
    """``path`` without its %%EOF marker: pdfium finds no trailer end, MuPDF reads the file."""
    data = path.read_bytes()
    path.write_bytes(data[: data.rindex(b"%%EOF")])
    return path


@pytest.mark.parametrize("damage", [_nineteen_byte_xref_entries, _without_eof], ids=["19-byte-entries", "no-eof"])
def test_a_file_pdfium_does_not_read_as_one_sound_revision_decodes_and_renders_no_jbig2(tmp_path, damage, no_decode):
    import pymupdf

    path = damage(jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan()))
    with pymupdf.open(path) as doc:
        assert (doc.is_repaired, doc.version_count) == (False, 1)

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)
    assert PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "mupdf-pages"), dpi=72) == []
    assert no_decode == []


# ---------------------------------------------------------------------------
# /JBIG2Globals name tokens are counted in the file bytes, whatever either reader resolves.
# ---------------------------------------------------------------------------


def _with_an_unreferenced_image_definition(path: Path) -> Path:
    """``path`` with an earlier, unreferenced definition of image object 5 that carries /JBIG2Globals."""
    stream = jbig2_scan()
    unused = (
        b"5 0 obj\n<< /Type /XObject /Subtype /Image %s /DecodeParms << /JBIG2Globals 4 0 R >> /Length %d >>\n"
        % (SCAN_ENTRIES, len(stream))
        + b"stream\n"
        + stream
        + b"\nendstream\nendobj\n"
    )
    data = path.read_bytes()
    header_end = data.index(b"\n") + 1
    shifted = data[:header_end] + unused + data[header_end:]
    # Every offset moves by the inserted definition's length.
    out = bytearray(shifted)
    import re

    table = re.compile(rb"(\d{10}) 00000 n")
    out = table.sub(lambda m: b"%010d 00000 n" % (int(m.group(1)) + len(unused)), bytes(out))
    startxref = int(out.rsplit(b"startxref", 1)[1].split()[0])
    out = out[: out.rindex(b"startxref")] + b"startxref\n%d\n%%%%EOF\n" % (startxref + len(unused))
    path.write_bytes(out)
    return path


def _with_duplicate_decode_parms(path: Path) -> Path:
    """A scan whose image dictionary holds /DecodeParms twice: with /JBIG2Globals, then null."""
    stream = jbig2_scan()
    return jbig2_scan_pdf(
        path, stream, b"/DecodeParms << /JBIG2Globals 6 0 R >> /DecodeParms null", (globals_object(b""),)
    )


def _with_globals_in_an_object_stream(path: Path) -> Path:
    """A scan whose file holds a Flate object stream (object 6) defining an object with /JBIG2Globals."""
    import zlib

    body = b"9 0 << /DecodeParms << /JBIG2Globals 7 0 R >> >>"
    packed = zlib.compress(body)
    objstm = b"<< /Type /ObjStm /N 1 /First 4 /Filter /FlateDecode /Length %d >>\nstream\n" % len(packed)
    return jbig2_scan_pdf(path, jbig2_scan(), extra_objects=(objstm + packed + b"\nendstream", globals_object(b"")))


GLOBALS_TOKEN_CASES = {
    "unreferenced-definition": lambda path: _with_an_unreferenced_image_definition(jbig2_scan_pdf(path, jbig2_scan())),
    "duplicate-decode-parms": _with_duplicate_decode_parms,
    "object-stream": _with_globals_in_an_object_stream,
}


@pytest.mark.parametrize("case", sorted(GLOBALS_TOKEN_CASES))
def test_a_jbig2globals_token_neither_reader_resolves_decodes_and_renders_no_jbig2(tmp_path, case, no_decode):
    path = GLOBALS_TOKEN_CASES[case](tmp_path / "scan.pdf")

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)
    assert PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "mupdf-pages"), dpi=72) == []
    assert no_decode == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_clean_inline_image_still_renders(tmp_path, engine):
    content = b"q 100 0 0 100 50 50 cm BI /W 2 /H 2 /CS /G /BPC 8 /F /AHx ID 00ff00ff> EI Q"
    path = content_pdf(tmp_path / "inline.pdf", content)

    assert len(PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72)) == 1


@pytest.mark.parametrize(
    "content,reason",
    [
        pytest.param(
            b"q 100 0 0 100 50 50 cm BI /W 16 /H 16 /CS /G /BPC 8 /F /JPXDecode ID " + NORMAL_JPX,
            "an inline image could not be delimited; render skipped",
            id="jpx-without-ei",
        ),
        pytest.param(
            b"BT (/JPXDecode) Tj ET",
            "a JPXDecode or JBIG2Decode name lies outside the inline images read; render skipped",
            id="codec-name-in-a-string",
        ),
    ],
)
def test_an_inline_image_codec_the_scan_cannot_place_blocks_the_render(tmp_path, content, reason):
    import pymupdf

    from datagrunt.core.pdf_io.extraction.render_guard import document_block_reason

    with pymupdf.open(content_pdf(tmp_path / "inline.pdf", content)) as doc:
        assert document_block_reason(doc) == reason


class TestGlobalsTokenCount:
    """The render scan and the agreement count /JBIG2Globals names; whatever cannot be counted is doubt."""

    @staticmethod
    def block_reason(path: Path):
        import pymupdf

        from datagrunt.core.pdf_io.extraction.render_guard import document_block_reason

        with pymupdf.open(path) as doc:
            return document_block_reason(doc)

    def test_a_document_without_a_file_name_cannot_be_counted(self, tmp_path):
        import pymupdf

        from datagrunt.core.pdf_io.extraction.render_guard import document_block_reason

        data = image_page_pdf(tmp_path / "flate.pdf", FLATE_GRAY_64, FLATE_GRAY_ENTRIES).read_bytes()
        with pymupdf.open(stream=data, filetype="pdf") as doc:
            assert document_block_reason(doc) == "the file's JBIG2 names could not be counted; render skipped"

    def test_a_token_across_two_chunks_is_counted_once(self, tmp_path, monkeypatch):
        from datagrunt.core.pdf_io.extraction import render_guard

        path = tmp_path / "bytes.bin"
        path.write_bytes(b"x" * 5 + b"/JBIG2Globals" + b"y" * 9 + b"/JBIG2#47lobals" + b"z" * 3)
        monkeypatch.setattr(render_guard, "_TOKEN_CHUNK_BYTES", 8)

        assert render_guard._file_token_counts(path, (render_guard.GLOBALS_TOKEN,)) == (2,)

    def test_a_file_over_the_budget_or_missing_cannot_be_counted(self, tmp_path, monkeypatch):
        from datagrunt.core.pdf_io.extraction import render_guard

        path = tmp_path / "bytes.bin"
        path.write_bytes(b"x" * 100)
        monkeypatch.setattr(render_guard, "_MAX_SCAN_BYTES", 99)

        assert render_guard._file_token_counts(path, (render_guard.GLOBALS_TOKEN,)) is None
        assert render_guard._file_token_counts(tmp_path / "missing.bin", (render_guard.GLOBALS_TOKEN,)) is None

    @staticmethod
    def object_stream_pdf(path: Path, entries: bytes, data: bytes) -> Path:
        objstm = b"<< /Type /ObjStm /N 1 /First 4 %s /Length %d >>\nstream\n" % (entries, len(data))
        objects = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
            b"<< /Length 0 >>\nstream\n\nendstream",
            objstm + data + b"\nendstream",
        ]
        return hand_written_pdf(path, objects)

    def test_an_unfiltered_object_stream_is_counted(self, tmp_path):
        body = b"9 0 << /DecodeParms << /JBIG2Globals 7 0 R >> >>"
        path = self.object_stream_pdf(tmp_path / "objstm.pdf", b"", body)

        assert self.block_reason(path) == "a /JBIG2Globals name is in the file; render skipped"

    @pytest.mark.parametrize(
        "entries,data",
        [
            pytest.param(b"/Filter /FlateDecode /DecodeParms << /Predictor 12 >>", b"x", id="decode-parameters"),
            pytest.param(b"/Filter /ASCIIHexDecode", b"3920>", id="another-filter"),
            pytest.param(b"/Filter /FlateDecode", b"not zlib", id="bad-zlib"),
            pytest.param(b"/Filter /FlateDecode", __import__("zlib").compress(b"9 0 null")[:-4], id="unended-zlib"),
        ],
    )
    def test_an_object_stream_that_cannot_be_decoded_blocks_the_render(self, tmp_path, entries, data):
        path = self.object_stream_pdf(tmp_path / "objstm.pdf", entries, data)

        assert self.block_reason(path) == "the file's JBIG2 names could not be counted; render skipped"

    @pytest.mark.parametrize("method", ["xref_get_key", "xref_stream_raw"])
    def test_an_object_stream_that_cannot_be_read_blocks_the_render(self, tmp_path, monkeypatch, method):
        import pymupdf

        path = self.object_stream_pdf(
            tmp_path / "objstm.pdf", b"/Filter /FlateDecode", __import__("zlib").compress(b"x")
        )
        original = getattr(pymupdf.Document, method)

        def fails_for_object_streams(document, xref, *args):
            if xref == 5 and (method == "xref_stream_raw" or args == ("Type",)):
                raise RuntimeError("unreadable")
            return original(document, xref, *args)

        monkeypatch.setattr(pymupdf.Document, method, fails_for_object_streams)

        assert self.block_reason(path) == "the file's JBIG2 names could not be counted; render skipped"

    def test_an_unreadable_object_table_cannot_be_counted(self):
        from datagrunt.core.pdf_io.extraction.render_guard import _object_stream_token_counts

        class NoTable:
            def xref_length(self):
                raise RuntimeError("no object table")

        assert _object_stream_token_counts(NoTable(), ()) is None

    def test_a_clean_document_has_no_token(self, tmp_path):
        assert self.block_reason(image_page_pdf(tmp_path / "flate.pdf", FLATE_GRAY_64, FLATE_GRAY_ENTRIES)) is None


def test_pdfium_reads_no_sound_revision_of_a_file_it_cannot_open(tmp_path):
    from datagrunt.core.pdf_io.extraction.pdfium_document import pdfium_reads_one_sound_revision_for_path

    assert pdfium_reads_one_sound_revision_for_path(tmp_path / "missing.pdf") is False


def test_an_image_stream_scan_over_the_byte_budget_blocks_the_render(tmp_path, monkeypatch):
    import pymupdf

    from datagrunt.core.pdf_io.extraction import render_guard

    path = image_page_pdf(tmp_path / "flate.pdf", FLATE_GRAY_64, FLATE_GRAY_ENTRIES)
    monkeypatch.setattr(render_guard, "_MAX_SCAN_BYTES", len(FLATE_GRAY_64) - 1)

    with pymupdf.open(path) as doc:
        reason = render_guard.document_block_reason(doc)

    assert reason == "image stream scan exceeded the per-document byte budget; render skipped"


def test_a_globals_name_in_a_compressed_content_stream_decodes_no_jbig2_in_pdfium(tmp_path, no_decode):
    # The name is only in the content stream decoded, not in the file's bytes.
    import zlib

    stream = jbig2_scan()
    content = zlib.compress(b"q 200 0 0 150 50 50 cm /Im0 Do Q BT (/JBIG2Globals) Tj ET")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d /Filter /FlateDecode >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n%s\nendstream"
        % (SCAN_ENTRIES, len(stream), stream),
    ]
    path = hand_written_pdf(tmp_path / "compressed.pdf", objects)
    assert b"JBIG2Globals" not in path.read_bytes()

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert _image_elements(document) == []
    assert any(JBIG2_UNCHECKED in warning for warning in _warnings_of(document)), _warnings_of(document)
    assert no_decode == []


def test_an_update_without_its_final_eof_decodes_and_renders_no_jbig2(tmp_path, no_decode):
    # pdfium finds one trailer end (it counts %%EOF markers); PyMuPDF follows /Prev to two revisions.
    import pymupdf
    import pypdfium2 as pdfium
    import pypdfium2.raw as pdfium_raw

    path = append_revision(
        jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan()), {1: b"<< /Type /Catalog /Pages 2 0 R /Lang (en) >>"}, 6
    )
    _without_eof(path)
    pdf = pdfium.PdfDocument(str(path))
    try:
        assert pdfium_raw.FPDF_GetTrailerEnds(pdf.raw, None, 0) == 1
        assert pdfium_raw.FPDF_DocumentHasValidCrossReferenceTable(pdf.raw)
    finally:
        pdf.close()
    with pymupdf.open(path) as doc:
        assert doc.version_count == 2

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)
    assert PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "mupdf-pages"), dpi=72) == []
    assert no_decode == []


def test_a_file_pymupdf_repairs_while_reading_it_agrees_nothing(tmp_path):
    # Object 5's xref entry points at object 4: PyMuPDF repairs the file only when it reads object 5.
    import pymupdf

    from datagrunt.core.pdf_io.extraction.render_guard import jbig2_agreement

    data = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan()).read_bytes()
    wrong = data.replace(b"%010d 00000 n" % data.index(b"5 0 obj"), b"%010d 00000 n" % data.index(b"4 0 obj"), 1)
    path = tmp_path / "wrong.pdf"
    path.write_bytes(wrong)

    with pymupdf.open(path) as doc:
        assert doc.is_repaired is False
        assert tuple(jbig2_agreement(doc)) == (frozenset(), ())
        assert doc.is_repaired is True


@pytest.mark.parametrize("engine", ENGINES)
def test_an_inline_jbig2_image_with_globals_in_a_compressed_content_stream_blocks_the_render(
    tmp_path, engine, no_decode
):
    # Its /JBIG2Globals name is only in the decoded content: the inline image check itself must block.
    import zlib

    inline = b"q 200 0 0 150 50 50 cm BI /W 64 /H 48 /CS /G /BPC 1 /F /JBIG2Decode /DP << /JBIG2Globals 5 0 R >> ID "
    content = zlib.compress(inline + jbig2_scan() + b"\nEI Q")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
        b"<< /Length %d /Filter /FlateDecode >>\nstream\n%s\nendstream" % (len(content), content),
        globals_object(b""),
    ]
    path = hand_written_pdf(tmp_path / "inline.pdf", objects)
    assert b"JBIG2Globals" not in path.read_bytes()

    assert PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72) == []
    assert no_decode == []


def test_a_jbig2_image_whose_decode_parameters_cannot_be_resolved_decodes_and_renders_no_jbig2(tmp_path, no_decode):
    # No /JBIG2Globals name anywhere: PyMuPDF cannot resolve the parameters, so they are not checked.
    path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan(), b"/DecodeParms 99 0 R")
    assert b"JBIG2Globals" not in path.read_bytes()

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)
    assert PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "mupdf-pages"), dpi=72) == []
    assert no_decode == []


def test_a_codec_name_escaped_twice_does_not_stand_for_a_name_outside_the_images(tmp_path):
    # /JPX#2344ecode is the name "JPX#44ecode" to a reader that unescapes once (pdfium): not JPXDecode.
    import pymupdf

    from datagrunt.core.pdf_io.extraction.render_guard import document_block_reason

    decoy = b"q 100 0 0 100 50 50 cm BI /W 48 /H 48 /CS /G /BPC 8 /F /JPX#2344ecode ID " + NORMAL_JPX + b"\nEI Q"
    path = content_pdf(tmp_path / "decoy.pdf", decoy + b" BT (/JPXDecode) Tj ET")

    with pymupdf.open(path) as doc:
        reason = document_block_reason(doc)

    assert reason == "an inline image: unrecognised image filter 'JPX#44ecode'; skipped (not decoded)"


def _with_an_object_stream_of_indirect_type(path: Path) -> Path:
    """A scan whose file holds a Flate object stream (object 6) whose /Type is object 8, the name /ObjStm."""
    import zlib

    packed = zlib.compress(b"9 0 << /DecodeParms << /JBIG2Globals 7 0 R >> >>")
    objstm = b"<< /Type 8 0 R /N 1 /First 4 /Filter /FlateDecode /Length %d >>\nstream\n" % len(packed)
    extra = (objstm + packed + b"\nendstream", globals_object(b""), b"/ObjStm")
    return jbig2_scan_pdf(path, jbig2_scan(), extra_objects=extra)


def test_an_object_stream_whose_type_is_indirect_is_counted(tmp_path, no_decode):
    path = _with_an_object_stream_of_indirect_type(tmp_path / "scan.pdf")
    assert b"JBIG2Globals" not in path.read_bytes()

    _assert_pdfium_decodes_no_jbig2(path, tmp_path, no_decode)
    assert PDFWriter(path, engine="pymupdf").render_pages_as_images(str(tmp_path / "mupdf-pages"), dpi=72) == []
    assert no_decode == []


def test_a_stream_whose_type_cannot_be_resolved_blocks_the_render(tmp_path):
    import pymupdf

    from datagrunt.core.pdf_io.extraction.render_guard import document_block_reason

    stream = b"<< /Type 99 0 R /Length 1 >>\nstream\nx\nendstream"
    path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan(), extra_objects=(stream,))

    with pymupdf.open(path) as doc:
        assert document_block_reason(doc) == "the file's JBIG2 names could not be counted; render skipped"


def _pymupdf_object_stream_pdf(path: Path) -> Path:
    """A file PyMuPDF writes with an xref stream and one object stream holding a /JBIG2Globals name."""
    import pymupdf

    source = jbig2_scan_pdf(path.with_name("source.pdf"), jbig2_scan())
    with pymupdf.open(source) as doc:
        extra = doc.get_new_xref()
        doc.update_object(extra, "<< /DecodeParms << /JBIG2Globals 5 0 R >> >>")
        doc.save(path, use_objstms=True, deflate=True)
    assert b"JBIG2Globals" not in path.read_bytes()
    return path


def test_an_object_stream_is_found_by_the_xref_entries_that_point_into_it(tmp_path, monkeypatch):
    # The object stream's /Type is read as some other name: its xref entries still name it.
    from datagrunt.core.pdf_io.extraction import render_guard

    path = _pymupdf_object_stream_pdf(tmp_path / "objstm.pdf")
    monkeypatch.setattr(render_guard, "_resolved_type", lambda doc, xref: "/Other")

    assert TestGlobalsTokenCount.block_reason(path) == ("a /JBIG2Globals name is in the file; render skipped")


def test_without_xref_entries_every_stream_without_a_type_is_counted(tmp_path, monkeypatch):
    # PyMuPDF cannot list its xref entries: an object stream is any stream with no /Type, and is decoded.
    from datagrunt.core.pdf_io.extraction import render_guard

    path = _pymupdf_object_stream_pdf(tmp_path / "objstm.pdf")
    real = render_guard._resolved_type
    monkeypatch.setattr(render_guard, "_compressed_object_streams", lambda doc, count: None)
    monkeypatch.setattr(
        render_guard, "_resolved_type", lambda doc, xref: "" if real(doc, xref) == "/ObjStm" else real(doc, xref)
    )

    assert TestGlobalsTokenCount.block_reason(path) == ("a /JBIG2Globals name is in the file; render skipped")


def test_a_jbig2_name_only_in_an_earlier_revision_still_needs_both_readers_to_agree(tmp_path, no_decode):
    # The update replaces the JBIG2 image with a Flate one: PyMuPDF sees no JBIG2 image, the bytes still name it.
    import pymupdf

    path = jbig2_scan_pdf(tmp_path / "scan.pdf", jbig2_scan())
    flate = b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n" % (FLATE_GRAY_ENTRIES, len(FLATE_GRAY_64))
    append_revision(path, {5: flate + FLATE_GRAY_64 + b"\nendstream"}, 6)
    with pymupdf.open(path) as doc:
        assert [info[8] for info in doc[0].get_images(full=True)] == ["FlateDecode"]
        assert doc.version_count == 2

    for engine in ENGINES:
        assert PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / engine), dpi=72) == [], engine
    assert no_decode == []


def test_an_updated_file_without_a_jbig2_name_still_renders(tmp_path):
    path = image_page_pdf(tmp_path / "flate.pdf", FLATE_GRAY_64, FLATE_GRAY_ENTRIES)
    append_revision(path, {1: b"<< /Type /Catalog /Pages 2 0 R /Lang (en) >>"}, 6)

    for engine in ENGINES:
        assert len(PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / engine), dpi=72)) == 1


def test_a_document_whose_xref_entries_cannot_be_listed_has_none(tmp_path):
    from datagrunt.core.pdf_io.extraction.render_guard import _compressed_object_streams

    assert _compressed_object_streams(object(), 3) is None


def test_an_object_that_cannot_be_read_as_a_stream_blocks_the_render(tmp_path, monkeypatch):
    import pymupdf

    path = image_page_pdf(tmp_path / "flate.pdf", FLATE_GRAY_64, FLATE_GRAY_ENTRIES)
    monkeypatch.setattr(pymupdf.Document, "xref_is_stream", lambda document, xref: 1 / 0)

    assert TestGlobalsTokenCount.block_reason(path) == "the file's JBIG2 names could not be counted; render skipped"


def _two_images_sharing_decode_parms_pdf(path: Path, first: bytes, entries: bytes, *, stray: bool) -> Path:
    """A page drawing two images (5 and 6) whose /DecodeParms are one object (7) holding /JBIG2Globals.

    With ``stray``, a ninth object also holds a /JBIG2Globals name, which no image refers to.
    """
    content = b"q 200 0 0 150 50 50 cm /Im0 Do Q q 200 0 0 150 300 50 cm /Im1 Do Q"
    image = b"<< /Type /XObject /Subtype /Image %s /DecodeParms 7 0 R /Length %d >>\nstream\n" % (entries, len(first))
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /XObject << /Im0 5 0 R /Im1 6 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        image + first + b"\nendstream",
        image + first + b"\nendstream",
        b"<< /JBIG2Globals 8 0 R >>",
        globals_object(b""),
        *((b"<< /DecodeParms << /JBIG2Globals 8 0 R >> >>",) if stray else ()),
    ]
    return hand_written_pdf(path, objects)


@pytest.mark.parametrize(
    "first,entries",
    [
        pytest.param(FLATE_GRAY_64, FLATE_GRAY_ENTRIES, id="flate-images"),
        pytest.param(jbig2_scan(), SCAN_ENTRIES, id="jbig2-images"),
    ],
)
@pytest.mark.parametrize("engine", ENGINES)
def test_images_sharing_one_decode_parms_object_do_not_hide_a_stray_globals_name(
    tmp_path, engine, first, entries, no_decode
):
    path = _two_images_sharing_decode_parms_pdf(tmp_path / "shared.pdf", first, entries, stray=True)

    assert PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "pages"), dpi=72) == []
    assert no_decode == []


@pytest.mark.parametrize(
    "content",
    [
        pytest.param(b"", id="name-in-another-key"),
        pytest.param(b" BT (/JBIG2Decode) Tj ET", id="name-in-another-key-and-a-name-outside"),
    ],
)
def test_a_codec_name_in_an_inline_image_that_is_not_its_filter_blocks_the_render(tmp_path, content):
    import pymupdf

    from datagrunt.core.pdf_io.extraction.render_guard import document_block_reason

    stream = b"q 100 0 0 100 50 50 cm BI /W 2 /H 2 /CS /G /BPC 8 /Foo /JPXDecode /F /AHx ID 00ff00ff> EI Q" + content

    with pymupdf.open(content_pdf(tmp_path / "decoy.pdf", stream)) as doc:
        reason = document_block_reason(doc)

    assert reason == "a JPXDecode or JBIG2Decode name lies outside the inline images read; render skipped"


def test_inline_codec_names_are_the_filter_names_only():
    from datagrunt.core.pdf_io.extraction.content_lexer import inline_images
    from datagrunt.core.pdf_io.extraction.render_guard import _codec_names

    found = inline_images(
        b"BI /F /JPX#2344ecode ID x EI BI /F [/AHx /JBIG2#44ecode] ID y EI BI /Foo /JPXDecode /F /AHx ID z EI"
    )

    assert [_codec_names(image) for image in found] == [0, 1, 0]


def test_the_inline_image_check_refuses_jbig2_with_globals_itself():
    # The document's /JBIG2Globals count blocks such a page too; this pins the inline check on its own.
    from datagrunt.core.pdf_io.extraction.render_guard import _inline_block_reason

    content = b"BI /W 64 /H 48 /CS /G /BPC 1 /F /JBIG2Decode /DP << /JBIG2Globals 5 0 R >> ID " + jbig2_scan() + b"\nEI"

    assert _inline_block_reason(content) == (
        "an inline image: JBIG2 decode parameters could not be checked for /JBIG2Globals; skipped (not decoded)"
    )
