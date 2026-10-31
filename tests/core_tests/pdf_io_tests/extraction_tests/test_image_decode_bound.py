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

from datagrunt import PDFReader, PDFWriter
from datagrunt.core.pdf_io.extraction import image_guard

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
    from datagrunt import PDFReader, PDFWriter

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
