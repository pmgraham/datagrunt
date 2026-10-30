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

    assert any("JBIG2 page" in w for w in _warnings_of(document))
    assert no_decode == []


def test_pymupdf_render_skips_a_jpx_soft_mask_bomb(tmp_path, no_decode):
    # A good base image with a JPEG 2000 bomb /SMask: a pymupdf page render
    # decodes the mask at its codestream size. The render gate must skip it.
    path = smask_bomb_pdf(tmp_path / "smask.pdf")
    out = tmp_path / "pages"
    paths = PDFWriter(path, engine="pymupdf").render_pages_as_images(str(out), dpi=72)

    assert paths == []
    assert no_decode == []


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
        used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return used if sys.platform == "darwin" else used * 1024

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
