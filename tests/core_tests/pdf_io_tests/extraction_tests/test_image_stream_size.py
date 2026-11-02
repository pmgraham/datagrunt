"""The pdfium engine skips an image whose stream decodes to more than its declared size, before pdfium decodes it.

pdfium decodes an image's FlateDecode, LZWDecode, RunLengthDecode,
ASCII85Decode and ASCIIHexDecode stages whole when it reads, extracts or
renders the image. These tests prove, on small generated PDFs, that the
pdfium engine checks every such stream (pdfium's raw copy of it) against the
image's ``/Width`` x ``/Height`` first:

* an image whose stream decodes past that size is skipped with a page
  warning and never decoded, on every pdfium path (``to_dicts``,
  ``extract_images``, page renders and OCR);
* an image that fits is still extracted and rendered;
* the PyMuPDF engine is unchanged (MuPDF stops at the declared size);
* peak memory stays bounded, measured in a subprocess.

pdfium decodes an inline image's first filter when it loads the page, before
any check can run; these tests cover an inline image whose later filter is
the one that decodes past its size.
"""

import subprocess
import sys
import textwrap
import zlib
from pathlib import Path

import pytest

from datagrunt import PDFReader, PDFWriter
from pdf_io_tests.extraction_tests._stream_fixtures import (
    MIB,
    ascii85,
    ascii_hex,
    flate_zeros,
    image_page_pdf,
    inline_image,
    lzw_encode,
    lzw_only_with_early_change_0,
    lzw_zeros,
    pages_pdf,
    png_rows,
    run_length,
    run_length_zeros,
)

ENGINES = ["pdfium", "pymupdf"]
PIXELS = bytes(range(256)) * 16  # a 64 x 64 gray image (over the 40-pixel minimum, so it is emitted)
GRAY_64 = b"/Width 64 /Height 64 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter "
PAST = 512 * 1024  # past 64 x 64 pixels at 16 bytes each, plus the slack

# Each filter (and a chain) with a stream that decodes past the size.
OVERSIZED = {
    "flate": (b"/FlateDecode", flate_zeros(8 * MIB)),
    "lzw": (b"/LZWDecode", lzw_zeros(8 * MIB)),
    "lzw-early-change-0": (b"/LZWDecode /DecodeParms << /EarlyChange 0 >>", lzw_only_with_early_change_0(PAST)),
    "run-length": (b"/RunLengthDecode", run_length_zeros(PAST)),
    "ascii85": (b"/ASCII85Decode", b"z" * (PAST // 4) + b"~>"),
    "ascii-hex": (b"/ASCIIHexDecode", b"00" * PAST + b">"),
    "chain": (b"[/ASCII85Decode /FlateDecode]", ascii85(flate_zeros(8 * MIB))),
}
# The same filters with a stream of the declared size, and predictor cases.
EXACT = {
    "flate": (b"/FlateDecode", zlib.compress(PIXELS)),
    "lzw": (b"/LZWDecode", lzw_encode(PIXELS)),
    "lzw-early-change-0": (b"/LZWDecode /DecodeParms << /EarlyChange 0 >>", lzw_encode(PIXELS, 0)),
    "run-length": (b"/RunLengthDecode", run_length(PIXELS)),
    "ascii85": (b"/ASCII85Decode", ascii85(PIXELS)),
    "ascii-hex": (b"/ASCIIHexDecode", ascii_hex(PIXELS)),
    "chain": (b"[/ASCII85Decode /FlateDecode]", ascii85(zlib.compress(PIXELS))),
    "png-predictor": (
        b"/FlateDecode /DecodeParms << /Predictor 15 /Columns 64 >>",
        zlib.compress(png_rows(PIXELS, 64)),
    ),
    "tiff-predictor": (b"/LZWDecode /DecodeParms << /Predictor 2 /Columns 64 >>", lzw_encode(PIXELS)),
}
CMYK_16_PNG = (
    b"/Width 64 /Height 64 /ColorSpace /DeviceCMYK /BitsPerComponent 16 /Filter /FlateDecode "
    b"/DecodeParms << /Predictor 15 /Colors 4 /BitsPerComponent 16 /Columns 64 >>"
)
OVER = "page 1 image 0: its stream decodes to more than its declared size; skipped (not decoded)"


@pytest.fixture
def no_pdfium_decode(monkeypatch):
    """Spy on every pdfium call that decodes an image stream or draws a page; each one fails the test."""
    import pypdfium2.raw as pdfium_raw

    from datagrunt.core.pdf_io.extraction import pdfium_document

    calls: list[str] = []

    def forbid(name):
        def spy(*args, **kwargs):
            calls.append(name)
            raise AssertionError(f"{name} was called on an image the check skips")

        return spy

    for name in (
        "FPDFImageObj_GetImageMetadata",
        "FPDFImageObj_GetBitmap",
        "FPDFImageObj_GetRenderedBitmap",
        "FPDFImageObj_GetImageDataDecoded",
    ):
        monkeypatch.setattr(pdfium_raw, name, forbid(name))
    monkeypatch.setattr(pdfium_document.PdfiumPage, "render_pil", forbid("PdfiumPage.render_pil"))
    return calls


@pytest.fixture
def no_pdfium_render(monkeypatch):
    """Fail the test if pdfium draws a page (the images here are small, so they may be decoded)."""
    from datagrunt.core.pdf_io.extraction import pdfium_document

    def forbid(*args, **kwargs):
        raise AssertionError("a page was drawn")

    monkeypatch.setattr(pdfium_document.PdfiumPage, "render_pil", forbid)


def _warnings(document: dict) -> list[str]:
    return [w for page in document["document"]["pages"] for w in (page.get("warnings") or [])]


def _images(document: dict) -> list:
    found = []
    for page in document["document"]["pages"]:
        found += [e for e in page.get("elements", []) if e.get("type") == "image"]
        found += list(page.get("images", []))
    return found


def oversized_pdf(tmp_path: Path, case: str, *, text: bool = True) -> Path:
    filters, stream = OVERSIZED[case]
    return image_page_pdf(tmp_path / f"{case}.pdf", stream, GRAY_64 + filters, text=text)


def exact_pdf(tmp_path: Path, case: str) -> Path:
    filters, stream = EXACT[case]
    return image_page_pdf(tmp_path / f"{case}.pdf", stream, GRAY_64 + filters)


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("case", sorted(OVERSIZED))
def test_pdfium_skips_an_image_whose_stream_decodes_past_its_declared_size(tmp_path, case, native, no_pdfium_decode):
    path = oversized_pdf(tmp_path, case)

    document = PDFReader(path, engine="pdfium", native=native).to_dicts(image_output_dir=str(tmp_path / "out"))

    assert OVER in _warnings(document)
    assert _images(document) == []
    assert no_pdfium_decode == []


@pytest.mark.parametrize("case", sorted(OVERSIZED))
def test_pdfium_extract_images_writes_none_of_them(tmp_path, case, no_pdfium_decode):
    assert PDFWriter(oversized_pdf(tmp_path, case), engine="pdfium").extract_images(str(tmp_path / "out")) == []
    assert no_pdfium_decode == []


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("case", sorted(EXACT))
def test_pdfium_still_extracts_an_image_of_its_declared_size(tmp_path, case, native):
    document = PDFReader(exact_pdf(tmp_path, case), engine="pdfium", native=native).to_dicts(
        image_output_dir=str(tmp_path / "out")
    )

    assert len(_images(document)) == 1
    assert _warnings(document) == []


def test_pdfium_still_extracts_a_16_bit_cmyk_image_with_a_predictor(tmp_path):
    # 8 bytes a pixel, the most the check assumes, and a tag byte a row.
    path = image_page_pdf(tmp_path / "cmyk.pdf", zlib.compress(png_rows(bytes(64 * 64 * 8), 64 * 8)), CMYK_16_PNG)

    document = PDFReader(path, engine="pdfium").to_dicts(image_output_dir=str(tmp_path / "out"))

    assert len(_images(document)) == 1
    assert _warnings(document) == []


@pytest.mark.parametrize("case", sorted(OVERSIZED))
def test_the_pymupdf_engine_still_extracts_it(tmp_path, case):
    # MuPDF decodes no more than the declared size, so its extraction is left as it was.
    document = PDFReader(oversized_pdf(tmp_path, case), engine="pymupdf").to_dicts(image_output_dir=str(tmp_path / "o"))

    assert len(_images(document)) == 1
    assert _warnings(document) == []


def test_pdfium_renders_no_page_drawing_it(tmp_path, no_pdfium_decode):
    pages = PDFWriter(oversized_pdf(tmp_path, "lzw"), engine="pdfium").render_pages_as_images(str(tmp_path / "p"))

    assert pages == []
    assert no_pdfium_decode == []


@pytest.mark.parametrize("native", [False, True])
def test_pdfium_does_not_ocr_a_scanned_page_drawing_it(tmp_path, native, no_pdfium_decode):
    document = PDFReader(oversized_pdf(tmp_path, "flate", text=False), engine="pdfium", native=native).to_dicts()

    assert OVER in _warnings(document)
    assert no_pdfium_decode == []


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("case", ["flate", "lzw-early-change-0", "png-predictor", "chain"])
def test_an_image_of_its_declared_size_still_renders(tmp_path, engine, case):
    path = exact_pdf(tmp_path, case)

    assert len(PDFWriter(path, engine=engine).render_pages_as_images(str(tmp_path / "p"), dpi=72)) == 1


INLINE_HEX_FLATE = b"/W 64 /H 64 /CS /G /BPC 8 /F [/AHx /Fl]"


@pytest.mark.parametrize("native", [False, True])
def test_pdfium_skips_an_inline_image_whose_later_filter_decodes_past_its_size(tmp_path, native, no_pdfium_decode):
    content = inline_image(INLINE_HEX_FLATE, ascii_hex(flate_zeros(8 * MIB)))
    path = pages_pdf(tmp_path / "inline.pdf", [content])

    document = PDFReader(path, engine="pdfium", native=native).to_dicts(image_output_dir=str(tmp_path / "out"))

    assert OVER in _warnings(document)
    assert _images(document) == []
    assert no_pdfium_decode == []


@pytest.mark.parametrize("native", [False, True])
def test_pdfium_still_extracts_an_inline_image_of_its_declared_size(tmp_path, native):
    path = pages_pdf(tmp_path / "inline.pdf", [inline_image(INLINE_HEX_FLATE, ascii_hex(zlib.compress(PIXELS)))])

    document = PDFReader(path, engine="pdfium", native=native).to_dicts(image_output_dir=str(tmp_path / "out"))

    assert len(_images(document)) == 1
    assert _warnings(document) == []


# ---------------------------------------------------------------------------
# The declared size: an image declaring more pixels than the bound is skipped.
# ---------------------------------------------------------------------------

HUGE_GRAY = b"/Width 20000 /Height 20000 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter "
PIXEL_BOUND = "page 1 image 0: image declares 20000 x 20000 = 400000000 pixels, over the 89478485-pixel bound; skipped"
HUGE_DECLARED = {
    "flate": (b"/FlateDecode", zlib.compress(b"")),
    "flate-past-its-size": (b"/FlateDecode", flate_zeros(8 * MIB)),
    "dct": (b"/DCTDecode", b"\xff\xd8\xff\xd9"),
    "unfiltered": (b"[]", bytes(64)),
}


def huge_pdf(tmp_path: Path, case: str, *, text: bool = True) -> Path:
    filters, stream = HUGE_DECLARED[case]
    return image_page_pdf(tmp_path / f"huge-{case}.pdf", stream, HUGE_GRAY + filters, text=text)


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("case", sorted(HUGE_DECLARED))
def test_pdfium_skips_an_image_declaring_more_pixels_than_the_bound(tmp_path, case, native, no_pdfium_decode):
    document = PDFReader(huge_pdf(tmp_path, case), engine="pdfium", native=native).to_dicts(
        image_output_dir=str(tmp_path / "out")
    )

    assert _warnings(document) == [PIXEL_BOUND]  # before the stream is read or checked
    assert _images(document) == []
    assert no_pdfium_decode == []


def test_pdfium_renders_no_page_drawing_an_image_declaring_more_pixels_than_the_bound(tmp_path, no_pdfium_decode):
    assert PDFWriter(huge_pdf(tmp_path, "flate"), engine="pdfium").render_pages_as_images(str(tmp_path / "p")) == []
    assert no_pdfium_decode == []


@pytest.mark.parametrize("native", [False, True])
def test_pdfium_does_not_ocr_a_scanned_page_drawing_an_image_declaring_more_pixels_than_the_bound(
    tmp_path, native, no_pdfium_decode
):
    document = PDFReader(huge_pdf(tmp_path, "flate", text=False), engine="pdfium", native=native).to_dicts()

    assert PIXEL_BOUND in _warnings(document)
    assert no_pdfium_decode == []


def test_an_image_declaring_exactly_the_pixel_bound_is_checked_as_usual(tmp_path):
    # 89,478,485 = 5 x 17,895,697: at the bound, not past it.
    entries = b"/Width 5 /Height 17895697 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /FlateDecode"
    path = image_page_pdf(tmp_path / "at-bound.pdf", zlib.compress(b""), entries)

    document = PDFReader(path, engine="pdfium").to_dicts()

    assert not any("pixel bound" in warning for warning in _warnings(document))


# ---------------------------------------------------------------------------
# One decode budget a document: past it, every image that needs a check is skipped.
# ---------------------------------------------------------------------------

BUDGET_SPENT = "its stream's decoded size could not be checked within the byte budget; skipped (not decoded)"


def _images_pdf(path: Path, pages: list[list[bytes]]) -> Path:
    """A PDF drawing, on each page, a line of text and one 64 x 64 gray Flate image per stream given for it."""
    from pdf_io_tests.extraction_tests._stream_fixtures import hand_written_pdf, text_content

    streams = [stream for page in pages for stream in page]
    font = 3 + 2 * len(pages)
    first_image = font + 1
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>"]
    kids = b" ".join(b"%d 0 R" % (3 + i) for i in range(len(pages)))
    objects.append(b"<< /Type /Pages /Kids [%s] /Count %d >>" % (kids, len(pages)))
    contents, number = [], first_image
    for page_index, page in enumerate(pages):
        names = b" ".join(b"/Im%d %d 0 R" % (i, number + i) for i in range(len(page)))
        draws = text_content(b" ".join(b"q 64 0 0 64 %d 100 cm /Im%d Do Q" % (70 * i, i) for i in range(len(page))))
        objects.append(
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 %d 0 R >> "
            b"/XObject << %s >> >> /Contents %d 0 R >>" % (font, names, 3 + len(pages) + page_index)
        )
        contents.append(b"<< /Length %d >>\nstream\n%s\nendstream" % (len(draws), draws))
        number += len(page)
    objects += contents
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects += [
        b"<< /Type /XObject /Subtype /Image %s/FlateDecode /Length %d >>\nstream\n%s\nendstream"
        % (GRAY_64, len(stream), stream)
        for stream in streams
    ]
    return hand_written_pdf(path, objects)


@pytest.mark.parametrize("native", [False, True])
def test_pdfium_skips_the_images_past_its_documents_decode_budget(tmp_path, monkeypatch, native, no_pdfium_render):
    from datagrunt.core.pdf_io.extraction import pdfium_document

    # Room for three 4,096-byte images and 100 bytes: the fourth is past it, and the fifth
    # (decoding to only 50 bytes) is skipped too, the budget being spent.
    monkeypatch.setattr(pdfium_document, "_DOCUMENT_DECODE_BUDGET_BYTES", 3 * 4096 + 100)
    page_1 = [zlib.compress(bytes((value,)) * 4096) for value in (1, 2, 3)]
    page_2 = [zlib.compress(bytes((4,)) * 4096), zlib.compress(bytes((5,)) * 50)]
    path = _images_pdf(tmp_path / "budget.pdf", [page_1, page_2])

    document = PDFReader(path, engine="pdfium", native=native).to_dicts(image_output_dir=str(tmp_path / "out"))

    assert len(_images(document)) == 3
    assert _warnings(document) == [f"page 2 image 0: {BUDGET_SPENT}"] * 2


def test_a_documents_decode_budget_is_two_gib():
    from datagrunt.core.pdf_io.extraction import pdfium_document

    assert pdfium_document._DOCUMENT_DECODE_BUDGET_BYTES == 2 << 30


# ---------------------------------------------------------------------------
# Peak memory, measured in a watchdog subprocess.
# ---------------------------------------------------------------------------

_RSS_BOUND_MIB = 400
_WATCHDOG_LIMIT_MIB = 1200

_SUBPROCESS = textwrap.dedent(
    """
    import os, resource, sys, tempfile, threading, time

    LIMIT = {limit} << 20

    def peak():
        if sys.platform.startswith("linux"):
            with open("/proc/self/status") as status:
                for line in status:
                    if line.startswith("VmHWM:"):
                        return int(line.split()[1]) * 1024
            raise RuntimeError("no VmHWM line in /proc/self/status")
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

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
        PDFReader(path, engine=engine).to_dicts(image_output_dir=out)
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
    assert result.returncode == 0, f"subprocess failed or was stopped: {result.stdout}\n{result.stderr[-600:]}"
    line = next((ln for ln in result.stdout.splitlines() if ln.startswith("PEAK ")), None)
    assert line is not None, f"no PEAK line: {result.stdout}\n{result.stderr[-600:]}"
    return int(line.split()[1])


def _xobject_lzw(tmp_path: Path) -> Path:
    return image_page_pdf(tmp_path / "lzw.pdf", lzw_zeros(768 * MIB), GRAY_64 + b"/LZWDecode")


def _xobject_flate(tmp_path: Path) -> Path:
    return image_page_pdf(tmp_path / "flate.pdf", flate_zeros(768 * MIB), GRAY_64 + b"/FlateDecode")


def _inline_hex_flate(tmp_path: Path) -> Path:
    content = inline_image(INLINE_HEX_FLATE, ascii_hex(flate_zeros(768 * MIB)))
    return pages_pdf(tmp_path / "inline.pdf", [content])


RSS_CASES = {
    "xobject-lzw": _xobject_lzw,
    "xobject-flate": _xobject_flate,
    "inline-hex-flate": _inline_hex_flate,
}


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("op", ["to_dicts", "render"])
@pytest.mark.parametrize("case", sorted(RSS_CASES))
def test_peak_rss_stays_bounded(tmp_path, case, engine, op):
    peak = _run_peak(RSS_CASES[case](tmp_path), engine, op)

    assert peak < _RSS_BOUND_MIB, f"{case} {engine}/{op} peaked at {peak} MiB (bound {_RSS_BOUND_MIB})"
