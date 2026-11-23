"""Tests for hybrid page unmapped-region OCR (#437, #479 Option 3): ``datagrunt.pdf._hybrid_ocr``.

Covers:
- Unit tests for target partitioning, non-AGL ligature control codes (``\\x07``, ``\\x1f``),
  region cropping with white padding, coordinate mapping back to page points, and validation.
- End-to-end pipeline / page-worker tests with real PDFs + fake tesseract:
  - hybrid scan page with unmapped invalid-char runs replaced by OCR words and original
    unmapped spans flagged ``superseded`` (never dropped);
  - non-AGL ligature control codes (``\\x07``, ``\\x1f``) in unmapped regions on a hybrid scan page;
  - lattice table cell containing an unmapped region merged before lattice/assembly;
  - ``ocr="never"`` leaving hybrid pages untouched;
  - digital pages and clean hybrid pages (``unmapped_regions == ()``) byte-for-byte untouched;
  - graceful page warnings when tesseract is unavailable, fails, times out, render fails, or
    the per-page OCR budget is spent (never dropping valid digital text);
  - bounded per-page region cap (``MAX_HYBRID_REGIONS`` / ``max_regions``);
  - sandboxed render worker execution respecting ``vision_dpi``, ``max_render_pixels``, and
    ``memory_cap_bytes``.
"""

import dataclasses
import os
import stat
from pathlib import Path

import pytest

from datagrunt.pdf import _hybrid_ocr, _page_worker
from datagrunt.pdf._hybrid_ocr import (
    MAX_HYBRID_REGIONS,
    crop_region,
    hybrid_page,
    is_valid_offset,
    map_word_to_page,
    partition_piece,
)
from datagrunt.pdf._options import VisionOptions
from datagrunt.pdf.elements import BBox, KeyValue, Page, Table
from datagrunt.pdf.ocr import OCRResult, OCRWord
from datagrunt.pdf.pdfium_engine import RawPage, extract_page, open_document
from datagrunt.pdf.pipeline import extract_document
from datagrunt.pdf.quality import SpanPiece, assess_page
from datagrunt.pdf.render import RenderedPage, RenderFailed, RenderRefused
from pdf_tests.builders import PageSpec, RasterImage, TextRun, build_pdf, hand_written_pdf, solid_png
from pdf_tests.test_ocr import posix_only, row, tsv
from pdf_tests.test_pdfium_spans import bfchar_cmap
from pdf_tests.test_pipeline import chain_page, no_tesseract, page_at
from pdf_tests.test_quality import raw_span
from pdf_tests.test_quality import row as char_row

W, H = 400.0, 300.0
SCAN_PNG = solid_png(300, 225, (0.95, 0.95, 0.95))


def _element_visible_parts(element: object) -> list[str]:
    """Return visible text fragments of one non-hidden document element."""
    if isinstance(element, KeyValue):
        return [f"{element.key}: {element.value}"]
    if isinstance(element, Table):
        return [*element.headers, *(cell for row_cells in element.rows for cell in row_cells)]
    if hasattr(element, "text"):
        return [str(element.text)]
    return []


def _visible_text(page: Page) -> str:
    """Join all visible text content on ``page`` (paragraphs, headings, key-values, tables)."""
    parts: list[str] = []
    for element in page.elements:
        if not element.is_hidden:
            parts.extend(_element_visible_parts(element))
    return " ".join(parts)


def _scan_image_xobject() -> bytes:
    """A 300 x 225 px 8-bit grayscale white XObject (>= 54 ppi on 400 x 300 pt -> scan cover 1.0)."""
    pixels = bytes([245]) * (300 * 225)
    return (
        b"<< /Type /XObject /Subtype /Image /Width 300 /Height 225 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Length %d >>\nstream\n%s\nendstream" % (len(pixels), pixels)
    )


def _hybrid_cid_pdf(
    tmp_path: Path,
    lines: tuple[tuple[bytes, float, float], ...],
    cmap_entries: dict[bytes, bytes],
    *,
    name: str = "hybrid.pdf",
    extra_ops: bytes = b"",
    with_scan: bool = True,
) -> Path:
    """Build a 400 x 300 pt PDF with a custom CID font (0.6 em advance) and optional full-bleed scan image."""
    cmap = bfchar_cmap(cmap_entries)
    draw_scan = b"q 400 0 0 300 0 0 cm /Im1 Do Q " if with_scan else b""
    shows = b" ".join(b"BT /F1 12 Tf %g %g Td <%s> Tj ET" % (x, y, hex_codes) for hex_codes, x, y in lines)
    content = draw_scan + extra_ops + shows
    cid_font = (
        b"<< /Type /Font /Subtype /CIDFontType0 /BaseFont /TestCID "
        b"/CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /DW 600 >>"
    )
    font = (
        b"<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H "
        b"/DescendantFonts [6 0 R] /ToUnicode 7 0 R >>"
    )
    cmap_stream = b"<< /Length %d >>\nstream\n%s\nendstream" % (len(cmap), cmap)
    resources = b"/XObject << /Im1 8 0 R >>" if with_scan else b""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> %s >> >>" % resources,
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        font,
        cid_font,
        cmap_stream,
    ]
    if with_scan:
        objects.append(_scan_image_xobject())
    return hand_written_pdf(tmp_path / name, objects)


def _ascii_cmap(extra_chars: str = "") -> tuple[dict[str, bytes], dict[bytes, bytes]]:
    """Map printable ASCII + ``extra_chars`` to 2-byte Identity-H codes starting at 0x0020."""
    chars = sorted({chr(c) for c in range(32, 127)} | set(extra_chars))
    to_code: dict[str, bytes] = {}
    entries: dict[bytes, bytes] = {}
    for ch in chars:
        cp = ord(ch)
        code = b"%04X" % cp
        to_code[ch] = code
        entries[code] = ch.encode("utf-16-be").hex().upper().encode()
    return to_code, entries


def _encode_with_unmapped(text: str, to_code: dict[str, bytes], unmapped: dict[str, bytes]) -> bytes:
    """Encode ``text`` to Identity-H hex, substituting ``unmapped`` symbols with unmapped CID codes."""
    return b"".join(unmapped[ch] if ch in unmapped else to_code[ch] for ch in text)


def _hybrid_pua_pdf(tmp_path: Path, *, with_scan: bool = True, name: str = "hybrid_pua.pdf") -> Path:
    """Hybrid (or digital) PDF: clean text plus one unmapped private-use word on the same page."""
    to_code, entries = _ascii_cmap("\ue001\ue002\ue003")
    line1 = _encode_with_unmapped("Quarterly report summary for region north", to_code, {})
    line2 = _encode_with_unmapped("Total: \ue001\ue002\ue003 USD", to_code, {})
    return _hybrid_cid_pdf(
        tmp_path,
        ((line1, 30.0, 240.0), (line2, 30.0, 200.0)),
        entries,
        with_scan=with_scan,
        name=name,
    )


def _hybrid_ligature_control_pdf(tmp_path: Path, *, name: str = "hybrid_lig.pdf") -> Path:
    """Hybrid scan PDF whose CID codes 0x0007 and 0x001F lack ToUnicode entries (#479 Option 3)."""
    to_code, entries = _ascii_cmap()
    unmapped = {"\x07": b"0007", "\x1f": b"001F"}
    line1 = _encode_with_unmapped("Standard engineering specification document", to_code, unmapped)
    line2 = _encode_with_unmapped("The ef\x07cient pro\x1fle passed.", to_code, unmapped)
    return _hybrid_cid_pdf(tmp_path, ((line1, 30.0, 240.0), (line2, 30.0, 200.0)), entries, name=name)


def _hybrid_lattice_pdf(tmp_path: Path, *, name: str = "hybrid_lattice.pdf") -> Path:
    """Hybrid scan PDF with a 2x2 ruled lattice table whose (1,1) cell holds an unmapped token."""
    to_code, entries = _ascii_cmap("\ue001\ue002\ue003")
    rules = (
        b"q 0 0 0 RG 1 w "
        b"40 200 m 280 200 l 40 170 m 280 170 l 40 140 m 280 140 l "
        b"40 140 m 40 200 l 160 140 m 160 200 l 280 140 m 280 200 l S Q "
    )
    lines = (
        (_encode_with_unmapped("Standard specification summary header line", to_code, {}), 40.0, 250.0),
        (_encode_with_unmapped("Metric", to_code, {}), 50.0, 180.0),
        (_encode_with_unmapped("Value", to_code, {}), 170.0, 180.0),
        (_encode_with_unmapped("Rating", to_code, {}), 50.0, 150.0),
        (_encode_with_unmapped("\ue001\ue002\ue003", to_code, {}), 170.0, 150.0),
    )
    return _hybrid_cid_pdf(tmp_path, lines, entries, extra_ops=rules, name=name)


def _hybrid_many_regions_pdf(tmp_path: Path, count: int, *, name: str = "hybrid_many.pdf") -> Path:
    """Hybrid scan PDF with ``count`` distinct unmapped regions and >= 75 % valid chars so it routes hybrid."""
    to_code, entries = _ascii_cmap("\ue001\ue002")
    headers = (
        "Header paragraph line one with plenty of valid readable digital text",
        "Header paragraph line two with plenty of valid readable digital text",
        "Header paragraph line three with plenty of valid readable digital text",
        "Header paragraph line four with plenty of valid readable digital text",
    )
    lines: list[tuple[bytes, float, float]] = [
        (_encode_with_unmapped(text, to_code, {}), 12.0, 285.0 - 14.0 * idx) for idx, text in enumerate(headers)
    ]
    for idx in range(count):
        col = idx % 5
        row_idx = idx // 5
        x = 12.0 + col * 72.0
        y = 215.0 - row_idx * 18.0
        lines.append((_encode_with_unmapped("\ue001\ue002", to_code, {}), x, y))
    return _hybrid_cid_pdf(tmp_path, tuple(lines), entries, name=name)


def _install_fake_tesseract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, responses: list[bytes]) -> Path:
    """Install a POSIX fake ``tesseract`` script that returns ``responses[i]`` on the i-th call and logs calls."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir(exist_ok=True)
    log_file = tmp_path / "tess_calls.log"
    resp_dir = tmp_path / "tess_resp"
    resp_dir.mkdir(exist_ok=True)
    for idx, payload in enumerate(responses):
        (resp_dir / f"{idx}.tsv").write_bytes(payload)
    script = bin_dir / "tesseract"
    script.write_text(
        "#!/bin/sh\n"
        f'LOG="{log_file}"\n'
        f'DIR="{resp_dir}"\n'
        'if [ "$1" = "--version" ]; then echo "tesseract 5.5.0"; exit 0; fi\n'
        "cat >/dev/null\n"
        "COUNT=0\n"
        'if [ -f "$LOG" ]; then COUNT=$(wc -l < "$LOG"); fi\n'
        'echo "$@" >> "$LOG"\n'
        'FILE="$DIR/$COUNT.tsv"\n'
        'if [ -f "$FILE" ]; then cat "$FILE"; exit 0; fi\n'
        'printf "level\\tpage_num\\tblock_num\\tpar_num\\tline_num\\tword_num'
        '\\tleft\\ttop\\twidth\\theight\\tconf\\ttext\\n"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    return log_file


def _install_failing_tesseract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, exit_code: int = 2) -> None:
    """Install a POSIX fake ``tesseract`` that exits non-zero with a stderr diagnostic."""
    bin_dir = tmp_path / "fail-bin"
    bin_dir.mkdir(exist_ok=True)
    script = bin_dir / "tesseract"
    script.write_text(
        f"#!/bin/sh\ncat >/dev/null\necho 'simulated tesseract crash' >&2\nexit {exit_code}\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")


class TestHybridUnits:
    """Pure unit tests for partitioning, cropping, coordinate mapping, and validation."""

    def test_valid_offset_checks_map_errors_and_invalid_unicode(self):
        span = raw_span("A\x07\x1f\ufffd\ue000B", char_row(6, 10.0, 20.0), map_errors=(1, 2))
        assert is_valid_offset(span, 0) is True
        assert is_valid_offset(span, 1) is False
        assert is_valid_offset(span, 2) is False
        assert is_valid_offset(span, 3) is False
        assert is_valid_offset(span, 4) is False
        assert is_valid_offset(span, 5) is True

    def test_partition_piece_keeps_clean_and_hidden_pieces_untouched(self):
        clean = SpanPiece(
            span=raw_span("Clean ASCII text", char_row(16, 10.0, 20.0)), start=0, end=16, hidden_reasons=()
        )
        hidden = SpanPiece(
            span=raw_span("\ufffd\ufffd", char_row(2, 10.0, 40.0)), start=0, end=2, hidden_reasons=("invisible",)
        )
        assert partition_piece(clean, 0, (W, H)) == ((clean,), ())
        assert partition_piece(hidden, 1, (W, H)) == ((hidden,), ())

    def test_partition_piece_splits_mixed_valid_and_invalid_tokens(self):
        span = raw_span("Total: \ufffd\ufffd\ufffd USD", char_row(14, 20.0, 50.0))
        piece = SpanPiece(span=span, start=0, end=14, hidden_reasons=())
        segments, targets = partition_piece(piece, 0, (W, H))
        assert [seg.text for seg in segments] == ["Total: ", "\ufffd\ufffd\ufffd", " USD"]
        assert len(targets) == 1
        assert (targets[0].piece_index, targets[0].start, targets[0].end) == (0, 7, 10)

    def test_partition_piece_keeps_non_agl_x07_and_x1f_glued_inside_words(self):
        span = raw_span("The ef\x07cient pro\x1fle", char_row(19, 20.0, 50.0), map_errors=(6, 16))
        piece = SpanPiece(span=span, start=0, end=19, hidden_reasons=())
        segments, targets = partition_piece(piece, 0, (W, H))
        assert [seg.text for seg in segments] == ["The ", "ef\x07cient", " ", "pro\x1fle"]
        assert [(t.start, t.end) for t in targets] == [(4, 12), (13, 19)]

    def test_crop_region_adds_white_padding_and_maps_word_back_to_page_points(self):
        pixels = bytearray([255] * (200 * 100))
        for y in range(20, 35):
            pixels[y * 200 + 50 : y * 200 + 90] = bytes(40)
        rendered = RenderedPage(pixels=bytes(pixels), px_width=200, px_height=100)
        crop = crop_region(rendered, (50.0, 20.0, 90.0, 35.0), dpi=72, max_render_pixels=262_144)
        assert (crop.crop_w, crop.crop_h) == (40, 15)
        assert crop.pad > 0
        assert crop.image.px_width == 40 + 2 * crop.pad
        assert crop.image.px_height == 15 + 2 * crop.pad
        assert crop.image.pixels[0] == 255
        center = (crop.pad + 7) * crop.image.px_width + (crop.pad + 20)
        assert crop.image.pixels[center] == 0

        local_word = OCRWord(
            text="Hello",
            bbox=BBox(x0=crop.pad + 2.0, y0=crop.pad + 1.0, x1=crop.pad + 38.0, y1=crop.pad + 14.0),
            confidence=92.5,
            line=0,
        )
        mapped = map_word_to_page(local_word, crop, dpi=72, fallback=(50.0, 20.0, 90.0, 35.0), page_size=(200.0, 100.0))
        assert mapped.bbox == BBox(x0=52.0, y0=21.0, x1=88.0, y1=34.0)
        assert mapped.confidence == 92.5

    def test_map_word_to_page_clamps_to_page_and_falls_back_on_padding_box(self):
        rendered = RenderedPage(pixels=bytes([255] * (100 * 100)), px_width=100, px_height=100)
        crop = crop_region(rendered, (80.0, 85.0, 100.0, 100.0), dpi=72, max_render_pixels=262_144)
        pad_only = OCRWord(text="X", bbox=BBox(x0=1.0, y0=1.0, x1=4.0, y1=4.0), confidence=80.0, line=0)
        mapped = map_word_to_page(pad_only, crop, dpi=72, fallback=(80.0, 85.0, 100.0, 100.0), page_size=(100.0, 100.0))
        assert mapped.bbox == BBox(x0=80.0, y0=85.0, x1=100.0, y1=100.0)
        assert mapped.bbox.within(100.0, 100.0)

    def test_hybrid_page_rejects_non_hybrid_route_and_bad_max_regions(self, tmp_path):
        path = build_pdf(
            tmp_path / "digital.pdf", [PageSpec(items=(TextRun("Clean digital page text here", 30.0, 50.0),))]
        )
        with open_document(path) as handle:
            raw = extract_page(handle, 0)
        assert isinstance(raw, RawPage)
        assessment = assess_page(raw, ocr="auto")
        assert assessment.route == "digital"
        with pytest.raises(ValueError, match="hybrid"):
            hybrid_page(raw, assessment, source=path)
        hybrid_assessment = dataclasses.replace(assessment, route="hybrid")
        with pytest.raises(ValueError, match="max_regions"):
            hybrid_page(raw, hybrid_assessment, source=path, max_regions=0)


class TestHybridEndToEnd:
    """End-to-end tests of hybrid unmapped-region OCR through ``_page_worker`` and ``extract_document``."""

    @posix_only
    def test_hybrid_unmapped_pua_run_replaced_by_ocr_and_original_superseded(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        log = _install_fake_tesseract(
            monkeypatch,
            tmp_path,
            [tsv(row(5, (18, 18, 60, 30), "94.0", "1,250"))],
        )

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        assert log.exists() and len(log.read_text(encoding="utf-8").splitlines()) == 1
        visible_text = _visible_text(page)
        assert "Quarterly report summary for region north" in visible_text
        assert "Total:" in visible_text
        assert "1,250" in visible_text
        assert "USD" in visible_text
        assert "\ue001" not in visible_text
        superseded = [el for el in page.elements if el.hidden_reasons == ("superseded",)]
        assert len(superseded) == 1
        assert superseded[0].text == "\ue001\ue002\ue003"
        assert page.quality is not None
        assert page.quality.ocr_mean_confidence == pytest.approx(94.0)
        assert page.quality.hidden_spans_flagged >= 1
        assert "\ue001" not in page.to_markdown()
        assert "\ue001\ue002\ue003" in page.to_markdown(include_hidden=True)

    @posix_only
    def test_hybrid_non_agl_ligature_control_codes_x07_x1f_replaced_and_superseded(self, tmp_path, monkeypatch):
        path = _hybrid_ligature_control_pdf(tmp_path)
        log = _install_fake_tesseract(
            monkeypatch,
            tmp_path,
            [
                tsv(row(5, (18, 18, 120, 30), "96.0", "efficient")),
                tsv(row(5, (18, 18, 100, 30), "92.0", "profile")),
            ],
        )

        doc = extract_document(path, ocr="auto")
        (page,) = doc.pages

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        assert len(log.read_text(encoding="utf-8").splitlines()) == 2
        visible_text = _visible_text(page)
        assert "Standard engineering specification document" in visible_text
        assert "efficient" in visible_text
        assert "profile" in visible_text
        assert "\x07" not in visible_text
        assert "\x1f" not in visible_text
        superseded = [el for el in page.elements if el.hidden_reasons == ("superseded",)]
        superseded_text = " ".join(el.text for el in superseded)
        assert "ef\x07cient" in superseded_text
        assert "pro\x1fle" in superseded_text
        assert page.quality is not None
        assert page.quality.ocr_mean_confidence == pytest.approx(94.0)

    @posix_only
    def test_hybrid_ocr_merges_before_lattice_table_assembly(self, tmp_path, monkeypatch):
        path = _hybrid_lattice_pdf(tmp_path)
        _install_fake_tesseract(
            monkeypatch,
            tmp_path,
            [tsv(row(5, (18, 18, 80, 28), "91.0", "A-Plus"))],
        )

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        tables = [el for el in page.elements if isinstance(el, Table)]
        assert len(tables) == 1
        assert tables[0].extraction_method == "lattice"
        assert tables[0].headers == ("Metric", "Value")
        assert tables[0].rows == (("Rating", "A-Plus"),)
        superseded = [el for el in page.elements if el.hidden_reasons == ("superseded",)]
        assert [el.text for el in superseded] == ["\ue001\ue002\ue003"]

    def test_ocr_never_leaves_hybrid_page_completely_untouched(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        called: list[object] = []
        monkeypatch.setattr(_page_worker, "hybrid_page", lambda *a, **kw: called.append(a))

        page = page_at(path, ocr="never")

        assert called == []
        assert page.route == "digital"
        assert [el for el in page.elements if el.hidden_reasons == ("superseded",)] == []
        assert "\ue001\ue002\ue003" in _visible_text(page)

    def test_digital_page_and_clean_hybrid_page_are_byte_for_byte_untouched(self, tmp_path, monkeypatch):
        digital_with_pua = _hybrid_pua_pdf(tmp_path, with_scan=False, name="digital_pua.pdf")
        clean_hybrid = build_pdf(
            tmp_path / "clean_hybrid.pdf",
            [
                PageSpec(
                    width=W,
                    height=H,
                    items=(
                        RasterImage(0.0, 0.0, W, H, SCAN_PNG),
                        TextRun("Clean searchable scan text with zero unmapped chars", 30.0, 60.0, render_mode=3),
                    ),
                )
            ],
        )
        called: list[object] = []
        monkeypatch.setattr(_page_worker, "hybrid_page", lambda *a, **kw: called.append(a))

        got_digital = page_at(digital_with_pua, ocr="auto")
        got_clean_hybrid = page_at(clean_hybrid, ocr="auto")

        assert called == []
        assert got_digital == chain_page(digital_with_pua, ocr="auto")
        assert got_clean_hybrid == chain_page(clean_hybrid, ocr="auto")
        assert got_digital.route == "digital"
        assert got_clean_hybrid.route == "hybrid"

    def test_graceful_warning_when_tesseract_is_unavailable(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        no_tesseract(monkeypatch, tmp_path)

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        visible_text = _visible_text(page)
        assert "Quarterly report summary for region north" in visible_text
        assert "Total:" in visible_text
        assert "\ue001\ue002\ue003" in visible_text
        assert [el for el in page.elements if el.hidden_reasons == ("superseded",)] == []
        assert any("tesseract" in w and "unavailable" in w for w in page.warnings)

    @posix_only
    def test_graceful_warning_when_tesseract_fails(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        _install_failing_tesseract(monkeypatch, tmp_path, exit_code=2)

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        assert "Quarterly report summary for region north" in _visible_text(page)
        assert [el for el in page.elements if el.hidden_reasons == ("superseded",)] == []
        assert any("hybrid OCR" in w and "failed" in w for w in page.warnings)

    def test_graceful_warning_when_render_is_refused_or_fails(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        monkeypatch.setattr(
            _hybrid_ocr, "render_page", lambda *a, **kw: (_ for _ in ()).throw(RenderRefused("too big"))
        )

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        assert "Quarterly report summary for region north" in _visible_text(page)
        assert any("not rendered" in w and "too big" in w for w in page.warnings)

        monkeypatch.setattr(_hybrid_ocr, "render_page", lambda *a, **kw: (_ for _ in ()).throw(RenderFailed("boom")))
        page2 = page_at(path, ocr="auto")
        assert (page2.route, page2.status) == ("hybrid", "ok")
        assert any("not rendered" in w and "boom" in w for w in page2.warnings)

    def test_graceful_warning_when_per_page_ocr_budget_is_spent(self, tmp_path, monkeypatch):
        path = _hybrid_ligature_control_pdf(tmp_path)
        calls: list[int] = []

        def fake_ocr(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                word = OCRWord(text="efficient", bbox=BBox(x0=18.0, y0=18.0, x1=90.0, y1=32.0), confidence=90.0, line=0)
                return OCRResult(words=(word,), status="ok", reason="")
            return OCRResult(words=(), status="spent", reason="OCR stopped after 0 of 1 bands")

        monkeypatch.setattr(_hybrid_ocr, "ocr_words", fake_ocr)
        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        visible_text = _visible_text(page)
        assert "efficient" in visible_text
        assert "pro\x1fle" in visible_text
        assert any("budget" in w for w in page.warnings)

    @posix_only
    def test_bounded_per_page_region_cap(self, tmp_path, monkeypatch):
        total_regions = MAX_HYBRID_REGIONS + 6
        path = _hybrid_many_regions_pdf(tmp_path, total_regions)
        responses = [tsv(row(5, (18, 18, 30, 24), "95.0", f"W{i}")) for i in range(total_regions)]
        log = _install_fake_tesseract(monkeypatch, tmp_path, responses)

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        assert len(log.read_text(encoding="utf-8").splitlines()) == MAX_HYBRID_REGIONS
        assert any(str(MAX_HYBRID_REGIONS) in w and str(total_regions) in w for w in page.warnings)
        superseded = [el for el in page.elements if el.hidden_reasons == ("superseded",)]
        assert len(superseded) >= 1
        assert sum(el.text.count("\ue001\ue002") for el in superseded) == MAX_HYBRID_REGIONS
        visible_text = _visible_text(page)
        assert "W0" in visible_text
        assert f"W{MAX_HYBRID_REGIONS - 1}" in visible_text
        assert f"W{MAX_HYBRID_REGIONS}" not in visible_text
        assert "\ue001\ue002" in visible_text

    @posix_only
    def test_sandbox_mode_renders_in_capped_render_worker(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        _install_fake_tesseract(
            monkeypatch,
            tmp_path,
            [tsv(row(5, (18, 18, 60, 30), "93.0", "9,999"))],
        )
        render_calls: list[dict[str, object]] = []
        real_render = _hybrid_ocr.render_page

        def spy_render(*args, **kwargs):
            render_calls.append(kwargs)
            return real_render(*args, **kwargs)

        monkeypatch.setattr(_hybrid_ocr, "render_page", spy_render)
        page = page_at(path, ocr="auto", vision_dpi=250, max_render_pixels=2_000_000, sandbox=True)

        assert len(render_calls) == 1
        assert render_calls[0]["dpi"] == 250
        assert render_calls[0]["memory_cap_bytes"] == VisionOptions().memory_cap_bytes
        assert "9,999" in _visible_text(page)
        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)

    def test_empty_ocr_result_preserves_original_span_unsuperseded(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        monkeypatch.setattr(_hybrid_ocr, "ocr_words", lambda *a, **kw: OCRResult(words=(), status="ok", reason=""))

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        assert "\ue001\ue002\ue003" in _visible_text(page)
        assert [el for el in page.elements if el.hidden_reasons == ("superseded",)] == []

    def test_multi_word_ocr_replacement_and_standalone_unmapped_region(self, tmp_path, monkeypatch):
        path = _hybrid_pua_pdf(tmp_path)
        words = (
            OCRWord(text="One", bbox=BBox(x0=18.0, y0=18.0, x1=32.0, y1=30.0), confidence=90.0, line=0),
            OCRWord(text="Thousand", bbox=BBox(x0=36.0, y0=18.0, x1=72.0, y1=30.0), confidence=86.0, line=0),
        )
        monkeypatch.setattr(_hybrid_ocr, "ocr_words", lambda *a, **kw: OCRResult(words=words, status="ok", reason=""))

        page = page_at(path, ocr="auto")

        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        visible = _visible_text(page)
        assert "One" in visible and "Thousand" in visible
        assert page.quality is not None and page.quality.ocr_mean_confidence == pytest.approx(88.0)

    def test_hybrid_page_with_empty_unmapped_regions_skips_render_and_ocr(self, tmp_path, monkeypatch):
        path = build_pdf(
            tmp_path / "clean_hybrid2.pdf",
            [
                PageSpec(
                    width=W,
                    height=H,
                    items=(
                        RasterImage(0.0, 0.0, W, H, SCAN_PNG),
                        TextRun("Clean searchable scan text with zero unmapped chars", 30.0, 60.0, render_mode=3),
                    ),
                )
            ],
        )
        with open_document(path) as handle:
            raw = extract_page(handle, 0)
        assert isinstance(raw, RawPage)
        assessment = assess_page(raw, ocr="auto")
        assert assessment.route == "hybrid" and assessment.unmapped_regions == ()
        monkeypatch.setattr(_hybrid_ocr, "_draw_hybrid_page", lambda *a, **kw: (_ for _ in ()).throw(AssertionError()))

        page = hybrid_page(raw, assessment, source=path)
        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)

    def test_synthetic_standalone_unmapped_region_adds_ocr_word(self, tmp_path, monkeypatch):
        path = build_pdf(
            tmp_path / "standalone.pdf",
            [
                PageSpec(
                    width=W,
                    height=H,
                    items=(
                        RasterImage(0.0, 0.0, W, H, SCAN_PNG),
                        TextRun("Clean searchable scan text with zero unmapped chars", 30.0, 60.0, render_mode=3),
                    ),
                )
            ],
        )
        with open_document(path) as handle:
            raw = extract_page(handle, 0)
        assert isinstance(raw, RawPage)
        assessment = dataclasses.replace(assess_page(raw, ocr="auto"), unmapped_regions=((30.0, 120.0, 120.0, 134.0),))
        word = OCRWord(text="Stamp", bbox=BBox(x0=18.0, y0=18.0, x1=60.0, y1=30.0), confidence=97.0, line=0)
        monkeypatch.setattr(_hybrid_ocr, "ocr_words", lambda *a, **kw: OCRResult(words=(word,), status="ok", reason=""))

        page = hybrid_page(raw, assessment, source=path)
        assert (page.route, page.status, page.integrity_verified) == ("hybrid", "ok", True)
        assert "Stamp" in _visible_text(page)
        assert page.quality is not None and page.quality.ocr_mean_confidence == pytest.approx(97.0)

    def test_validation_guards_on_unit_helpers(self, tmp_path):
        span = raw_span("Ab", char_row(2, 10.0, 20.0))
        with pytest.raises(TypeError, match="RawSpan"):
            is_valid_offset("not-a-span", 0)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="out of range"):
            is_valid_offset(span, 2)
        with pytest.raises(TypeError, match="SpanPiece"):
            partition_piece("bad", 0, (W, H))  # type: ignore[arg-type]
        bgr = RenderedPage(pixels=bytes(18), px_width=3, px_height=2, channels=3)
        with pytest.raises(ValueError, match="grayscale"):
            crop_region(bgr, (0.0, 0.0, 1.0, 1.0), dpi=72, max_render_pixels=262_144)
        gray = RenderedPage(pixels=bytes(16), px_width=4, px_height=4, channels=1)
        crop = crop_region(gray, (0.0, 0.0, 4.0, 4.0), dpi=72, max_render_pixels=16)
        assert crop.pad == 0
        with pytest.raises(TypeError, match="OCRWord"):
            map_word_to_page(
                "bad",  # type: ignore[arg-type]
                crop,
                dpi=72,
                fallback=(0.0, 0.0, 4.0, 4.0),
                page_size=(4.0, 4.0),
            )
