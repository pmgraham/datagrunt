"""Scan fixtures for the v5 vision route (#369, A5a): orientation, OCR and bands on scanned pages.

A text page in Helvetica (a standard font, which pdfium draws from its own
bundled glyphs, the same on every platform) is rendered with
``render_page`` and re-embedded as a scanned image (``pdf_tests.builders``,
in ``tmp_path``): upright and at /Rotate 90, 180 and 270, and once with an
invisible text layer over it (a searchable scan). A 36" x 48" sheet is
built too. A word's known position is pdfium's char boxes on the digital
page, turned as the scan's /Rotate turns it: derived from the fixture,
never from one platform's font metrics.
"""

import io
from pathlib import Path

import pypdfium2 as pdfium
import pytest
from PIL import Image

from datagrunt.pdf.ocr import bands, normalized, ocr_words, orientation, rotated
from datagrunt.pdf.render import RenderedPage, effective_dpi, render_page, render_size
from pdf_tests.builders import PageSpec, RasterImage, TextRun, build_pdf
from pdf_tests.test_ocr import needs_tesseract

VISION_DPI = 300
MAX_RENDER_PIXELS = 16_000_000
CAP = 1 << 30
TIMEOUT_S = 120.0
LETTER = (612.0, 792.0)
SHEET = (36 * 72.0, 48 * 72.0)  # 36" x 48", in points

SENTENCES = (
    "The quick brown fox jumps over the lazy dog near the river bank.",
    "Pack my box with five dozen liquor jugs before the morning train.",
    "How vexingly quick daft zebras jump when the music starts to play.",
    "Sphinx of black quartz, judge my vow and read the whole sentence.",
)
LINES = tuple(TextRun(sentence, 72.0, 120.0 + index * 24.0, size=14.0) for index, sentence in enumerate(SENTENCES))
TOKENS = [token for sentence in SENTENCES for token in sentence.split()]

Box = tuple[float, float, float, float]


def rendered(path: Path, dpi: int = VISION_DPI) -> RenderedPage:
    return render_page(path, 0, dpi=dpi, memory_cap_bytes=CAP, timeout_s=TIMEOUT_S)


def png_of(page: RenderedPage) -> bytes:
    buffer = io.BytesIO()
    Image.frombytes("L", (page.px_width, page.px_height), page.pixels).save(buffer, format="PNG")
    return buffer.getvalue()


def word_boxes(path: Path) -> list[tuple[str, Box]]:
    """Each whitespace-separated word of page 1's text with the union of its chars' boxes, in top-left points."""
    document = pdfium.PdfDocument(str(path))
    try:
        page = document[0]
        text_page = page.get_textpage()
        height = page.get_height()
        words: list[tuple[str, Box]] = []
        text, box = "", None
        for index in range(text_page.count_chars()):
            char = text_page.get_text_range(index, 1)
            if not char or char.isspace():
                if text:
                    words.append((text, box))
                text, box = "", None
                continue
            left, bottom, right, top = text_page.get_charbox(index)
            char_box = (left, height - top, right, height - bottom)
            box = char_box if box is None else (*map(min, box[:2], char_box[:2]), *map(max, box[2:], char_box[2:]))
            text += char
        if text:
            words.append((text, box))
        return words
    finally:
        document.close()


def turned(box: Box, rotate: int, width: float, height: float) -> Box:
    """``box`` on an upright ``width`` x ``height`` page, on the page displayed turned clockwise by ``rotate``."""
    x0, y0, x1, y1 = box
    if rotate == 90:
        return (height - y1, x0, height - y0, x1)
    if rotate == 180:
        return (width - x1, height - y1, width - x0, height - y0)
    if rotate == 270:
        return (y0, width - x1, y1, width - x0)
    return box


def centre(box: Box) -> tuple[float, float]:
    return ((box[0] + box[2]) / 2, (box[1] + box[3]) / 2)


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> dict[str, Path]:
    """The digital page, its scans at /Rotate 0, 90, 180 and 270, and the searchable scan."""
    directory = tmp_path_factory.mktemp("scans")
    digital = build_pdf(directory / "digital.pdf", [PageSpec(items=LINES)])
    scan = RasterImage(0.0, 0.0, *LETTER, png_of(rendered(digital)))
    paths = {"digital": digital}
    for rotate in (0, 90, 180, 270):
        paths[f"scan-{rotate}"] = build_pdf(directory / f"scan-{rotate}.pdf", [PageSpec(items=(scan,), rotate=rotate)])
    invisible = tuple(TextRun(line.text, line.x, line.y, size=line.size, render_mode=3) for line in LINES)
    paths["searchable"] = build_pdf(directory / "searchable.pdf", [PageSpec(items=(scan, *invisible))])
    return paths


@pytest.fixture(scope="module")
def read(built):
    """read(name): the scan rendered, normalized, oriented, righted, banded and OCR'd (once a scan)."""
    results: dict[str, tuple] = {}

    def reading(name: str):
        if name not in results:
            page = normalized(rendered(built[name]))
            found = orientation(page, timeout_s=TIMEOUT_S)
            size = (LETTER[1], LETTER[0]) if page.px_width > page.px_height else LETTER
            words = ocr_words(
                bands(rotated(page, found.degrees)),
                dpi=VISION_DPI,
                rotation=found.degrees,
                page_size_pt=size,
                timeout_s=TIMEOUT_S,
            )
            results[name] = (found, words)
        return results[name]

    return reading


def assert_words_read_where_drawn(words, truth: list[tuple[str, Box]], rotate: int) -> None:
    """At least 90 % of the known words are read, and every word known and read once is within 3 pt of its place."""
    read_texts = [word.text for word in words.words]
    assert len(set(TOKENS) & set(read_texts)) >= 0.9 * len(set(TOKENS))
    once = [(text, box) for text, box in truth if TOKENS.count(text) == 1 and read_texts.count(text) == 1]
    assert len(once) >= 0.9 * len({token for token in TOKENS if TOKENS.count(token) == 1})
    for text, box in once:
        (word,) = [word for word in words.words if word.text == text]
        expected = centre(turned(box, rotate, *LETTER))
        found = centre((word.bbox.x0, word.bbox.y0, word.bbox.x1, word.bbox.y1))
        assert abs(found[0] - expected[0]) <= 3.0 and abs(found[1] - expected[1]) <= 3.0, (text, found, expected)


class TestFixtures:
    """The fixtures are what they claim: scans whose text is only pixels, turned by /Rotate, and a searchable one."""

    def test_the_digital_page_holds_the_sentences(self, built):
        assert [text for text, _box in word_boxes(built["digital"])] == TOKENS

    @pytest.mark.parametrize("rotate", [0, 90, 180, 270])
    def test_a_scan_has_no_text_and_displays_turned(self, built, rotate):
        page = rendered(built[f"scan-{rotate}"], dpi=72)

        assert word_boxes(built[f"scan-{rotate}"]) == []
        assert (page.px_width, page.px_height) == ((792, 612) if rotate in (90, 270) else (612, 792))

    def test_the_searchable_scan_has_the_text_invisibly(self, built):
        # Its text layer is the digital page's, and draws nothing: it renders as the plain scan does.
        assert word_boxes(built["searchable"]) == word_boxes(built["digital"])
        assert rendered(built["searchable"]) == rendered(built["scan-0"])


@needs_tesseract
class TestScans:
    """OSD rights each scan; OCR reads its sentences where the digital page drew them."""

    @pytest.mark.parametrize("rotate", [0, 90, 180, 270])
    def test_osd_finds_each_rotation(self, read, rotate):
        found, _words = read(f"scan-{rotate}")

        assert (found.status, found.degrees) == ("ok", (360 - rotate) % 360)

    @pytest.mark.parametrize("rotate", [0, 90, 180, 270])
    def test_ocr_reads_the_sentences_where_they_were_drawn(self, built, read, rotate):
        _found, words = read(f"scan-{rotate}")

        assert words.status == "ok"
        assert_words_read_where_drawn(words, word_boxes(built["digital"]), rotate)

    def test_ocr_reads_a_searchable_scan_where_its_text_layer_is(self, built, read):
        _found, words = read("searchable")

        assert words.status == "ok"
        assert_words_read_where_drawn(words, word_boxes(built["searchable"]), 0)


class TestLargeSheet:
    """A 36" x 48" sheet renders within max_render_pixels and bands within it."""

    def test_the_sheet_bands_within_the_pixel_bound(self, tmp_path):
        text = tuple(
            TextRun(sentence, 144.0, 288.0 + index * 72.0, size=36.0) for index, sentence in enumerate(SENTENCES)
        )
        path = build_pdf(tmp_path / "sheet.pdf", [PageSpec(width=SHEET[0], height=SHEET[1], items=text)])
        dpi = effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, *SHEET)

        page = normalized(rendered(path, dpi=dpi))
        sheet_bands = bands(page)

        assert dpi == 96
        assert (page.px_width, page.px_height) == render_size(*SHEET, dpi) == (3456, 4608)
        assert page.px_width * page.px_height <= MAX_RENDER_PIXELS
        assert [band.top_px for band in sheet_bands] == [0, 840, 1680, 2520, 3360, 4200]
        assert all(band.px_height <= 900 and len(band.pixels) <= 900 * page.px_width for band in sheet_bands)
        rows = [0] * page.px_height
        for band in sheet_bands:
            for row in range(band.top_px, band.top_px + band.px_height):
                rows[row] += 1
        assert set(rows) == {1, 2}
        assert sum(len(band.pixels) for band in sheet_bands) <= 2 * MAX_RENDER_PIXELS
