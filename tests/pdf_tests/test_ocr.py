"""Tests for the v5 vision route's page normalization, bands and tesseract CLI (#369, A5a): ``datagrunt.pdf.ocr``.

Pages here are built pixel by pixel, or drawn with Pillow's bundled font,
so every expected value is derived in the test, never pinned from one
platform's fonts or tesseract build. A fake ``tesseract`` (a POSIX shell
script put first on PATH) stands in for the CLI where a test needs to
control what it does; tests that need the real one skip when it is absent
(CI installs ``tesseract-ocr``).
"""

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
from PIL import Image, ImageDraw, ImageFont, ImageOps

from datagrunt.pdf import ocr
from datagrunt.pdf._executables import trusted_executable
from datagrunt.pdf.elements import BBox
from datagrunt.pdf.ocr import (
    Band,
    OCRResult,
    OCRWord,
    Orientation,
    bands,
    normalized,
    ocr_words,
    orientation,
    rotated,
    tesseract_available,
)
from datagrunt.pdf.render import RenderedPage

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="the fake tesseract is a POSIX shell script")
needs_tesseract = pytest.mark.skipif(
    shutil.which("tesseract") is None, reason="tesseract is not installed (CI installs tesseract-ocr)"
)


def bbox(x0: float, y0: float, x1: float, y1: float) -> BBox:
    return BBox(x0=x0, y0=y0, x1=x1, y1=y1)


def page_of(rows: list[list[int]]) -> RenderedPage:
    """A rendered page whose pixels are ``rows`` (each a list of 0..255 grey levels)."""
    return RenderedPage(
        pixels=bytes(value for row in rows for value in row), px_width=len(rows[0]), px_height=len(rows)
    )


class TestNormalized:
    """normalized() is Pillow's 1 % autocontrast over the page's grey levels."""

    def test_the_1_percent_cutoff_ignores_one_pixel_in_a_hundred_at_each_end(self):
        # 100 pixels: the 1 % cutoff drops the darkest one (0) and the lightest one (255),
        # so 50..210 stretches to 0..255. A 0 % cutoff would keep 0..255 (no change);
        # a 2 % cutoff would also drop 50 and 210, stretching 100..200 instead.
        levels = [0, 50] + [100] * 48 + [200] * 48 + [210, 255]
        page = page_of([levels[row * 10 : row * 10 + 10] for row in range(10)])

        result = normalized(page)

        stretched = {0: 0, 50: 0, 100: 79, 200: 239, 210: 255, 255: 255}  # int((v - 50) * 255 / 160), clamped
        assert result.pixels == bytes(stretched[value] for value in levels)
        assert (result.px_width, result.px_height) == (10, 10)

    def test_a_flat_page_comes_back_unchanged(self):
        page = page_of([[128] * 7 for _ in range(3)])

        assert normalized(page) == page

    def test_a_page_already_spanning_black_to_white_within_the_cutoff_is_unchanged(self):
        # 200 pixels, half black and half white: nothing to stretch.
        page = page_of([[0] * 20 for _ in range(5)] + [[255] * 20 for _ in range(5)])

        assert normalized(page) == page

    def test_rows_stay_in_place(self):
        # A dark left column and a light right one: each pixel maps where it was.
        # 96..224 spans 128 levels, so the stretch (255 / 128) is exact in binary.
        page = page_of([[96, 224]] * 50)

        assert normalized(page).pixels == bytes([0, 255] * 50)

    def test_a_non_page_is_refused(self):
        with pytest.raises(TypeError, match=re.escape("page must be a RenderedPage, got bytes")):
            normalized(b"\x00")

    def test_a_bgr_page_is_refused(self):
        # OCR reads 8-bit gray: a 24-bit BGR render (#400's background check) is never read as gray.
        page = RenderedPage(pixels=bytes(18), px_width=3, px_height=2, channels=3)

        for call in (normalized, bands, lambda bgr: rotated(bgr, 90), lambda bgr: orientation(bgr, timeout_s=1.0)):
            with pytest.raises(ValueError, match=re.escape("page must be 8-bit gray (1 channel), got 3 channels")):
                call(page)


def striped(width: int, height: int) -> RenderedPage:
    """A ``width`` x ``height`` page whose every row has its own grey level (row index mod 256)."""
    return RenderedPage(
        pixels=bytes(row % 256 for row in range(height) for _ in range(width)), px_width=width, px_height=height
    )


def rows_of(band: Band) -> range:
    return range(band.top_px, band.top_px + band.px_height)


class TestBand:
    """Band is a validated, frozen record of a horizontal strip of a page."""

    def test_valid(self):
        band = Band(top_px=840, pixels=bytes(6), px_width=3, px_height=2)

        assert (band.top_px, band.px_width, band.px_height, len(band.pixels)) == (840, 3, 2, 6)
        assert "pixels" not in repr(band)

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"pixels": bytearray(6)}, TypeError, "pixels must be bytes, got bytearray", id="bytearray"),
            pytest.param({"pixels": bytes(7)}, ValueError, "pixels must hold 3 x 2 = 6 bytes, got 7", id="length"),
            pytest.param({"top_px": -1}, ValueError, "top_px must be >= 0, got -1", id="top"),
            pytest.param({"top_px": True}, TypeError, "top_px must be an int", id="top-bool"),
            pytest.param({"px_width": 0, "pixels": b""}, ValueError, "px_width must be >= 1, got 0", id="width"),
            pytest.param({"px_height": 2.0}, TypeError, "px_height must be an int", id="height"),
        ],
    )
    def test_invalid_fields_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            Band(**{"top_px": 0, "pixels": bytes(6), "px_width": 3, "px_height": 2, **fields})


class TestBands:
    """bands() cuts a page into 900 px bands stepping by 840 px (60 px overlap), the last one ending at the bottom."""

    @pytest.mark.parametrize("height", [1, 59, 60, 899, 900])
    def test_a_page_up_to_900_px_tall_is_one_band(self, height):
        page = striped(3, height)

        (band,) = bands(page)

        assert band == Band(top_px=0, pixels=page.pixels, px_width=3, px_height=height)

    @pytest.mark.parametrize(
        "height,expected",
        [
            pytest.param(901, [(0, 900), (840, 61)], id="901"),
            pytest.param(1740, [(0, 900), (840, 900)], id="1740-two-full-bands"),
            pytest.param(1741, [(0, 900), (840, 900), (1680, 61)], id="1741"),
            pytest.param(1760, [(0, 900), (840, 900), (1680, 80)], id="1760"),
            pytest.param(3300, [(0, 900), (840, 900), (1680, 900), (2520, 780)], id="letter-at-300-dpi"),
        ],
    )
    def test_bands_step_by_840_px_and_the_last_ends_at_the_bottom(self, height, expected):
        result = bands(striped(2, height))

        assert [(band.top_px, band.px_height) for band in result] == expected

    def test_each_band_is_its_rows_of_the_page(self):
        page = striped(5, 2000)

        for band in bands(page):
            start, end = band.top_px * 5, (band.top_px + band.px_height) * 5
            assert band.pixels == page.pixels[start:end]
            assert band.px_width == 5

    @pytest.mark.parametrize(
        "band_height,overlap", [(900, 60), (10, 1), (10, 5), (7, 3), (2, 1)], ids=["spec", "10-1", "10-5", "7-3", "2-1"]
    )
    def test_every_row_is_in_one_or_two_bands_and_neighbours_overlap_by_the_overlap(self, band_height, overlap):
        for height in range(1, 4 * band_height + 3):
            result = bands(striped(1, height), height=band_height, overlap=overlap)
            seen = [0] * height
            for band in result:
                for row in rows_of(band):
                    seen[row] += 1
            assert set(seen) <= {1, 2}, (height, seen)
            assert result[-1].top_px + result[-1].px_height == height
            assert all(band.px_height == band_height for band in result[:-1])
            for upper, lower in zip(result, result[1:]):
                assert lower.top_px - upper.top_px == band_height - overlap
                assert upper.top_px + upper.px_height - lower.top_px == overlap
            assert len(result) <= height

    @pytest.mark.parametrize(
        "options,error,message",
        [
            pytest.param({"height": 0}, ValueError, "height must be >= 1, got 0", id="height"),
            pytest.param({"overlap": 0}, ValueError, "overlap must be >= 1, got 0", id="overlap"),
            pytest.param({"height": 900.0}, TypeError, "height must be an int", id="height-float"),
            pytest.param({"overlap": True}, TypeError, "overlap must be an int", id="overlap-bool"),
            pytest.param(
                {"height": 100, "overlap": 100},
                ValueError,
                "overlap (100) must be at most half of height (100)",
                id="eq",
            ),
            pytest.param(
                {"height": 100, "overlap": 51},
                ValueError,
                "overlap (51) must be at most half of height (100)",
                id="half",
            ),
        ],
    )
    def test_invalid_arguments_raise(self, options, error, message):
        with pytest.raises(error, match=re.escape(message)):
            bands(striped(1, 10), **options)

    def test_half_the_height_is_the_largest_overlap(self):
        result = bands(striped(1, 300), height=100, overlap=50)

        assert [band.top_px for band in result] == [0, 50, 100, 150, 200]

    def test_a_non_page_is_refused(self):
        with pytest.raises(TypeError, match=re.escape("page must be a RenderedPage, got str")):
            bands("page")


class TestRotated:
    """rotated() turns a page clockwise by a quarter turn or more (Pillow's transpose)."""

    PAGE = page_of([[1, 2, 3], [4, 5, 6]])

    @pytest.mark.parametrize(
        "degrees,rows",
        [
            pytest.param(0, [[1, 2, 3], [4, 5, 6]], id="0"),
            pytest.param(90, [[4, 1], [5, 2], [6, 3]], id="90"),
            pytest.param(180, [[6, 5, 4], [3, 2, 1]], id="180"),
            pytest.param(270, [[3, 6], [2, 5], [1, 4]], id="270"),
        ],
    )
    def test_the_page_turns_clockwise(self, degrees, rows):
        assert rotated(self.PAGE, degrees) == page_of(rows)

    @pytest.mark.parametrize(
        "degrees,error,message",
        [
            pytest.param(45, ValueError, "degrees must be 0, 90, 180 or 270, got 45", id="45"),
            pytest.param(-90, ValueError, "degrees must be 0, 90, 180 or 270, got -90", id="negative"),
            pytest.param(True, TypeError, "degrees must be an int, got True", id="bool"),
            pytest.param(90.0, TypeError, "degrees must be an int, got 90.0", id="float"),
        ],
    )
    def test_other_turns_are_refused(self, degrees, error, message):
        with pytest.raises(error, match=re.escape(message)):
            rotated(self.PAGE, degrees)

    def test_a_non_page_is_refused(self):
        with pytest.raises(TypeError, match=re.escape("page must be a RenderedPage, got NoneType")):
            rotated(None, 90)


class TestRecords:
    """Orientation, OCRWord and OCRResult are validated, frozen records."""

    def test_valid_orientations(self):
        assert Orientation(degrees=90, confidence=9.12, status="ok", reason="").confidence == 9.12
        assert Orientation(degrees=0, confidence=None, status="undetermined", reason="too few").degrees == 0

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"degrees": 45}, ValueError, "degrees must be 0, 90, 180 or 270, got 45", id="degrees"),
            pytest.param({"confidence": float("nan")}, ValueError, "confidence must be finite", id="nan"),
            pytest.param({"status": "fine"}, ValueError, "status must be 'ok' or 'undetermined'", id="status"),
            pytest.param({"confidence": None}, ValueError, "an ok orientation has a confidence", id="ok-none"),
            pytest.param({"reason": "why"}, ValueError, "an ok orientation has no reason, got 'why'", id="ok-reason"),
            pytest.param(
                {"status": "failed", "degrees": 0, "reason": "x"},
                ValueError,
                "an orientation that is not ok has no confidence, got 9.0",
                id="failed-confidence",
            ),
            pytest.param(
                {"status": "failed", "confidence": None, "reason": "x"},
                ValueError,
                "an orientation that is not ok is 0 degrees, got 90",
                id="failed-degrees",
            ),
            pytest.param(
                {"status": "timeout", "degrees": 0, "confidence": None},
                ValueError,
                "reason must be a non-blank str",
                id="failed-reason",
            ),
        ],
    )
    def test_invalid_orientations_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            Orientation(**{"degrees": 90, "confidence": 9.0, "status": "ok", "reason": "", **fields})

    def test_a_valid_word(self):
        word = OCRWord(text="fox", bbox=bbox(1.0, 2.0, 3.0, 4.0), confidence=96.05, line=3)

        assert (word.text, word.bbox.x1, word.confidence, word.line) == ("fox", 3.0, 96.05, 3)

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"text": " "}, ValueError, "text must be a non-blank str", id="blank"),
            pytest.param({"bbox": (1.0, 2.0, 3.0, 4.0)}, TypeError, "bbox must be a BBox, got tuple", id="bbox"),
            pytest.param({"confidence": 100.5}, ValueError, "confidence must be <= 100.0, got 100.5", id="high"),
            pytest.param({"confidence": -1.0}, ValueError, "confidence must be >= 0.0, got -1.0", id="low"),
            pytest.param({"line": -1}, ValueError, "line must be >= 0, got -1", id="line-negative"),
            pytest.param({"line": 1.0}, TypeError, "line must be an int", id="line-float"),
            pytest.param({"line": True}, TypeError, "line must be an int", id="line-bool"),
        ],
    )
    def test_invalid_words_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            OCRWord(**{"text": "fox", "bbox": bbox(1.0, 2.0, 3.0, 4.0), "confidence": 50.0, "line": 0, **fields})

    def test_valid_results(self):
        word = OCRWord(text="fox", bbox=bbox(1.0, 2.0, 3.0, 4.0), confidence=96.0, line=0)

        assert OCRResult(words=(word,), status="ok", reason="").words == (word,)
        assert OCRResult(words=(word,), status="timeout", reason="band 2 of 2").status == "timeout"

    @pytest.mark.parametrize(
        "fields,error,message",
        [
            pytest.param({"words": []}, TypeError, "words must be a tuple, got list", id="list"),
            pytest.param({"words": ("fox",)}, TypeError, "words[0] must be a OCRWord, got str", id="word"),
            pytest.param({"status": "undetermined"}, ValueError, "status must be 'ok' or 'unavailable'", id="status"),
            pytest.param({"reason": "why"}, ValueError, "an ok result has no reason, got 'why'", id="ok-reason"),
            pytest.param({"status": "failed"}, ValueError, "reason must be a non-blank str", id="failed-reason"),
        ],
    )
    def test_invalid_results_raise(self, fields, error, message):
        with pytest.raises(error, match=re.escape(message)):
            OCRResult(**{"words": (), "status": "ok", "reason": "", **fields})


def osd(rotate: str, confidence: str = "9.12", degrees: str = "270") -> bytes:
    """tesseract's --psm 0 report (5.x and 4.x print the same lines)."""
    lines = (
        "Page number: 0",
        f"Orientation in degrees: {degrees}",
        f"Rotate: {rotate}",
        f"Orientation confidence: {confidence}",
        "Script: Latin",
        "Script confidence: 13.10",
    )
    return ("\n".join(lines) + "\n").encode()


class TestOsdReport:
    """The OSD report's Rotate (the clockwise turn that rights the page) and its confidence are parsed strictly."""

    @pytest.mark.parametrize("rotate", [0, 90, 180, 270])
    def test_rotate_is_read_not_orientation_in_degrees(self, rotate):
        report = osd(str(rotate), degrees=str((rotate + 90) % 360))  # a decoy that never equals Rotate

        assert ocr._osd(report) == Orientation(degrees=rotate, confidence=9.12, status="ok", reason="")

    @pytest.mark.parametrize(
        "report,message",
        [
            pytest.param(b"", "tesseract's OSD report has no Rotate line: b''", id="empty"),
            pytest.param(osd("90").replace(b"Rotate: 90\n", b""), "has no Rotate line", id="no-rotate"),
            pytest.param(
                osd("90", "9.12").replace(b"Orientation confidence", b"Confidence"),
                "no Orientation confidence line",
                id="no-confidence",
            ),
            pytest.param(osd("45"), "tesseract's OSD gave Rotate: 45, not 0, 90, 180 or 270", id="45"),
            pytest.param(osd("ninety"), "tesseract's OSD gave Rotate: ninety, not 0, 90, 180 or 270", id="word"),
            pytest.param(osd("90", "nan"), "tesseract's OSD gave a confidence of nan, not a finite number", id="nan"),
            pytest.param(
                osd("90", "high"), "tesseract's OSD gave a confidence of high, not a finite number", id="text"
            ),
        ],
    )
    def test_a_report_that_does_not_parse_is_a_failure_with_its_reason(self, report, message):
        result = ocr._osd(report)

        assert (result.status, result.degrees, result.confidence) == ("failed", 0, None)
        assert message in result.reason


TSV_HEADER = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext"


def tsv(*rows: str, newline: str = "\n") -> bytes:
    """tesseract's TSV: its header, then ``rows``."""
    return (newline.join((TSV_HEADER, *rows)) + newline).encode()


def row(
    level: int, box: tuple[int, int, int, int], conf: str, text: str, line: tuple[int, int, int] = (1, 1, 1)
) -> str:
    """A TSV row: ``line`` is its block, paragraph and line numbers."""
    left, top, width, height = box
    block, paragraph, number = line
    return f"{level}\t1\t{block}\t{paragraph}\t{number}\t1\t{left}\t{top}\t{width}\t{height}\t{conf}\t{text}"


BAND_AT_840 = Band(top_px=840, pixels=bytes(100 * 900), px_width=100, px_height=900)


class TestTsv:
    """TSV word rows (level 5) are read into words placed on the page: their band's offset added."""

    def test_word_rows_are_read_and_placed_below_the_band_top(self):
        stdout = tsv(
            row(1, (0, 0, 100, 900), "-1", ""),
            row(2, (10, 20, 70, 15), "-1", ""),
            row(4, (10, 20, 70, 15), "-1", ""),
            row(5, (10, 20, 30, 15), "96.050018", "Hello"),
            row(5, (45, 21, 35, 14), "91", "world,"),  # tesseract 4 prints an int confidence
        )

        assert ocr._tsv_words(stdout, BAND_AT_840) == [
            ocr._Placed("Hello", 10, 860, 40, 875, 96.050018, (840, 1, 1, 1)),
            ocr._Placed("world,", 45, 861, 80, 875, 91.0, (840, 1, 1, 1)),
        ]

    def test_words_without_text_or_confidence_are_skipped(self):
        stdout = tsv(row(5, (1, 1, 5, 5), "-1", "x"), row(5, (1, 1, 5, 5), "40", " "), row(5, (1, 1, 5, 5), "0", "y"))

        assert ocr._tsv_words(stdout, BAND_AT_840) == [ocr._Placed("y", 1, 841, 6, 846, 0.0, (840, 1, 1, 1))]

    def test_only_word_rows_are_words(self):
        # Only level 5 is a word, whatever text or confidence another level's row carries.
        stdout = tsv(*(row(level, (1, 1, 5, 5), "50", "x") for level in (1, 2, 3, 4)), row(5, (1, 1, 5, 5), "50", "y"))

        assert ocr._tsv_words(stdout, BAND_AT_840) == [ocr._Placed("y", 1, 841, 6, 846, 50.0, (840, 1, 1, 1))]

    def test_each_word_keeps_its_bands_top_and_its_block_paragraph_and_line(self):
        stdout = tsv(
            row(5, (10, 20, 30, 15), "90", "The", line=(1, 1, 1)),
            row(5, (45, 20, 30, 15), "90", "fox", line=(1, 1, 1)),
            row(5, (10, 40, 30, 15), "90", "ran", line=(1, 1, 2)),
            row(5, (10, 60, 30, 15), "90", "far", line=(1, 2, 1)),
            row(5, (10, 80, 30, 15), "90", "off", line=(2, 1, 1)),
        )

        lines = [word.line for word in ocr._tsv_words(stdout, BAND_AT_840)]

        assert lines == [(840, 1, 1, 1), (840, 1, 1, 1), (840, 1, 1, 2), (840, 1, 2, 1), (840, 2, 1, 1)]

    def test_crlf_line_ends_are_read(self):
        stdout = tsv(row(5, (1, 2, 3, 4), "50", "fox"), newline="\r\n")

        assert ocr._tsv_words(stdout, BAND_AT_840) == [ocr._Placed("fox", 1, 842, 4, 846, 50.0, (840, 1, 1, 1))]

    @pytest.mark.parametrize(
        "stdout,message",
        [
            pytest.param(b"", "tesseract's TSV has an unexpected header: ''", id="empty"),
            pytest.param(b"level\ttext\n", "tesseract's TSV has an unexpected header: 'level\\ttext'", id="header"),
            pytest.param(tsv("5\t1\t1"), "tesseract's TSV line 2: 3 fields, not 12", id="fields"),
            pytest.param(tsv(row(5, (1, 2, 3, 4), "50", "a\tb")), "line 2: 13 fields, not 12", id="tab"),
            pytest.param(
                tsv(row(5, (1, 2, 3, 4), "50", "x").replace("\t1\t2\t", "\tone\t2\t")), "line 2: left", id="int"
            ),
            pytest.param(tsv(row(5, (1, -2, 3, 4), "50", "x")), "line 2: top must be >= 0, got -2", id="negative"),
            pytest.param(
                tsv(row(5, (1, 2, 3, 4), "50", "x").replace("5\t1\t1\t", "5\t1\tone\t")),
                "line 2: block_num",
                id="block",
            ),
            pytest.param(tsv(row(5, (1, 2, 3, 4), "50", "x", line=(1, -1, 1))), "line 2: par_num", id="paragraph"),
            pytest.param(tsv(row(5, (1, 2, 3, 4), "50", "x", line=(1, 1, 2.5))), "line 2: line_num", id="line"),
            pytest.param(tsv(row(5, (1, 2, 3, 4), "100.5", "x")), "line 2: conf must be <= 100.0", id="conf-high"),
            pytest.param(tsv(row(5, (1, 2, 3, 4), "nan", "x")), "line 2: conf must be finite", id="conf-nan"),
            pytest.param(tsv(row(5, (1, 2, 3, 4), "most", "x")), "line 2: conf", id="conf-text"),
            pytest.param(tsv() + b"\xff\n", "tesseract's TSV is not UTF-8", id="utf-8"),
        ],
    )
    def test_malformed_tsv_is_a_reason_not_words(self, stdout, message):
        reason = ocr._tsv_words(stdout, BAND_AT_840)

        assert isinstance(reason, str)
        assert message in reason


UPPER = Band(top_px=0, pixels=bytes(900), px_width=1, px_height=900)
LOWER = Band(top_px=840, pixels=bytes(900), px_width=1, px_height=900)
LAST = Band(top_px=1680, pixels=bytes(900), px_width=1, px_height=900)


def placed(text: str, box: tuple[int, int, int, int], confidence: float) -> "ocr._Placed":
    """A word placed on the righted page: its line is keyed by its top row (its band's, block, paragraph and line)."""
    return ocr._Placed(text, *box, confidence, (0, 1, 1, box[1]))


class TestMerged:
    """A word read in both bands of a shared strip (same text, IoU >= 0.5) is kept once: its higher confidence."""

    SEAM = (10, 850, 40, 880)  # rows 850..880: in both UPPER and LOWER

    @pytest.mark.parametrize(
        "upper,lower,kept",
        [
            pytest.param(95.0, 90.0, "upper", id="upper-higher"),
            pytest.param(90.0, 95.0, "lower", id="lower-higher"),
            pytest.param(90.0, 90.0, "upper", id="tie"),
        ],
    )
    def test_a_word_read_twice_is_kept_once_with_the_higher_confidence(self, upper, lower, kept):
        # The lower band's copy sits a row lower (IoU 29 / 31), so the two copies differ.
        first, second = placed("fox", self.SEAM, upper), placed("fox", (10, 851, 40, 881), lower)
        before, after = placed("the", (0, 100, 20, 120), 80.0), placed("dog", (0, 1000, 20, 1020), 80.0)

        merged = ocr._merged([(UPPER, [before, first]), (LOWER, [second, after])])

        assert merged == [before, first if kept == "upper" else second, after]

    def test_an_iou_of_exactly_one_half_is_the_same_word(self):
        # 10 x 10 inside 20 x 10: intersection 100, union 200.
        wide, narrow = placed("fox", (0, 850, 20, 860), 90.0), placed("fox", (0, 850, 10, 860), 80.0)

        assert ocr._merged([(UPPER, [wide]), (LOWER, [narrow])]) == [wide]

    def test_an_iou_under_one_half_is_two_words(self):
        # 10 x 10 inside 21 x 10: intersection 100, union 210.
        wide, narrow = placed("fox", (0, 850, 21, 860), 90.0), placed("fox", (0, 850, 10, 860), 80.0)

        assert ocr._merged([(UPPER, [wide]), (LOWER, [narrow])]) == [wide, narrow]

    def test_different_text_in_the_same_place_is_two_words(self):
        upper, lower = placed("fox", self.SEAM, 90.0), placed("f0x", self.SEAM, 80.0)

        assert ocr._merged([(UPPER, [upper]), (LOWER, [lower])]) == [upper, lower]

    def test_a_zero_area_box_is_never_the_same_word(self):
        upper, lower = placed("fox", (10, 850, 10, 880), 90.0), placed("fox", (10, 850, 10, 880), 80.0)

        assert ocr._merged([(UPPER, [upper]), (LOWER, [lower])]) == [upper, lower]

    def test_each_seam_is_merged(self):
        first = [placed("a", (0, 850, 10, 870), 90.0), placed("b", (0, 1690, 10, 1710), 70.0)]
        second = [placed("a", (0, 850, 10, 870), 60.0), placed("b", (0, 1690, 10, 1710), 75.0)]
        third = [placed("b", (0, 1690, 10, 1710), 50.0)]

        merged = ocr._merged([(UPPER, first[:1]), (LOWER, second), (LAST, third)])

        assert merged == [first[0], second[1]]

    def test_one_band_is_its_words(self):
        words = [placed("a", (0, 0, 5, 5), 1.0), placed("a", (0, 0, 5, 5), 2.0)]

        assert ocr._merged([(UPPER, words)]) == words


class TestSeamFragments:
    """A word cut by its band's cut edge, wholly in the strip it shares, is dropped when the other band reads it whole.

    UPPER is rows 0..899 and LOWER rows 840..1739: they share rows 840..899.
    UPPER's cut edge is its bottom (a box ending at row 899 has a y1 of 900);
    LOWER's is its top (a box starting at row 840 has a y0 of 840). A box
    within 1 px of the edge touches it (#416).
    """

    def test_a_fragment_at_the_lower_bands_top_is_dropped_for_the_upper_bands_whole_word(self):
        whole = placed("quick", (10, 830, 60, 870), 96.0)  # rows 830..869: whole in UPPER, 30 rows above its edge
        fragment = placed("qnick", (10, 840, 60, 870), 99.0)  # LOWER's read of the rows below its top edge

        assert ocr._merged([(UPPER, [whole]), (LOWER, [fragment])]) == [whole]

    def test_a_fragment_at_the_upper_bands_bottom_is_dropped_for_the_lower_bands_whole_word(self):
        fragment = placed("qnick", (10, 880, 60, 900), 99.0)  # UPPER's read of the rows above its bottom edge
        whole = placed("quick", (10, 880, 60, 920), 96.0)  # rows 880..919: whole in LOWER, 40 rows below its edge

        assert ocr._merged([(UPPER, [fragment]), (LOWER, [whole])]) == [whole]

    @pytest.mark.parametrize("gap,cut", [(0, True), (1, True), (2, False)])
    def test_a_word_within_1_px_of_its_bands_cut_edge_is_cut(self, gap, cut):
        upper_whole, lower_whole = placed("over", (10, 820, 60, 860), 90.0), placed("lazy", (100, 870, 150, 910), 90.0)
        lower_part = placed("ovcr", (10, 840 + gap, 60, 860), 90.0)  # its top `gap` rows below LOWER's top
        upper_part = placed("lazv", (100, 870, 150, 900 - gap), 90.0)  # its bottom `gap` rows above UPPER's bottom

        merged = ocr._merged([(UPPER, [upper_whole, upper_part]), (LOWER, [lower_part, lower_whole])])

        assert merged == ([upper_whole, lower_whole] if cut else [upper_whole, upper_part, lower_part, lower_whole])

    def test_a_cut_word_reaching_past_the_shared_strip_is_kept(self):
        # Each touches its band's cut edge but reaches a row the other band does not hold: only it read that row.
        upper_whole, lower_whole = placed("over", (10, 820, 60, 860), 90.0), placed("lazy", (100, 870, 150, 910), 90.0)
        lower_part = placed("ovcr", (10, 840, 60, 901), 90.0)  # to row 900, below the strip
        upper_part = placed("lazv", (100, 839, 150, 900), 90.0)  # from row 839, above the strip

        merged = ocr._merged([(UPPER, [upper_whole, upper_part]), (LOWER, [lower_part, lower_whole])])

        assert merged == [upper_whole, upper_part, lower_part, lower_whole]

    @pytest.mark.parametrize(
        "neighbour",
        [
            pytest.param((61, 830, 90, 870), id="sharing-its-right-edge"),
            pytest.param((10, 800, 61, 841), id="sharing-its-top-row-edge"),
        ],
    )
    def test_a_cut_word_the_other_band_does_not_read_is_kept(self, neighbour):
        # Words read once are never dropped: nothing in UPPER overlaps the cut word (a shared edge is no overlap).
        # Its top is 1 row below LOWER's top edge, so it is cut; UPPER's word ends on that row's top edge.
        beside = placed("fox", neighbour, 90.0)
        cut = placed("qnick", (10, 841, 61, 870), 90.0)

        assert ocr._merged([(UPPER, [beside]), (LOWER, [cut])]) == [beside, cut]

    def test_a_line_both_bands_cut_is_kept_in_both(self):
        # Rows 840..899 exactly: UPPER's copy touches its bottom, LOWER's its top; neither band read the line whole.
        upper, lower = placed("SORT", (10, 840, 60, 900), 0.0), placed("Company", (10, 840, 60, 899), 96.0)

        assert ocr._merged([(UPPER, [upper]), (LOWER, [lower])]) == [upper, lower]

    def test_the_whole_copy_is_kept_over_a_cut_copy_of_higher_confidence(self):
        # The same text, IoU 0.75: the copy cut by LOWER's top is a fragment, whatever its confidence.
        whole, cut = placed("fox", (10, 830, 40, 870), 80.0), placed("fox", (10, 840, 40, 870), 95.0)

        assert ocr._merged([(UPPER, [whole]), (LOWER, [cut])]) == [whole]

    def test_each_seam_drops_its_own_fragments(self):
        first, last = placed("ab", (0, 830, 10, 870), 90.0), placed("cd", (0, 1720, 10, 1760), 90.0)
        second = [placed("xb", (0, 840, 10, 870), 90.0), placed("cx", (0, 1720, 10, 1740), 90.0)]

        assert ocr._merged([(UPPER, [first]), (LOWER, second), (LAST, [last])]) == [first, last]


class TestDisplayedBox:
    """A word's box maps back to displayed-page points: through the rotation that righted the page, then 72 / dpi."""

    WIDTH, HEIGHT = 40, 30

    @pytest.mark.parametrize("rotation", [0, 90, 180, 270])
    @pytest.mark.parametrize("box", [(5, 7, 12, 10), (0, 0, 40, 1), (39, 29, 40, 30), (0, 3, 1, 30)])
    def test_a_box_found_on_the_righted_page_maps_back_where_it_was_drawn(self, rotation, box):
        # Pillow is the oracle: draw the box on the displayed page, right the page with rotated(),
        # find the box there, and map it back.
        image = Image.new("L", (self.WIDTH, self.HEIGHT), 255)
        image.paste(0, box)
        displayed = RenderedPage(pixels=image.tobytes(), px_width=self.WIDTH, px_height=self.HEIGHT)
        righted = rotated(displayed, rotation)
        found = ImageOps.invert(Image.frombytes("L", (righted.px_width, righted.px_height), righted.pixels)).getbbox()

        mapped = ocr._displayed_box(placed("w", found, 90.0), rotation, (self.WIDTH, self.HEIGHT), 144)

        x0, y0, x1, y1 = box
        assert mapped == bbox(x0 / 2, y0 / 2, x1 / 2, y1 / 2)  # 72 / 144 points a pixel


def fake_tesseract(tmp_path: Path, monkeypatch, body: str, stdout: bytes = b"") -> Path:
    """Put a fake ``tesseract`` (``#!/bin/sh`` + ``body``) first on PATH; ``{d}`` in ``body`` is its directory.

    The directory's ``stdout`` file holds ``stdout``, for the script to cat.
    """
    directory = tmp_path / "fake"
    directory.mkdir()
    (directory / "stdout").write_bytes(stdout)
    script = directory / "tesseract"
    script.write_text("#!/bin/sh\n" + body.replace("{d}", str(directory)) + "\n")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{directory}{os.pathsep}{os.environ['PATH']}")
    return directory


READS_STDIN = 'cat > /dev/null\ncat "{d}/stdout"'
RECORDS = (
    'printf "%s\\n" "$@" > "{d}/argv"\n'
    'printf "%s" "${OMP_THREAD_LIMIT-unset}" > "{d}/omp"\n'
    'cat > "{d}/stdin"\n'
    'cat "{d}/stdout"'
)
PAGE_3_BY_2 = page_of([[0, 64, 128], [192, 255, 7]])
PAGE_50_BY_1000 = striped(50, 1000)  # at 72 DPI: a 50 x 1000 pt page, in bands of rows 0..899 and 840..999


def words_of(page: RenderedPage, **options) -> OCRResult:
    """ocr_words over ``page``'s bands, at 72 DPI (1 px a point), upright."""
    arguments = {"dpi": 72, "rotation": 0, "page_size_pt": (float(page.px_width), float(page.px_height))}
    return ocr_words(bands(page), **{**arguments, "timeout_s": 30.0, **options})


NO_TESSERACT = "no tesseract in an absolute PATH directory"


def planted(directory: Path, version: str, marker: Path) -> Path:
    """An executable ``tesseract`` in ``directory`` that leaves ``marker`` and answers ``--version`` as ``version``."""
    directory.mkdir(parents=True, exist_ok=True)
    script = directory / "tesseract"
    script.write_text(f'#!/bin/sh\n: > "{marker}"\necho "tesseract {version}"\n')
    script.chmod(0o755)
    return script


@posix_only
class TestSearchPath:
    """OCR runs the tesseract the shared resolver finds (``_executables.trusted_executable``), by its absolute path.

    How the resolver searches PATH is pinned in test_executables.py; these
    pin that every OCR entry point goes through it. A relative PATH entry
    names a directory under the working directory, such as a folder of
    untrusted PDFs: a ``tesseract`` planted there must never run.
    """

    def test_a_tesseract_planted_in_the_working_directory_is_never_run(self, tmp_path, monkeypatch):
        work = tmp_path / "work"
        planted(work, "9.9.9", tmp_path / "planted-ran")
        monkeypatch.chdir(work)
        planted(tmp_path / "trusted", "5.3.4", tmp_path / "trusted-ran")
        monkeypatch.setenv("PATH", os.pathsep.join([".", str(tmp_path / "trusted")]))

        assert tesseract_available() is True
        orientation(PAGE_3_BY_2, timeout_s=10.0)
        words_of(PAGE_3_BY_2)

        assert (tmp_path / "trusted-ran").exists()
        assert not (tmp_path / "planted-ran").exists()

    def test_a_tesseract_only_in_the_working_directory_is_unavailable(self, tmp_path, monkeypatch):
        planted(tmp_path, "9.9.9", tmp_path / "planted-ran")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("PATH", ".")

        assert tesseract_available() is False
        assert orientation(PAGE_3_BY_2, timeout_s=5.0) == Orientation(
            degrees=0, confidence=None, status="unavailable", reason=NO_TESSERACT
        )
        assert words_of(PAGE_3_BY_2) == OCRResult(words=(), status="unavailable", reason=NO_TESSERACT)
        assert not (tmp_path / "planted-ran").exists()

    def test_a_tesseract_in_an_absolute_entry_is_used(self, tmp_path, monkeypatch):
        planted(tmp_path / "trusted", "5.3.4", tmp_path / "trusted-ran")
        monkeypatch.setenv("PATH", str(tmp_path / "trusted"))

        assert tesseract_available() is True
        assert (tmp_path / "trusted-ran").exists()

    def test_tesseract_runs_by_the_absolute_path_found(self, tmp_path, monkeypatch):
        trusted = planted(tmp_path / "trusted", "5.3.4", tmp_path / "trusted-ran")
        monkeypatch.setenv("PATH", str(tmp_path / "trusted"))
        commands = []
        popen = subprocess.Popen

        def recorded(argv, *args, **kwargs):
            commands.append(argv)
            return popen(argv, *args, **kwargs)

        monkeypatch.setattr(ocr.subprocess, "Popen", recorded)

        tesseract_available()
        orientation(PAGE_3_BY_2, timeout_s=10.0)
        words_of(PAGE_3_BY_2)

        assert [argv[0] for argv in commands] == [str(trusted)] * 3
        assert os.path.isabs(commands[0][0])


class TestMissingTesseract:
    """No tesseract is a recorded status, 'unavailable', never an empty success or an exception."""

    def test_no_tesseract_on_path_is_unavailable(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PATH", str(tmp_path))

        assert tesseract_available() is False
        assert orientation(PAGE_3_BY_2, timeout_s=5.0) == Orientation(
            degrees=0, confidence=None, status="unavailable", reason=NO_TESSERACT
        )
        assert words_of(PAGE_3_BY_2) == OCRResult(words=(), status="unavailable", reason=NO_TESSERACT)

    def test_a_tesseract_that_cannot_start_is_unavailable(self, tmp_path, monkeypatch):
        missing = str(tmp_path / "gone" / "tesseract")
        monkeypatch.setattr(ocr, "trusted_executable", lambda name: missing)

        assert tesseract_available() is False
        result = orientation(PAGE_3_BY_2, timeout_s=5.0)
        assert (result.status, result.degrees, result.confidence) == ("unavailable", 0, None)
        assert result.reason.startswith(f"cannot start tesseract ({missing}): ")
        assert words_of(PAGE_3_BY_2).status == "unavailable"


@posix_only
class TestCli:
    """The CLI runs from an argv list (never a shell), with OMP_THREAD_LIMIT=1, the image as PGM on stdin."""

    def test_orientation_runs_psm_0_on_the_page_as_pgm(self, tmp_path, monkeypatch):
        fake = fake_tesseract(tmp_path, monkeypatch, RECORDS, stdout=osd("270", "4.75"))
        monkeypatch.setenv("OMP_THREAD_LIMIT", "8")

        result = orientation(PAGE_3_BY_2, timeout_s=10.0)

        assert result == Orientation(degrees=270, confidence=4.75, status="ok", reason="")
        assert (fake / "argv").read_text().split("\n")[:-1] == ["stdin", "stdout", "--psm", "0"]
        assert (fake / "omp").read_text() == "1"
        assert (fake / "stdin").read_bytes() == b"P5\n3 2\n255\n" + bytes([0, 64, 128, 192, 255, 7])

    def test_ocr_words_runs_psm_3_tsv_in_the_language_on_each_band_as_pgm(self, tmp_path, monkeypatch):
        # psm 3 (automatic page segmentation), chosen over psm 6 (one uniform block) on the forced-OCR corpus sample
        # (A5b): higher word recall and precision against the digital text, pooled over 31 pages.
        fake = fake_tesseract(tmp_path, monkeypatch, RECORDS, stdout=tsv())

        result = words_of(PAGE_3_BY_2, language="deu+eng")

        assert result == OCRResult(words=(), status="ok", reason="")
        argv = (fake / "argv").read_text().split("\n")[:-1]
        assert argv == ["stdin", "stdout", "--psm", "3", "-l", "deu+eng", "tsv"]
        assert (fake / "omp").read_text() == "1"
        assert (fake / "stdin").read_bytes() == b"P5\n3 2\n255\n" + PAGE_3_BY_2.pixels

    def test_words_from_every_band_land_on_the_page(self, tmp_path, monkeypatch):
        # The same report for each band: the word sits 20 rows below each band's top.
        fake_tesseract(tmp_path, monkeypatch, READS_STDIN, stdout=tsv(row(5, (10, 20, 30, 15), "90", "fox")))

        result = words_of(PAGE_50_BY_1000)

        assert result.status == "ok"
        assert [word.bbox for word in result.words] == [bbox(10.0, 20.0, 40.0, 35.0), bbox(10.0, 860.0, 40.0, 875.0)]

    def test_lines_are_numbered_across_the_page_in_the_order_read(self, tmp_path, monkeypatch):
        # The same report for each band, its lines numbered alike: a band's line 1 is not another band's.
        report = tsv(
            row(5, (1, 20, 9, 15), "90", "The", line=(1, 1, 1)),
            row(5, (12, 20, 9, 15), "90", "fox", line=(1, 1, 1)),
            row(5, (1, 400, 9, 15), "90", "ran", line=(2, 1, 1)),
        )
        fake_tesseract(tmp_path, monkeypatch, READS_STDIN, stdout=report)

        result = words_of(PAGE_50_BY_1000)

        assert [(word.text, word.line) for word in result.words] == [
            ("The", 0),
            ("fox", 0),
            ("ran", 1),
            ("The", 2),
            ("fox", 2),
            ("ran", 3),
        ]

    def test_a_band_that_fails_ends_the_page_with_the_words_read_before_it(self, tmp_path, monkeypatch):
        body = (
            "cat > /dev/null\n"
            'if [ -e "{d}/called" ]; then echo "Error: the second band" >&2; exit 3; fi\n'
            ': > "{d}/called"\n'
            'cat "{d}/stdout"'
        )
        fake_tesseract(tmp_path, monkeypatch, body, stdout=tsv(row(5, (10, 20, 30, 15), "90", "fox")))

        result = words_of(PAGE_50_BY_1000)

        assert result.status == "failed"
        assert [word.text for word in result.words] == ["fox"]
        reason = "band 2 of 2 (rows 840 to 999): tesseract exited with code 3; its stderr ends: Error: the second band"
        assert result.reason == reason

    def test_a_tsv_that_does_not_parse_fails_its_band(self, tmp_path, monkeypatch):
        fake_tesseract(tmp_path, monkeypatch, READS_STDIN, stdout=b"not a tsv\n")

        result = words_of(PAGE_3_BY_2)

        assert result == OCRResult(
            words=(),
            status="failed",
            reason="band 1 of 1 (rows 0 to 1): tesseract's TSV has an unexpected header: 'not a tsv'",
        )

    def test_too_few_characters_for_osd_is_undetermined_not_failed(self, tmp_path, monkeypatch):
        body = (
            "cat > /dev/null\nprintf 'Too few characters. Skipping this page\\nError during processing.\\n' >&2\nexit 1"
        )
        fake_tesseract(tmp_path, monkeypatch, body)

        assert orientation(PAGE_3_BY_2, timeout_s=10.0) == Orientation(
            degrees=0,
            confidence=None,
            status="undetermined",
            reason="tesseract's OSD found too few characters to orient the page",
        )

    def test_any_other_osd_error_is_failed_with_its_stderr(self, tmp_path, monkeypatch):
        body = "cat > /dev/null\necho \"Failed loading language 'osd'\" >&2\nexit 1"
        fake_tesseract(tmp_path, monkeypatch, body)

        result = orientation(PAGE_3_BY_2, timeout_s=10.0)

        assert (result.status, result.degrees, result.confidence) == ("failed", 0, None)
        assert result.reason == "tesseract exited with code 1; its stderr ends: Failed loading language 'osd'"

    def test_a_tesseract_that_stops_reading_the_image_fails(self, tmp_path, monkeypatch):
        fake_tesseract(tmp_path, monkeypatch, "exit 0")  # reads nothing: a 2 MB page cannot fit in the pipe
        page = striped(2000, 1000)

        result = orientation(page, timeout_s=10.0)

        assert (result.status, result.degrees, result.confidence) == ("failed", 0, None)
        assert result.reason.startswith("tesseract did not read the whole image: ")


@posix_only
class TestBounds:
    """Every call is bounded: killed past its timeout (its whole session), its stdout read within a byte bound."""

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_call_past_its_timeout_is_killed_and_reaped(self, tmp_path, monkeypatch):
        fake_tesseract(tmp_path, monkeypatch, "exec sleep 60")

        began = time.monotonic()
        result = orientation(PAGE_3_BY_2, timeout_s=0.5)
        elapsed = time.monotonic() - began

        assert result == Orientation(
            degrees=0, confidence=None, status="timeout", reason="tesseract did not finish within 0.5 s and was killed"
        )
        assert elapsed < 4.0  # ~0.5 s here; the sleep would take 60

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_wrappers_child_holding_stdout_is_killed_with_it(self, tmp_path, monkeypatch):
        # A wrapper script whose child (not exec'd) inherits stdout: killing the wrapper alone
        # would leave the read waiting for the child's 60 s.
        body = 'sleep 60 &\necho $! > "{d}/child"\nwait'
        fake = fake_tesseract(tmp_path, monkeypatch, body)

        began = time.monotonic()
        result = words_of(PAGE_3_BY_2, timeout_s=0.5)
        elapsed = time.monotonic() - began

        assert result.status == "timeout"
        assert elapsed < 4.0
        child = int((fake / "child").read_text())
        assert gone(child, within_s=5.0)

    def test_stdout_up_to_its_bound_is_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ocr, "_MAX_STDOUT_BYTES", 1000)
        fake_tesseract(tmp_path, monkeypatch, "head -c 1000 /dev/zero")

        call = ocr._call([trusted_executable("tesseract")], (), 10.0)

        assert (call.status, call.reason, len(call.stdout)) == ("ok", "", 1000)

    def test_stdout_past_its_bound_fails_the_call(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ocr, "_MAX_STDOUT_BYTES", 1000)
        fake_tesseract(tmp_path, monkeypatch, "head -c 1001 /dev/zero")

        call = ocr._call([trusted_executable("tesseract")], (), 10.0)

        assert (call.status, call.reason, call.stdout) == (
            "failed",
            "tesseract wrote over 1000 bytes to stdout and was killed",
            b"",
        )

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_stdout_past_its_bound_kills_the_call_at_once(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ocr, "_MAX_STDOUT_BYTES", 1000)
        fake_tesseract(tmp_path, monkeypatch, "head -c 1001 /dev/zero\nexec sleep 60")

        began = time.monotonic()
        call = ocr._call([trusted_executable("tesseract")], (), 30.0)
        elapsed = time.monotonic() - began

        assert call.status == "failed"
        assert elapsed < 4.0  # killed when the bound was passed, not at the 30 s timeout

    def test_a_timeout_is_a_status_not_an_exception(self, tmp_path, monkeypatch):
        fake_tesseract(tmp_path, monkeypatch, "exec sleep 60")

        call = ocr._call([trusted_executable("tesseract")], (b"P5\n1 1\n255\n", b"\x00"), 0.2)

        assert (call.status, call.reason) == ("timeout", "tesseract did not finish within 0.2 s and was killed")

    def test_where_its_session_cannot_be_signalled_the_process_itself_is_killed(self, tmp_path, monkeypatch):
        def refused(pid, signal_number):
            raise PermissionError(1, "Operation not permitted")

        monkeypatch.setattr(ocr.os, "killpg", refused)
        fake_tesseract(tmp_path, monkeypatch, "exec sleep 60")

        assert ocr._call([trusted_executable("tesseract")], (), 0.2).status == "timeout"

    def test_without_sessions_the_process_itself_is_killed(self, tmp_path, monkeypatch):
        # Windows' path: no session to kill, so the process alone.
        monkeypatch.setattr(ocr, "_OWN_SESSION", False)
        monkeypatch.setattr(ocr.os, "killpg", lambda pid, signal_number: pytest.fail("no session to kill"))
        fake_tesseract(tmp_path, monkeypatch, "exec sleep 60")

        assert ocr._call([trusted_executable("tesseract")], (), 0.2).status == "timeout"


class FailingStdin:
    """A stdin whose reader has gone: every write and close fails."""

    def __init__(self):
        self.closes = 0

    def write(self, data: bytes) -> int:
        raise BrokenPipeError(32, "Broken pipe")

    def close(self) -> None:
        self.closes += 1
        raise BrokenPipeError(32, "Broken pipe")


class TestPipes:
    """The payload is written whole or the failure reported; a pipe that is not one is a programming error."""

    def test_a_write_that_fails_is_reported_and_the_pipe_closed(self):
        stdin = FailingStdin()

        assert ocr._fed(stdin, (b"P5\n1 1\n255\n", b"\x00")) == "[Errno 32] Broken pipe"
        assert stdin.closes == 1

    def test_a_missing_pipe_raises(self):
        with pytest.raises(ValueError, match="tesseract's stdin must be a pipe"):
            ocr._fed(None, ())
        with pytest.raises(ValueError, match="tesseract's stdout must be a pipe"):
            ocr._bounded_read(None, 10)


def gone(pid: int, *, within_s: float) -> bool:
    """Whether ``pid`` no longer exists within ``within_s`` seconds (an orphan is reaped by init)."""
    deadline = time.monotonic() + within_s
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


class TestEnding:
    """How a finished call is reported: ok only if it exited 0 with its whole stdout, having read the whole image."""

    @pytest.mark.parametrize(
        "ending,status,reason",
        [
            pytest.param((0, False, b"out", ""), "ok", "", id="ok"),
            pytest.param((0, True, b"out", ""), "ok", "", id="ok-as-the-timer-fired"),
            pytest.param(
                (-9, True, b"", ""), "timeout", "tesseract did not finish within 2.0 s and was killed", id="timeout"
            ),
            pytest.param(
                (-9, True, None, "warn"),
                "timeout",
                "tesseract did not finish within 2.0 s and was killed; its stderr ends: warn",
                id="timeout-first",
            ),
            pytest.param(
                (-9, False, None, ""),
                "failed",
                "tesseract wrote over 16777216 bytes to stdout and was killed",
                id="bound",
            ),
            pytest.param((None, False, b"", ""), "failed", "tesseract could not be reaped", id="unreaped"),
            pytest.param((2, False, b"", ""), "failed", "tesseract exited with code 2", id="exit"),
            pytest.param(
                (-11, False, b"", "boom"),
                "failed",
                "tesseract exited with code -11; its stderr ends: boom",
                id="signal",
            ),
        ],
    )
    def test_endings(self, ending, status, reason):
        returncode, expired, stdout, stderr = ending

        call = ocr._ending(returncode, expired, stdout, "", stderr, 2.0)

        assert (call.status, call.reason) == (status, reason)
        assert call.stdout == (stdout if status == "ok" else b"")
        assert call.stderr == stderr

    def test_an_image_not_read_whole_fails_even_on_exit_0(self):
        call = ocr._ending(0, False, b"out", "[Errno 32] Broken pipe", "", 2.0)

        assert (call.status, call.reason) == (
            "failed",
            "tesseract did not read the whole image: [Errno 32] Broken pipe",
        )


@posix_only
class TestTesseractAvailable:
    """tesseract_available(): on PATH, and its --version answers as tesseract within a timeout."""

    def test_a_tesseract_that_answers_is_available(self, tmp_path, monkeypatch):
        fake = fake_tesseract(tmp_path, monkeypatch, 'printf "%s\\n" "$@" > "{d}/argv"\necho "tesseract 5.3.4"')

        assert tesseract_available() is True
        assert (fake / "argv").read_text() == "--version\n"

    @pytest.mark.parametrize(
        "body", ["echo 'tesseract 5.3.4'\nexit 1", "echo 'not tesseract'", "exit 0"], ids=["exit-1", "other", "silent"]
    )
    def test_a_tesseract_that_does_not_answer_is_not_available(self, tmp_path, monkeypatch, body):
        fake_tesseract(tmp_path, monkeypatch, body)

        assert tesseract_available() is False

    @pytest.mark.no_cover  # coverage's tracer (settrace before 3.12) would be what is timed
    def test_a_tesseract_that_hangs_is_not_available_within_its_timeout(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ocr, "_VERSION_TIMEOUT_S", 0.5)
        fake_tesseract(tmp_path, monkeypatch, "exec sleep 60")

        began = time.monotonic()
        assert tesseract_available() is False
        assert time.monotonic() - began < 4.0


class TestArguments:
    """orientation() and ocr_words() validate their arguments before anything runs."""

    @pytest.mark.parametrize(
        "change,error,message",
        [
            pytest.param({"page_bands": ()}, ValueError, "page_bands must hold at least one band", id="no-bands"),
            pytest.param({"page_bands": [None]}, TypeError, "page_bands must be a tuple, got list", id="list"),
            pytest.param({"dpi": 0}, ValueError, "dpi must be >= 1, got 0", id="dpi"),
            pytest.param({"rotation": 45}, ValueError, "rotation must be 0, 90, 180 or 270, got 45", id="rotation"),
            pytest.param({"page_size_pt": (50.0,)}, ValueError, "page_size_pt must have 2 values, got 1", id="size"),
            pytest.param({"page_size_pt": (0.0, 1000.0)}, ValueError, "page_size_pt[0] must be > 0", id="size-0"),
            pytest.param({"timeout_s": 0.0}, ValueError, "timeout_s must be > 0", id="timeout"),
            pytest.param({"language": "eng --psm"}, ValueError, "language must be tesseract language names", id="lang"),
            pytest.param({"language": ""}, ValueError, "language must be tesseract language names", id="lang-blank"),
            pytest.param(
                {"rotation": 90},
                ValueError,
                "band 1 is 50 px wide; a 50 x 1000 px page righted by 90 degrees is 1000 px wide",
                id="band-width",
            ),
            pytest.param(
                {"page_size_pt": (50.0, 900.0)},
                ValueError,
                "band 2 (rows 840 to 999) runs past the 900 px tall righted page",
                id="band-rows",
            ),
        ],
    )
    def test_invalid_ocr_words_arguments_raise(self, change, error, message, monkeypatch):
        monkeypatch.setattr(ocr, "trusted_executable", lambda name: pytest.fail("tesseract must not be looked up"))
        arguments = {"page_bands": bands(PAGE_50_BY_1000), "dpi": 72, "rotation": 0, "page_size_pt": (50.0, 1000.0)}
        arguments |= {"timeout_s": 10.0, **change}

        with pytest.raises(error, match=re.escape(message)):
            ocr_words(arguments.pop("page_bands"), **arguments)

    @pytest.mark.parametrize(
        "page,timeout_s,error,message",
        [
            pytest.param(b"", 10.0, TypeError, "page must be a RenderedPage, got bytes", id="page"),
            pytest.param(PAGE_3_BY_2, float("inf"), ValueError, "timeout_s must be finite", id="timeout"),
        ],
    )
    def test_invalid_orientation_arguments_raise(self, page, timeout_s, error, message, monkeypatch):
        monkeypatch.setattr(ocr, "trusted_executable", lambda name: pytest.fail("tesseract must not be looked up"))

        with pytest.raises(error, match=re.escape(message)):
            orientation(page, timeout_s=timeout_s)


SENTENCES = (
    "The quick brown fox jumps over the lazy dog near the river bank.",
    "Pack my box with five dozen liquor jugs before the morning train.",
    "How vexingly quick daft zebras jump when the music starts to play.",
    "Sphinx of black quartz, judge my vow and read the whole sentence.",
    "Every scanned page should orient itself before the words are read.",
    "Bright vixens jump; dozy fowl quack while the farmer counts sheep.",
)


def drawn_page() -> RenderedPage:
    """Six sentences in Pillow's bundled font (the same on every platform), 2550 x 1650 px, upright."""
    font = ImageFont.load_default(size=36)
    image = Image.new("L", (2550, 1650), 255)
    draw = ImageDraw.Draw(image)
    for index, sentence in enumerate(SENTENCES):
        draw.text((150, 150 + index * 70), sentence, fill=0, font=font)
    return RenderedPage(pixels=image.tobytes(), px_width=image.width, px_height=image.height)


@needs_tesseract
class TestRealTesseract:
    """The installed tesseract: OSD's Rotate is the clockwise turn rotated() takes to right the page."""

    def test_it_is_available(self):
        assert tesseract_available() is True

    @pytest.mark.parametrize("turned", [0, 90, 180, 270])
    def test_osd_names_the_turn_that_rights_a_turned_page(self, turned):
        upright = drawn_page()
        page = rotated(upright, turned)  # the page as displayed: turned clockwise from upright

        found = orientation(normalized(page), timeout_s=60.0)

        assert (found.status, found.degrees) == ("ok", (360 - turned) % 360)
        assert found.confidence is not None and found.confidence > 0.0
        assert rotated(page, found.degrees) == upright

    def test_too_little_text_is_undetermined(self):
        blank = RenderedPage(pixels=bytes([255]) * (800 * 600), px_width=800, px_height=600)

        found = orientation(blank, timeout_s=60.0)

        assert (found.status, found.degrees, found.confidence) == ("undetermined", 0, None)

    def test_the_words_are_read(self):
        page = drawn_page()

        result = ocr_words(bands(page), dpi=300, rotation=0, page_size_pt=(612.0, 396.0), timeout_s=60.0)

        expected = {word for sentence in SENTENCES for word in sentence.split()}
        assert result.status == "ok"
        assert len(expected & {word.text for word in result.words}) >= 0.9 * len(expected)

    @pytest.mark.parametrize(
        "top",
        [pytest.param(825, id="across-the-lower-bands-top"), pytest.param(870, id="across-the-upper-bands-bottom")],
    )
    def test_a_line_crossing_a_band_edge_is_read_once(self, top):
        # The bands are rows 0..899 and 840..1649. The line's ink (36 px tall, from ~7 rows below `top`) crosses row
        # 840 or row 899: one band reads it whole, the other reads its cut part as other words (#416).
        font = ImageFont.load_default(size=36)
        image = Image.new("L", (2550, 1650), 255)
        draw = ImageDraw.Draw(image)
        draw.text((150, 700), SENTENCES[1], fill=0, font=font)
        draw.text((150, top), SENTENCES[0], fill=0, font=font)
        page = RenderedPage(pixels=image.tobytes(), px_width=image.width, px_height=image.height)

        result = ocr_words(bands(page), dpi=300, rotation=0, page_size_pt=(612.0, 396.0), timeout_s=60.0)

        expected = SENTENCES[0].split()
        line = [word.text for word in result.words if word.bbox.y0 > 790 * 72 / 300]  # below the first line
        assert result.status == "ok"
        assert len(line) == len(expected)
        assert sum(text in expected for text in line) >= 0.9 * len(expected)
