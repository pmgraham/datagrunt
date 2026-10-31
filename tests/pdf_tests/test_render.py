"""Tests for the v5 guarded page render (#369, A5a): ``datagrunt.pdf.render``.

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``), or
written by hand (``hand_written_pdf``) for content ReportLab cannot write.
Expected pixel sizes are derived from pdfium's own page geometry in the same
run, never pinned from one platform's fonts.
"""

import math
import re

import pytest

from datagrunt.pdf import DatagruntPDFError
from datagrunt.pdf.render import RenderRefused, effective_dpi, render_size

# The spec's defaults (PDFReader / PDFWriter): vision_dpi and max_render_pixels.
VISION_DPI = 300
MAX_RENDER_PIXELS = 16_000_000

LETTER = (612.0, 792.0)
# A 36" x 48" engineering sheet, in points.
SHEET_36_BY_48 = (36 * 72.0, 48 * 72.0)


def spec_formula(target_dpi: int, max_render_pixels: int, width_pt: float, height_pt: float) -> int:
    """The spec's effective DPI, written as the spec writes it."""
    return min(target_dpi, int(72.0 * math.sqrt(max_render_pixels / (width_pt * height_pt))))


class TestEffectiveDpi:
    """effective_dpi bounds a render's pixels by max_render_pixels (spec §3.3 step 4)."""

    def test_a_letter_page_at_300_dpi_keeps_300(self):
        assert effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, *LETTER) == 300

    def test_a_36_by_48_inch_sheet_at_300_dpi_stays_within_max_render_pixels(self):
        dpi = effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, *SHEET_36_BY_48)
        width_px, height_px = render_size(*SHEET_36_BY_48, dpi)

        assert dpi == spec_formula(VISION_DPI, MAX_RENDER_PIXELS, *SHEET_36_BY_48) == 96
        assert width_px * height_px <= MAX_RENDER_PIXELS
        # At the target DPI the sheet would be ~155.5 M pixels.
        unbounded = render_size(*SHEET_36_BY_48, VISION_DPI)
        assert unbounded[0] * unbounded[1] > 9 * MAX_RENDER_PIXELS

    @pytest.mark.parametrize("target_dpi", [36, 72, 150, 200, 300, 600, 1200])
    @pytest.mark.parametrize("max_render_pixels", [262_144, 4_000_000, 16_000_000, 100_000_000])
    def test_every_size_renders_within_max_render_pixels_or_is_refused(self, target_dpi, max_render_pixels):
        # Sides from 0.01 pt to 1e8 pt: pdfium reads a MediaBox of any size (1 x 5,000,000 and 1e8 x 1 load).
        sides = [10.0**exponent for exponent in range(-2, 9)] + [612.0, 792.0, 2592.0, 3456.0, 14400.0]
        for width_pt in sides:
            for height_pt in sides:
                try:
                    dpi = effective_dpi(target_dpi, max_render_pixels, width_pt, height_pt)
                except RenderRefused:
                    continue
                width_px, height_px = render_size(width_pt, height_pt, dpi)
                assert dpi == spec_formula(target_dpi, max_render_pixels, width_pt, height_pt)
                assert 1 <= dpi <= target_dpi
                assert width_px >= 1 and height_px >= 1
                assert width_px * height_px <= max_render_pixels, (width_pt, height_pt, dpi)

    def test_the_lowest_dpi_returned_is_1(self):
        # 72 * sqrt(262144 / 24576**2) = 1.5: one DPI renders the page at 341 x 341 px.
        assert effective_dpi(VISION_DPI, 262_144, 24576.0, 24576.0) == 1
        assert render_size(24576.0, 24576.0, 1) == (341, 341)

    def test_a_page_too_large_for_1_dpi_is_refused(self):
        # 72 * sqrt(16e6 / 1e12) = 0.29: even 1 DPI would be 1.9e8 pixels.
        message = "a 1000000.0 x 1000000.0 pt page renders over the 16000000-pixel bound at 1 DPI (bound: 0.29 DPI)"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, 1e6, 1e6)

    def test_an_area_beyond_float_range_is_refused(self):
        # 1e200 * 1e200 overflows to inf: the bound is 0 DPI.
        message = "a 1e+200 x 1e+200 pt page renders over the 16000000-pixel bound at 1 DPI (bound: 0.00 DPI)"
        with pytest.raises(RenderRefused, match=re.escape(message)):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, 1e200, 1e200)

    def test_an_area_that_underflows_to_zero_is_refused_not_divided_by(self):
        # 1e-200 * 1e-200 underflows to 0.0: the page fits at the target DPI, and renders 0 px wide there.
        with pytest.raises(RenderRefused, match=re.escape("a 1e-200 x 1e-200 pt page renders 0 px wide at 300 DPI")):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, 1e-200, 1e-200)

    @pytest.mark.parametrize(
        "size,message",
        [
            pytest.param((1e8, 1.0), "a 100000000.0 x 1.0 pt page renders 0 px tall at 28 DPI", id="1e8-by-1"),
            pytest.param((0.01, 100.0), "a 0.01 x 100.0 pt page renders 0 px wide at 300 DPI", id="sliver"),
        ],
    )
    def test_a_page_under_one_pixel_across_is_refused(self, size, message):
        # Rounding a side up to 1 px would break the bound: 1e8 x 1 pt at 28 DPI is 38.9 M x 1 px.
        with pytest.raises(RenderRefused, match=re.escape(message)):
            effective_dpi(VISION_DPI, MAX_RENDER_PIXELS, *size)

    def test_render_refused_is_a_datagrunt_pdf_error(self):
        assert issubclass(RenderRefused, DatagruntPDFError)

    @pytest.mark.parametrize(
        "args,error,message",
        [
            pytest.param((35, MAX_RENDER_PIXELS, *LETTER), ValueError, "target_dpi must be >= 36, got 35", id="dpi"),
            pytest.param((True, MAX_RENDER_PIXELS, *LETTER), TypeError, "target_dpi must be an int", id="dpi-bool"),
            pytest.param((300.0, MAX_RENDER_PIXELS, *LETTER), TypeError, "target_dpi must be an int", id="dpi-float"),
            pytest.param(
                (300, 262_143, *LETTER), ValueError, "max_render_pixels must be >= 262144, got 262143", id="pixels"
            ),
            pytest.param((300, 16e6, *LETTER), TypeError, "max_render_pixels must be an int", id="pixels-float"),
            pytest.param((300, MAX_RENDER_PIXELS, 0.0, 792.0), ValueError, "width_pt must be > 0", id="width-zero"),
            pytest.param((300, MAX_RENDER_PIXELS, 612.0, -1.0), ValueError, "height_pt must be > 0", id="height"),
            pytest.param(
                (300, MAX_RENDER_PIXELS, math.nan, 792.0), ValueError, "width_pt must be finite", id="width-nan"
            ),
            pytest.param(
                (300, MAX_RENDER_PIXELS, 612.0, math.inf), ValueError, "height_pt must be finite", id="height-inf"
            ),
            pytest.param((300, MAX_RENDER_PIXELS, "612", 792.0), TypeError, "width_pt must be an int or float", id="w"),
        ],
    )
    def test_invalid_arguments_raise(self, args, error, message):
        with pytest.raises(error, match=re.escape(message)):
            effective_dpi(*args)


class TestRenderSize:
    """render_size is the pixel size a page renders to: each displayed side at ``dpi``, rounded down."""

    @pytest.mark.parametrize(
        "size,dpi,expected",
        [
            pytest.param(LETTER, 300, (2550, 3300), id="letter"),
            pytest.param((611.9, 791.9), 300, (2549, 3299), id="rounds-down"),
            pytest.param((0.2, 100.0), 300, (0, 416), id="under-one-pixel"),
        ],
    )
    def test_each_side_is_rounded_down(self, size, dpi, expected):
        assert render_size(*size, dpi) == expected

    @pytest.mark.parametrize(
        "args,error,message",
        [
            pytest.param((*LETTER, 0), ValueError, "dpi must be >= 1, got 0", id="dpi-zero"),
            pytest.param((*LETTER, 1.5), TypeError, "dpi must be an int", id="dpi-float"),
            pytest.param((0.0, 792.0, 300), ValueError, "width_pt must be > 0", id="width"),
            pytest.param((612.0, math.inf, 300), ValueError, "height_pt must be finite", id="height"),
        ],
    )
    def test_invalid_arguments_raise(self, args, error, message):
        with pytest.raises(error, match=re.escape(message)):
            render_size(*args)
