"""Tests for the v5 vision route's page normalization, bands and tesseract CLI (#369, A5a): ``datagrunt.pdf.ocr``.

Pages here are built pixel by pixel, so every expected value is derived in
the test, never pinned from one platform's fonts or tesseract build.
"""

import re

import pytest

from datagrunt.pdf.ocr import normalized
from datagrunt.pdf.render import RenderedPage


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
