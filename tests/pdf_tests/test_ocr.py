"""Tests for the v5 vision route's page normalization, bands and tesseract CLI (#369, A5a): ``datagrunt.pdf.ocr``.

Pages here are built pixel by pixel, so every expected value is derived in
the test, never pinned from one platform's fonts or tesseract build.
"""

import re

import pytest

from datagrunt.pdf.ocr import Band, bands, normalized
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
