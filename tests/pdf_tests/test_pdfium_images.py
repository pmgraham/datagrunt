"""Tests for the v5 pypdfium2 engine's images (#365 T4).

Fixtures are built with the ReportLab toolkit (``pdf_tests.builders``), whose
coordinates are top-left points in the unrotated page space, or written by
hand (``hand_written_pdf``) for images ReportLab cannot draw: JPEG, Flate and
CCITT streams written byte for byte, inline images, ``cm`` transforms and
forms with a /Matrix. Hand-written content is in PDF user space (bottom-left
origin) on a 400 x 300 pt page, so a display y is ``300 - y``.
"""

import base64
import ctypes
import dataclasses
import gc
import hashlib
import io
import pickle
import re
import struct
import subprocess
import sys
import traceback
import zlib
from functools import partial
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
import pytest
from PIL import Image

from datagrunt.pdf import BBox, DatagruntPDFError, Figure, PDFCorruptError, PDFEncryptedError, _pdfium_images, elements
from datagrunt.pdf._path_segments import Matrix, NonFiniteGeometry
from datagrunt.pdf.pdfium_engine import (
    ImageLoader,
    ImageStats,
    PageImages,
    RawImage,
    open_document,
    page_images,
    to_display_box,
)
from pdf_tests.builders import FormXObject, PageSpec, RasterImage, break_page, build_pdf, hand_written_pdf, solid_png

SHA = hashlib.sha256(b"raw stream").hexdigest()

# RawImage fields for a valid Flate RGB image.
VALID = {
    "bbox": (10.0, 20.0, 110.0, 70.0),
    "px_width": 64,
    "px_height": 32,
    "declared_px": (64, 32),
    "object_index": 0,
    "raw_sha256": SHA,
    "filters": ("FlateDecode",),
    "bits_per_pixel": 24,
    "colorspace": "DeviceRGB",
}


def raw_image(**overrides) -> RawImage:
    """A valid RawImage with ``overrides`` applied."""
    return RawImage(**{**VALID, **overrides})


def stats(
    nonfinite: int = 0, oversized: int = 0, cover: float = 0.0, unsized: int = 0, over_budget: int = 0
) -> ImageStats:
    return ImageStats(
        skipped_nonfinite_images=nonfinite,
        skipped_oversized_images=oversized,
        skipped_unsized_images=unsized,
        skipped_over_budget_images=over_budget,
        max_cover_fraction=cover,
    )


class TestRawImage:
    """RawImage is a frozen, slotted, picklable, validated record."""

    def test_pickle_round_trip(self):
        original = raw_image(filters=("ASCII85Decode", "FlateDecode"), bits_per_pixel=None, colorspace=None)

        assert pickle.loads(pickle.dumps(original)) == original

    def test_frozen_and_slotted(self):
        item = raw_image()

        with pytest.raises(dataclasses.FrozenInstanceError):
            item.px_width = 1  # type: ignore[misc]
        assert not hasattr(item, "__dict__")

    def test_a_stream_not_read_has_no_hash(self):
        assert raw_image(raw_sha256=None, bits_per_pixel=None, colorspace=None).raw_sha256 is None

    def test_int_box_values_are_stored_as_floats_and_zero_pixels_are_kept(self):
        # pdfium reads a missing or zero /Width as 0 px: a raw fact, kept here.
        item = raw_image(bbox=(10, 20, 10, 20), px_width=0, px_height=0)

        assert item.bbox == (10.0, 20.0, 10.0, 20.0)
        assert all(type(value) is float for value in item.bbox)
        assert (item.px_width, item.px_height) == (0, 0)

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"bbox": (0.0, 0.0, 1.0)}, ValueError, "bbox must have 4 values, got 3", id="bbox-size"),
            pytest.param({"bbox": [0.0, 0.0, 1.0, 1.0]}, TypeError, "bbox must be a tuple, got list", id="bbox-list"),
            pytest.param({"bbox": (0.0, 0.0, float("nan"), 1.0)}, ValueError, "bbox[2] must be finite", id="bbox-nan"),
            pytest.param(
                {"bbox": (5.0, 0.0, 1.0, 1.0)},
                ValueError,
                "bbox must have x0 <= x1 and y0 <= y1, got (5.0, 0.0, 1.0, 1.0)",
                id="bbox-reversed",
            ),
            pytest.param({"px_width": -1}, ValueError, "px_width must be >= 0, got -1", id="width-negative"),
            pytest.param({"px_height": True}, TypeError, "px_height must be an int, got True", id="height-bool"),
            pytest.param({"object_index": -1}, ValueError, "object_index must be >= 0, got -1", id="index"),
            pytest.param(
                {"raw_sha256": SHA.upper()}, ValueError, "raw_sha256 must be 64 lowercase hex characters", id="sha"
            ),
            pytest.param({"raw_sha256": 7}, TypeError, "raw_sha256 must be a str, got 7", id="sha-type"),
            pytest.param({"declared_px": [64, 32]}, TypeError, "declared_px must be a tuple, got list", id="declared"),
            pytest.param(
                {"declared_px": (64,)}, ValueError, "declared_px must have 2 values, got 1", id="declared-size"
            ),
            pytest.param(
                {"declared_px": (131072, 1)},
                ValueError,
                "declared_px[0] must be <= 131071, got 131072",
                id="declared-max",
            ),
            pytest.param(
                {"declared_px": (-1, 1)}, ValueError, "declared_px[0] must be >= 0, got -1", id="declared-min"
            ),
            pytest.param({"px_width": 131072}, ValueError, "px_width must be <= 131071, got 131072", id="width-max"),
            pytest.param(
                {"filters": ["FlateDecode"]}, TypeError, "filters must be a tuple of str, got list", id="filters"
            ),
            pytest.param({"filters": (b"DCT",)}, TypeError, "filters[0] must be a str", id="filter-bytes"),
            pytest.param({"bits_per_pixel": 0}, ValueError, "bits_per_pixel must be >= 1, got 0", id="bpp-zero"),
            pytest.param({"bits_per_pixel": 8.0}, TypeError, "bits_per_pixel must be an int, got 8.0", id="bpp"),
            pytest.param(
                {"colorspace": "RGB"},
                ValueError,
                "colorspace must be None or one of ('DeviceGray', 'DeviceRGB', 'DeviceCMYK', 'CalGray', 'CalRGB', "
                "'Lab', 'ICCBased', 'Separation', 'DeviceN', 'Indexed', 'Pattern'), got 'RGB'",
                id="colorspace",
            ),
            pytest.param({"colorspace": 2}, TypeError, "colorspace must be a str or None, got 2", id="colorspace-type"),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            raw_image(**overrides)


class TestPageImagesAndStats:
    """PageImages and ImageStats are frozen, slotted, picklable, validated records."""

    def test_pickle_round_trip(self):
        original = PageImages(images=(raw_image(),), stats=stats(1, 2, 0.5, unsized=3, over_budget=4))

        assert pickle.loads(pickle.dumps(original)) == original
        with pytest.raises(dataclasses.FrozenInstanceError):
            original.stats = stats()  # type: ignore[misc]
        assert not hasattr(original, "__dict__") and not hasattr(original.stats, "__dict__")

    def test_int_cover_fraction_is_stored_as_a_float(self):
        assert type(stats(cover=1).max_cover_fraction) is float

    @pytest.mark.parametrize(
        "make,error,message",
        [
            pytest.param(
                lambda: stats(nonfinite=-1), ValueError, "skipped_nonfinite_images must be >= 0, got -1", id="nonfinite"
            ),
            pytest.param(
                lambda: stats(oversized=True), TypeError, "skipped_oversized_images must be an int, got True", id="big"
            ),
            pytest.param(
                lambda: stats(unsized=-1), ValueError, "skipped_unsized_images must be >= 0, got -1", id="unsized"
            ),
            pytest.param(
                lambda: stats(over_budget=1.0),
                TypeError,
                "skipped_over_budget_images must be an int, got 1.0",
                id="budget",
            ),
            pytest.param(lambda: stats(cover=1.5), ValueError, "max_cover_fraction must be <= 1.0, got 1.5", id="over"),
            pytest.param(
                lambda: stats(cover=-0.25), ValueError, "max_cover_fraction must be >= 0.0, got -0.25", id="under"
            ),
            pytest.param(
                lambda: stats(cover=float("nan")), ValueError, "max_cover_fraction must be finite", id="cover-nan"
            ),
            pytest.param(
                lambda: PageImages(images=[raw_image()], stats=stats()),
                TypeError,
                "images must be a tuple, got list",
                id="images-list",
            ),
            pytest.param(
                lambda: PageImages(images=(raw_image(), "x"), stats=stats()),
                TypeError,
                "images[1] must be a RawImage, got str",
                id="images-item",
            ),
            pytest.param(
                lambda: PageImages(images=(), stats=0), TypeError, "stats must be an ImageStats, got int", id="stats"
            ),
        ],
    )
    def test_invalid_field_raises(self, make, error, message):
        with pytest.raises(error, match=re.escape(message)):
            make()


# A 64 x 32 red PNG: ReportLab embeds it as 8-bit RGB under ASCII85 + Flate.
PNG = solid_png(64, 32, (1.0, 0.0, 0.0))
REPORTLAB_FILTERS = ("ASCII85Decode", "FlateDecode")
# 4 x 2 RGB samples 0..23, written unfiltered (inline) or Flate-compressed.
SAMPLES = bytes(range(24))
FLATE = zlib.compress(SAMPLES)


def one_page(tmp_path: Path, *items, rotate: int = 0, crop_box=None, name: str = "page.pdf", **encryption) -> Path:
    """A one-page 400 x 300 pt PDF holding ``items``."""
    spec = PageSpec(width=400.0, height=300.0, items=items, rotate=rotate, crop_box=crop_box)
    return build_pdf(tmp_path / name, [spec], **encryption)


def content_page(tmp_path: Path, content: bytes, *, extra: tuple[bytes, ...] = (), resources: bytes = b"") -> Path:
    """A hand-written 400 x 300 pt page drawing ``content``; ``extra`` objects are numbered from 5."""
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 300] /Contents 4 0 R /Resources << %s >> >>" % resources,
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        *extra,
    ]
    return hand_written_pdf(tmp_path / "hand.pdf", objects)


def image_xobject(data: bytes, entries: bytes) -> bytes:
    """An image XObject whose stream is ``data``, byte for byte."""
    header = b"<< /Type /XObject /Subtype /Image %s /Length %d >>\nstream\n" % (entries, len(data))
    return header + data + b"\nendstream"


RGB_4X2 = b"/Width 4 /Height 2 /ColorSpace /DeviceRGB /BitsPerComponent 8"
IM = b"/XObject << /Im 5 0 R >>"


def xobject_page(
    tmp_path: Path, data: bytes, entries: bytes, content: bytes = b"q 80 0 0 40 10 20 cm /Im Do Q"
) -> Path:
    """A page drawing one image XObject (stream ``data``) at user space (10, 20)-(90, 60)."""
    return content_page(tmp_path, content, extra=(image_xobject(data, entries),), resources=IM)


def jpeg_bytes(size=(16, 8), rgb=(10, 200, 30)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, rgb).save(buffer, "JPEG", quality=90)
    return buffer.getvalue()


def images_of(path: Path, password: str | None = None) -> PageImages:
    """The page_images of page 1 of ``path``."""
    return page_images(open_document(path, password=password), 0)


def rounded(box) -> tuple[float, ...]:
    """A box rounded to 3 decimals (pdfium matrices are 32-bit floats)."""
    return tuple(round(value, 3) for value in box)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


ROTATIONS = [0, 90, 180, 270]
# Top-left (50, 20)-(350, 260) on the 400 x 300 page.
CROPS = [pytest.param(None, id="no-crop"), pytest.param((50.0, 20.0, 350.0, 260.0), id="offset-crop")]


def displayed_box(box, rotation, crop_box) -> tuple[float, ...]:
    """The displayed box of the top-left page box ``box``, computed by hand per rotation."""
    x0, y0, x1, y1 = crop_box or (0.0, 0.0, 400.0, 300.0)
    across, up = x1 - x0, y1 - y0

    def turned(u, v):
        u, v = u - x0, v - y0
        return {0: (u, v), 90: (up - v, u), 180: (across - u, up - v), 270: (v, across - u)}[rotation]

    (a, b), (c, d) = turned(box[0], box[1]), turned(box[2], box[3])
    return (min(a, c), min(b, d), max(a, c), max(b, d))


class TestPlacement:
    """An image's bbox is its unit square under its full matrix, on the displayed page."""

    @pytest.mark.parametrize("crop_box", CROPS)
    @pytest.mark.parametrize("rotate", ROTATIONS)
    def test_known_top_left_position_on_every_rotation_and_crop(self, tmp_path, rotate, crop_box):
        path = one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG), rotate=rotate, crop_box=crop_box)

        (image,) = images_of(path).images

        assert rounded(image.bbox) == displayed_box((60.0, 40.0, 180.0, 120.0), rotate, crop_box)
        assert (image.px_width, image.px_height, image.object_index) == (64, 32, 0)
        assert (image.filters, image.bits_per_pixel, image.colorspace) == (REPORTLAB_FILTERS, 24, "DeviceRGB")

    def test_an_image_in_a_scaled_form_lands_at_its_page_position(self, tmp_path):
        form = FormXObject(
            x=100.0, y=50.0, w=150.0, h=60.0, items=(RasterImage(10.0, 5.0, 40.0, 20.0, PNG),), scale=2.0
        )
        path = one_page(tmp_path, form)

        # Observed: inside a form, pdfium's image matrix and bounds are in the form's space.
        page = pdfium.PdfDocument(path)[0]
        inner = pdfium_c.FPDFFormObj_GetObject(pdfium_c.FPDFPage_GetObject(page.raw, 0), 0)
        matrix = pdfium_c.FS_MATRIX()
        assert pdfium_c.FPDFPageObj_GetMatrix(inner, matrix)
        assert (matrix.a, matrix.d, matrix.e, matrix.f) == (40.0, 20.0, 10.0, 35.0)
        (image,) = images_of(path).images

        # Form (u, v) at scale 2 from the form's top-left (100, 50) is page (100 + 2u, 50 + 2v).
        assert rounded(image.bbox) == (120.0, 60.0, 200.0, 100.0)
        assert image.object_index == 1

    def test_nested_form_matrices_compose(self, tmp_path):
        # Observed: an image's matrix is its CTM inside its form (that form's
        # /Matrix included); a form object's matrix is the CTM at its Do.
        form_content = b"q 10 0 0 5 1 2 cm /Im Do Q"
        form = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Matrix [3 0 0 3 5 7] "
        form += b"/Resources << /XObject << /Im 6 0 R >> >> /Length %d >>\nstream\n%s\nendstream" % (
            len(form_content),
            form_content,
        )
        image = image_xobject(FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        path = content_page(
            tmp_path, b"q 1 0 0 1 100 50 cm /X Do Q", extra=(form, image), resources=b"/XObject << /X 5 0 R >>"
        )

        (got,) = images_of(path).images

        # Unit square -> scale (10, 5) + (1, 2) -> form matrix 3x + (5, 7) -> page + (100, 50):
        # user space (108, 63)-(138, 78), displayed y = 300 - y.
        assert rounded(got.bbox) == (108.0, 222.0, 138.0, 237.0)
        assert (got.object_index, got.px_width, got.px_height) == (1, 4, 2)

    def test_a_rotating_matrix_gives_the_bounding_box_of_the_turned_square(self, tmp_path):
        # A quarter turn: the unit square's corners land at (90, 20), (90, 60), (50, 20), (50, 60).
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode", b"q 0 40 -40 0 90 20 cm /Im Do Q")

        (image,) = images_of(path).images

        assert rounded(image.bbox) == (50.0, 240.0, 90.0, 280.0)

    def test_objects_are_numbered_by_the_shared_page_walk(self, tmp_path):
        # Text, a path and two draws of one XObject: images keep their walk indices.
        content = b"BT /F1 12 Tf 20 270 Td (Hi) Tj ET 10 250 m 200 250 l S q 80 0 0 40 10 20 cm /Im Do Q "
        content += b"q 20 0 0 10 300 20 cm /Im Do Q"
        font = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
        image = image_xobject(FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        path = content_page(tmp_path, content, extra=(image, font), resources=IM + b" /Font << /F1 6 0 R >>")

        got = images_of(path).images

        assert [item.object_index for item in got] == [2, 3]
        assert got[0].raw_sha256 == got[1].raw_sha256 == sha(FLATE)


class TestInlineImages:
    """Inline images (BI ... ID ... EI) are image objects like XObjects."""

    def test_an_inline_image_is_read_with_its_data_hashed(self, tmp_path):
        path = content_page(tmp_path, b"q 80 0 0 40 10 20 cm BI /W 4 /H 2 /CS /RGB /BPC 8 ID " + SAMPLES + b" EI Q")

        (image,) = images_of(path).images

        assert rounded(image.bbox) == (10.0, 240.0, 90.0, 280.0)
        assert (image.px_width, image.px_height, image.filters) == (4, 2, ())
        assert (image.bits_per_pixel, image.colorspace) == (24, "DeviceRGB")
        assert image.raw_sha256 == sha(SAMPLES)

    def test_an_abbreviated_inline_filter_reads_by_its_full_name(self, tmp_path):
        encoded = SAMPLES.hex().encode() + b">"
        content = b"q 80 0 0 40 10 20 cm BI /W 4 /H 2 /CS /RGB /BPC 8 /F /AHx ID " + encoded + b" EI Q"

        (image,) = images_of(content_page(tmp_path, content)).images

        assert image.filters == ("ASCIIHexDecode",)
        assert image.raw_sha256 == sha(encoded)


class TestRawStreamHash:
    """raw_sha256 hashes the embedded stream as written: stable, and the same for the same stream."""

    def test_a_jpeg_hashes_to_the_jpeg_bytes_written(self, tmp_path):
        jpeg = jpeg_bytes()
        path = xobject_page(
            tmp_path, jpeg, b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode"
        )

        first, second = images_of(path).images, images_of(path).images

        assert first == second
        (image,) = first
        assert image.raw_sha256 == sha(jpeg)
        assert (image.filters, image.px_width, image.px_height) == (("DCTDecode",), 16, 8)

    def test_a_flate_stream_hashes_to_the_compressed_bytes_written(self, tmp_path):
        (image,) = images_of(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode")).images

        assert image.raw_sha256 == sha(FLATE) != sha(SAMPLES)
        assert image.filters == ("FlateDecode",)

    def test_the_same_pixels_jpeg_and_flate_encoded_hash_differently(self, tmp_path):
        pixels = Image.new("RGB", (16, 8), (10, 200, 30))
        flate = zlib.compress(pixels.tobytes())
        entries = b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter "

        (as_jpeg,) = images_of(xobject_page(tmp_path, jpeg_bytes(), entries + b"/DCTDecode")).images
        (as_flate,) = images_of(xobject_page(tmp_path, flate, entries + b"/FlateDecode")).images

        assert as_jpeg.raw_sha256 != as_flate.raw_sha256

    def test_encryption_does_not_change_the_hash(self, tmp_path):
        # Observed: pdfium decrypts the stream; the raw bytes are still filter-encoded.
        image = RasterImage(60.0, 40.0, 120.0, 80.0, PNG)
        plain = one_page(tmp_path, image, name="plain.pdf")
        locked = one_page(tmp_path, image, name="locked.pdf", user_password="user-pw")

        assert images_of(plain).images == images_of(locked, password="user-pw").images


class TestPixelMetadata:
    """bits_per_pixel and colorspace are pdfium's view of the decoded image."""

    @pytest.mark.parametrize(
        "data,entries,expected",
        [
            pytest.param(
                bytes([0xAA, 0x55]),
                b"/Width 8 /Height 2 /ColorSpace /DeviceGray /BitsPerComponent 1",
                (1, "DeviceGray"),
                id="gray-1bit",
            ),
            pytest.param(bytes([0xAA, 0x55]), b"/Width 8 /Height 2 /ImageMask true", (1, None), id="image-mask"),
            # pdfium converts CMYK to RGB: 24 bits per pixel.
            pytest.param(
                bytes(range(32)),
                b"/Width 4 /Height 2 /ColorSpace /DeviceCMYK /BitsPerComponent 8",
                (24, "DeviceCMYK"),
                id="cmyk",
            ),
            pytest.param(
                bytes([0, 1, 1, 0]),
                b"/Width 2 /Height 2 /ColorSpace [/Indexed /DeviceRGB 1 <ff0000 00ff00>] /BitsPerComponent 8",
                (8, "Indexed"),
                id="indexed",
            ),
        ],
    )
    def test_bits_per_pixel_and_colorspace(self, tmp_path, data, entries, expected):
        (image,) = images_of(xobject_page(tmp_path, data, entries)).images

        assert (image.bits_per_pixel, image.colorspace) == expected

    @pytest.mark.parametrize(
        "data,entries,size",
        [
            pytest.param(
                bytes(24), b"/Width 0 /Height 2 /ColorSpace /DeviceRGB /BitsPerComponent 8", (0, 2), id="zero"
            ),
            pytest.param(bytes(24), b"/Height 2 /ColorSpace /DeviceRGB /BitsPerComponent 8", (0, 2), id="missing"),
            pytest.param(b"", RGB_4X2, (4, 2), id="empty-stream"),
        ],
    )
    def test_an_image_pdfium_cannot_load_is_kept_without_pixel_metadata(self, tmp_path, data, entries, size):
        (image,) = images_of(xobject_page(tmp_path, data, entries)).images

        assert (image.px_width, image.px_height) == size
        assert (image.bits_per_pixel, image.colorspace, image.raw_sha256) == (None, None, sha(data))


def unread_image(bbox, object_index: int, **facts) -> RawImage:
    """The RawImage of an image whose stream was not read: no hash, no pixel size, no pixel metadata."""
    fields = {"px_width": 0, "px_height": 0, "raw_sha256": None, "bits_per_pixel": None, "colorspace": None}
    return RawImage(bbox=bbox, object_index=object_index, **{**fields, **facts})


def cover_of(path: Path) -> float:
    return images_of(path).stats.max_cover_fraction


class TestCoverFraction:
    """max_cover_fraction: the most of the visible page one image paints (A3's full-bleed signal)."""

    @pytest.mark.parametrize("rotate", ROTATIONS)
    def test_a_full_page_image_covers_all_of_it_on_every_rotation(self, tmp_path, rotate):
        assert cover_of(one_page(tmp_path, RasterImage(0.0, 0.0, 400.0, 300.0, PNG), rotate=rotate)) == 1.0

    def test_a_small_image_covers_its_share(self, tmp_path):
        # 120 x 80 of 400 x 300.
        assert cover_of(one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG))) == pytest.approx(0.08)

    def test_only_the_part_on_the_visible_page_counts(self, tmp_path):
        # 60 x 50 of the 120 x 100 image is on the page.
        assert cover_of(one_page(tmp_path, RasterImage(340.0, 250.0, 120.0, 100.0, PNG))) == pytest.approx(0.025)

    @pytest.mark.parametrize(
        "image,expected",
        [
            pytest.param(RasterImage(0.0, 0.0, 400.0, 300.0, PNG), 1.0, id="whole-media-box"),
            # Inside the 300 x 240 crop: x 50-100, y 20-100.
            pytest.param(RasterImage(0.0, 0.0, 100.0, 100.0, PNG), 50 * 80 / (300 * 240), id="corner"),
        ],
    )
    def test_the_fraction_is_of_the_crop_box(self, tmp_path, image, expected):
        assert cover_of(one_page(tmp_path, image, crop_box=(50.0, 20.0, 350.0, 260.0))) == pytest.approx(expected)

    def test_a_turned_image_covers_its_painted_area_not_its_bbox(self, tmp_path):
        # A diamond, corners (200, 50), (350, 200), (200, 350), (50, 200): area 45000,
        # less the 2500 triangle above y = 300. Its bbox would cover 0.75.
        path = xobject_page(
            tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode", b"q 150 150 -150 150 200 50 cm /Im Do Q"
        )

        assert cover_of(path) == pytest.approx(42500 / 120000)

    def test_the_largest_image_counts_and_no_image_covers_nothing(self, tmp_path):
        small, large = RasterImage(10.0, 10.0, 40.0, 30.0, PNG), RasterImage(100.0, 100.0, 200.0, 150.0, PNG)

        assert cover_of(one_page(tmp_path, small, large, small)) == pytest.approx(0.25)
        assert images_of(one_page(tmp_path, name="blank.pdf")) == PageImages(images=(), stats=stats())


# pdfium keeps the CTM in 32-bit floats: five of these overflow it to (inf, nan, nan, inf, 0, 0).
OVERFLOW = b"1000000000 0 0 1000000000 0 0 cm " * 5


def writes_matrix(*values):
    def stand_in(obj, matrix):
        matrix.a, matrix.b, matrix.c, matrix.d, matrix.e, matrix.f = values
        return 1

    return stand_in


class TestSkippedImages:
    """Non-finite and oversized images are skipped and counted; the page survives."""

    def test_images_drawn_under_an_overflowing_cm_are_skipped(self, tmp_path):
        content = b"BT /F1 12 Tf 20 270 Td (Visible) Tj ET q " + OVERFLOW + b"/Im Do BI /W 4 /H 2 /CS /RGB /BPC 8 ID "
        content += SAMPLES + b" EI Q q 80 0 0 40 10 20 cm /Im Do Q"
        font = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
        image = image_xobject(FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        path = content_page(tmp_path, content, extra=(image, font), resources=IM + b" /Font << /F1 6 0 R >>")

        got = images_of(path)

        assert [(item.object_index, rounded(item.bbox)) for item in got.images] == [(3, (10.0, 240.0, 90.0, 280.0))]
        assert got.stats == stats(nonfinite=2, cover=80 * 40 / 120000)

    def test_every_image_in_a_form_drawn_under_an_overflowing_cm_is_skipped(self, tmp_path):
        form_content = b"q 10 0 0 5 1 2 cm /Im Do Q BI /W 4 /H 2 /CS /RGB /BPC 8 ID " + SAMPLES + b" EI"
        form = b"<< /Type /XObject /Subtype /Form /BBox [0 0 400 300] /Resources << /XObject << /Im 6 0 R >> >> "
        form += b"/Length %d >>\nstream\n%s\nendstream" % (len(form_content), form_content)
        image = image_xobject(FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        path = content_page(
            tmp_path, b"q " + OVERFLOW + b"/X Do Q", extra=(form, image), resources=b"/XObject << /X 5 0 R >>"
        )

        assert images_of(path) == PageImages(images=(), stats=stats(nonfinite=2))

    def test_a_non_finite_matrix_from_pdfium_skips_the_image(self, tmp_path, monkeypatch):
        handle = open_document(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        monkeypatch.setattr(pdfium_c, "FPDFPageObj_GetMatrix", writes_matrix(80.0, 0.0, 0.0, float("inf"), 0.0, 0.0))

        assert page_images(handle, 0) == PageImages(images=(), stats=stats(nonfinite=1))

    def test_a_raw_stream_over_the_asset_limit_is_recorded_without_its_hash(self, tmp_path, monkeypatch):
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        monkeypatch.setattr(_pdfium_images, "_MAX_ASSET_BYTES", len(FLATE) - 1)

        unread = unread_image((10.0, 240.0, 90.0, 280.0), 0, declared_px=(4, 2), filters=("FlateDecode",))
        assert images_of(path) == PageImages(images=(unread,), stats=stats(oversized=1, cover=80 * 40 / 120000))

    def test_a_raw_stream_at_the_asset_limit_is_read(self, tmp_path, monkeypatch):
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        monkeypatch.setattr(_pdfium_images, "_MAX_ASSET_BYTES", len(FLATE))

        assert [item.raw_sha256 for item in images_of(path).images] == [sha(FLATE)]

    def test_the_raw_stream_limit_is_the_asset_limit(self):
        assert _pdfium_images._MAX_ASSET_BYTES == elements._MAX_ASSET_BYTES == 1 << 30


@pytest.fixture
def gc_disabled():
    """Run a test with the cyclic GC off, so only refcounting can close handles."""
    was_enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        yield
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()


@pytest.fixture
def page_closes(monkeypatch):
    """Record each explicit PdfPage close() call (not a parent's cascade)."""
    calls = []

    def counting_close(self, *args, _original=pdfium.PdfPage.close, **kwargs):
        if not kwargs.get("_by_parent", False):
            calls.append("PdfPage")
        return _original(self, *args, **kwargs)

    monkeypatch.setattr(pdfium.PdfPage, "close", counting_close)
    return calls


def failing(value):
    """A pdfium stand-in that reports failure by returning ``value``."""
    return lambda *args: value


def copies_short(name):
    """A stand-in for pdfium function ``name`` that reports a length, then copies one byte less."""
    original = getattr(pdfium_c, name)

    def stand_in(*args):
        size = original(*args)
        return size if args[-2] is None else size - 1

    return stand_in


def writes_colorspace(value):
    def stand_in(image, page, metadata):
        metadata.colorspace = value
        return 1

    return stand_in


# (pdfium function, stand-in, message): each makes page_images raise PDFCorruptError for page 1.
PDFIUM_FAILURES = [
    ("FPDFImageObj_GetImageFilterCount", failing(-1), "page 1: pdfium cannot count the filters of object 0"),
    (
        "FPDFImageObj_GetImageFilter",
        failing(0),
        "page 1: pdfium cannot read filter 0 of object 0 (0 bytes; the bound is 4096)",
    ),
    (
        "FPDFImageObj_GetImageFilter",
        failing(4097),
        "page 1: pdfium cannot read filter 0 of object 0 (4097 bytes; the bound is 4096)",
    ),
    ("FPDFImageObj_GetImageFilter", copies_short("FPDFImageObj_GetImageFilter"), "cannot read filter 0 of object 0"),
    (
        "FPDFImageObj_GetImageDataRaw",
        copies_short("FPDFImageObj_GetImageDataRaw"),
        "cannot read the raw stream of object 0",
    ),
    ("FPDFImageObj_GetImageMetadata", failing(0), "page 1: pdfium cannot read the metadata of object 0"),
    ("FPDFImageObj_GetImagePixelSize", failing(0), "page 1: pdfium cannot read the pixel size of object 0"),
    (
        "FPDFImageObj_GetImageMetadata",
        writes_colorspace(12),
        "page 1: pdfium cannot read the metadata of object 0: unknown colour space 12",
    ),
    ("FPDFPageObj_GetMatrix", failing(0), "page 1: pdfium cannot read the matrix of object 0"),
    ("FPDFPage_CountObjects", failing(-1), "page 1: pdfium cannot count the objects of the page"),
    ("FPDFPage_GetObject", failing(None), "page 1: pdfium cannot read object 0 of the page"),
]


class TestPageImagesErrors:
    """page_images raises PDFCorruptError for the page when pdfium fails, and closes the page."""

    def test_closes_the_page(self, tmp_path, page_closes):
        page_images(open_document(one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG))), 0)

        assert page_closes == ["PdfPage"]

    @pytest.mark.parametrize("function,stand_in,message", PDFIUM_FAILURES)
    def test_pdfium_failure_raises_for_the_page_and_closes_it(
        self, tmp_path, monkeypatch, page_closes, function, stand_in, message
    ):
        handle = open_document(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        with pytest.raises(PDFCorruptError, match=re.escape(message)) as caught:
            page_images(handle, 0)

        assert caught.value.page_number == 1
        assert page_closes == ["PdfPage"]

    def test_unloadable_page_raises_for_that_page_only(self, tmp_path):
        pages = [PageSpec(items=(RasterImage(10.0, 10.0, 10.0 * n, 10.0, PNG),)) for n in (1, 2, 3)]
        handle = open_document(break_page(build_pdf(tmp_path / "three.pdf", pages), 2))

        with pytest.raises(PDFCorruptError, match="Failed to load page") as caught:
            page_images(handle, 1)

        assert caught.value.page_number == 2
        assert [rounded(page_images(handle, index).images[0].bbox)[2] for index in (0, 2)] == [20.0, 40.0]

    @pytest.mark.parametrize(
        "index,error,message",
        [
            pytest.param(-1, IndexError, "index must be in 0..0, got -1", id="negative"),
            pytest.param(1, IndexError, "index must be in 0..0, got 1", id="past-end"),
            pytest.param(True, TypeError, "index must be an int, got True (bool)", id="bool"),
        ],
    )
    def test_invalid_index_raises(self, tmp_path, index, error, message):
        handle = open_document(one_page(tmp_path))

        with pytest.raises(error, match=re.escape(message)):
            page_images(handle, index)

    def test_non_handle_and_closed_handle_raise(self, tmp_path):
        handle = open_document(one_page(tmp_path))
        handle.close()

        with pytest.raises(TypeError, match=re.escape("handle must be a DocumentHandle, got str")):
            page_images("doc.pdf", 0)
        with pytest.raises(ValueError, match="document handle is closed"):
            page_images(handle, 0)


PASSWORD = "s3cret-pw"


def loader(**overrides) -> ImageLoader:
    fields = {"source": Path("/docs/doc.pdf"), "page_index": 0, "image": raw_image()}
    return ImageLoader(**{**fields, **overrides})


class TestImageLoaderRecord:
    """ImageLoader is a frozen, slotted, picklable record that never shows its password."""

    def test_pickle_round_trip_keeps_the_password(self):
        original = loader(page_index=2, image=raw_image(object_index=5), password=PASSWORD)

        # Our own object, pickled and unpickled in this test: no untrusted data.
        restored = pickle.loads(pickle.dumps(original))

        assert restored == original
        assert restored.password == PASSWORD

    def test_frozen_and_slotted(self):
        item = loader()

        with pytest.raises(dataclasses.FrozenInstanceError):
            item.page_index = 1  # type: ignore[misc]
        assert not hasattr(item, "__dict__")

    def test_the_password_is_not_in_repr_str_equality_or_hash(self):
        locked = loader(password=PASSWORD)

        assert PASSWORD not in repr(locked) and PASSWORD not in str(locked)
        assert "password" not in repr(locked)
        assert locked == loader(password="other") == loader()
        assert hash(locked) == hash(loader())

    def test_names_the_image_it_verifies(self):
        image_loader = loader(image=raw_image(object_index=5))

        assert (image_loader.object_index, image_loader.raw_sha256) == (5, SHA)

    def test_a_str_source_becomes_an_absolute_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        assert loader(source="doc.pdf").source == tmp_path / "doc.pdf"

    @pytest.mark.parametrize(
        "overrides,error,message",
        [
            pytest.param({"source": b"doc.pdf"}, TypeError, "source must be a str or os.PathLike, got bytes", id="src"),
            pytest.param({"page_index": -1}, ValueError, "page_index must be >= 0, got -1", id="page"),
            pytest.param({"page_index": True}, TypeError, "page_index must be an int, got True", id="page-bool"),
            pytest.param({"image": object()}, TypeError, "image must be a RawImage, got object", id="image"),
            pytest.param(
                {"image": raw_image(raw_sha256=None)},
                ValueError,
                "image has no raw_sha256 (its stream was not read): there is nothing to verify a read against",
                id="no-hash",
            ),
        ],
    )
    def test_invalid_field_raises(self, overrides, error, message):
        with pytest.raises(error, match=re.escape(message)):
            loader(**overrides)

    @pytest.mark.parametrize(
        "password,error,message",
        [
            pytest.param(12345678, TypeError, "password must be a str or None, got int", id="type"),
            pytest.param(PASSWORD + "\x00x", ValueError, "password must not contain NUL", id="nul"),
        ],
    )
    def test_an_invalid_password_raises_without_showing_it(self, password, error, message):
        with pytest.raises(error, match=re.escape(message)) as caught:
            loader(password=password)

        assert str(password) not in str(caught.value) and PASSWORD not in str(caught.value)


JPEG_ENTRIES = b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode"


def loader_for(path: Path, image: RawImage, page_index: int = 0, password: str | None = None) -> ImageLoader:
    """The loader of ``image`` (from page ``page_index`` of ``path``)."""
    return ImageLoader(source=path, page_index=page_index, image=image, password=password)


def only_loader(path: Path, password: str | None = None) -> ImageLoader:
    """The loader of the one image on page 1 of ``path``."""
    (image,) = images_of(path, password=password).images
    return loader_for(path, image, password=password)


def open_pdfium_documents() -> int:
    """Count pypdfium2 documents whose handle is still open, without collecting."""
    return sum(
        1
        for obj in gc.get_objects()
        if isinstance(obj, pdfium.PdfDocument) and obj._finalizer is not None and obj._finalizer.alive
    )


def chained_text(error: BaseException) -> str:
    """The error and every error it chains to, formatted as a traceback would print them."""
    return "".join(traceback.format_exception(error))


class TestRawBytes:
    """raw_bytes() returns the raw stream, after checking the object is still that image."""

    def test_returns_the_jpeg_bytes_written(self, tmp_path):
        jpeg = jpeg_bytes()

        assert only_loader(xobject_page(tmp_path, jpeg, JPEG_ENTRIES)).raw_bytes() == jpeg

    def test_reads_an_inline_image_and_an_image_in_a_form(self, tmp_path):
        inline = content_page(tmp_path, b"q 80 0 0 40 10 20 cm BI /W 4 /H 2 /CS /RGB /BPC 8 ID " + SAMPLES + b" EI Q")
        form = FormXObject(x=100.0, y=50.0, w=150.0, h=60.0, items=(RasterImage(10.0, 5.0, 40.0, 20.0, PNG),))
        in_form = one_page(tmp_path, form)
        (image,) = images_of(in_form).images

        assert only_loader(inline).raw_bytes() == SAMPLES
        assert hashlib.sha256(loader_for(in_form, image).raw_bytes()).hexdigest() == image.raw_sha256

    def test_reads_an_image_on_a_later_page(self, tmp_path):
        pages = [PageSpec(), PageSpec(items=(RasterImage(10.0, 10.0, 40.0, 30.0, PNG),))]
        path = build_pdf(tmp_path / "two.pdf", pages)
        (image,) = page_images(open_document(path), 1).images

        assert hashlib.sha256(loader_for(path, image, page_index=1).raw_bytes()).hexdigest() == image.raw_sha256

    def test_an_unpickled_loader_reads_an_encrypted_pdf(self, tmp_path):
        path = one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG), user_password=PASSWORD)
        original = only_loader(path, password=PASSWORD)

        # Our own object, pickled and unpickled in this test: no untrusted data.
        restored = pickle.loads(pickle.dumps(original))

        assert restored.raw_bytes() == original.raw_bytes()
        assert hashlib.sha256(restored.raw_bytes()).hexdigest() == original.raw_sha256

    def test_a_wrong_password_fails_without_showing_either_password(self, tmp_path):
        path = one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG), user_password=PASSWORD)
        wrong = dataclasses.replace(only_loader(path, password=PASSWORD), password="not-the-" + PASSWORD)

        with pytest.raises(PDFEncryptedError, match="password is incorrect") as caught:
            wrong.raw_bytes()

        assert PASSWORD not in chained_text(caught.value)

    def test_closes_the_page_and_the_document(self, tmp_path, page_closes, gc_disabled):
        image_loader = only_loader(xobject_page(tmp_path, jpeg_bytes(), JPEG_ENTRIES))
        page_closes.clear()
        before = open_pdfium_documents()

        image_loader.raw_bytes()

        assert page_closes == ["PdfPage"]
        assert open_pdfium_documents() == before

    def test_a_missing_file_fails_loud(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            loader(source=tmp_path / "gone.pdf").raw_bytes()

    def test_an_oversized_raw_stream_is_refused(self, tmp_path, monkeypatch):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        monkeypatch.setattr(_pdfium_images, "_MAX_ASSET_BYTES", len(FLATE) - 1)

        message = (
            f"page 1: the raw stream of object 0 is {len(FLATE)} bytes, over the {len(FLATE) - 1}-byte asset limit"
        )
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            image_loader.raw_bytes()

    def test_a_pdfium_failure_raises_for_the_page(self, tmp_path, monkeypatch, page_closes):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        page_closes.clear()
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImageDataRaw", copies_short("FPDFImageObj_GetImageDataRaw"))

        with pytest.raises(PDFCorruptError, match="page 1: pdfium cannot read the raw stream of object 0") as caught:
            image_loader.raw_bytes()

        assert caught.value.page_number == 1
        assert page_closes == ["PdfPage"]


def rewrite(path: Path, content: bytes, *extra: bytes, resources: bytes = IM) -> None:
    """Replace the file at ``path`` with a hand-written page drawing ``content``."""
    content_page(path.parent, content, extra=extra, resources=resources).replace(path)


class TestChangedFile:
    """A loader refuses to read anything but the image it was made for."""

    def changed(self, image_loader: ImageLoader, detail: str):
        expected = (
            f"{image_loader.source}: page 1, object {image_loader.object_index} is not the image this loader "
            f"was made for: the file changed ({detail})"
        )
        for read in (image_loader.raw_bytes, image_loader.png_bytes):
            with pytest.raises(DatagruntPDFError, match=re.escape(expected)):
                read()

    def test_a_different_image_in_its_place(self, tmp_path):
        path = xobject_page(tmp_path, jpeg_bytes(), JPEG_ENTRIES)
        image_loader = only_loader(path)
        other = jpeg_bytes(rgb=(200, 10, 30))
        rewrite(path, b"q 80 0 0 40 10 20 cm /Im Do Q", image_xobject(other, JPEG_ENTRIES))

        self.changed(image_loader, f"its raw stream hashes to {sha(other)}, not {image_loader.raw_sha256}")

    def test_another_kind_of_object_in_its_place(self, tmp_path):
        path = xobject_page(tmp_path, jpeg_bytes(), JPEG_ENTRIES)
        image_loader = only_loader(path)
        rewrite(path, b"10 10 m 100 10 l S")

        self.changed(image_loader, "object 0 is a path object, not an image")

    def test_fewer_objects_on_the_page(self, tmp_path):
        path = xobject_page(tmp_path, jpeg_bytes(), JPEG_ENTRIES, b"10 10 m 100 10 l S q 80 0 0 40 10 20 cm /Im Do Q")
        image_loader = only_loader(path)
        rewrite(path, b"10 10 m 100 10 l S")

        self.changed(image_loader, "the page has 1 object(s)")

    def test_fewer_pages(self, tmp_path):
        pages = [PageSpec(), PageSpec(items=(RasterImage(10.0, 10.0, 40.0, 30.0, PNG),))]
        path = build_pdf(tmp_path / "two.pdf", pages)
        (image,) = page_images(open_document(path), 1).images
        image_loader = loader_for(path, image, page_index=1)
        build_pdf(path, pages[:1])

        expected = (
            f"{path}: page 2, object 0 is not the image this loader was made for: the file changed (it has 1 page(s))"
        )
        with pytest.raises(DatagruntPDFError, match=re.escape(expected)):
            image_loader.raw_bytes()

    def test_the_error_never_shows_the_password(self, tmp_path):
        image = RasterImage(60.0, 40.0, 120.0, 80.0, PNG)
        path = one_page(tmp_path, image, user_password=PASSWORD)
        image_loader = only_loader(path, password=PASSWORD)
        one_page(
            tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, solid_png(8, 8, (0.0, 0.0, 1.0))), user_password=PASSWORD
        )

        with pytest.raises(DatagruntPDFError, match="the file changed") as caught:
            image_loader.raw_bytes()

        assert PASSWORD not in chained_text(caught.value)


def ccitt_g4(image: Image.Image) -> bytes:
    """The CCITT Group 4 data Pillow (libtiff) writes for a 1-bit image: its TIFF's one strip."""
    buffer = io.BytesIO()
    image.save(buffer, "TIFF", compression="group4")
    tiff = Image.open(io.BytesIO(buffer.getvalue()))
    (offset,), (count,) = tiff.tag_v2[273], tiff.tag_v2[279]
    return buffer.getvalue()[offset : offset + count]


def decoded(png: bytes) -> Image.Image:
    """``png`` opened with Pillow, checked to be a PNG."""
    assert png.startswith(b"\x89PNG\r\n\x1a\n")
    image = Image.open(io.BytesIO(png))
    image.load()
    return image


PNG_SCRIPT = """
import hashlib, pickle, sys
loader = pickle.loads(bytes.fromhex(sys.argv[1]))
sys.stdout.write(hashlib.sha256(loader.png_bytes()).hexdigest())
"""


def png_sha_in_a_fresh_process(image_loader: ImageLoader) -> str:
    """The SHA-256 of ``image_loader.png_bytes()``, computed by a new interpreter from its pickle."""
    # Our own loader, pickled for a child interpreter we start: no untrusted data.
    argument = pickle.dumps(image_loader).hex()
    done = subprocess.run(
        [sys.executable, "-c", PNG_SCRIPT, argument], capture_output=True, check=True, timeout=120, text=True
    )
    return done.stdout


class TestPngBytes:
    """png_bytes() decodes the image with pdfium at its own pixel size and encodes it as PNG."""

    def test_a_flate_image_decodes_to_its_samples(self, tmp_path):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))

        got = decoded(image_loader.png_bytes())

        assert (got.mode, got.size, got.tobytes()) == ("RGB", (4, 2), SAMPLES)

    def test_an_inline_image_decodes_to_its_samples(self, tmp_path):
        path = content_page(tmp_path, b"q 80 0 0 40 10 20 cm BI /W 4 /H 2 /CS /RGB /BPC 8 ID " + SAMPLES + b" EI Q")

        assert decoded(only_loader(path).png_bytes()).tobytes() == SAMPLES

    def test_a_jpeg_decodes_at_its_own_size_not_its_drawn_size(self, tmp_path):
        # Drawn 80 x 40 pt; decoded at its 16 x 8 samples.
        got = decoded(only_loader(xobject_page(tmp_path, jpeg_bytes(rgb=(10, 200, 30)), JPEG_ENTRIES)).png_bytes())

        assert (got.mode, got.size) == ("RGB", (16, 8))
        assert all(abs(a - b) <= 3 for a, b in zip(got.getpixel((5, 3)), (10, 200, 30)))

    def test_a_ccitt_image_decodes_to_its_bilevel_pixels(self, tmp_path):
        original = Image.new("1", (16, 4), 1)
        for x in range(0, 16, 2):
            original.putpixel((x, 1), 0)
        entries = b"/Width 16 /Height 4 /ColorSpace /DeviceGray /BitsPerComponent 1 /Filter /CCITTFaxDecode "
        entries += b"/DecodeParms << /K -1 /Columns 16 /Rows 4 /BlackIs1 true >>"

        got = decoded(only_loader(xobject_page(tmp_path, ccitt_g4(original), entries)).png_bytes())

        assert (got.mode, got.size) == ("L", (16, 4))
        assert got.tobytes() == original.convert("L").tobytes()

    def test_a_1_bit_gray_image_decodes_to_black_and_white(self, tmp_path):
        entries = b"/Width 8 /Height 2 /ColorSpace /DeviceGray /BitsPerComponent 1"

        got = decoded(only_loader(xobject_page(tmp_path, bytes([0xAA, 0x55]), entries)).png_bytes())

        assert got.mode == "L" and list(got.tobytes()) == [255, 0] * 4 + [0, 255] * 4

    def test_the_bytes_are_the_same_in_this_and_two_fresh_processes(self, tmp_path):
        image_loader = only_loader(one_page(tmp_path, RasterImage(60.0, 40.0, 120.0, 80.0, PNG)))
        here = image_loader.png_bytes()

        assert image_loader.png_bytes() == here
        assert png_sha_in_a_fresh_process(image_loader) == png_sha_in_a_fresh_process(image_loader) == sha(here)
        assert decoded(here).tobytes() == Image.open(io.BytesIO(PNG)).convert("RGB").tobytes()

    def test_the_pixel_bound_is_pillows_default_decompression_bomb_bound(self):
        assert _pdfium_images._MAX_DECODED_PIXELS == elements._MAX_ASSET_BYTES // 4 // 3 == 89_478_485

    def test_an_image_over_the_pixel_bound_is_refused_before_decoding(self, tmp_path, monkeypatch):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        monkeypatch.setattr(_pdfium_images, "_MAX_DECODED_PIXELS", 7)
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetBitmap", lambda image: pytest.fail("decoded anyway"))

        message = "page 1: object 0 decodes to 4 x 2 = 8 pixels, over the 7-pixel bound"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            image_loader.png_bytes()
        assert hashlib.sha256(image_loader.raw_bytes()).hexdigest() == image_loader.raw_sha256

    def test_an_image_at_the_pixel_bound_is_decoded(self, tmp_path, monkeypatch):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        monkeypatch.setattr(_pdfium_images, "_MAX_DECODED_PIXELS", 8)

        assert decoded(image_loader.png_bytes()).size == (4, 2)

    def test_a_bitmap_bigger_than_its_header_is_refused_and_freed(self, tmp_path, monkeypatch):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        destroyed = []
        destroy = pdfium_c.FPDFBitmap_Destroy
        monkeypatch.setattr(_pdfium_images, "_MAX_DECODED_PIXELS", 8)
        monkeypatch.setattr(pdfium_c, "FPDFBitmap_GetWidth", failing(5))
        monkeypatch.setattr(pdfium_c, "FPDFBitmap_Destroy", lambda bitmap: destroyed.append(destroy(bitmap)))

        with pytest.raises(DatagruntPDFError, match=re.escape("object 0 decodes to 5 x 2 = 10 pixels")):
            image_loader.png_bytes()
        assert len(destroyed) == 1

    @pytest.mark.parametrize(
        "function,stand_in,message",
        [
            ("FPDFImageObj_GetImagePixelSize", failing(0), "page 1: pdfium cannot read the pixel size of object 0"),
            ("FPDFImageObj_GetBitmap", failing(None), "page 1: pdfium cannot decode object 0"),
            ("FPDFBitmap_GetFormat", failing(5), "page 1: pdfium cannot decode object 0: unknown bitmap format 5"),
            ("FPDFBitmap_GetStride", failing(11), "page 1: pdfium cannot decode object 0: a 4 x 2 bitmap of stride 11"),
            ("FPDFBitmap_GetBuffer", failing(None), "page 1: pdfium cannot read the decoded pixels of object 0"),
        ],
    )
    def test_a_pdfium_failure_raises_for_the_page_and_frees_the_bitmap(
        self, tmp_path, monkeypatch, page_closes, function, stand_in, message
    ):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        page_closes.clear()
        created, destroyed = [], []
        get_bitmap, destroy = pdfium_c.FPDFImageObj_GetBitmap, pdfium_c.FPDFBitmap_Destroy
        monkeypatch.setattr(
            pdfium_c, "FPDFImageObj_GetBitmap", lambda image: created.append(get_bitmap(image)) or created[-1]
        )
        monkeypatch.setattr(pdfium_c, "FPDFBitmap_Destroy", lambda bitmap: destroyed.append(destroy(bitmap)))
        monkeypatch.setattr(pdfium_c, function, stand_in)

        with pytest.raises(PDFCorruptError, match=re.escape(message)) as caught:
            image_loader.png_bytes()

        assert caught.value.page_number == 1
        assert page_closes == ["PdfPage"]
        assert len(destroyed) == len([bitmap for bitmap in created if bitmap])

    def test_an_image_without_a_pixel_size_is_refused_before_decoding(self, tmp_path, monkeypatch):
        image_loader = only_loader(
            xobject_page(tmp_path, bytes(24), b"/Width 0 /Height 2 /ColorSpace /DeviceRGB /BitsPerComponent 8")
        )
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetBitmap", lambda image: pytest.fail("decoded anyway"))

        message = "page 1: object 0 has no pixel size pdfium can decode (0 x 2)"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            image_loader.png_bytes()


class TestFigureThroughALoader:
    """A Figure built from a RawImage reads both byte forms through its ImageLoader, across a pickle."""

    def test_raw_bytes_verify_and_png_bytes_decode_after_pickling(self, tmp_path):
        jpeg = jpeg_bytes()
        path = xobject_page(tmp_path, jpeg, JPEG_ENTRIES)
        (image,) = images_of(path).images
        image_loader = loader_for(path, image)
        figure = Figure(
            element_id="p0001_e000",
            page_number=1,
            reading_order=0,
            bbox=BBox(x0=image.bbox[0], y0=image.bbox[1], x1=image.bbox[2], y1=image.bbox[3]),
            sha256=image.raw_sha256,
            width_px=image.px_width,
            height_px=image.px_height,
            _loader=image_loader.png_bytes,
            _raw_loader=image_loader.raw_bytes,
        )

        # Our own object, pickled and unpickled in this test: no untrusted data.
        restored = pickle.loads(pickle.dumps(figure))

        assert restored == figure
        assert restored.read_raw_bytes() == jpeg
        assert decoded(restored.read_bytes()).size == (16, 8)


def test_colorspace_names_follow_pdfiums_enum():
    names = [f"FPDF_COLORSPACE_{name.upper()}" for name in _pdfium_images.COLORSPACES]

    assert [getattr(pdfium_c, name) for name in names] == list(range(1, 12))


class TestPlacementGeometry:
    """image_placement over matrices pdfium's 32-bit CTM never reads, but composed form matrices can."""

    def test_a_corner_beyond_float_range_is_non_finite_geometry(self):
        to_display = partial(to_display_box, rotation_deg=0, page_box=(0.0, 0.0, 400.0, 300.0))
        # Each entry is finite; corner (1, 1) maps to 1e308 + 1e308 = inf.
        huge = Matrix(1e308, 0.0, 1e308, 1.0, 0.0, 0.0)

        with pytest.raises(NonFiniteGeometry, match=re.escape("its corner (inf, 1.0) is not finite")):
            _pdfium_images.image_placement(huge, (0.0, 0.0, 400.0, 300.0), to_display)


class TestBitmapFormats:
    """Every bitmap format pdfium decodes to becomes a PNG of the pixels it holds."""

    @pytest.mark.parametrize(
        "bitmap_format,mode,pixel",
        [
            pytest.param(pdfium_c.FPDFBitmap_Gray, "L", 0x80, id="gray"),
            pytest.param(pdfium_c.FPDFBitmap_BGR, "RGB", (0x11, 0x22, 0x33), id="bgr"),
            pytest.param(pdfium_c.FPDFBitmap_BGRx, "RGB", (0x11, 0x22, 0x33), id="bgrx"),
            pytest.param(pdfium_c.FPDFBitmap_BGRA, "RGBA", (0x11, 0x22, 0x33, 0x44), id="bgra"),
        ],
    )
    def test_each_format_reads_as_its_pillow_mode(self, bitmap_format, mode, pixel):
        bitmap = pdfium_c.FPDFBitmap_CreateEx(3, 2, bitmap_format, None, 0)
        try:
            # FillRect takes 0xAARRGGBB; a gray bitmap keeps the colour's luminance.
            assert pdfium_c.FPDFBitmap_FillRect(bitmap, 0, 0, 3, 2, 0x44112233 if mode != "L" else 0xFF808080)
            got = decoded(_pdfium_images._bitmap_png(bitmap, "object 0"))
        finally:
            pdfium_c.FPDFBitmap_Destroy(bitmap)

        assert (got.mode, got.size, got.getpixel((2, 1))) == (mode, (3, 2), pixel)


def j2k(size=(16, 8), *, mode: str = "L", siz=None, tile=None, components: int | None = None) -> bytes:
    """A Pillow raw J2K codestream; ``siz``, ``tile`` and ``components`` overwrite its SIZ header."""
    buffer = io.BytesIO()
    Image.new(mode, size).save(buffer, "JPEG2000", no_jp2=True)
    data = bytearray(buffer.getvalue())
    at = data.find(b"\xff\x4f\xff\x51")
    if siz is not None:
        data[at + 8 : at + 16] = struct.pack(">II", *siz)
    if tile is not None:
        data[at + 24 : at + 32] = struct.pack(">II", *tile)
    if components is not None:
        (csiz,) = struct.unpack_from(">H", data, at + 40)
        entry = bytes(data[at + 42 : at + 45])
        data[at + 4 : at + 6] = struct.pack(">H", 38 + 3 * components)
        data[at + 40 : at + 42 + 3 * csiz] = struct.pack(">H", components) + entry * components
    return bytes(data)


def jpeg_with_frame(width: int, height: int) -> bytes:
    """A 16 x 16 gray JPEG whose frame header (SOF0) claims ``width`` x ``height``."""
    buffer = io.BytesIO()
    Image.new("L", (16, 16), 128).save(buffer, "JPEG")
    data = bytearray(buffer.getvalue())
    sof = data.find(b"\xff\xc0")
    data[sof + 5 : sof + 9] = struct.pack(">HH", height, width)
    return bytes(data)


def gray(width: int, height: int, codec: bytes) -> bytes:
    return b"/Width %d /Height %d /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter %s" % (width, height, codec)


JPX_16 = gray(16, 16, b"/JPXDecode")
DCT_16 = gray(16, 16, b"/DCTDecode")
DRAWN = 80 * 40 / 120000  # the cover of an image drawn by xobject_page


@pytest.fixture
def decode_calls(monkeypatch):
    """Record every pdfium call that can decode an image's pixels; none of them reaches pdfium."""
    calls = []
    for name in ("FPDFImageObj_GetImageMetadata", "FPDFImageObj_GetBitmap", "FPDFImageObj_GetRenderedBitmap"):
        monkeypatch.setattr(pdfium_c, name, lambda *args, _name=name: calls.append(_name))
    return calls


BOMB_SCRIPT = r"""
import os, resource, sys, threading, time

LIMIT = 1536 << 20  # bytes of peak RSS; past it the process ends at once


def peak() -> int:
    # Linux keeps ru_maxrss across fork + exec, so a child of a large process
    # (pytest) reports the parent's peak; VmHWM is this process's own, reset at exec.
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("no VmHWM line in /proc/self/status")
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # macOS reports bytes


def watch() -> None:
    # ctypes releases the GIL during pdfium calls, so this runs while pdfium allocates.
    while True:
        if peak() > LIMIT:
            sys.stdout.write(f"killed: peak RSS over {LIMIT >> 20} MiB\n")
            sys.stdout.flush()
            os._exit(99)
        time.sleep(0.005)


threading.Thread(target=watch, daemon=True).start()
from datagrunt.pdf import DatagruntPDFError
from datagrunt.pdf.pdfium_engine import ImageLoader, open_document, page_images

(image,) = page_images(open_document(sys.argv[1]), 0).images
print(f"page_images: px {image.px_width} x {image.px_height}", flush=True)
try:
    ImageLoader(source=sys.argv[1], page_index=0, image=image).png_bytes()
except DatagruntPDFError as exc:
    print(f"refused: {exc}")
print(f"peak {peak() >> 20} MiB")
"""


def run_memory_limited(path: Path) -> subprocess.CompletedProcess:
    """BOMB_SCRIPT on ``path`` in a fresh interpreter that exits (99) once its peak RSS passes 1.5 GiB."""
    return subprocess.run(
        [sys.executable, "-c", BOMB_SCRIPT, str(path)], capture_output=True, check=False, timeout=120, text=True
    )


class TestDecodeBounds:
    """pdfium decodes DCT at the JPEG frame's size and JPX at the codestream's: those sizes are bounded, not /Width."""

    def test_a_jpx_bomb_is_recorded_at_its_codestream_size_without_decoding(self, tmp_path, decode_calls):
        bomb = j2k((16, 16), siz=(16000, 16000), tile=(16000, 16000))

        got = images_of(xobject_page(tmp_path, bomb, JPX_16))

        (image,) = got.images
        assert (image.px_width, image.px_height, image.declared_px) == (16000, 16000, (16, 16))
        assert (image.bits_per_pixel, image.colorspace, image.raw_sha256) == (None, None, sha(bomb))
        assert got.stats == stats(oversized=1, cover=DRAWN)
        assert decode_calls == []

    def test_a_jpx_bomb_is_refused_before_decoding(self, tmp_path, decode_calls):
        bomb = j2k((16, 16), siz=(16000, 16000), tile=(16000, 16000))
        image_loader = only_loader(xobject_page(tmp_path, bomb, JPX_16))

        message = "page 1: object 0 decodes to 16000 x 16000 = 256000000 pixels, over the 89478485-pixel bound"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            image_loader.png_bytes()
        assert image_loader.raw_bytes() == bomb
        assert decode_calls == []

    def test_a_jpx_tiled_past_the_tile_bound_is_refused(self, tmp_path, decode_calls):
        # Pillow's one 16 x 8 tile, repeated over 2000 x 2000: 125 x 250 tiles. OpenJPEG
        # allocates ~12 KB per tile as it reads the header (observed: 15625 tiles took 234 MB).
        bomb = j2k((16, 8), siz=(2000, 2000))
        path = xobject_page(tmp_path, bomb, gray(2000, 2000, b"/JPXDecode"))

        got = images_of(path)

        assert got.stats == stats(oversized=1, cover=DRAWN)
        with pytest.raises(DatagruntPDFError, match=re.escape("object 0 decodes in 31250 tiles, over the 4096-tile")):
            only_loader(path).png_bytes()
        assert decode_calls == []

    def test_the_jpx_tile_bound(self):
        assert _pdfium_images._MAX_JPX_TILES == 4096

    def test_jpx_pixel_metadata_is_never_read(self, tmp_path, monkeypatch):
        # pdfium decodes a JPX image in full to report its bits per pixel and colour space.
        calls = []
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImageMetadata", lambda *args: calls.append(args))
        path = xobject_page(tmp_path, j2k((16, 8)), gray(16, 8, b"/JPXDecode"))

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height, image.bits_per_pixel, image.colorspace) == (16, 8, None, None)
        assert got.stats == stats(cover=DRAWN)
        assert decoded(only_loader(path).png_bytes()).size == (16, 8)
        assert calls == []

    @pytest.mark.parametrize(
        "data,entries",
        [
            pytest.param(j2k((16, 16), siz=(20000, 20000), tile=(20000, 20000)), JPX_16, id="jpx"),
            pytest.param(jpeg_with_frame(30000, 30000), DCT_16, id="dct"),
        ],
    )
    def test_a_bomb_stays_small_in_a_memory_limited_process(self, tmp_path, data, entries):
        done = run_memory_limited(xobject_page(tmp_path, data, entries))

        assert done.returncode == 0, done.stdout + done.stderr
        assert "refused: " in done.stdout and "over the 89478485-pixel bound" in done.stdout
        assert int(re.search(r"peak (\d+) MiB", done.stdout).group(1)) < 400, done.stdout

    def test_a_jpeg_bomb_is_sized_by_its_frame_and_refused_before_decoding(self, tmp_path, decode_calls):
        bomb = jpeg_with_frame(26000, 26000)
        path = xobject_page(tmp_path, bomb, DCT_16)

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height, image.declared_px) == (26000, 26000, (16, 16))
        assert got.stats == stats(oversized=1, cover=DRAWN)
        message = "page 1: object 0 decodes to 26000 x 26000 = 676000000 pixels, over the 89478485-pixel bound"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            loader_for(path, image).png_bytes()
        assert decode_calls == []

    @pytest.mark.parametrize(
        "frame,declared",
        [
            pytest.param((16, 16), (12000, 12000), id="declared-larger"),
            pytest.param((300, 200), (16, 16), id="frame-larger"),
        ],
    )
    def test_a_jpeg_decodes_at_its_frame_size_whatever_its_dictionary_says(self, tmp_path, frame, declared):
        path = xobject_page(tmp_path, jpeg_with_frame(*frame), gray(*declared, b"/DCTDecode"))

        (image,) = images_of(path).images

        assert (image.px_width, image.px_height, image.declared_px) == (*frame, declared)
        assert (image.bits_per_pixel, image.colorspace) == (8, "DeviceGray")
        assert decoded(loader_for(path, image).png_bytes()).size == frame

    @pytest.mark.parametrize(
        "name,wrap",
        [
            pytest.param(b"ASCII85Decode", lambda data: base64.a85encode(data) + b"~>", id="ascii85"),
            pytest.param(b"ASCIIHexDecode", lambda data: data.hex().encode() + b">", id="asciihex"),
        ],
    )
    def test_an_ascii_wrapped_jpeg_is_sized_by_its_frame(self, tmp_path, name, wrap):
        # ReportLab writes every JPEG as [/ASCII85Decode /DCTDecode].
        wrapped = wrap(jpeg_bytes((16, 8)))
        entries = b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter [/%s /DCTDecode]" % name
        path = xobject_page(tmp_path, wrapped, entries)

        (image,) = images_of(path).images

        assert (image.px_width, image.px_height, image.raw_sha256) == (16, 8, sha(wrapped))
        assert (image.filters, image.bits_per_pixel, image.colorspace) == (
            (name.decode(), "DCTDecode"),
            24,
            "DeviceRGB",
        )
        assert decoded(loader_for(path, image).png_bytes()).size == (16, 8)

    def test_an_ascii_wrapped_jpeg_bomb_is_refused(self, tmp_path, decode_calls):
        wrapped = base64.a85encode(jpeg_with_frame(26000, 26000)) + b"~>"
        entries = (
            b"/Width 16 /Height 16 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter [/ASCII85Decode /DCTDecode]"
        )

        got = images_of(xobject_page(tmp_path, wrapped, entries))

        assert [(item.px_width, item.px_height) for item in got.images] == [(26000, 26000)]
        assert got.stats == stats(oversized=1, cover=DRAWN)
        assert decode_calls == []

    def test_a_jpeg_behind_flate_is_never_decoded(self, tmp_path, monkeypatch, decode_calls):
        decoded_reads = []
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImageDataDecoded", lambda *args: decoded_reads.append(args))
        flated = zlib.compress(jpeg_bytes((16, 8)))
        entries = b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter [/FlateDecode /DCTDecode]"
        path = xobject_page(tmp_path, flated, entries)

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height, image.declared_px, image.raw_sha256) == (0, 0, (16, 8), sha(flated))
        assert got.stats == stats(unsized=1, cover=DRAWN)
        message = "page 1: object 0 is a DCTDecode stream behind FlateDecode: its size cannot be read without decoding"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            loader_for(path, image).png_bytes()
        assert (decode_calls, decoded_reads) == ([], [])

    @pytest.mark.parametrize("codec", [b"DCTDecode", b"JPXDecode"])
    def test_an_unreadable_header_is_never_decoded(self, tmp_path, decode_calls, codec):
        path = xobject_page(tmp_path, b"not a codestream", gray(16, 16, b"/" + codec))

        got = images_of(path)

        assert [(item.px_width, item.px_height, item.declared_px) for item in got.images] == [(0, 0, (16, 16))]
        assert got.stats == stats(unsized=1, cover=DRAWN)
        message = f"page 1: object 0 has a {codec.decode()} header that cannot be read"
        with pytest.raises(DatagruntPDFError, match=re.escape(message)):
            loader_for(path, got.images[0]).png_bytes()
        assert decode_calls == []

    def test_an_ascii_wrapped_stream_over_its_bound_is_never_decoded(self, tmp_path, monkeypatch):
        # ASCII85's "z" makes 4 bytes of 1, so the wrapped stream is bounded before pdfium decodes it.
        wrapped = base64.a85encode(jpeg_bytes((16, 8))) + b"~>"
        entries = b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter [/ASCII85Decode /DCTDecode]"
        monkeypatch.setattr(_pdfium_images, "_MAX_ASCII_WRAPPED_BYTES", len(wrapped) - 1)
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImageDataDecoded", lambda *args: pytest.fail("decoded"))

        got = images_of(xobject_page(tmp_path, wrapped, entries))

        assert [(item.px_width, item.px_height) for item in got.images] == [(0, 0)]
        assert got.stats == stats(unsized=1, cover=DRAWN)

    def test_the_ascii_wrapped_bound_keeps_the_decoded_stream_within_a_quarter_of_the_asset_limit(self):
        assert _pdfium_images._MAX_ASCII_WRAPPED_BYTES == elements._MAX_ASSET_BYTES // 16

    @pytest.mark.parametrize("width", [-5, 0, 131072])
    def test_an_impossible_width_is_unknown_and_never_decoded(self, tmp_path, decode_calls, width):
        # pdfium reads /Width -5 as 4294967291 and decodes nothing wider than 131071.
        path = xobject_page(
            tmp_path, bytes(24), b"/Width %d /Height 2 /ColorSpace /DeviceRGB /BitsPerComponent 8" % width
        )

        got = images_of(path)

        assert [(item.px_width, item.px_height, item.declared_px) for item in got.images] == [(0, 2, (0, 2))]
        assert got.stats == stats(unsized=1, cover=DRAWN)
        assert decode_calls == []

    def test_the_widest_image_pdfium_decodes_is_kept(self, tmp_path):
        path = xobject_page(tmp_path, zlib.compress(bytes(100)), gray(131071, 1, b"/FlateDecode"))

        (image,) = images_of(path).images

        assert (image.px_width, image.declared_px, image.bits_per_pixel) == (131071, (131071, 1), 8)

    def test_a_jpx_past_the_component_bound_is_refused_before_decoding(self, tmp_path, decode_calls):
        bomb = j2k((16, 8), components=5)
        path = xobject_page(tmp_path, bomb, gray(16, 8, b"/JPXDecode"))

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height) == (0, 0)
        assert got.stats == stats(oversized=1, cover=DRAWN)
        with pytest.raises(DatagruntPDFError, match=re.escape("5 components, over the 4-component bound")):
            only_loader(path).png_bytes()
        assert decode_calls == []
        assert _pdfium_images._MAX_JPX_COMPONENTS == 4

    def test_a_jpx_past_the_decoded_samples_bound_is_refused_before_decoding(self, tmp_path, monkeypatch, decode_calls):
        monkeypatch.setattr(_pdfium_images, "_MAX_DECODED_SAMPLES", 16 * 8 * 2, raising=False)
        rgba_jpx = j2k((16, 8), mode="RGBA")
        path = xobject_page(tmp_path, rgba_jpx, gray(16, 8, b"/JPXDecode"))

        got = images_of(path)

        assert got.stats == stats(oversized=1, cover=DRAWN)
        with pytest.raises(DatagruntPDFError, match="decoded samples"):
            only_loader(path).png_bytes()
        assert decode_calls == []

    @pytest.mark.parametrize("codec", [b"JBIG2Decode", b"Crypt"], ids=["jbig2", "crypt"])
    def test_an_unsized_filter_is_recorded_without_size_and_never_decoded(self, tmp_path, decode_calls, codec):
        path = xobject_page(tmp_path, bytes(16), gray(16, 8, b"/" + codec))

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height) == (0, 0)
        assert got.stats == stats(unsized=1, cover=DRAWN)
        with pytest.raises(DatagruntPDFError, match="cannot be read"):
            only_loader(path).png_bytes()
        assert decode_calls == []

    def test_an_ascii_decoded_stream_over_the_asset_limit_is_not_allocated(self, tmp_path, monkeypatch):
        wrapped = jpeg_bytes((16, 8)).hex().encode() + b">"
        entries = b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter [/ASCIIHexDecode /DCTDecode]"
        real_create = ctypes.create_string_buffer

        def guarded_create(init, size=None):
            if isinstance(init, int) and init > _pdfium_images._MAX_ASSET_BYTES:
                pytest.fail(f"allocated {init}-byte buffer over the asset limit")
            return real_create(init, size)

        monkeypatch.setattr(ctypes, "create_string_buffer", guarded_create)
        monkeypatch.setattr(
            _pdfium_images.pdfium_c,
            "FPDFImageObj_GetImageDataDecoded",
            lambda image, buf, length: _pdfium_images._MAX_ASSET_BYTES + 1,
        )

        got = images_of(xobject_page(tmp_path, wrapped, entries))

        assert [(item.px_width, item.px_height) for item in got.images] == [(0, 0)]
        assert got.stats.skipped_unsized_images + got.stats.skipped_oversized_images == 1

    @pytest.mark.parametrize(
        "filter_spec,behind",
        [
            pytest.param(b"[/FlateDecode /FlateDecode]", "FlateDecode", id="flate-flate"),
            pytest.param(b"[/LZWDecode /FlateDecode]", "LZWDecode", id="lzw-flate"),
            pytest.param(b"[/FlateDecode /ASCII85Decode]", "FlateDecode", id="flate-ascii85"),
        ],
    )
    def test_a_simple_filter_behind_a_non_ascii_filter_is_never_decoded(
        self, tmp_path, decode_calls, filter_spec, behind
    ):
        path = xobject_page(tmp_path, zlib.compress(FLATE), RGB_4X2 + b" /Filter " + filter_spec)

        got = images_of(path)

        (image,) = got.images
        assert (image.px_width, image.px_height) == (0, 0)
        assert got.stats == stats(unsized=1, cover=DRAWN)
        with pytest.raises(
            DatagruntPDFError, match=re.escape(f"behind {behind}: its size cannot be read without decoding")
        ):
            only_loader(path).png_bytes()
        assert decode_calls == []

    @pytest.mark.parametrize(
        "name,wrap",
        [
            pytest.param(b"ASCII85Decode", lambda data: base64.a85encode(data) + b"~>", id="ascii85-flate"),
            pytest.param(b"ASCIIHexDecode", lambda data: data.hex().encode() + b">", id="asciihex-flate"),
        ],
    )
    def test_a_simple_filter_behind_an_ascii_filter_is_decoded(self, tmp_path, name, wrap):
        path = xobject_page(tmp_path, wrap(FLATE), RGB_4X2 + b" /Filter [/%s /FlateDecode]" % name)

        (image,) = images_of(path).images

        assert (image.px_width, image.px_height) == (4, 2)
        assert decoded(only_loader(path).png_bytes()).size == (4, 2)


def drawn_times(tmp_path: Path, count: int, data: bytes = FLATE, entries: bytes = RGB_4X2 + b" /Filter /FlateDecode"):
    """A page drawing one image XObject ``count`` times, in a row."""
    content = b"".join(b"q 20 0 0 10 %d 20 cm /Im Do Q " % (10 + 25 * index) for index in range(count))
    return xobject_page(tmp_path, data, entries, content)


@pytest.fixture
def raw_reads(monkeypatch):
    """Record each FPDFImageObj_GetImageDataRaw call as "size" (a length query) or "copy"; pdfium still runs."""
    calls = []
    original = pdfium_c.FPDFImageObj_GetImageDataRaw

    def recording(image, buffer, length):
        calls.append("size" if buffer is None else "copy")
        return original(image, buffer, length)

    monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImageDataRaw", recording)
    return calls


class TestPageBudget:
    """A page's stream reads are budgeted: past the budget images are recorded unread and counted."""

    def test_the_page_budget_is_twice_the_asset_limit(self):
        assert _pdfium_images._MAX_PAGE_STREAM_BYTES == 2 * elements._MAX_ASSET_BYTES

    def test_streams_past_the_budget_are_recorded_without_reading_them(self, tmp_path, monkeypatch, raw_reads):
        monkeypatch.setattr(_pdfium_images, "_MAX_PAGE_STREAM_BYTES", 2 * len(FLATE))

        got = images_of(drawn_times(tmp_path, 5))

        assert [item.raw_sha256 for item in got.images] == [sha(FLATE)] * 2 + [None] * 3
        assert got.images[4] == unread_image(
            (110.0, 270.0, 130.0, 280.0), 4, declared_px=(4, 2), filters=("FlateDecode",)
        )
        assert (got.stats.skipped_over_budget_images, got.stats.skipped_oversized_images) == (3, 0)
        # Once spent, the budget stops even the length queries, which copy the stream too.
        assert raw_reads == ["size", "copy", "size", "copy"]

    def test_a_stream_that_does_not_fit_spends_the_budget(self, tmp_path, monkeypatch, raw_reads):
        monkeypatch.setattr(_pdfium_images, "_MAX_PAGE_STREAM_BYTES", 3 * len(FLATE) // 2)

        got = images_of(drawn_times(tmp_path, 4))

        assert [item.raw_sha256 for item in got.images] == [sha(FLATE), None, None, None]
        assert got.stats.skipped_over_budget_images == 3
        assert raw_reads == ["size", "copy", "size"]

    def test_an_oversized_stream_is_charged_to_the_budget(self, tmp_path, monkeypatch, raw_reads):
        monkeypatch.setattr(_pdfium_images, "_MAX_ASSET_BYTES", len(FLATE) - 1)
        monkeypatch.setattr(_pdfium_images, "_MAX_PAGE_STREAM_BYTES", 2 * len(FLATE))

        got = images_of(drawn_times(tmp_path, 3))

        assert [item.raw_sha256 for item in got.images] == [None, None, None]
        assert (got.stats.skipped_oversized_images, got.stats.skipped_over_budget_images) == (2, 1)
        assert raw_reads == ["size", "size"]

    def test_an_ascii_wrapped_codestream_is_charged_its_decoded_size_too(self, tmp_path, monkeypatch):
        wrapped = base64.a85encode(jpeg_bytes((16, 8))) + b"~>"
        entries = b"/Width 16 /Height 8 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter [/ASCII85Decode /DCTDecode]"
        # Room for two raw streams, not for two raw streams and their decoded JPEGs.
        monkeypatch.setattr(_pdfium_images, "_MAX_PAGE_STREAM_BYTES", 2 * len(wrapped))

        got = images_of(drawn_times(tmp_path, 2, wrapped, entries))

        assert [item.raw_sha256 for item in got.images] == [sha(wrapped), None]
        assert got.stats.skipped_over_budget_images == 1


class TestChangedDictionary:
    """The same stream bytes under a new image dictionary are a changed file, not a different PNG."""

    @pytest.mark.parametrize(
        "entries,found",
        [
            pytest.param(
                gray(24, 1, b"/FlateDecode"),
                "FlateDecode, 24 x 1 px (declared 24 x 1), 8 bits per pixel, colour space DeviceGray",
                id="size",
            ),
            pytest.param(
                b"/Width 4 /Height 2 /ColorSpace [/CalRGB << /WhitePoint [0.9505 1 1.089] >>] /BitsPerComponent 8 "
                b"/Filter /FlateDecode",
                "FlateDecode, 4 x 2 px (declared 4 x 2), 24 bits per pixel, colour space CalRGB",
                id="colour-space",
            ),
        ],
    )
    def test_a_new_dictionary_is_a_changed_file(self, tmp_path, entries, found):
        path = xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode")
        image_loader = only_loader(path)
        rewrite(path, b"q 80 0 0 40 10 20 cm /Im Do Q", image_xobject(FLATE, entries))

        expected = "FlateDecode, 4 x 2 px (declared 4 x 2), 24 bits per pixel, colour space DeviceRGB"
        message = f"the file changed (the image now reads {found}; the loader expects {expected})"
        for read in (image_loader.raw_bytes, image_loader.png_bytes):
            with pytest.raises(DatagruntPDFError, match=re.escape(message)):
                read()


class TestPngMemory:
    """png_bytes encodes from pdfium's bitmap in place: no copy of the decoded pixels."""

    def test_the_decoded_bitmap_is_never_copied(self, tmp_path, monkeypatch):
        image_loader = only_loader(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        monkeypatch.setattr(ctypes, "string_at", lambda *args: pytest.fail("copied the decoded bitmap"))

        assert decoded(image_loader.png_bytes()).tobytes() == SAMPLES


class TestBoundaryMutants:
    """Edges the first round's tests left open (review M6)."""

    def test_rounding_never_takes_the_cover_over_1(self):
        # A turned full-bleed image whose clipped area computes as a hair over the page.
        matrix = Matrix(
            2601.9954700100498,
            -597.5013533044593,
            597.5013533044593,
            2601.9954700100498,
            -1293.7484116572546,
            -606.2470583527952,
        )
        page_box = (0.0, 0.0, 612.0, 792.0)
        corners = [matrix.apply(x, y) for x, y in ((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0))]
        to_display = partial(to_display_box, rotation_deg=0, page_box=page_box)

        assert _pdfium_images._area(_pdfium_images._clipped(corners, page_box)) / (612 * 792) > 1.0
        assert _pdfium_images.image_placement(matrix, page_box, to_display).cover_fraction == 1.0

    def test_the_last_colour_space_pdfium_names_is_read(self, tmp_path, monkeypatch):
        handle = open_document(xobject_page(tmp_path, FLATE, RGB_4X2 + b" /Filter /FlateDecode"))
        monkeypatch.setattr(pdfium_c, "FPDFImageObj_GetImageMetadata", writes_colorspace(11))

        assert [item.colorspace for item in page_images(handle, 0).images] == ["Pattern"]
