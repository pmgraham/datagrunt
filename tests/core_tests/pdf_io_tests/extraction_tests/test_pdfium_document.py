"""Tests for the PdfiumDocument / PdfiumPage wrapper."""

import pytest

from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument


def _rotated_text_pdf(tmp_path, rotation):
    """Build a one-page PDF with text near the top and bottom, then rotate it."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 100), "Top of the page line", fontsize=18)
    page.insert_text((72, 700), "Bottom of the page line", fontsize=18)
    page.set_rotation(rotation)
    path = tmp_path / f"rotated_{rotation}.pdf"
    doc.save(str(path))
    doc.close()
    return str(path)


# Visible box (left, bottom, right, top) in PDF user space per page-box layout.
# "none" is a plain 612x792 page. The other three all expose (50, 40, 530, 720)
# but reach it differently: an offset CropBox, a MediaBox with a non-zero
# origin, and that MediaBox inherited from the /Pages node (absent from the
# page dictionary, which pypdfium2's get_mediabox() does not see).
_FULL_PAGE_BOX = (0.0, 0.0, 612.0, 792.0)
_OFFSET_BOX = (50.0, 40.0, 530.0, 720.0)
_OFFSET_BOX_PDF = "[50 40 530 720]"
_VISIBLE_BOX = {
    "none": _FULL_PAGE_BOX,
    "cropbox": _OFFSET_BOX,
    "mediabox": _OFFSET_BOX,
    "inherited_mediabox": _OFFSET_BOX,
}
# The red square's PDF user-space bounds: PyMuPDF's Rect(150, 200, 250, 300) is
# top-left based on the 792pt-tall page, so y runs 792-300 .. 792-200.
_IMAGE_BOUNDS = (150.0, 492.0, 250.0, 592.0)


def _box_layout_pdf(tmp_path, rotation, kind):
    """One page: a text run and a red 100x100 image, under the ``kind`` box layout."""
    import re

    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    page.insert_text((72, 100), "Top line", fontsize=18)
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
    pix.set_rect(pix.irect, (255, 0, 0))
    page.insert_image(pymupdf.Rect(150, 200, 250, 300), stream=pix.tobytes("png"))
    if kind == "cropbox":
        # PyMuPDF takes a top-left-based rect; this writes /CropBox [50 40 530 720].
        page.set_cropbox(pymupdf.Rect(50, 72, 530, 752))
    page.set_rotation(rotation)
    if kind == "mediabox":
        doc.xref_set_key(page.xref, "MediaBox", _OFFSET_BOX_PDF)
    elif kind == "inherited_mediabox":
        parent_xref = int(doc.xref_get_key(page.xref, "Parent")[1].split()[0])
        doc.xref_set_key(parent_xref, "MediaBox", _OFFSET_BOX_PDF)
        page_obj = doc.xref_object(page.xref, compressed=True)
        doc.update_object(page.xref, re.sub(r"/MediaBox\s*\[[^\]]*\]", "", page_obj))
    path = tmp_path / f"{kind}_{rotation}.pdf"
    doc.save(str(path))
    doc.close()
    return str(path)


def _expected_display_edges(bounds, rotation, visible):
    """Issue #384's mapping: bottom-left unrotated bounds -> top-left display edges."""
    left, bottom, right, top = bounds
    vl, vb, vr, vt = visible
    return {
        0: (left - vl, vt - top, right - vl, vt - bottom),
        90: (bottom - vb, left - vl, top - vb, right - vl),
        180: (vr - right, bottom - vb, vr - left, top - vb),
        270: (vt - top, vr - right, vt - bottom, vr - left),
    }[rotation]


def _raw_text_bounds(pdf):
    """The single text object's bounds as pypdfium2 reports them (user space)."""
    import pypdfium2 as pdfium
    import pypdfium2.raw as raw

    doc = pdfium.PdfDocument(pdf)
    try:
        page = doc[0]
        (obj,) = list(page.get_objects(filter=(raw.FPDF_PAGEOBJ_TEXT,)))
        return obj.get_bounds()
    finally:
        doc.close()


def _pixel_box(img, predicate):
    """``(x0, y0, x1, y1)`` of the pixels whose RGB satisfies ``predicate``."""
    from PIL import ImageChops

    masks = [band.point(lambda v, i=i: 255 if predicate(i, v) else 0) for i, band in enumerate(img.split())]
    mask = ImageChops.multiply(ImageChops.multiply(masks[0], masks[1]), masks[2])
    box = mask.getbbox()
    assert box is not None, "no matching pixels rendered"
    return box


def _is_red(channel, value):
    return value > 200 if channel == 0 else value < 60


def _is_ink(channel, value):
    return value < 128


class TestPdfiumDocument:
    def test_len_and_page(self, sample_pdf):
        with PdfiumDocument(sample_pdf) as doc:
            assert len(doc) == 1
            page = doc.page(0)
            w, h = page.size()
            assert w > 0 and h > 0
            assert "Quarterly Report" in page.full_text()

    def test_text_items_carry_own_text_and_size(self, sample_pdf):
        with PdfiumDocument(sample_pdf) as doc:
            items = list(doc.page(0).text_items())
            assert items
            it = items[0]
            assert isinstance(it.text, str) and it.text
            assert it.size > 0

    def test_image_items_filtered(self, sample_pdf, small_image_pdf):
        with PdfiumDocument(sample_pdf) as doc:
            assert len(list(doc.page(0).image_items())) >= 1  # 100x100 kept
        with PdfiumDocument(small_image_pdf) as doc:
            assert list(doc.page(0).image_items()) == []  # 20x20 filtered

    def test_render_pil(self, sample_pdf):
        with PdfiumDocument(sample_pdf) as doc:
            img = doc.page(0).render_pil(dpi=72)
            assert img.size[0] > 0 and img.mode == "RGB"


class TestRotatedPageCoordinates:
    """``get_size()`` is rotation-aware but ``obj.get_bounds()`` is not.

    Mixing the two when flipping y produced negative / out-of-range coordinates
    on /Rotate 90|270 pages. Coordinates must land within ``[0, page_height]``.
    """

    @pytest.mark.parametrize("rotation", [90, 180, 270])
    def test_rotated_text_y_within_page_height(self, tmp_path, rotation):
        pdf = _rotated_text_pdf(tmp_path, rotation)
        with PdfiumDocument(pdf) as doc:
            page = doc.page(0)
            _, page_height = page.size()
            items = list(page.text_items())
        assert items
        for it in items:
            assert 0 <= it.y_top <= page_height, f"y_top {it.y_top} out of [0,{page_height}] (rot {rotation})"
            assert 0 <= it.y_bot <= page_height, f"y_bot {it.y_bot} out of [0,{page_height}] (rot {rotation})"

    @pytest.mark.parametrize("rotation", [90, 180, 270])
    def test_rotated_image_bbox_within_page(self, tmp_path, rotation):
        import pymupdf

        doc = pymupdf.open()
        page = doc.new_page(width=612, height=792)
        pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
        pix.set_rect(pix.irect, (255, 0, 0))
        page.insert_image(pymupdf.Rect(72, 100, 172, 250), stream=pix.tobytes("png"))
        page.set_rotation(rotation)
        path = tmp_path / f"rot_img_{rotation}.pdf"
        doc.save(str(path))
        doc.close()

        with PdfiumDocument(str(path)) as d:
            pg = d.page(0)
            page_width, page_height = pg.size()
            images = list(pg.image_items())
        assert images
        for im in images:
            box = im.bbox
            assert 0 <= box.y and box.y + box.h <= page_height, (
                f"image y span out of [0,{page_height}] (rot {rotation})"
            )
            assert 0 <= box.x and box.x + box.w <= page_width, f"image x span out of [0,{page_width}] (rot {rotation})"


def test_image_items_threshold_keeps_small_image_when_lowered(small_image_pdf):
    """small_image_pdf holds a 20px image: dropped at default 40, kept at 10."""
    with PdfiumDocument(small_image_pdf) as doc:
        with doc.page(0) as page:
            assert len(list(page.image_items())) == 0
        with doc.page(0) as page:
            assert len(list(page.image_items(min_image_dimension=10))) == 1


def test_image_items_threshold_drops_large_image_when_raised(sample_pdf):
    """sample_pdf holds a 100px image: kept at default 40, dropped at 150."""
    with PdfiumDocument(sample_pdf) as doc:
        with doc.page(0) as page:
            assert len(list(page.image_items())) == 1
        with doc.page(0) as page:
            assert len(list(page.image_items(min_image_dimension=150))) == 0


class TestUnrotatedCoordinatesRegressionGuard:
    """Lock the rotation-0 coordinates so the rotation fix can't drift them.

    The literals were captured from the pre-fix code. Text coordinates are
    compared with a 1pt tolerance, not byte-for-byte: the baseline PDF uses a
    non-embedded font, so pdfium derives glyph boxes from platform font metrics
    that vary sub-pixel across its per-platform binaries (≤0.34pt between macOS
    arm64 and linux x86_64 at the same pypdfium2 5.9.0 / pdfium 150.0.7869.0). A
    real rotation regression moves coordinates by tens of points, so the 1pt
    window still guards the intent while staying portable across CI runners.
    """

    @staticmethod
    def _baseline_pdf(tmp_path):
        import pymupdf

        doc = pymupdf.open()
        page = doc.new_page(width=612, height=792)
        page.insert_text((72, 72), "Quarterly Report", fontsize=24)
        page.insert_text((72, 120), "Body line one here.", fontsize=11)
        pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
        pix.set_rect(pix.irect, (255, 0, 0))
        page.insert_image(pymupdf.Rect(72, 200, 172, 300), stream=pix.tobytes("png"))
        path = tmp_path / "baseline.pdf"
        doc.save(str(path))
        doc.close()
        return str(path)

    def test_unrotated_text_coords_unchanged(self, tmp_path):
        pdf = self._baseline_pdf(tmp_path)
        with PdfiumDocument(pdf) as doc:
            items = list(doc.page(0).text_items())
        coords = {it.text: (it.x0, it.x1, it.y_top, it.y_bot) for it in items}
        assert coords["Quarterly Report"] == pytest.approx((73.03, 249.22, 54.5, 77.02), abs=1.0)
        assert coords["Body line one here."] == pytest.approx((72.8, 165.82, 112.12, 122.3), abs=1.0)

    def test_unrotated_image_bbox_unchanged(self, tmp_path):
        pdf = self._baseline_pdf(tmp_path)
        with PdfiumDocument(pdf) as doc:
            images = list(doc.page(0).image_items())
        assert len(images) == 1
        assert images[0].bbox.to_dict() == {"x": 72.0, "y": 200.0, "w": 100.0, "h": 100.0}


_BOX_KINDS = list(_VISIBLE_BOX)
_ROTATIONS = [0, 90, 180, 270]


class TestVisibleBoxOffsetCoordinates:
    """Element boxes are relative to the page's VISIBLE box, not (0, 0) (#384).

    pdfium reports object bounds in unrotated PDF user space. The page renders
    the visible box (CropBox intersected with MediaBox, inherited from the page
    tree), so display coordinates must subtract that box's origin. Ignoring it
    shifted every element by the crop origin -- 40-50pt on these fixtures.
    """

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    @pytest.mark.parametrize("kind", _BOX_KINDS)
    def test_size_is_visible_box_display_size(self, tmp_path, kind, rotation):
        """``size()`` and element coordinates share one frame: the visible box."""
        vl, vb, vr, vt = _VISIBLE_BOX[kind]
        width, height = vr - vl, vt - vb
        expected = (height, width) if rotation in (90, 270) else (width, height)
        with PdfiumDocument(_box_layout_pdf(tmp_path, rotation, kind)) as doc:
            assert doc.page(0).size() == pytest.approx(expected)

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    @pytest.mark.parametrize("kind", _BOX_KINDS)
    def test_boxes_follow_visible_box_mapping(self, tmp_path, kind, rotation):
        pdf = _box_layout_pdf(tmp_path, rotation, kind)
        visible = _VISIBLE_BOX[kind]
        with PdfiumDocument(pdf) as doc:
            page = doc.page(0)
            (item,) = list(page.text_items())
            (image,) = list(page.image_items())
        text_expected = _expected_display_edges(_raw_text_bounds(pdf), rotation, visible)
        assert (item.x0, item.y_top, item.x1, item.y_bot) == pytest.approx(text_expected, abs=0.01)
        x0, y0, x1, y1 = _expected_display_edges(_IMAGE_BOUNDS, rotation, visible)
        assert image.bbox.to_dict() == {"x": x0, "y": y0, "w": x1 - x0, "h": y1 - y0}

    @pytest.mark.parametrize("rotation", _ROTATIONS)
    @pytest.mark.parametrize("kind", _BOX_KINDS)
    def test_boxes_match_rendered_pixels(self, tmp_path, kind, rotation):
        """Ground truth: at 72 dpi one pixel is one point of display space."""
        with PdfiumDocument(_box_layout_pdf(tmp_path, rotation, kind)) as doc:
            page = doc.page(0)
            (item,) = list(page.text_items())
            (image,) = list(page.image_items())
            img = page.render_pil(dpi=72)
        assert image.bbox.to_xyxy() == pytest.approx(_pixel_box(img, _is_red), abs=1.0)
        # Glyph boxes hug the inked glyphs to within anti-aliasing.
        ink = _pixel_box(img, _is_ink)
        assert (item.x0, item.y_top, item.x1, item.y_bot) == pytest.approx(ink, abs=1.5)
