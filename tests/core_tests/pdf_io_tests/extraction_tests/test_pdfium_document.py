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
