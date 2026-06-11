"""Tests for PyMuPDFBackend."""

import pytest

from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend
from datagrunt.core.pdf_io.extraction.shapes import ImageBlock, PageAnalysis, TextBlock


class TestPyMuPDFBackend:
    def test_analyze_page(self, sample_pdf):
        a = PyMuPDFBackend(sample_pdf).analyze_page(0)
        assert isinstance(a, PageAnalysis) and a.has_text_layer is True

    def test_text_blocks_classified(self, sample_pdf):
        blocks = PyMuPDFBackend(sample_pdf).extract_text_blocks(0)
        assert blocks and all(isinstance(b, TextBlock) for b in blocks)
        assert "header" in {b.classification for b in blocks}

    def test_images(self, sample_pdf, tmp_path):
        imgs = PyMuPDFBackend(sample_pdf).extract_images(0, output_dir=str(tmp_path))
        assert all(isinstance(i, ImageBlock) for i in imgs)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PyMuPDFBackend("nope.pdf")

    @staticmethod
    def _two_image_swapped_draw_order_pdf(tmp_path):
        """Build a 2-image page whose draw order differs from resource order.

        Inserts a distinctively-sized blue image top-left (80px, lower xref,
        ``fzImg0``) and a green image bottom-right (60px, higher xref,
        ``fzImg1``), then rewrites the content stream so the green image is
        *drawn first*. After this, ``page.get_images()`` still lists images in
        resource/xref order ``[blue@top-left, green@bottom-right]`` while
        ``get_text("dict")`` reports them in draw order
        ``[green@bottom-right, blue@top-left]`` -- the exact mismatch that breaks
        index-based bbox pairing. The two images differ in pixel size so each
        emitted block can be identified by ``width_px`` regardless of order.

        Returns ``(pdf_path, blue_corner, green_corner)`` -- the ``(x, y)``
        top-left corner each image was actually placed at, in datagrunt's
        top-left-origin convention.
        """
        import pymupdf

        doc = pymupdf.open()
        page = doc.new_page(width=400, height=400)
        blue = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 80, 80))
        blue.set_rect(blue.irect, (0, 0, 255))
        green = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 60, 60))
        green.set_rect(green.irect, (0, 255, 0))
        page.insert_image(pymupdf.Rect(20, 20, 100, 100), stream=blue.tobytes("png"))
        page.insert_image(pymupdf.Rect(280, 300, 340, 360), stream=green.tobytes("png"))
        page.clean_contents()  # normalize so we can reliably find the two draw blocks

        content_xref = page.get_contents()[0]
        stream = doc.xref_stream(content_xref).decode("latin-1")
        blue_block = "q 80 0 0 80 20 300 cm /fzImg0 Do Q"
        green_block = "q 60 0 0 60 280 40 cm /fzImg1 Do Q"
        # Swap the two draw blocks so green (higher xref) is drawn before blue.
        swapped = (
            stream.replace(blue_block, "__BLUE__")
            .replace(green_block, blue_block)
            .replace("__BLUE__", green_block)
        )
        assert swapped != stream, "content-stream swap did not match expected draw blocks"
        doc.update_stream(content_xref, swapped.encode("latin-1"))

        path = tmp_path / "swapped.pdf"
        doc.save(str(path))
        doc.close()
        return str(path), (20.0, 20.0), (280.0, 300.0)

    def test_image_bbox_paired_by_xref_not_index(self, tmp_path):
        """Each image must get ITS OWN bbox even when draw order != resource order.

        Regression for the index-based pairing bug: with draw order swapped, the
        old code paired ``get_images()[idx]`` with ``image_blocks[idx]`` and gave
        each image the *other* image's position. Resolving by xref fixes it.
        """
        pdf, blue_corner, green_corner = self._two_image_swapped_draw_order_pdf(tmp_path)
        images = PyMuPDFBackend(pdf).extract_images(0)
        assert len(images) == 2
        # Identify each image by its distinctive pixel size, then assert it landed
        # at the position where THAT image was actually placed.
        by_size = {im.width_px: im for im in images}
        assert set(by_size) == {80, 60}
        blue, green = by_size[80], by_size[60]
        assert (round(blue.bbox.x, 1), round(blue.bbox.y, 1)) == blue_corner
        assert (round(green.bbox.x, 1), round(green.bbox.y, 1)) == green_corner
        assert all(im.bbox.w > 0 and im.bbox.h > 0 for im in images)
