"""Tests for the PdfiumDocument / PdfiumPage wrapper."""

from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument


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
