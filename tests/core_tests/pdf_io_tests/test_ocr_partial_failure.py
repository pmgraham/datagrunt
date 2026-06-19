"""OCR failure must not discard an image-only page (issue #96).

When OCR fails (e.g. the tesseract binary is missing) on an image-only page,
the page must still appear in the parsed output with its already-extracted
image/text content and a page-level warning marker, rather than the whole page
being dropped. This exercises both the native pdfium schema and the structured
(unified element) schema, plus ``get_sample``.
"""

from datagrunt.core.pdf_io import pdfcomponents
from datagrunt.core.pdf_io.engines import PDFReaderPdfiumEngine, PDFReaderPyMuPDFEngine
from datagrunt.core.pdf_io.extraction import pdfium_native
from datagrunt.core.pdf_io.extraction.pdfium_native import PdfiumNativeReader
from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend


def _tesseract_not_found(*args, **kwargs):
    """Raise the real pytesseract error that a missing binary would raise."""
    import pytesseract

    raise pytesseract.TesseractNotFoundError()


class TestNativeSchemaOcrFailure:
    """Native pdfium schema keeps the page when OCR fails."""

    def test_parse_page_keeps_image_and_marks_ocr_false(self, scanned_pdf, tmp_path, monkeypatch):
        monkeypatch.setattr(pdfium_native, "ocr_data_to_blocks", _tesseract_not_found)
        page = PdfiumNativeReader(scanned_pdf).parse_page(0, image_output_dir=str(tmp_path))

        assert page["page_number"] == 1
        assert page["ocr"] is False
        assert len(page["images"]) >= 1  # already-extracted image survives
        assert page.get("warnings"), "expected a page-level OCR warning"
        assert any("ocr" in w.lower() for w in page["warnings"])

    def test_to_dicts_native_keeps_page(self, scanned_pdf, tmp_path, monkeypatch):
        monkeypatch.setattr(pdfium_native, "ocr_data_to_blocks", _tesseract_not_found)
        document = PDFReaderPdfiumEngine(scanned_pdf, structured=False).to_dicts(image_output_dir=str(tmp_path))
        pages = document["document"]["pages"]
        assert len(pages) == 1  # page not dropped
        assert pages[0]["ocr"] is False
        assert len(pages[0]["images"]) >= 1

    def test_get_sample_native_does_not_raise(self, scanned_pdf, monkeypatch):
        monkeypatch.setattr(pdfium_native, "ocr_data_to_blocks", _tesseract_not_found)
        page = PDFReaderPdfiumEngine(scanned_pdf, structured=False).get_sample()
        assert page["page_number"] == 1
        assert page["ocr"] is False


class TestStructuredSchemaOcrFailure:
    """Structured (unified) schema keeps the page when OCR fails."""

    def _force_ocr_failure(self, monkeypatch, assembler):
        monkeypatch.setattr(assembler.backend, "ocr_page", _tesseract_not_found)

    def test_parse_page_keeps_image_and_marks_warning(self, scanned_pdf, tmp_path, monkeypatch):
        assembler = pdfcomponents.DocumentAssembler(scanned_pdf)
        self._force_ocr_failure(monkeypatch, assembler)
        page = assembler.parse_page(0, image_output_dir=str(tmp_path))

        assert page["page_number"] == 1
        # The already-extracted image element survives even though OCR failed.
        assert any(e["type"] == "image" for e in page["elements"])
        assert page.get("warnings"), "expected a page-level OCR warning"
        assert any("ocr" in w.lower() for w in page["warnings"])

    def test_get_sample_structured_does_not_raise(self, scanned_pdf, monkeypatch):
        monkeypatch.setattr(PyMuPDFBackend, "ocr_page", _tesseract_not_found)
        page = PDFReaderPyMuPDFEngine(scanned_pdf).get_sample()
        assert page["page_number"] == 1
        assert any(e["type"] == "image" for e in page["elements"])
