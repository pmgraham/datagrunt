"""Tests for the PDFium extractor functions."""

from datagrunt.core.pdf_io import pdfium_extractors


class TestTopLeftXYWH:
    """Test suite for _topleft_xywh coordinate conversion."""

    def test_converts_bottom_left_to_top_left(self):
        # page is 792 tall; bbox left=100 bottom=692 right=200 top=742
        result = pdfium_extractors._topleft_xywh([100, 692, 200, 742], 792)
        assert result == {"x": 100.0, "y": 50.0, "w": 100.0, "h": 50.0}


class TestImportPdfium:
    """Test suite for _import_pdfium."""

    def test_returns_module_and_raw(self):
        pdfium, raw = pdfium_extractors._import_pdfium()
        assert hasattr(pdfium, "PdfDocument")
        assert hasattr(raw, "FPDF_PAGEOBJ_TEXT")
