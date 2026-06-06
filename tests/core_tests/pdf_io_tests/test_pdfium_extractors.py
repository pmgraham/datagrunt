"""Tests for the PDFium extractor functions."""

import os

import pytest

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


class TestParsePdfiumPage:
    """Test suite for parse_pdfium_page."""

    def test_extracts_text_and_dimensions(self, sample_pdf):
        page = pdfium_extractors.parse_pdfium_page(sample_pdf, 0)
        assert page["page_number"] == 1
        assert page["width"] > 0
        assert page["height"] > 0
        assert "Quarterly Report" in page["text"]
        assert page["ocr"] is False

    def test_text_objects_have_position_and_font(self, sample_pdf):
        page = pdfium_extractors.parse_pdfium_page(sample_pdf, 0)
        assert len(page["text_objects"]) > 0
        obj = page["text_objects"][0]
        assert set(obj.keys()) == {"text", "bbox", "position", "font_size"}
        assert set(obj["position"].keys()) == {"x", "y", "w", "h"}

    def test_images_metadata_only_without_output_dir(self, sample_pdf):
        page = pdfium_extractors.parse_pdfium_page(sample_pdf, 0)
        assert len(page["images"]) > 0
        img = page["images"][0]
        assert img["file"] is None
        assert img["extracted"] is False
        assert img["px_width"] > 0
        assert img["px_height"] > 0

    def test_images_written_to_output_dir(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        page = pdfium_extractors.parse_pdfium_page(sample_pdf, 0, image_output_dir=str(out))
        img = page["images"][0]
        assert img["extracted"] is True
        assert img["file"] is not None
        assert os.path.isfile(img["file"])


class TestOcrFallback:
    """Test suite for the OCR fallback in parse_pdfium_page."""

    def test_ocr_recovers_text_on_image_only_page(self, scanned_pdf, tesseract_available):
        if not tesseract_available:
            pytest.skip("tesseract binary not available")
        page = pdfium_extractors.parse_pdfium_page(scanned_pdf, 0)
        assert page["ocr"] is True
        assert "HELLO" in page["text"].upper()
        assert len(page["text_objects"]) > 0
        assert page["text_objects"][0]["font_size"] is None


class TestCombineAndFlatten:
    """Test suite for combine_pdfium_pages and flatten_pdfium_document."""

    def test_combine_wraps_pages_in_envelope(self, sample_pdf):
        page = pdfium_extractors.parse_pdfium_page(sample_pdf, 0)
        doc = pdfium_extractors.combine_pdfium_pages(sample_pdf, 1, [page], [])
        assert doc["document"]["page_count"] == 1
        assert doc["document"]["source"] == sample_pdf
        assert doc["document"]["errors"] is None
        assert len(doc["document"]["pages"]) == 1

    def test_combine_keeps_errors(self, sample_pdf):
        doc = pdfium_extractors.combine_pdfium_pages(sample_pdf, 1, [], ["Page 1: boom"])
        assert doc["document"]["errors"] == ["Page 1: boom"]

    def test_flatten_yields_text_and_image_records(self, sample_pdf):
        page = pdfium_extractors.parse_pdfium_page(sample_pdf, 0)
        doc = pdfium_extractors.combine_pdfium_pages(sample_pdf, 1, [page], [])
        records = pdfium_extractors.flatten_pdfium_document(doc)
        types = {r["type"] for r in records}
        assert "text" in types
        assert "image" in types
        text_rec = next(r for r in records if r["type"] == "text")
        assert set(text_rec.keys()) == {
            "page", "type", "text", "font_size",
            "x", "y", "w", "h", "bbox", "file",
            "px_width", "px_height", "ocr",
        }
        assert text_rec["page"] == 1

    def test_flatten_empty_document(self):
        doc = {"document": {"source": "x", "page_count": 0, "errors": None, "pages": []}}
        assert pdfium_extractors.flatten_pdfium_document(doc) == []


class TestDedupePdfiumImages:
    """Test suite for dedupe_pdfium_images."""

    def test_collapses_byte_identical_images(self, tmp_path):
        # Two files with identical bytes -> one should be removed and repointed.
        a = tmp_path / "a.png"
        b = tmp_path / "b.png"
        a.write_bytes(b"PNGDATA")
        b.write_bytes(b"PNGDATA")
        document = {
            "document": {
                "source": "x",
                "page_count": 1,
                "errors": None,
                "pages": [
                    {
                        "page_number": 1,
                        "width": 10.0,
                        "height": 10.0,
                        "text": "",
                        "text_objects": [],
                        "images": [
                            {"file": str(a), "bbox": [], "position": {}, "px_width": 1, "px_height": 1, "extracted": True},
                            {"file": str(b), "bbox": [], "position": {}, "px_width": 1, "px_height": 1, "extracted": True},
                        ],
                        "ocr": False,
                    }
                ],
            }
        }
        removed = pdfium_extractors.dedupe_pdfium_images(document)
        assert removed == 1
        assert not b.exists()
        imgs = document["document"]["pages"][0]["images"]
        assert imgs[0]["file"] == str(a)
        assert imgs[1]["file"] == str(a)

    def test_skips_missing_and_unwritten_files(self, tmp_path):
        document = {
            "document": {
                "source": "x",
                "page_count": 1,
                "errors": None,
                "pages": [
                    {
                        "page_number": 1,
                        "width": 10.0,
                        "height": 10.0,
                        "text": "",
                        "text_objects": [],
                        "images": [
                            {"file": None, "bbox": [], "position": {}, "px_width": 1, "px_height": 1, "extracted": False},
                            {"file": str(tmp_path / "gone.png"), "bbox": [], "position": {}, "px_width": 1, "px_height": 1, "extracted": True},
                        ],
                        "ocr": False,
                    }
                ],
            }
        }
        assert pdfium_extractors.dedupe_pdfium_images(document) == 0
