"""Tests for PdfiumNativeReader (native pdfium schema)."""

from datagrunt.core.pdf_io.extraction.pdfium_native import PdfiumNativeReader


class TestPdfiumNativeReader:
    def test_parse_page_native_schema(self, sample_pdf):
        page = PdfiumNativeReader(sample_pdf).parse_page(0)
        assert set(page.keys()) == {"page_number", "width", "height", "text", "text_objects", "images", "ocr"}
        assert page["page_number"] == 1
        assert "Quarterly Report" in page["text"]

    def test_text_object_and_image_shapes(self, sample_pdf, tmp_path):
        page = PdfiumNativeReader(sample_pdf).parse_page(0, image_output_dir=str(tmp_path))
        obj = page["text_objects"][0]
        assert set(obj.keys()) == {"text", "bbox", "position", "font_size"}
        assert set(obj["position"].keys()) == {"x", "y", "w", "h"}
        img = page["images"][0]
        assert set(img.keys()) == {"file", "bbox", "position", "px_width", "px_height", "extracted"}

    def test_combine_and_flatten(self, sample_pdf):
        reader = PdfiumNativeReader(sample_pdf)
        page = reader.parse_page(0)
        doc = reader.combine(1, [page], [])
        assert doc["document"]["page_count"] == 1
        records = reader.flatten(doc)
        assert {r["type"] for r in records} >= {"text", "image"}
        text_rec = next(r for r in records if r["type"] == "text")
        assert set(text_rec.keys()) == {
            "page", "type", "text", "font_size", "x", "y", "w", "h", "bbox", "file", "px_width", "px_height", "ocr"
        }

    def test_to_markdown_normalizes_crlf(self):
        # pdfium emits \r\n line endings; markdown must not leak \r and must
        # split paragraphs on blank lines (\r\n\r\n).
        doc = {
            "document": {
                "pages": [
                    {"text": "Page 1 line one\r\nstill para one\r\n\r\nPage 1 paragraph two"}
                ]
            }
        }
        markdown = PdfiumNativeReader.to_markdown(doc)
        assert "\r" not in markdown
        assert markdown == "Page 1 line one\nstill para one\n\nPage 1 paragraph two\n"

    def test_dedupe_images(self, tmp_path):
        a, b = tmp_path / "a.png", tmp_path / "b.png"
        a.write_bytes(b"X")
        b.write_bytes(b"X")
        doc = {"document": {"pages": [{"images": [{"file": str(a)}, {"file": str(b)}]}]}}
        removed = PdfiumNativeReader.dedupe_images(doc)
        assert removed == 1 and not b.exists()
