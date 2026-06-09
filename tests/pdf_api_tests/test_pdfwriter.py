"""Tests for the public PDFWriter API."""

import json
import os
import pytest

from datagrunt.pdf_api.pdfwriter import PDFWriter


class TestPDFWriter:
    """Test suite for PDFWriter."""

    def test_write_json_default(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_json()
        assert os.path.basename(path) == "output.json"
        with open(path) as f:
            data = json.load(f)
        assert data["document"]["total_pages"] == 1

    def test_write_json_custom_name_and_images(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        img_dir = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        path = writer.write_json(
            export_filename="report.json", image_output_dir=str(img_dir)
        )
        assert os.path.basename(path) == "report.json"
        with open(path) as f:
            data = json.load(f)
        img_paths = [
            e["metadata"]["file_path"]
            for page in data["document"]["pages"]
            for e in page["elements"]
            if e["type"] == "image"
        ]
        assert img_paths and all(os.path.isfile(p) for p in img_paths)

    def test_write_json_newline_delimited(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_json_newline_delimited()
        assert os.path.basename(path) == "output.jsonl"
        with open(path) as f:
            lines = [line for line in f if line.strip()]
        assert len(lines) >= 1

    def test_extract_images(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        paths = writer.extract_images(output_dir=str(out))
        assert paths and all(os.path.isfile(p) for p in paths)

    def test_engine_normalized(self, sample_pdf):
        writer = PDFWriter(sample_pdf, engine="Py Mu PDF")
        assert writer.engine == "pymupdf"

    @staticmethod
    def _two_page_dupe_pdf(tmp_path):
        """Build a 2-page PDF with the same image embedded on each page."""
        import pymupdf

        doc = pymupdf.open()
        pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 120, 120))
        pix.set_rect(pix.irect, (0, 128, 255))
        img = pix.tobytes("png")
        for _ in range(2):
            page = doc.new_page(width=300, height=300)
            page.insert_image(pymupdf.Rect(50, 50, 170, 170), stream=img)
        path = tmp_path / "dupe.pdf"
        doc.save(str(path))
        doc.close()
        return str(path)

    def test_extract_images_dedupes_by_default(self, tmp_path):
        pdf = self._two_page_dupe_pdf(tmp_path)
        writer = PDFWriter(pdf)
        paths = writer.extract_images(output_dir=str(tmp_path / "imgs"))
        assert len(paths) == 1

    def test_extract_images_dedupe_can_be_disabled(self, tmp_path):
        pdf = self._two_page_dupe_pdf(tmp_path)
        writer = PDFWriter(pdf)
        paths = writer.extract_images(output_dir=str(tmp_path / "imgs2"), dedupe=False)
        assert len(paths) == 2

    def test_write_json_dedupes_by_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        pdf = self._two_page_dupe_pdf(tmp_path)
        writer = PDFWriter(pdf)
        jpath = writer.write_json(image_output_dir=str(tmp_path / "imgs3"))
        with open(jpath) as f:
            doc = json.load(f)
        img_paths = [
            e["metadata"]["file_path"]
            for pg in doc["document"]["pages"]
            for e in pg["elements"]
            if e["type"] == "image"
        ]
        assert len(img_paths) == 2 and len(set(img_paths)) == 1

    def test_write_json_threads_drop_layout_tables(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        calls = []
        original = ParsedDocument.drop_layout_tables
        monkeypatch.setattr(
            ParsedDocument,
            "drop_layout_tables",
            lambda self, *a, **k: (calls.append(True), original(self, *a, **k))[1],
        )
        PDFWriter(sample_pdf).write_json(drop_layout_tables=True)
        assert calls == [True]

    def test_write_markdown_default(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_markdown()
        assert os.path.basename(path) == "output.md"
        with open(path) as f:
            content = f.read()
        assert len(content) > 0

    def test_write_markdown_custom_name_and_images(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        img_dir = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        path = writer.write_markdown(
            export_filename="report.md", image_output_dir=str(img_dir)
        )
        assert os.path.basename(path) == "report.md"
        with open(path) as f:
            content = f.read()
        assert len(content) > 0
        assert "report_page" in content

    def test_write_markdown_pdfium_native(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf, engine="pdfium", native=True)
        path = writer.write_markdown()
        assert os.path.basename(path) == "output.md"
        with open(path) as f:
            content = f.read()
        assert len(content) > 0


class TestPDFWriterJsonAndDictInputs:
    """Tests for initializing PDFWriter with JSON files or dictionaries."""

    @pytest.fixture
    def dummy_structured_dict(self):
        return {
            "document": {
                "source": "dummy.pdf",
                "total_pages": 1,
                "pages": [
                    {
                        "page_number": 1,
                        "width": 100.0,
                        "height": 100.0,
                        "elements": [
                            {"id": "el1", "type": "header", "content": "Header Text", "position": {"x": 10, "y": 10, "w": 80, "h": 10}},
                            {"id": "el2", "type": "body_text", "content": "Hello body", "position": {"x": 10, "y": 30, "w": 80, "h": 10}},
                            {"id": "el3", "type": "image", "content": None, "position": {"x": 10, "y": 50, "w": 80, "h": 10}, "metadata": {"file_path": "/dummy/img.png"}},
                        ]
                    }
                ]
            }
        }

    def test_writer_with_dict(self, dummy_structured_dict, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(dummy_structured_dict)
        assert writer._parsed_dict == dummy_structured_dict
        assert writer.total_pages == 1

        # Test write_json
        jpath = writer.write_json("test_out.json")
        assert os.path.exists(jpath)
        with open(jpath) as f:
            data = json.load(f)
        assert data["document"]["source"] == "dummy.pdf"

        # Test write_json_newline_delimited
        jlpath = writer.write_json_newline_delimited("test_out.jsonl")
        assert os.path.exists(jlpath)
        with open(jlpath) as f:
            lines = f.readlines()
        assert len(lines) == 3

        # Test write_markdown
        mdpath = writer.write_markdown("test_out.md")
        assert os.path.exists(mdpath)
        with open(mdpath) as f:
            content = f.read()
        assert "# Header Text" in content
        assert "Hello body" in content
        assert "![img.png]" in content
        assert "dummy/img.png" in content

        # Test extract_images
        img_paths = writer.extract_images()
        assert img_paths == ["/dummy/img.png"]

    def test_writer_with_json_file(self, tmp_path, dummy_structured_dict, monkeypatch):
        monkeypatch.chdir(tmp_path)
        json_file = tmp_path / "doc.json"
        with open(json_file, "w") as f:
            json.dump(dummy_structured_dict, f)

        writer = PDFWriter(str(json_file))
        assert writer._parsed_dict == dummy_structured_dict
        assert writer.total_pages == 1

        # Test write_markdown
        mdpath = writer.write_markdown("test_out_json.md")
        assert os.path.exists(mdpath)
        with open(mdpath) as f:
            content = f.read()
        assert "# Header Text" in content
        assert "Hello body" in content

