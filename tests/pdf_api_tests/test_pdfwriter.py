"""Tests for the public PDFWriter API."""

import json
import os

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
