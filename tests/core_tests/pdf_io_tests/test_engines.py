"""Tests for PDF engines."""

import json
import os

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.core.pdf_io.engines import (
    PDFEngineProperties,
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
    set_export_filename,
)


class TestSetExportFilename:
    """Test suite for the output path resolver."""

    def test_returns_default_when_none(self):
        assert set_export_filename("output.json", None) == "output.json"

    def test_returns_override_when_given(self):
        assert set_export_filename("output.json", "custom.json") == "custom.json"


class TestPDFEngineProperties:
    """Test suite for the engine properties dataclass."""

    def test_defaults(self):
        props = PDFEngineProperties(filepath="x.pdf")
        assert props.json_export_filename == "output.json"
        assert props.json_newline_export_filename == "output.jsonl"
        assert props.images_export_dir == "output_images"
        assert "pymupdf" in props.valid_engines


class TestPDFReaderEngine:
    """Test suite for the PyMuPDF reader engine."""

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PDFReaderPyMuPDFEngine("nope.pdf")

    def test_to_dicts(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        doc = engine.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(doc["document"]["pages"]) == 1

    def test_get_sample(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        page = engine.get_sample()
        assert page["page_number"] == 1

    def test_to_dataframe(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        df = engine.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 2
        assert {"id", "type", "page", "x", "content", "metadata"} <= set(df.columns)

    def test_to_arrow_table(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        table = engine.to_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows >= 2



class TestPDFWriterEngine:
    """Test suite for the PyMuPDF writer engine."""

    def test_write_json_default_name(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json()
        assert os.path.basename(path) == "output.json"
        with open(path) as f:
            data = json.load(f)
        assert data["document"]["total_pages"] == 1

    def test_write_json_custom_name(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json(export_filename="custom.json")
        assert os.path.basename(path) == "custom.json"
        assert os.path.isfile(path)

    def test_write_json_newline_delimited(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json_newline_delimited()
        assert os.path.basename(path) == "output.jsonl"
        with open(path) as f:
            lines = [line for line in f if line.strip()]
        assert len(lines) >= 1
        json.loads(lines[0])  # each line is valid JSON

    def test_extract_images(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        paths = engine.extract_images(output_dir=str(out))
        assert len(paths) >= 1
        assert all(os.path.isfile(p) for p in paths)


class TestPDFWriterDedupe:
    """Test suite for image de-duplication in the writer engine."""

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
        out = tmp_path / "imgs"
        engine = PDFWriterPyMuPDFEngine(pdf)
        paths = engine.extract_images(output_dir=str(out))
        # The two byte-identical images collapse to a single file.
        assert len(paths) == 1
        assert os.path.isfile(paths[0])
        assert len([f for f in os.listdir(out) if f.endswith(".png")]) == 1

    def test_extract_images_dedupe_disabled(self, tmp_path):
        pdf = self._two_page_dupe_pdf(tmp_path)
        out = tmp_path / "imgs2"
        engine = PDFWriterPyMuPDFEngine(pdf)
        paths = engine.extract_images(output_dir=str(out), dedupe=False)
        assert len(paths) == 2

    def test_write_json_dedupes_image_references(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        pdf = self._two_page_dupe_pdf(tmp_path)
        engine = PDFWriterPyMuPDFEngine(pdf)
        jpath = engine.write_json(image_output_dir=str(tmp_path / "imgs3"))
        with open(jpath) as f:
            doc = json.load(f)
        img_paths = [
            e["metadata"]["file_path"]
            for pg in doc["document"]["pages"]
            for e in pg["elements"]
            if e["type"] == "image"
        ]
        # Both image elements reference the same (single) on-disk file.
        assert len(img_paths) == 2
        assert len(set(img_paths)) == 1
        assert all(os.path.isfile(p) for p in img_paths)


class TestDropLayoutTablesThreading:
    """Verify the drop_layout_tables flag is plumbed through the engines."""

    def _spy(self, monkeypatch):
        import datagrunt.core.pdf_io.pdfcomponents as pc

        calls = []
        original = pc.drop_layout_tables

        def spy(document, *args, **kwargs):
            calls.append(True)
            return original(document, *args, **kwargs)

        monkeypatch.setattr(pc, "drop_layout_tables", spy)
        return calls

    def test_reader_to_dicts_invokes_filter_when_true(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReaderPyMuPDFEngine(sample_pdf).to_dicts(drop_layout_tables=True)
        assert calls == [True]

    def test_reader_to_dicts_default_does_not_filter(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReaderPyMuPDFEngine(sample_pdf).to_dicts()
        assert calls == []

    def test_reader_to_dataframe_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReaderPyMuPDFEngine(sample_pdf).to_dataframe(drop_layout_tables=True)
        assert calls == [True]

    def test_writer_write_json_threads_flag(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        calls = self._spy(monkeypatch)
        PDFWriterPyMuPDFEngine(sample_pdf).write_json(drop_layout_tables=True)
        assert calls == [True]
