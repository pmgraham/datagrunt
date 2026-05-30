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
