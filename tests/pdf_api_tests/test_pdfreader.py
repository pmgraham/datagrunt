"""Tests for the public PDFReader API."""

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.pdf_api.pdfreader import PDFReader


class TestPDFReader:
    """Test suite for PDFReader."""

    def test_init_normalizes_engine(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="Py Mu PDF")
        assert reader.engine == "pymupdf"
        assert reader.is_pdf

    def test_to_dicts(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(doc["document"]["pages"]) == 1

    def test_get_sample(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        page = reader.get_sample()
        assert page["page_number"] == 1

    def test_to_dataframe(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 2

    def test_to_arrow_table(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        assert isinstance(reader.to_arrow_table(), pa.Table)

    def test_empty_pdf_returns_empty_objects(self, empty_pdf):
        reader = PDFReader(empty_pdf)
        assert reader.to_dicts() == {}
        assert reader.to_dataframe().is_empty()

    def test_invalid_engine_raises(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="ghostscript")
        with pytest.raises(ValueError):
            reader.to_dicts()


class TestDropLayoutTablesPassthrough:
    """Verify PDFReader threads drop_layout_tables to the filter."""

    def _spy(self, monkeypatch):
        from datagrunt.core.pdf_io.pdfcomponents import ParsedDocument

        calls = []
        original = ParsedDocument.drop_layout_tables
        monkeypatch.setattr(
            ParsedDocument,
            "drop_layout_tables",
            lambda self, *a, **k: (calls.append(True), original(self, *a, **k))[1],
        )
        return calls

    def test_to_dicts_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_dicts(drop_layout_tables=True)
        assert calls == [True]

    def test_to_dataframe_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_dataframe(drop_layout_tables=True)
        assert calls == [True]

    def test_to_arrow_table_threads_flag(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_arrow_table(drop_layout_tables=True)
        assert calls == [True]

    def test_default_off(self, sample_pdf, monkeypatch):
        calls = self._spy(monkeypatch)
        PDFReader(sample_pdf).to_dicts()
        assert calls == []


class TestTopLevelExports:
    """Verify PDF classes are importable from the package root."""

    def test_top_level_imports(self):
        from datagrunt import PDFReader, PDFWriter
        assert PDFReader.__name__ == "PDFReader"
        assert PDFWriter.__name__ == "PDFWriter"


class TestPDFReaderPdfiumEngine:
    """End-to-end PDFReader tests using the pdfium engine."""

    def test_reader_pdfium_to_dicts(self, sample_pdf):
        doc = PDFReader(sample_pdf, engine="pdfium", native=True).to_dicts()
        page = doc["document"]["pages"][0]
        assert "text" in page
        assert "text_objects" in page

    def test_reader_pdfium_normalized_engine_name(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="PDF ium")
        assert reader.engine == "pdfium"


class TestPDFReaderStructured:
    def test_reader_structured_unified_schema(self, sample_pdf):
        from datagrunt import PDFReader

        doc = PDFReader(sample_pdf, engine="pdfium").to_dicts()
        page = doc["document"]["pages"][0]
        assert "elements" in page and "classification" in page
