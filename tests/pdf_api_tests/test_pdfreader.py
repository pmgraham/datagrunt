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
