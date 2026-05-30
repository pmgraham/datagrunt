"""Tests for the PDF engine factory."""

import pytest

from datagrunt.core.pdf_io.engines import (
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
)
from datagrunt.core.pdf_io.factories import PDFEngineFactory


class TestPDFEngineFactory:
    """Test suite for PDFEngineFactory."""

    def test_create_reader(self, sample_pdf):
        factory = PDFEngineFactory(sample_pdf, "pymupdf")
        assert isinstance(factory.create_reader(), PDFReaderPyMuPDFEngine)

    def test_create_writer(self, sample_pdf):
        factory = PDFEngineFactory(sample_pdf, "pymupdf")
        assert isinstance(factory.create_writer(), PDFWriterPyMuPDFEngine)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PDFEngineFactory("nope.pdf", "pymupdf")

    def test_invalid_engine_raises(self, sample_pdf):
        with pytest.raises(ValueError):
            PDFEngineFactory(sample_pdf, "ghostscript")

    def test_engine_normalized(self, sample_pdf):
        factory = PDFEngineFactory(sample_pdf, "Py Mu PDF")
        assert factory.engine == "pymupdf"
