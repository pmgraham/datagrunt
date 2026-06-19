"""Tests for clean error handling of encrypted / password-protected PDFs."""

import pypdfium2 as pdfium
import pytest

from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument
from datagrunt.pdf_api.pdfreader import PDFReader


class TestEncryptedPdf:
    """Encrypted PDFs must surface a clear, catchable datagrunt-level error."""

    def test_pdfium_document_raises_clear_error(self, encrypted_pdf):
        with pytest.raises(ValueError) as excinfo:
            PdfiumDocument(encrypted_pdf)
        message = str(excinfo.value).lower()
        assert "encrypted" in message or "password" in message
        # Must not leak the raw backend exception type.
        assert not isinstance(excinfo.value, pdfium.PdfiumError)

    @pytest.mark.parametrize(
        "engine,native",
        [
            ("pdfium", True),
            ("pdfium", False),
            ("pymupdf", False),
        ],
    )
    def test_reader_raises_clear_error(self, encrypted_pdf, engine, native):
        reader = PDFReader(encrypted_pdf, engine=engine, native=native)
        with pytest.raises(ValueError) as excinfo:
            reader.to_dicts()
        message = str(excinfo.value).lower()
        assert "encrypted" in message or "password" in message
        assert not isinstance(excinfo.value, pdfium.PdfiumError)
