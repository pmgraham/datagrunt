"""PDF extraction backends (engine-agnostic, class-based)."""

from datagrunt.core.pdf_io.extraction.base import ExtractionBackend
from datagrunt.core.pdf_io.extraction.pdfium_backend import PdfiumBackend
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument
from datagrunt.core.pdf_io.extraction.pdfium_native import PdfiumNativeReader
from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend
from datagrunt.core.pdf_io.extraction.tables import PdfPlumberTableExtractor
from datagrunt.core.pdf_io.extraction.text_block_builder import TextBlockBuilder

__all__ = [
    "ExtractionBackend",
    "PyMuPDFBackend",
    "PdfiumBackend",
    "PdfiumNativeReader",
    "PdfiumDocument",
    "PdfPlumberTableExtractor",
    "TextBlockBuilder",
]
