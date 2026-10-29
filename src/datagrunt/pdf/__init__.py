"""datagrunt v5 PDF document model (preview).

Typed, immutable element types for extracted PDF content. This package is not
re-exported from the top-level ``datagrunt`` package; the v4 ``PDFReader`` /
``PDFWriter`` API is unchanged.
"""

from datagrunt.pdf.elements import (
    HIDDEN_REASONS,
    BBox,
    BulletList,
    DocumentElement,
    DocumentMetadata,
    Figure,
    Heading,
    HiddenReason,
    KeyValue,
    Page,
    PageImage,
    PageQuality,
    Paragraph,
    PDFDocument,
    Section,
    Table,
)
from datagrunt.pdf.exceptions import DatagruntPDFError, PDFCorruptError, PDFEncryptedError

__all__ = [
    "BBox",
    "PageQuality",
    "DocumentMetadata",
    "Heading",
    "Paragraph",
    "KeyValue",
    "Table",
    "BulletList",
    "Figure",
    "PageImage",
    "Page",
    "Section",
    "PDFDocument",
    "DocumentElement",
    "HiddenReason",
    "HIDDEN_REASONS",
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
]
