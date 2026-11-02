"""datagrunt's PDF subsystem (5.0): ``PDFReader``, ``PDFWriter`` and the document model they produce.

Typed, immutable element types for extracted PDF content, their exceptions,
and the reader and writer. ``datagrunt`` re-exports the reader, the writer,
``PDFDocument``, the element types and the three exceptions of spec §2.1
(``DatagruntPDFError``, ``PDFEncryptedError``, ``PDFCorruptError``);
``PDFSourceChangedError`` and ``BundleCorruptError``, which the writer's and
the reader's bundle paths raise, are exported here only.

The element types and exceptions need only datagrunt's core dependencies.
``PDFReader`` and ``PDFWriter`` need the [pdf] extra (pypdfium2 and Pillow),
so each is imported on first access (``__getattr__``, PEP 562): importing
this package never imports the extra, and without it, constructing either
class raises ``ImportError`` with the install hint
(:mod:`datagrunt.pdf._extra`).
"""

from typing import TYPE_CHECKING

from datagrunt.pdf._extra import LAZY_CLASSES as _LAZY_CLASSES
from datagrunt.pdf._extra import load_class as _load_class
from datagrunt.pdf.elements import (
    HIDDEN_REASONS,
    BBox,
    BulletList,
    DocumentElement,
    DocumentMetadata,
    ExtractionMethod,
    Figure,
    Heading,
    HiddenReason,
    KeyValue,
    Page,
    PageImage,
    PageQuality,
    PageRoute,
    PageStatus,
    Paragraph,
    PDFDocument,
    Section,
    Table,
    make_element_id,
)
from datagrunt.pdf.exceptions import (
    BundleCorruptError,
    DatagruntPDFError,
    PDFCorruptError,
    PDFEncryptedError,
    PDFSourceChangedError,
)

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
    "PageStatus",
    "PageRoute",
    "ExtractionMethod",
    "make_element_id",
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
    "PDFSourceChangedError",
    "BundleCorruptError",
    "PDFReader",
    "PDFWriter",
]

if TYPE_CHECKING:
    from datagrunt.pdf.reader import PDFReader
    from datagrunt.pdf.writer import PDFWriter


def __getattr__(name: str) -> type:
    """``PDFReader`` or ``PDFWriter``, imported on first access (see the module notes)."""
    if name not in _LAZY_CLASSES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return _load_class(name)


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_CLASSES})
