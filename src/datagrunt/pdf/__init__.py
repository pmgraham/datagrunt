"""datagrunt's PDF subsystem (5.0): ``PDFReader``, ``PDFWriter`` and the document model they produce.

Typed, immutable element types for extracted PDF content, their exceptions,
and the reader and writer. ``datagrunt`` re-exports the reader, the writer,
``PDFDocument``, the element types and the three exceptions of spec §2.1
(``DatagruntPDFError``, ``PDFEncryptedError``, ``PDFCorruptError``);
``PDFSourceChangedError`` and ``BundleCorruptError``, which the writer's and
the reader's bundle paths raise, are exported here only.

The element types and exceptions need only datagrunt's core dependencies.
``PDFReader``, ``PDFWriter`` and the bundle functions ``load_bundle`` and
``write_bundle`` need the [pdf] extra (pypdfium2 and Pillow), so each is
imported on first access (``__getattr__``, PEP 562): importing this package
never imports the extra, and without it, constructing either class or
calling either function raises ``ImportError`` with the install hint
(:mod:`datagrunt.pdf._extra`).
"""

from typing import TYPE_CHECKING

from datagrunt.pdf._extra import LAZY_CLASSES as _LAZY_CLASSES
from datagrunt.pdf._extra import LAZY_FUNCTIONS as _LAZY_FUNCTIONS
from datagrunt.pdf._extra import load as _load
from datagrunt.pdf.elements import (
    HIDDEN_REASONS,
    PAGE_ROUTES,
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
    "PAGE_ROUTES",
    "ExtractionMethod",
    "make_element_id",
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
    "PDFSourceChangedError",
    "BundleCorruptError",
    "PDFReader",
    "PDFWriter",
    "load_bundle",
    "write_bundle",
]

if TYPE_CHECKING:
    from datagrunt.pdf.bundle import load_bundle, write_bundle
    from datagrunt.pdf.reader import PDFReader
    from datagrunt.pdf.writer import PDFWriter

_LAZY_NAMES = frozenset({*_LAZY_CLASSES, *_LAZY_FUNCTIONS})


def __getattr__(name: str) -> object:
    """``PDFReader``, ``PDFWriter``, ``load_bundle`` or ``write_bundle``, imported on first access (module notes)."""
    if name not in _LAZY_NAMES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return _load(name)


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_NAMES})
