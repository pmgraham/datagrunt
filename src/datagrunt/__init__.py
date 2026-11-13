"""
Datagrunt

A Python library designed to simplify the way you work with CSV, Excel, Parquet, and PDF files. This
module provides inferred CSV delimiters and helper methods for reading and
writing CSV files. PDF support (``PDFReader``, ``PDFWriter``) needs the [pdf]
extra: ``uv pip install "datagrunt[pdf]"``.

``import datagrunt`` loads only the PDF element types and exceptions and the
``Key`` schema marker (:mod:`datagrunt.rationalization`), none of the readers
and writers. Each API loads, with the libraries it uses, the first time one
of its names is used: ``from datagrunt import CSVReader`` (or
``datagrunt.CSVReader``) loads the CSV reader and writer, and the Excel,
Parquet and PDF APIs stay unloaded until one of their names is used.

Example:
    A simple example of how to use the main functionality of your package:

    from datagrunt import CSVReader

    csv_file = 'electric_vehicle_population_data.csv'
    engine = 'duckdb'

    dg = CSVReader(csv_file, engine=engine)

    dg.get_sample()

Attributes:
    __version__: A string representing the version of this module.
    __author__: The name of the package author.
    __license__: The license under which the package is released.
"""

__version__ = "5.3.9"
__author__ = "Martin Graham"
__license__ = "MIT"

# Import key classes, functions, or submodules that should be available at
# the package level
import logging
from importlib import import_module as _import_module
from types import MappingProxyType as _MappingProxyType
from typing import TYPE_CHECKING

from datagrunt.pdf import (
    BBox,
    BulletList,
    DatagruntPDFError,
    DocumentMetadata,
    Figure,
    Heading,
    KeyValue,
    Page,
    PageImage,
    PageQuality,
    Paragraph,
    PDFCorruptError,
    PDFDocument,
    PDFEncryptedError,
    Section,
    Table,
)
from datagrunt.pdf._extra import LAZY_CLASSES as _LAZY_CLASSES
from datagrunt.pdf._extra import LAZY_FUNCTIONS as _LAZY_FUNCTIONS
from datagrunt.pdf._extra import load as _load
from datagrunt.rationalization import Informational, Key  # needs no extra: imports neither pydantic nor httpx
from datagrunt.rationalization._extra import load as _load_rationalize

# You can define __all__ to specify what gets imported with "from package
# import *"
__all__ = [
    "CSVReader",
    "CSVWriter",
    "ExcelReader",
    "ExcelWriter",
    "ParquetReader",
    "ParquetWriter",
    "PDFReader",
    "PDFWriter",
    "PDFDocument",
    "BBox",
    "DocumentMetadata",
    "PageQuality",
    "Page",
    "Section",
    "Heading",
    "Paragraph",
    "KeyValue",
    "Table",
    "BulletList",
    "Figure",
    "PageImage",
    "DatagruntPDFError",
    "PDFEncryptedError",
    "PDFCorruptError",
    "CorpusEntry",
    "CorpusFailure",
    "CorpusManifest",
    "load_bundle",
    "write_bundle",
    "extract_corpus",
    "load_corpus",
    "Key",
    "Informational",
    "rationalize",
    "RationalizedResult",
    "BatchItem",
    "BatchPlan",
    "prepare_batch",
    "ingest_batch",
]

# Each CSV, Excel and Parquet name, and the module that defines it: imported at its first access (#439).
_API_NAMES = _MappingProxyType(
    {
        "CSVReader": "datagrunt.csv_api",
        "CSVWriter": "datagrunt.csv_api",
        "ExcelReader": "datagrunt.excel_api",
        "ExcelWriter": "datagrunt.excel_api",
        "ParquetReader": "datagrunt.parquet_api",
        "ParquetWriter": "datagrunt.parquet_api",
    }
)
_PDF_LAZY_NAMES = frozenset({*_LAZY_CLASSES, *_LAZY_FUNCTIONS})
_PDF_DIRECT_NAMES = _MappingProxyType(
    {
        "CorpusEntry": "datagrunt.pdf.corpus",
        "CorpusFailure": "datagrunt.pdf.corpus",
        "CorpusManifest": "datagrunt.pdf.corpus",
        "load_bundle": "datagrunt.pdf._bundle_load",
        "load_corpus": "datagrunt.pdf.corpus",
    }
)
_RATIONALIZE_NAMES = frozenset(
    {"rationalize", "RationalizedResult", "BatchItem", "BatchPlan", "prepare_batch", "ingest_batch"}
)
# The subpackages that ``import datagrunt`` bound as its attributes when it imported these APIs eagerly: each is
# still an attribute, imported at its first access.
_SUBPACKAGES = frozenset({"core", "csv_api", "excel_api", "parquet_api"})

if TYPE_CHECKING:
    from datagrunt.csv_api import CSVReader, CSVWriter
    from datagrunt.excel_api import ExcelReader, ExcelWriter
    from datagrunt.parquet_api import ParquetReader, ParquetWriter
    from datagrunt.pdf import (
        CorpusEntry,
        CorpusFailure,
        CorpusManifest,
        PDFReader,
        PDFWriter,
        extract_corpus,
        load_bundle,
        load_corpus,
        write_bundle,
    )
    from datagrunt.rationalization import (
        BatchItem,
        BatchPlan,
        RationalizedResult,
        ingest_batch,
        prepare_batch,
        rationalize,
    )


def __getattr__(name: str) -> object:
    """Each API's name, imported at its first access (module notes).

    A CSV, Excel or Parquet name is then kept as the package's attribute, so
    later accesses find it there. ``PDFReader``, ``PDFWriter``, ``write_bundle``
    and ``extract_corpus`` never need the [pdf] extra to be named: without it,
    constructing or calling them raises ``ImportError`` with the install hint
    (see :mod:`datagrunt.pdf`). ``rationalize`` and ``RationalizedResult``
    likewise resolve through :mod:`datagrunt.rationalization._extra`.

    Raises:
        AttributeError: If ``name`` is none of this package's names.
        ImportError: As importing the API that defines ``name`` raises it.
    """
    if name in _PDF_LAZY_NAMES:
        return _load(name)  # never kept: without the [pdf] extra it is a stand-in, and the extra can arrive later
    if name in _PDF_DIRECT_NAMES:
        value = getattr(_import_module(_PDF_DIRECT_NAMES[name]), name)
        globals()[name] = value
        return value
    if name in _RATIONALIZE_NAMES:
        return _load_rationalize(name)
    if name in _API_NAMES:
        value = getattr(_import_module(_API_NAMES[name]), name)
        globals()[name] = value
        return value
    if name in _SUBPACKAGES:  # the import system binds an imported subpackage as the package's attribute
        return _import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals(), *_PDF_LAZY_NAMES, *_PDF_DIRECT_NAMES, *_RATIONALIZE_NAMES, *_API_NAMES, *_SUBPACKAGES})


# Optionally, you can include a logger for your package
logging.getLogger(__name__).addHandler(logging.NullHandler())
