"""
Datagrunt

A Python library designed to simplify the way you work with CSV, Excel, Parquet, and PDF files. This
module provides inferred CSV delimiters and helper methods for reading and
writing CSV files. PDF support (``PDFReader``, ``PDFWriter``) needs the [pdf]
extra: ``uv pip install "datagrunt[pdf]"``.

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

__version__ = "4.6.45"
__author__ = "Martin Graham"
__license__ = "MIT"

# Import key classes, functions, or submodules that should be available at
# the package level
import logging
from typing import TYPE_CHECKING

from datagrunt.csv_api import CSVReader, CSVWriter
from datagrunt.excel_api import ExcelReader, ExcelWriter
from datagrunt.parquet_api import ParquetReader, ParquetWriter
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
from datagrunt.pdf._extra import load as _load

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
]

if TYPE_CHECKING:
    from datagrunt.pdf import PDFReader, PDFWriter


def __getattr__(name: str) -> object:
    """``PDFReader`` and ``PDFWriter`` load on first access: ``import datagrunt`` never needs the [pdf] extra.

    Without the extra, constructing either raises ``ImportError`` with the install hint
    (see :mod:`datagrunt.pdf`).
    """
    if name not in _LAZY_CLASSES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return _load(name)


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_CLASSES})


# Optionally, you can include a logger for your package
logging.getLogger(__name__).addHandler(logging.NullHandler())
