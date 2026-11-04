"""
Datagrunt

A Python library designed to simplify the way you work with CSV, Excel, Parquet, and PDF files. This
module provides inferred CSV delimiters and helper methods for reading and
writing CSV files. PDF support (``PDFReader``, ``PDFWriter``) needs the [pdf]
extra: ``uv pip install "datagrunt[pdf]"``.

Each API is imported at the first access of one of its names (PEP 562): the
CSV, Excel and Parquet readers and writers, and ``PDFReader`` and
``PDFWriter``. ``import datagrunt``, which every PDF worker process does as it
starts, loads none of them (#439); ``from datagrunt import CSVReader`` imports
the CSV API and binds its ``CSVReader``, the same object as before.

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

__version__ = "5.0.0"
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
# The subpackages that ``import datagrunt`` bound as its attributes when it imported these APIs eagerly: each is
# still an attribute, imported at its first access.
_SUBPACKAGES = frozenset({"core", "csv_api", "excel_api", "parquet_api"})

if TYPE_CHECKING:
    from datagrunt.csv_api import CSVReader, CSVWriter
    from datagrunt.excel_api import ExcelReader, ExcelWriter
    from datagrunt.parquet_api import ParquetReader, ParquetWriter
    from datagrunt.pdf import PDFReader, PDFWriter


def __getattr__(name: str) -> object:
    """Each API's name, imported at its first access (module notes).

    ``PDFReader`` and ``PDFWriter`` never need the [pdf] extra to be named:
    without it, constructing either raises ``ImportError`` with the install
    hint (see :mod:`datagrunt.pdf`).

    Raises:
        AttributeError: If ``name`` is none of this package's names.
        ImportError: As importing the API that defines ``name`` raises it.
    """
    if name in _LAZY_CLASSES:
        return _load(name)
    if name in _API_NAMES:
        return getattr(_import_module(_API_NAMES[name]), name)
    if name in _SUBPACKAGES:
        return _import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_CLASSES, *_API_NAMES, *_SUBPACKAGES})


# Optionally, you can include a logger for your package
logging.getLogger(__name__).addHandler(logging.NullHandler())
