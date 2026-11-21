"""Initializes the core module of the datagrunt package.

Re-exports are resolved lazily on first access (PEP 562, #458) so importing a
single ``datagrunt.core.*`` subpackage does not eagerly import its siblings and
concurrent first imports cannot deadlock on opposing module-lock orders.
"""

from importlib import import_module as _import_module
from types import MappingProxyType as _MappingProxyType
from typing import TYPE_CHECKING

__all__ = [
    # CSV IO
    "CSVEngineFactory",
    "CSVReaderDuckDBEngine",
    "CSVReaderPolarsEngine",
    "CSVReaderPyArrowEngine",
    "CSVWriterDuckDBEngine",
    "CSVWriterPolarsEngine",
    "CSVWriterPyArrowEngine",
    "CSVEngineProperties",
    # CSV Components
    "CSVComponents",
    "CSVColumnNameNormalizer",
    "CSVColumns",
    "CSVDelimiter",
    "CSVDialect",
    "CSVRows",
    "CSVStringSample",
    # Databases
    "DuckDBQueries",
    # Excel IO
    "ExcelComponents",
    "ExcelEngineFactory",
    "ExcelEngineProperties",
    "ExcelReaderEngine",
    "ExcelWriterEngine",
    # File IO
    "FileProperties",
    # Parquet IO
    "ParquetComponents",
    "ParquetEngineFactory",
    "ParquetEngineProperties",
    "ParquetReaderEngine",
    "ParquetWriterEngine",
]

# Each re-exported core name and the subpackage that defines it: imported at its first access (#458).
_CORE_NAMES = _MappingProxyType(
    {
        # CSV IO
        "CSVEngineFactory": "datagrunt.core.csv_io",
        "CSVReaderDuckDBEngine": "datagrunt.core.csv_io",
        "CSVReaderPolarsEngine": "datagrunt.core.csv_io",
        "CSVReaderPyArrowEngine": "datagrunt.core.csv_io",
        "CSVWriterDuckDBEngine": "datagrunt.core.csv_io",
        "CSVWriterPolarsEngine": "datagrunt.core.csv_io",
        "CSVWriterPyArrowEngine": "datagrunt.core.csv_io",
        "CSVEngineProperties": "datagrunt.core.csv_io",
        # CSV Components
        "CSVComponents": "datagrunt.core.csv_io",
        "CSVColumnNameNormalizer": "datagrunt.core.csv_io",
        "CSVColumns": "datagrunt.core.csv_io",
        "CSVDelimiter": "datagrunt.core.csv_io",
        "CSVDialect": "datagrunt.core.csv_io",
        "CSVRows": "datagrunt.core.csv_io",
        "CSVStringSample": "datagrunt.core.csv_io",
        # Databases
        "DuckDBQueries": "datagrunt.core.databases",
        # Excel IO
        "ExcelComponents": "datagrunt.core.excel_io",
        "ExcelEngineFactory": "datagrunt.core.excel_io",
        "ExcelEngineProperties": "datagrunt.core.excel_io",
        "ExcelReaderEngine": "datagrunt.core.excel_io",
        "ExcelWriterEngine": "datagrunt.core.excel_io",
        # File IO
        "FileProperties": "datagrunt.core.file_io",
        # Parquet IO
        "ParquetComponents": "datagrunt.core.parquet_io",
        "ParquetEngineFactory": "datagrunt.core.parquet_io",
        "ParquetEngineProperties": "datagrunt.core.parquet_io",
        "ParquetReaderEngine": "datagrunt.core.parquet_io",
        "ParquetWriterEngine": "datagrunt.core.parquet_io",
    }
)
# The subpackages that ``import datagrunt.core`` bound as its attributes when it imported them eagerly: each is
# still an attribute, imported at its first access.
_SUBPACKAGES = frozenset({"csv_io", "databases", "excel_io", "file_io", "parquet_io"})

if TYPE_CHECKING:
    from datagrunt.core.csv_io import (
        CSVColumnNameNormalizer,
        CSVColumns,
        CSVComponents,
        CSVDelimiter,
        CSVDialect,
        CSVEngineFactory,
        CSVEngineProperties,
        CSVReaderDuckDBEngine,
        CSVReaderPolarsEngine,
        CSVReaderPyArrowEngine,
        CSVRows,
        CSVStringSample,
        CSVWriterDuckDBEngine,
        CSVWriterPolarsEngine,
        CSVWriterPyArrowEngine,
    )
    from datagrunt.core.databases import DuckDBQueries
    from datagrunt.core.excel_io import (
        ExcelComponents,
        ExcelEngineFactory,
        ExcelEngineProperties,
        ExcelReaderEngine,
        ExcelWriterEngine,
    )
    from datagrunt.core.file_io import FileProperties
    from datagrunt.core.parquet_io import (
        ParquetComponents,
        ParquetEngineFactory,
        ParquetEngineProperties,
        ParquetReaderEngine,
        ParquetWriterEngine,
    )


def __getattr__(name: str) -> object:
    """Resolve core re-exports and subpackages on first access (#458).

    Each re-exported name is kept as an attribute of :mod:`datagrunt.core`
    after first access so later lookups find it there directly.

    Raises:
        AttributeError: If ``name`` is none of this package's names.
    """
    if name in _CORE_NAMES:
        value = getattr(_import_module(_CORE_NAMES[name]), name)
        globals()[name] = value
        return value
    if name in _SUBPACKAGES:  # the import system binds an imported subpackage as the package's attribute
        return _import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals(), *_CORE_NAMES, *_SUBPACKAGES})
