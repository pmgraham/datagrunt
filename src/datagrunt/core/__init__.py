"""Initializes the core module of the datagrunt package."""

from datagrunt.core.ai import (
    CSV_SCHEMA_PROMPT,
    CSV_SCHEMA_SYSTEM_INSTRUCTIONS,
    GENERATE_SQL_QUERY,
    SUGGEST_DATA_TRANSFORMATIONS,
    AIEngineFactory,
    AIEngineProperties,
    GoogleAIEngine,
    prompts,
)
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
from datagrunt.core.databases import DuckDBDatabase, DuckDBQueries
from datagrunt.core.file_io import FileProperties
from datagrunt.core.pdf_io import (
    PDFComponents,
    PDFEngineFactory,
    PDFEngineProperties,
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
    dedupe_document_images,
    drop_layout_tables,
    flatten_document_elements,
    parse_document,
    parse_page,
    set_export_filename,
)

__all__ = [
    # AI
    "AIEngineFactory",
    "AIEngineProperties",
    "GoogleAIEngine",
    "CSV_SCHEMA_PROMPT",
    "CSV_SCHEMA_SYSTEM_INSTRUCTIONS",
    "GENERATE_SQL_QUERY",
    "SUGGEST_DATA_TRANSFORMATIONS",
    "prompts",
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
    "DuckDBDatabase",
    "DuckDBQueries",
    # File IO
    "FileProperties",
    # PDF IO
    "PDFComponents",
    "PDFEngineFactory",
    "PDFEngineProperties",
    "PDFReaderPyMuPDFEngine",
    "PDFWriterPyMuPDFEngine",
    "parse_page",
    "parse_document",
    "flatten_document_elements",
    "dedupe_document_images",
    "drop_layout_tables",
    "set_export_filename",
]
