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
from datagrunt.core.databases import DuckDBQueries
from datagrunt.core.file_io import FileProperties
from datagrunt.core.pdf_io import (
    DocumentAssembler,
    ExtractionBackend,
    ParsedDocument,
    PDFComponents,
    PDFEngineFactory,
    PDFEngineProperties,
    PdfiumBackend,
    PdfiumNativeReader,
    PdfPlumberTableExtractor,
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
    PyMuPDFBackend,
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
    "DuckDBQueries",
    # File IO
    "FileProperties",
    # PDF IO
    "PDFComponents",
    "DocumentAssembler",
    "ParsedDocument",
    "PDFEngineFactory",
    "PDFEngineProperties",
    "PDFReaderPyMuPDFEngine",
    "PDFWriterPyMuPDFEngine",
    "set_export_filename",
    "ExtractionBackend",
    "PdfiumBackend",
    "PdfiumNativeReader",
    "PdfPlumberTableExtractor",
    "PyMuPDFBackend",
]
