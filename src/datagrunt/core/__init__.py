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
    CSVRows,
    CSVStringSample,
    CSVWriterDuckDBEngine,
    CSVWriterPolarsEngine,
)
from datagrunt.core.databases import DuckDBDatabase, DuckDBQueries
from datagrunt.core.file_io import FileProperties

__all__ = [
    # AI
    'AIEngineFactory',
    'AIEngineProperties',
    'GoogleAIEngine',
    'CSV_SCHEMA_PROMPT',
    'CSV_SCHEMA_SYSTEM_INSTRUCTIONS',
    'GENERATE_SQL_QUERY',
    'SUGGEST_DATA_TRANSFORMATIONS',
    'prompts',
    # CSV IO
    'CSVEngineFactory',
    'CSVReaderDuckDBEngine',
    'CSVReaderPolarsEngine',
    'CSVWriterDuckDBEngine',
    'CSVWriterPolarsEngine',
    'CSVEngineProperties',
    'EngineProperties',
    'CSVComponents',
    'CSVColumnNameNormalizer',
    'CSVColumns',
    'CSVDelimiter',
    'CSVDialect',
    'CSVRows',
    'CSVStringSample',
    # Databases
    'DuckDBDatabase',
    'DuckDBQueries',
    # File IO
    'FileProperties',
]
