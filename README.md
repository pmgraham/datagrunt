# Welcome To Datagrunt

Datagrunt is a Python library designed to simplify the way you work with CSV and PDF files. It provides a streamlined approach to reading, processing, and transforming your data into various formats, making data manipulation efficient and intuitive.

## Why Datagrunt?

Born out of real-world frustration, Datagrunt eliminates the need for repetitive coding when handling CSV and PDF files. Whether you're a data analyst, data engineer, or data scientist, Datagrunt empowers you to focus on insights, not tedious data wrangling.

### What Datagrunt Is Not
Datagrunt is not an extension of or a replacement for DuckDB, Polars, or PyArrow, nor is it a comprehensive data processing solution. Instead, it's designed to simplify the way you work with CSV and PDF files — solving the pain point of inferring delimiters when a CSV structure is unknown, and turning PDFs into structured, queryable data. Datagrunt provides an easy way to convert CSV files to dataframes and export them to various formats, and to extract text, tables, and images from PDFs. One of Datagrunt's value propositions is its relative simplicity and ease of use.

## Key Features

- **Intelligent Delimiter Inference:** Datagrunt automatically detects and applies the correct delimiter for your CSV files.
- **Path Object Support:** Full support for both string paths and `pathlib.Path` objects for modern, cross-platform file handling.
- **Multiple Processing Engines:** Choose from three powerful engines - [DuckDB](https://duckdb.org), [Polars](https://pola.rs), and [PyArrow](https://arrow.apache.org/docs/python/) - to handle your data processing needs.
- **Flexible Data Transformation:** Easily convert your processed CSV data into various formats including CSV, Excel, JSON, JSONL, and Parquet.
- **PDF Parsing & OCR:** Extract text, tables, and images from PDF files as dicts, DataFrames, or JSON, with optional [Tesseract](https://github.com/tesseract-ocr/tesseract) OCR for scanned pages. Powered by the permissively-licensed **PDFium** engine by default, with **PyMuPDF** available as an alternative.
- **AI-Powered Schema Analysis:** Use Google's Gemini models to automatically generate detailed schema reports for your CSV files, including data types, column classifications, and data quality checks.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

### Powertools Under The Hood
| Tool | Description |
|-------------------|----------------------------|
| [DuckDB](https://duckdb.org)| Fast in-process analytical database with excellent SQL support |
| [Polars](https://pola.rs) | Multi-threaded DataFrame library written in Rust, optimized for performance |
| [PyArrow](https://arrow.apache.org/docs/python/) | Python bindings for Apache Arrow with efficient columnar data processing |
| [Google Gemini](https://deepmind.google/technologies/gemini/) | A powerful family of generative AI models for schema analysis |
| [PDFium](https://github.com/pypdfium2-team/pypdfium2) | Default PDF engine (via `pypdfium2`) — permissively licensed (BSD-3 / Apache-2.0); fast text + image extraction, with a structured mode at parity with PyMuPDF |
| [pdfplumber](https://github.com/jsvine/pdfplumber) | Table detection and extraction (MIT), shared by both PDF engines |
| [PyMuPDF](https://pymupdf.readthedocs.io/) | Alternative PDF engine for text, tables, and images (AGPL-3.0 / commercial) |
| [Tesseract](https://github.com/tesseract-ocr/tesseract) | OCR for scanned/image-only pages (optional) |

## Installation

We recommend using [UV](https://docs.astral.sh/uv/). However, you may get started with Datagrunt in seconds using UV or pip.

Get started with UV:

```bash
uv pip install datagrunt
```

Get started with pip:

```bash
pip install datagrunt
```

> **PDF parsing** is an optional extra — install it with `uv pip install "datagrunt[pdf]"`. See [PDF parsing](#pdf-parsing) below for details and OCR setup.

## Quick Start

### Reading CSV Files with Multiple Engine Options

```python
from datagrunt import CSVReader
from pathlib import Path

# Load your CSV file with different engines
# Accepts both string paths and Path objects
csv_file = 'electric_vehicle_population_data.csv'
csv_path = Path('electric_vehicle_population_data.csv')

# Choose your engine: 'polars' (default), 'duckdb', or 'pyarrow'
reader_polars = CSVReader(csv_file, engine='polars')    # String path - fast DataFrame ops
reader_duckdb = CSVReader(csv_path, engine='duckdb')    # Path object - best for SQL queries
reader_pyarrow = CSVReader(csv_file, engine='pyarrow')  # Arrow ecosystem integration

# Get a sample of the data
reader_duckdb.get_sample()
```

### DuckDB Integration for Performant SQL Queries

```python
from datagrunt import CSVReader

# Set up DuckDB engine for SQL capabilities
dg = CSVReader('electric_vehicle_population_data.csv', engine='duckdb')

# Construct your SQL query using the auto-generated table name
query = f"""
WITH core AS (
    SELECT
        City AS city,
        "VIN (1-10)" AS vin
    FROM {dg.db_table}
)
SELECT
    city,
    COUNT(vin) AS vehicle_count
FROM core
GROUP BY 1
ORDER BY 2 DESC
"""

# Execute the query and get results as a Polars DataFrame
df = dg.query_data(query).pl()
print(df)
```

### Exporting Data to Multiple Formats

```python
from datagrunt import CSVWriter
from pathlib import Path

# Create writer with your preferred engine (accepts both strings and Path objects)
input_file = Path('input.csv')
writer = CSVWriter(input_file, engine='duckdb')  # Default for exports

# Export to various formats
writer.write_csv('output.csv')          # Clean CSV export
writer.write_excel('output.xlsx')       # Excel workbook
writer.write_json('output.json')        # JSON format
writer.write_parquet('output.parquet')  # Parquet for analytics

# Use PyArrow engine for optimized Parquet exports
writer_arrow = CSVWriter('input.csv', engine='pyarrow')  # String path also works
writer_arrow.write_parquet('optimized.parquet')  # Native Arrow Parquet
```

### AI-Powered Schema Analysis

```python
from datagrunt import CSVSchemaReportAIGenerated
from pathlib import Path
import os

# Generate detailed schema reports with AI (accepts both strings and Path objects)
api_key = os.environ.get("GEMINI_API_KEY")
data_file = Path('your_data.csv')

schema_analyzer = CSVSchemaReportAIGenerated(
    filepath=data_file,  # Path object works seamlessly
    engine='google',
    api_key=api_key
)

# Get comprehensive schema analysis
report = schema_analyzer.generate_csv_schema_report(
    model='gemini-2.5-flash',
    return_json=True
)

print(report)  # Detailed JSON schema with data types, classifications, and more
```

## PDF parsing

PDF support is an optional extra:

```bash
uv pip install "datagrunt[pdf]"
# or
pip install "datagrunt[pdf]"
```

OCR of scanned pages additionally requires the **Tesseract** system binary
(e.g. `brew install tesseract` on macOS, `apt-get install tesseract-ocr` on
Debian/Ubuntu). On Windows, Tesseract runs natively (no WSL needed) via the
[UB-Mannheim installer](https://github.com/UB-Mannheim/tesseract/wiki) or a
package manager (`winget install UB-Mannheim.TesseractOCR`,
`choco install tesseract`, or `scoop install tesseract`); after installing,
either add the Tesseract directory to your PATH or point pytesseract at it
with `pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"`.
Native-text PDFs, tables, and embedded images work without it.

```python
from datagrunt import PDFReader, PDFWriter

# Parse a PDF into the unified document structure (PDFium engine by default).
reader = PDFReader("report.pdf")
document = reader.to_dicts()           # {"document": {"pages": [...]}}
df = reader.to_dataframe()             # one row per extracted element

# Write JSON and extract embedded images to disk.
writer = PDFWriter("report.pdf")
writer.write_json("report.json", image_output_dir="report_images")
writer.extract_images(output_dir="report_images")
```

### Choosing a PDF engine

`PDFReader` and `PDFWriter` accept an `engine` argument:

```python
# Default: PDFium — permissively licensed (BSD-3 / Apache-2.0).
reader = PDFReader("report.pdf")                      # engine="pdfium"

# Lean, fast mode: text + positioned text objects + images, no table detection.
reader = PDFReader("report.pdf", native=True)

# Alternative engine: PyMuPDF (AGPL-3.0 / commercial).
reader = PDFReader("report.pdf", engine="pymupdf")
```

Both engines emit the same **unified element schema** by default, so output is
interchangeable. **PDFium** is the default because it is **permissively
licensed** (unlike PyMuPDF, which is AGPL-3.0 / commercial) and is faster on
text-heavy documents. Table detection (via pdfplumber) and OCR work identically
on either engine.

- `native=True` (PDFium only) switches to a lean schema — full page text,
  positioned text objects, and images, with **no table detection** — which is
  dramatically faster (~20–80×) when you don't need structured tables.
- Image-only / scanned pages fall back to **Tesseract OCR** automatically on
  both engines (requires the Tesseract binary; see above).

When images are written to disk, byte-identical duplicates (common with repeated
icons or backgrounds) are collapsed to a single file and all references are
repointed to it. Pass `dedupe=False` / `dedupe_images=False` to keep every copy.

On graphically dense PDFs, line-based table detection can pick up decorative
boxes and rule lines as 1×N or N×1 "tables". Pass `drop_layout_tables=True` to
the reader (`to_dicts`, `to_dataframe`, `to_arrow_table`) or writer
(`write_json`, `write_json_newline_delimited`) to discard those and keep only
tables with at least two rows and two columns. It is off by default.

## Engine Comparison

| Feature | Polars | DuckDB | PyArrow |
|---------|--------|--------|---------|
| **Best for** | DataFrame operations | SQL queries & analytics | Arrow ecosystem integration |
| **Performance** | Fast in-memory processing | Excellent for large datasets | Optimized columnar operations |
| **Default for** | CSVReader | CSVWriter | - |
| **Export Quality** | Good | Excellent (especially JSON) | Native Parquet support |

_The engines above apply to CSV processing. PDF parsing uses the **PDFium** engine by default (permissively licensed), with **PyMuPDF** available via `engine="pymupdf"` — see [PDF parsing](#pdf-parsing)._

## Primary Classes

- **`CSVReader`**: Read and process CSV files with intelligent delimiter detection
- **`CSVWriter`**: Export CSV data to multiple formats (CSV, Excel, JSON, Parquet)
- **`CSVSchemaReportAIGenerated`**: Generate AI-powered schema analysis reports
- **`PDFReader`**: Parse PDF files into text, tables, and images as dicts, Polars DataFrames, or PyArrow tables
- **`PDFWriter`**: Write parsed PDF output to JSON or JSONL and extract embedded images to disk

## Full Documentation

For complete documentation, detailed examples, and advanced usage patterns, see:
📖 **[Complete Documentation](https://www.datagrunt.io/docs)**

## License

This project is licensed under the [MIT License](https://opensource.org/license/mit)

## Acknowledgements

A HUGE thank you to the open source community and the creators of [DuckDB](https://duckdb.org), [Polars](https://pola.rs), and [PyArrow](https://arrow.apache.org/docs/python/) for their fantastic libraries that power Datagrunt.

## Source Repository

[https://github.com/pmgraham/datagrunt](https://github.com/pmgraham/datagrunt)
