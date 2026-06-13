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
- **Robust by Default:** Fail-fast validation with clear errors (invalid engine names, missing paths, directories, encrypted PDFs), graceful handling of empty files, no `UnicodeDecodeError` when constructing a reader over a non-UTF-8 file, and sane comment semantics — only leading `#` lines are treated as comments, so `#`-prefixed data rows such as hex colors are preserved on all engines.
- **PDF Parsing & OCR:** Extract text, tables, and images from PDF files as dicts, DataFrames, or JSON, with optional [Tesseract](https://github.com/tesseract-ocr/tesseract) OCR for scanned pages. Powered by the permissively-licensed **PDFium** engine by default, with **PyMuPDF** available as an alternative.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

### Powertools Under The Hood
| Tool | Description |
|-------------------|----------------------------|
| [DuckDB](https://duckdb.org)| Fast in-process analytical database with excellent SQL support |
| [Polars](https://pola.rs) | Multi-threaded DataFrame library written in Rust, optimized for performance |
| [PyArrow](https://arrow.apache.org/docs/python/) | Python bindings for Apache Arrow with efficient columnar data processing |
| [PDFium](https://github.com/pypdfium2-team/pypdfium2) | Default PDF engine (via `pypdfium2`) — permissively licensed (BSD-3 / Apache-2.0); fast text + image extraction, with a structured mode at parity with PyMuPDF |
| [pdfplumber](https://github.com/jsvine/pdfplumber) | Table detection and extraction (MIT), shared by both PDF engines |
| [PyMuPDF](https://pymupdf.readthedocs.io/) | Alternative PDF engine for text, tables, and images (AGPL-3.0 / commercial) |
| [Tesseract](https://github.com/tesseract-ocr/tesseract) | OCR for scanned/image-only pages (optional) |

## Installation

We recommend using [uv](https://docs.astral.sh/uv/) as the default package manager.

To install Datagrunt using `uv`:

```bash
uv pip install datagrunt
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

# Get a sample of the data (streams the first rows — the whole file is never
# materialized, so sampling large files stays memory-bounded on every engine)
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

With the DuckDB engine, repeated `query_data()` calls on the same reader reuse
a single import: the CSV is loaded into DuckDB once per reader instance, so
follow-up queries skip the file import entirely and run dramatically faster.

Because that reuse keeps a DuckDB connection open, hold the reader in a `with`
block (or call `reader.close()`) to release it deterministically when you are
done. The reader stays usable afterward — a later call transparently reopens:

```python
with CSVReader('vehicles.csv', engine='duckdb') as dg:
    df = dg.query_data(f"SELECT city, COUNT(*) FROM {dg.db_table} GROUP BY 1").pl()
# connection is closed here, even if the block raises
```

### Consistent Column Names with `normalize_columns`

Pass `normalize_columns=True` at construction to work in normalized column names (lowercase, underscores, collision-safe) everywhere — including SQL:

```python
from datagrunt import CSVReader

dg = CSVReader('electric_vehicle_population_data.csv', engine='duckdb', normalize_columns=True)

# The DuckDB table is imported with normalized names, so you write your
# query and read your results in the same vocabulary — no aliases needed.
query = f"SELECT city, vin_1_10 FROM {dg.db_table} LIMIT 5"
df = dg.query_data(query).pl()

# Every other output honors the same setting
dg.to_dataframe()   # columns: city, vin_1_10, ...
dg.get_sample()     # same normalized names
```

`CSVWriter(..., normalize_columns=True)` does the same for every exported file. The older per-call form (`to_dataframe(normalize_columns=True)`) still works but is deprecated and emits a `DeprecationWarning`.

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

Every `write_*` method — including `write_parquet` — honors `lenient=True` for
ragged CSVs, and empty source files produce empty output instead of an error.

## PDF parsing

PDF support is an optional extra:

```bash
uv pip install "datagrunt[pdf]"
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
  both engines (requires the Tesseract binary; see above). If OCR is
  unavailable or fails, the page is **not dropped** — it is kept with whatever
  text and images were extracted, plus a page-level warning, so extraction is
  always complete.
- Encrypted / password-protected PDFs raise a clear `ValueError` on every
  engine instead of a raw backend exception.

### Parallel Processing & Concurrency
By default, `PDFReader` and `PDFWriter` run sequentially (`workers=1`). On the default **PDFium engine**, you can enable parallel processing on multi-core systems by passing a `workers` count greater than `1`:
```python
if __name__ == '__main__':
    # Run with 8 processes to parse pages concurrently
    reader = PDFReader("report.pdf", workers=8)
    document = reader.to_dicts()
```

`workers` applies to the **PDFium engine only**. The PyMuPDF engine always
parses pages sequentially because the underlying MuPDF library is not
thread-safe — passing `workers > 1` with `engine="pymupdf"` logs a warning and
is ignored. Use the default PDFium engine when you need parallel
(process-based) parsing.

#### Why `if __name__ == '__main__':` is required
Because PDFium is not thread-safe within a single process, `datagrunt` uses a process pool (`ProcessPoolExecutor` with the `spawn` start context on macOS and Windows) to parse pages concurrently.

Under Python's `spawn` start context, child processes import the main module to initialize. If you call `PDFReader` or `PDFWriter` with `workers > 1` outside of a `if __name__ == '__main__':` block, the child processes will recursively spawn their own process pools, leading to a crash or infinite recursion loop.

For single-page documents, `datagrunt` automatically bypasses the process pool and executes sequentially to avoid process spawning overhead.

#### Distributed Runtimes Fallback
When running inside managed distributed environments (e.g. **Apache Spark**, **Apache Beam**, **Apache Flink**, or **Celery**), nested process spawning is restricted or causes container sandbox permission errors. `datagrunt` automatically detects these environments (by checking variables like `SPARK_ENV_LOADED`, `BEAM_WORKER_ID`, etc.) and falls back to sequential, parent-process execution to ensure robust, conflict-free operation.

When images are written to disk, byte-identical duplicates (common with repeated
icons or backgrounds) are collapsed to a single file and all references are
repointed to it. Pass `dedupe=False` / `dedupe_images=False` to keep every copy.

On graphically dense PDFs, line-based table detection can pick up decorative
boxes and rule lines as 1×N or N×1 "tables". Pass `drop_layout_tables=True` to
the reader (`to_dicts`, `to_dataframe`, `to_arrow_table`) or writer
(`write_json`, `write_json_newline_delimited`) to discard those and keep only
tables with at least two rows and two columns. It is off by default.

### Batch processing (many PDFs)

For large corpora, process documents in parallel across processes — one document
per process, each single-threaded. (Per-page threads do not speed up extraction:
the dominant cost, table detection, is pure-Python and GIL-bound.)

```python
from datagrunt import process_pdfs

paths = ["a.pdf", "b.pdf", "c.pdf"]
results = process_pdfs(paths, "out/")   # writes out/<stem>.json (+ out/<stem>_images/)
# results: [{"source": "a.pdf", "status": "success", "json_path": "out/a.json"}, ...]
```

A malformed PDF is reported as a `{"status": "error", ...}` record without
aborting the rest of the batch. Pass `images=False`, `dedupe_images=False`, or
`drop_layout_tables=True` to control per-document output, and `max_workers=N` to
cap processes (defaults to the CPU count).

Because it uses a process pool (the `spawn` start method on macOS/Windows), call
`process_pdfs` from an importable script under an `if __name__ == "__main__":`
guard — not from a REPL, `python -c`, or piped stdin, where worker processes
cannot re-import the entry module.

**Apache Beam / Dataflow:** do **not** call `process_pdfs` inside a pipeline —
it would nest process pools and oversubscribe the CPU. Instead, map the
per-document work in a `DoFn` / `beam.Map` using `PDFWriter(path, workers=1)` and
let the runner fan documents out. Dataflow runs multiple worker processes per VM
(sidestepping the GIL) and autoscales VMs, so throughput scales with total worker
cores.

## Engine Comparison

| Feature | Polars | DuckDB | PyArrow |
|---------|--------|--------|---------|
| **Best for** | DataFrame operations | SQL queries & analytics | Arrow ecosystem integration |
| **Performance** | Fast in-memory processing | Excellent for large datasets | Optimized columnar operations |
| **Default for** | CSVReader | CSVWriter | - |
| **Export Quality** | Good | Excellent (especially JSON) | Native Parquet support |

_The engines above apply to CSV processing. Whichever you pick, results are consistent: leading `#` comment lines, leading blank lines, logical record counts (quoted embedded newlines count as one record), and column-name normalization — including collision handling like `Col A,col_a` → `col_a, col_a_1` — behave identically across all three. Mid-file lines starting with `#` are kept as data on all three engines. PDF parsing uses the **PDFium** engine by default (permissively licensed), with **PyMuPDF** available via `engine="pymupdf"` — see [PDF parsing](#pdf-parsing)._

## Primary Classes

- **`CSVReader`**: Read and process CSV files with intelligent delimiter detection
- **`CSVWriter`**: Export CSV data to multiple formats (CSV, Excel, JSON, Parquet)
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
