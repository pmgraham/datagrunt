# Welcome To Datagrunt

[![CI](https://github.com/pmgraham/datagrunt/actions/workflows/ci.yml/badge.svg)](https://github.com/pmgraham/datagrunt/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/datagrunt?label=pypi)](https://pypi.org/project/datagrunt/)
[![Python versions](https://img.shields.io/pypi/pyversions/datagrunt)](https://pypi.org/project/datagrunt/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/pmgraham/datagrunt/blob/main/LICENSE)

Datagrunt is a Python library designed to simplify the way you work with CSV, Excel, Parquet, and PDF files. It provides a streamlined approach to reading, processing, and transforming your data into various formats, making data manipulation efficient and intuitive.

## Why Datagrunt?

Born out of real-world frustration, Datagrunt eliminates the need for repetitive coding when handling CSV, Excel, Parquet, and PDF files. Whether you're a data analyst, data engineer, or data scientist, Datagrunt empowers you to focus on insights, not tedious data wrangling.

### What Datagrunt Is Not
Datagrunt is not an extension of or a replacement for DuckDB, Polars, or PyArrow, nor is it a comprehensive data processing solution. Instead, it's designed to simplify the way you work with CSV, Excel, Parquet, and PDF files — solving the pain point of inferring delimiters when a CSV structure is unknown, reading Excel workbooks sheet by sheet, reading and converting columnar Parquet data, and turning PDFs into structured, queryable data. Datagrunt provides an easy way to convert CSV, Excel, and Parquet files to dataframes and export them to various formats, and to extract text, tables, and images from PDFs. One of Datagrunt's value propositions is its relative simplicity and ease of use.

## Key Features

- **Intelligent Delimiter Inference:** Datagrunt automatically detects and applies the correct delimiter for your CSV files.
- **Rust-Accelerated Core (v4.0+):** CSV delimiter and dialect inference run in a bundled Rust extension (`datagrunt._native`) for multiple-times-faster scanning of large files. The native engine is the default and is required on supported platforms (it ships as a prebuilt wheel). A byte-for-byte-equivalent pure-Python implementation lives alongside it as the differential-parity oracle — validated against the Rust engine in CI and selectable via a hidden diagnostics toggle — so results are identical no matter which path runs.
- **Path Object Support:** Full support for both string paths and `pathlib.Path` objects for modern, cross-platform file handling.
- **Multiple Processing Engines:** Choose from three powerful engines - [DuckDB](https://duckdb.org), [Polars](https://pola.rs), and [PyArrow](https://arrow.apache.org/docs/python/) - to handle your data processing needs.
- **Flexible Data Transformation:** Easily convert your processed CSV, Excel, and Parquet data into various formats including CSV, Excel, JSON, JSONL, and Parquet.
- **Excel Reading & Writing:** Read `.xlsx`/`.xls` workbooks sheet by sheet into DataFrames, dicts, Arrow, or SQL, and export any sheet (or all of them) to CSV, JSON, JSONL, Parquet, or Excel.
- **Parquet Reading & Writing:** Read `.parquet` files into DataFrames, dicts, Arrow tables, or SQL, and export to CSV, JSON, JSONL, Parquet, or Excel. Backed by a single Polars engine with full `read_options`/`write_options` passthrough.
- **Robust by Default:** Fail-fast validation with clear errors (invalid engine names, missing paths, directories, encrypted PDFs), graceful handling of empty files, no `UnicodeDecodeError` when constructing a reader over a non-UTF-8 file, and sane comment semantics — only leading `#` lines are treated as comments, so `#`-prefixed data rows such as hex colors are preserved on all engines.
- **PDF Parsing & OCR:** Extract text, tables, and images from PDF files as dicts, DataFrames, or JSON, with optional [Tesseract](https://github.com/tesseract-ocr/tesseract) OCR for scanned pages. Powered by the permissively-licensed **PDFium** engine by default, with **PyMuPDF** available as an alternative.
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

### Powertools Under The Hood
| Tool | Description |
|-------------------|----------------------------|
| [Rust](https://www.rust-lang.org) / [PyO3](https://pyo3.rs) | Bundled native extension (`datagrunt._native`, abi3) that powers CSV delimiter & dialect inference; shipped as platform wheels and validated for byte-for-byte parity against a pure-Python reference |
| [DuckDB](https://duckdb.org)| Fast in-process analytical database with excellent SQL support |
| [Polars](https://pola.rs) | Multi-threaded DataFrame library written in Rust, optimized for performance |
| [PyArrow](https://arrow.apache.org/docs/python/) | Python bindings for Apache Arrow with efficient columnar data processing |
| [PDFium](https://github.com/pypdfium2-team/pypdfium2) | Default PDF engine (via `pypdfium2`) — permissively licensed (BSD-3 / Apache-2.0); fast text + image extraction, with a structured mode at parity with PyMuPDF |
| [pdfplumber](https://github.com/jsvine/pdfplumber) | Table detection and extraction (MIT), shared by both PDF engines |
| [PyMuPDF](https://pymupdf.readthedocs.io/) | Alternative PDF engine for text, tables, and images (AGPL-3.0 / commercial). Note: selecting `engine='pymupdf'` pulls an AGPL-3.0 component into your environment — evaluate those license obligations for your use case, or stay on the default PDFium engine |
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

With the DuckDB engine, datagrunt imports the CSV once per reader or writer and
reuses it: repeated reads (`to_dataframe`, `to_arrow_table`, `get_sample`),
`query_data` calls, and multi-format exports (`write_csv` + `write_excel` +
`write_json` + `write_parquet`) all skip re-importing the file, so follow-up
operations run dramatically faster.

You never have to manage this — the connection is released automatically when the
reader or writer goes out of scope (its connection is reference-counted). If you
want to release it *early* (for
example, to free memory deterministically in a long-running process), use the
reader as a context manager or call `close()`; this is optional and the object
stays usable afterward:

```python
with CSVReader('vehicles.csv', engine='duckdb') as dg:
    df = dg.query_data(f"SELECT city, COUNT(*) FROM {dg.db_table} GROUP BY 1").pl()
# connection released here, even if the block raises
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

## Reading and writing Excel

`ExcelReader` and `ExcelWriter` read `.xlsx`/`.xls`-family workbooks **sheet by
sheet**. Reading is backed by a single canonical engine — Polars + calamine
(via `fastexcel`, a core dependency) — so there is no `engine` argument. A
non-Excel or missing path is rejected at construction (`ValueError` /
`FileNotFoundError`).

```python
from datagrunt import ExcelReader, ExcelWriter

xl = ExcelReader("workbook.xlsx")
xl.sheets                      # ['Sheet1', 'Sheet2', ...]
xl.to_dataframe()              # first sheet as a Polars DataFrame
xl.to_dataframe(sheet="Sheet2")  # by name
xl.to_dicts(sheet=1)           # by position
xl.to_arrow_table()            # also: get_sample(), all accept sheet=
xl.query_data(f"SELECT * FROM {xl.db_table}", sheet="Sheet2")

# Read options are keyword arguments forwarded verbatim to pl.read_excel
# (constructor default or per call). Pass any pl.read_excel parameter:
xl.to_dataframe(has_header=False)
xl.to_dataframe(read_options={"skip_rows": 2, "n_rows": 100})

w = ExcelWriter("workbook.xlsx")
w.write_csv("out.csv")                    # first sheet
w.write_parquet("out.parquet", sheet="Sheet2")
w.write_csv("out.csv", all_sheets=True)   # one file per sheet: out_Sheet1.csv, ...
w.write_excel("all.xlsx", all_sheets=True) # one multi-tab workbook
```

Every reader/writer method takes an optional `sheet=` (name or zero-based
index), defaulting to the first sheet; an invalid sheet raises a `ValueError`
listing the available sheets, and empty/blank workbooks return empty results.
The keys `source`, `sheet_id`, and `sheet_name` are reserved (sheet selection is
controlled via `sheet=`).

`normalize_columns=True` (constructor or per call) normalizes column names
exactly as the CSV API does. Values beginning with `=`, `+`, `-`, `@` are
written verbatim — sanitize at the application layer if your source is
untrusted and the output may be opened in a spreadsheet (CWE-1236).

> **How read options work (same for Excel and Parquet).** In both subsystems
> you pass read options as **keyword arguments**, and Datagrunt forwards them
> **verbatim** to the underlying Polars function — `pl.read_excel` for Excel,
> `pl.read_parquet` for Parquet. The *mechanism is identical*; only the
> available option names differ, because the two Polars functions differ.
> `pl.read_excel` happens to expose a parameter that is itself a dict named
> `read_options` (the calamine engine's option bag, where `skip_rows`/`n_rows`
> live), which is why you see `read_options={...}` above. `pl.read_parquet` has
> no such dict — its options (`columns`, `n_rows`, …) are plain top-level
> parameters. So `xl.to_dataframe(read_options={"skip_rows": 2})` and
> `pq.to_dataframe(columns=["id"], n_rows=100)` use the **same** Datagrunt
> keyword-passthrough; the shape of the arguments comes straight from Polars.

## Reading and writing Parquet

`ParquetReader` and `ParquetWriter` work with `.parquet` files as a
**single-table columnar format** — there are no sheets and no `engine` argument.
Reading is backed by a single canonical Polars engine. A non-Parquet or missing
path is rejected at construction (`ValueError` / `FileNotFoundError`).

```python
from datagrunt import ParquetReader, ParquetWriter

reader = ParquetReader("data.parquet")
reader.get_sample()                          # first rows as a Polars DataFrame (n_rows= adjusts the sample size)
reader.to_dataframe()                        # full file as a Polars DataFrame
reader.to_arrow_table()                      # Apache Arrow Table
reader.to_dicts()                            # list of row dicts
reader.query_data(f"SELECT * FROM {reader.db_table} LIMIT 5")

# Read options are keyword arguments forwarded verbatim to pl.read_parquet
# (the same passthrough as Excel; only "source" is reserved). Pass any
# pl.read_parquet parameter — they are plain top-level kwargs, not a dict:
reader_subset = ParquetReader("data.parquet", columns=["id", "name"], n_rows=1000)
```

```python
writer = ParquetWriter("data.parquet")
writer.write_csv("out.csv")
writer.write_json("out.json")
writer.write_json_newline_delimited("out.jsonl")
writer.write_parquet("out.parquet")
writer.write_excel("out.xlsx")

# write_options are forwarded to the Polars writer:
writer.write_parquet("recompressed.parquet", compression="zstd")
```

`normalize_columns=True` normalizes column names exactly as the CSV and Excel
APIs do. Empty or blank source files return empty results from reader methods
and produce 0-byte output files from writer methods. Values beginning with `=`,
`+`, `-`, `@` are written verbatim — sanitize at the application layer if your
source is untrusted and the output may be opened in a spreadsheet (CWE-1236).

> **Read options work exactly as they do for Excel** — keyword arguments
> forwarded verbatim to Polars (here `pl.read_parquet`). There is no
> Datagrunt-specific convention and no dict to wrap them in: `pl.read_parquet`
> simply exposes its options as top-level parameters (`columns`, `n_rows`, …),
> whereas `pl.read_excel` groups some of its calamine options inside a dict
> named `read_options`. Same Datagrunt passthrough, different Polars signatures.

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

# Render each whole page to an image file (rasterization, not embedded-image
# extraction). Returns the written paths, in page order.
writer.render_pages_as_images(output_dir="report_pages", dpi=300, image_format="png")
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

### Filtering small embedded images

By default, embedded images smaller than **40 px** on either side are dropped as
layout artifacts (rule lines, separators, icon slivers). Pass the keyword-only
`min_image_dimension` argument to `PDFReader` or `PDFWriter` to change that
threshold:

```python
# Keep smaller images — e.g. capture logos, signatures, or small icons.
reader = PDFReader("report.pdf", min_image_dimension=15)

# Filter more aggressively — keep only large figures.
reader = PDFReader("report.pdf", min_image_dimension=200)

# Keep every image, including 1×1 px artifacts (disable the filter).
reader = PDFReader("report.pdf", min_image_dimension=0)
```

The threshold applies to **both PDF engines** and every conversion/output method,
and defaults to `40` (the long-standing behavior). It must be a non-negative
integer; invalid values raise immediately when the reader/writer is constructed.

### Rendering pages as images

`extract_images` pulls the images **embedded** in a PDF. To rasterize **whole
pages** instead — one image file per page — use `render_pages_as_images`:

```python
writer = PDFWriter("report.pdf")

# One PNG per page at 300 DPI, written to ./report_pages/, returned in page order.
paths = writer.render_pages_as_images(output_dir="report_pages", dpi=300, image_format="png")

# JPEG at a lower resolution.
writer.render_pages_as_images(output_dir="report_pages", dpi=150, image_format="jpg")
```

- `image_format` is one of `png`, `jpg`, or `jpeg` (case-insensitive); any other
  value raises `ValueError`. Both engines produce identical output.
- Works on **both engines**; on **PDFium** it honors the `workers` count for
  parallel rendering (PyMuPDF renders sequentially).
- Extraction stays **complete**: a page that fails to render is logged and
  skipped, never aborting the batch — the returned list contains every page that
  rendered successfully, in page order.

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

#### Multiprocessing Guard Requirement
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

## Engine Comparison

| Feature | Polars | DuckDB | PyArrow |
|---------|--------|--------|---------|
| **Best for** | DataFrame operations | SQL queries & analytics | Arrow ecosystem integration |
| **Performance** | Fast in-memory processing | Excellent for large datasets | Optimized columnar operations |
| **Default for** | CSVReader | CSVWriter | - |
| **Export Quality** | Good | Excellent (especially JSON) | Native Parquet support |

_The engines above apply to CSV processing. Whichever you pick, results are consistent: leading `#` comment lines, leading blank lines, logical record counts (quoted embedded newlines count as one record), and column-name normalization — including collision handling like `Col A,col_a` → `col_a, col_a_1` — behave identically across all three. Mid-file lines starting with `#` are kept as data on all three engines. PDF parsing uses the **PDFium** engine by default (permissively licensed), with **PyMuPDF** available via `engine="pymupdf"` — see [PDF parsing](#pdf-parsing)._

## Primary Classes

### Readers
- **`CSVReader`**: Read and process CSV files with intelligent delimiter detection
- **`ExcelReader`**: Read Excel workbooks sheet by sheet into Polars DataFrames, dicts, PyArrow tables, or SQL query results
- **`ParquetReader`**: Read Parquet files into Polars DataFrames, dicts, PyArrow tables, or SQL query results
- **`PDFReader`**: Parse PDF files into text, tables, and images as dicts, Polars DataFrames, or PyArrow tables

### Writers
- **`CSVWriter`**: Export CSV data to multiple formats (CSV, Excel, JSON, Parquet)
- **`ExcelWriter`**: Export a sheet (or all sheets) of an Excel workbook to CSV, Excel, JSON, JSONL, or Parquet
- **`ParquetWriter`**: Export Parquet files to multiple formats (CSV, Excel, JSON, JSONL, Parquet)
- **`PDFWriter`**: Write parsed PDF output to JSON or JSONL, extract embedded images, and render whole pages to image files

## Full Documentation

For complete documentation, detailed examples, and advanced usage patterns, see:
📖 **[Complete Documentation](https://www.datagrunt.io/docs)**

## License

This project is licensed under the [MIT License](https://opensource.org/license/mit)

## Acknowledgements

A HUGE thank you to the open source community and the creators of [DuckDB](https://duckdb.org), [Polars](https://pola.rs), and [PyArrow](https://arrow.apache.org/docs/python/) for their fantastic libraries that power Datagrunt.

## Source Repository

[https://github.com/pmgraham/datagrunt](https://github.com/pmgraham/datagrunt)
