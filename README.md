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
- **PDF Parsing, OCR & Corpus Bundles:** Read PDFs into a typed document of headings, paragraphs, key-value pairs, tables, lists, and figures — as DataFrames, Arrow tables, dicts, Markdown, or SQL — and write JSON, JSONL, Markdown, Parquet, CSV, images, checksummed single-document bundles (`write_bundle`, `render_pages="vision_only"`), or parallel multi-document corpora (`extract_corpus`, `load_corpus`). Hidden text is flagged, never dropped, and scanned pages are read with optional [Tesseract](https://github.com/tesseract-ocr/tesseract) OCR. Powered by the permissively-licensed **PDFium** engine with an in-process zero-subprocess fast path (`sandbox=False`) and optional process-isolated sandboxing (`sandbox=True`).
- **Schema-Driven PDF Rationalization (v5.1+):** Extract Pydantic records across PDF documents, bundles, or corpora (`rationalize()`) with adaptive section coalescing, multimodal VLM routing on scanned/image-only pages (`multimodal=True`), normalized identity keys (`Key(normalizer="alnum_upper")`), non-disputing `Informational` fields, cross-family consensus (`min_families=2`), 3-witness quote & key grounding, cross-unit entity folding (`fold_across_units=True`), grounded-union result tiers (`grounded_records`, `include_grounded_union=True`), and offline cloud batch execution (`prepare_batch`, `ingest_batch`).
- **Pythonic API:** Enjoy a clean and intuitive API that integrates seamlessly into your existing Python workflows.

### Powertools Under The Hood
| Tool | Description |
|-------------------|----------------------------|
| [Rust](https://www.rust-lang.org) / [PyO3](https://pyo3.rs) | Bundled native extension (`datagrunt._native`, abi3) that powers CSV delimiter & dialect inference; shipped as platform wheels and validated for byte-for-byte parity against a pure-Python reference |
| [DuckDB](https://duckdb.org)| Fast in-process analytical database with excellent SQL support |
| [Polars](https://pola.rs) | Multi-threaded DataFrame library written in Rust, optimized for performance |
| [PyArrow](https://arrow.apache.org/docs/python/) | Python bindings for Apache Arrow with efficient columnar data processing |
| [PDFium](https://github.com/pypdfium2-team/pypdfium2) | The PDF engine (via `pypdfium2`) — permissively licensed (BSD-3 / Apache-2.0); text, vector rules, images, and page renders |
| [Pillow](https://python-pillow.org) | Image encoding and OCR preprocessing for PDF pages (MIT-CMU) |
| [Tesseract](https://github.com/tesseract-ocr/tesseract) | OCR for scanned/image-only pages, run as the system `tesseract` program (optional) |

## Installation

We recommend using [uv](https://docs.astral.sh/uv/) as the default package manager.

To install Datagrunt using `uv`:

```bash
uv pip install datagrunt
```

Datagrunt 5.0 requires Python 3.11 or newer.

> **PDF parsing** is an optional extra — install it with `uv pip install "datagrunt[pdf]"`. For **schema-driven PDF extraction** (`rationalize`), install `uv pip install "datagrunt[pdf,rationalize]"`. See [PDF parsing](#pdf-parsing) and [Schema-driven extraction (`rationalize`)](#schema-driven-extraction-rationalize) below.

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

The extra installs [pypdfium2](https://github.com/pypdfium2-team/pypdfium2)
(PDFium) and [Pillow](https://python-pillow.org). `import datagrunt` works
without it; constructing a `PDFReader` or `PDFWriter` without it raises an
`ImportError` that names the install command.

Datagrunt 5.0 reads every PDF with one engine, PDFium, into a typed document:
headings, paragraphs, key-value pairs, tables, lists, and figures, in reading
order. Each element carries its page, its bounding box (in points, top-left
origin), and the headings it sits under (`scope_path`). Coming from 4.x?
`engine=`, `native=`, `drop_layout_tables=`, and the PyMuPDF, pdfplumber, and
pytesseract dependencies are gone, and the output shapes changed; the
[migration guide](https://www.datagrunt.io/docs/guides/migrating-to-5/) lists every change.

### Reading a PDF

```python
from datagrunt import PDFReader

reader = PDFReader("report.pdf")

df = reader.to_dataframe()          # Polars: one row per element, in document order
arrow = reader.to_arrow_table()     # the same rows as a PyArrow table
rows = reader.to_dicts()            # the same rows as dicts
reader.get_sample()                 # the first 20 rows (n_rows=...)
markdown = reader.to_markdown()     # the whole document as Markdown

# The typed document model
for table in reader.tables:
    print(table.scope_path, table.to_dataframe())
reader.headings, reader.paragraphs, reader.key_values, reader.lists, reader.figures
reader.sections, reader.pages, reader.page_images, reader.metadata

# SQL over the element rows (an in-memory DuckDB connection per reader)
result = reader.query_data(f"SELECT kind, count(*) AS n FROM {reader.db_table} GROUP BY kind")
result.pl()                         # also .pa() and .dicts()
```

Every row has the same columns: `doc_id`, `element_id`, `page_number`,
`reading_order`, `kind` (`heading`, `paragraph`, `key_value`, `table`, `list`,
or `figure`), `scope_path`, `text`, `markdown`, `bbox`, `route`,
`integrity_verified`, `hidden_reasons`, and `image_only_reasons` (the
element's page's; see [Scanned pages and OCR](#scanned-pages-and-ocr)).

- `PDFReader` writes nothing to disk, not even temporary files. Figures load
  lazily (`figure.read_bytes()`); write them with `PDFWriter`.
- Each page has a `page_image`. A digital or hybrid page's is drawn at the
  reader's `dpi` only when you read it (`page.page_image.read_bytes()`, a PNG);
  a scanned page's is the image OCR read. An error page has none, nor does a
  page too large to draw (a warning on the page says so). A page image is
  drawn from the source PDF, so if the file at its path no longer holds the
  document's bytes, reading it raises `datagrunt.pdf.PDFSourceChangedError`.
- A heading's `level` (1 to 6) is ranked over the whole document by its font
  size and boldness (`font_size_pt`, `is_bold`), largest first, so a level
  means the same prominence on every page. Hidden headings never shift it.
- The PDF is read on first use, then cached for the reader's life. Errors in the
  arguments are raised at construction: a missing path raises
  `FileNotFoundError`; a file that isn't a PDF raises `ValueError`.
- An encrypted PDF needs `password=`; without it, reading raises
  `PDFEncryptedError` (also a `ValueError`). A structurally unreadable PDF
  raises `PDFCorruptError` (also a `RuntimeError`). Both derive from
  `DatagruntPDFError`.
- `PDFReader` also reads a bundle directory (see `write_bundle` below), its
  `manifest.json`, or a document's JSON as a dict.
- `query_data` runs your SQL as written: building it from untrusted input is
  your application's concern. DuckDB never installs an extension on its own; a
  query that needs one raises until you `INSTALL` it.

### Writing outputs

```python
from datagrunt import PDFWriter

writer = PDFWriter("report.pdf")

writer.write_json("out/report.json")          # the whole document, lossless
writer.write_jsonl("out/report.jsonl")        # one element per line
writer.write_markdown("out/report.md")
writer.write_parquet("out/report.parquet")    # nested columns kept
writer.write_csv("out/report.csv")            # bbox as four columns, lists as JSON arrays
writer.extract_images("out/images")           # each distinct figure once, as <sha256>.png
writer.render_pages_as_images("out/pages")    # every page as page_0001.png, page_0002.png, ...
writer.write_bundle("out/report_bundle")      # all of the above in one checked directory
```

- Every output path is required; there are no default filenames. Each `write_*`
  method returns the absolute path it wrote, and `extract_images` /
  `render_pages_as_images` return the paths they wrote; all of them create
  missing directories. `write_bundle` returns the `PDFDocument` it bundled,
  and its parent directory must exist.
- Every file is written atomically (a temporary file, then a rename), so a
  failed write never leaves a partial file. An existing file is replaced.
- `extract_images(output_dir, dedupe=False)` writes every figure, named by its
  element id (`p0001_e003.png`).
- `render_pages_as_images(output_dir, dpi=None)` writes 8-bit grayscale PNGs at
  the writer's `dpi` (or the one you pass), through a memory-capped renderer. A
  page that can't be rendered is logged and skipped; the others are written.
- `write_bundle(output_dir, *, overwrite=False, include_images=True, render_pages=True)`
  writes `json/document.json`, `json/elements.jsonl`, `markdown/document.md`,
  `markdown/pages/page_NNNN.md`, `images/<sha256>.png`, `page_images/page_NNNN.png`
  (plus OCR bands on scanned pages), and a `manifest.json` recording every file's
  size and SHA-256. Pass `render_pages="vision_only"` to render page PNGs only for
  `vision` and `image_only` pages while skipping renders on clean digital pages.
  `output_dir` must not exist or must be empty; if it already holds an identical
  bundle of the same PDF and options, that bundle is reused (`overwrite=True`
  replaces any other valid bundle there; a directory without a valid bundle
  manifest is never overwritten).
  Bundles and document JSON written by datagrunt 5.0+ load cleanly through
  `PDFReader("out/report_bundle")` or `load_bundle("out/report_bundle")`,
  verifying every file against `manifest.json` (`BundleCorruptError` on mismatch;
  `PDFSourceChangedError` if a source PDF mutates during extraction).
- `extract_corpus(inputs, output_dir, *, max_workers=1, render_pages="vision_only", ocr="auto", ...)`
  extracts a directory or sequence of PDFs across warm worker processes (scheduled
  largest-file-first via Graham LPT), reuses unchanged bundles incrementally by
  SHA-256 and options, isolates per-document errors into `manifest.failures` when
  `strict=False`, and writes an atomic `corpus_manifest.json` reloadable via
  `load_corpus("out/corpus")` (or passed directly to `rationalize()`).
- `write_csv` writes every value as extracted: a cell that starts with `=`,
  `+`, `-`, or `@` is not escaped, so guarding a spreadsheet against formula
  injection is your application's concern.
- A `PDFWriter` also accepts a `PDFDocument` (for example `reader.document`).
  It has no PDF to render, so `render_pages_as_images` and `write_bundle` raise
  `ValueError`.

### Hidden text

Text a human reader can't see is flagged, never dropped. Each element's
`hidden_reasons` lists why: `off_page`, `invisible`, `tiny_font`,
`transparent`, `matches_background`, `zero_scale`, `duplicate`, `superseded`,
or `unchecked_background`. An empty list means the element is visible. Markdown
(`to_markdown()`, `write_markdown()` and a bundle's Markdown) leaves hidden
elements out by default; pass `include_hidden=True` to keep each behind a
`<!-- hidden: reasons -->` comment. JSON and the DataFrame always keep every
element. Filter on the column before you pass text on, for example to an LLM:

```python
import polars as pl

visible = reader.to_dataframe().filter(pl.col("hidden_reasons").list.len() == 0)
```

### Scanned pages and OCR

Each page is routed `digital`, `hybrid`, or `vision` (`page.route`). A vision
page (a scan, or a page with too little text or mostly unreadable characters)
is rendered at `vision_dpi` and read by the **Tesseract** program; there is no
Python wrapper. The page's own digital text is kept, flagged `superseded`.

Install Tesseract with its English (`eng`) and orientation (`osd`) data, e.g.
`brew install tesseract` on macOS or `apt-get install tesseract-ocr` on
Debian/Ubuntu. On Windows, use the
[UB-Mannheim installer](https://github.com/UB-Mannheim/tesseract/wiki) or a
package manager (`winget install UB-Mannheim.TesseractOCR`,
`choco install tesseract`, or `scoop install tesseract`). Datagrunt runs the
`tesseract` it finds in an absolute directory on your `PATH`, never one in the
working directory, so add the Tesseract directory to `PATH`.

Every page reports how extraction went (`page.status`):

- `ok`: extracted on its route.
- `ocr_fallback`: the page couldn't be read digitally, so it was rendered and
  read by OCR.
- `ocr_unavailable`: a vision page OCR couldn't read (no Tesseract, say); its
  digital text is kept.
- `error`: nothing could be read; `page.error` says why. The rest of the
  document still extracts.

A page whose text can't stand and that nothing read has its content only in
its page image. `page.image_only_reasons` says why: `too_little_text` (too few
readable characters on a page with an image or drawing), `garbled_text` (too
few of its characters are valid text), or `extraction_failed` (an error page).
It's empty for a page whose text stands or that OCR read, and
`document.image_only_pages` lists the marked pages. With `ocr="never"`, a scan
stays `route=digital`, `status=ok` and is marked; `ocr="auto"` routes exactly
those pages to OCR (a page that can't be loaded at all is an error page either
way). The marker is in the JSON, the `image_only_reasons` column,
each marked page's Markdown header (`<!-- page 2 route=digital status=ok
image_only=too_little_text -->`) and a bundle's `manifest.json`
(`image_only_pages`), so a downstream reader can pick the page images it must
read:

```python
reader = PDFReader("scans.pdf", ocr="never")
for page in reader.pages:
    if page.image_only_reasons and page.page_image is not None:
        png = page.page_image.read_bytes()   # drawn from the PDF; an error page has none
```

A bundle written with `render_pages=False` or `include_images=False`, or one
whose render of a marked page fails, warns (`UserWarning`) that it holds no
full copy of those pages (a figure on one may still be written); their
`missing` entries in `manifest.json` say so too. During `rationalize()`,
`multimodal=True` (the default) attaches rendered page/band PNGs for `vision`
and `image_only` pages directly to vision-capable model requests rather than
trusting empty or garbled text; pass `multimodal=False` (or omit page renders)
to skip `image_only` pages in `result.skipped_units` (`reason="image_only"`, or
`"page_error"` for an error page).

A page read by OCR at a mean word confidence under 70 carries a warning that
it may be turned the wrong way or unreadable, so its text shouldn't be trusted
as is; the text is kept. Pass `orientation_fallback=True` to run a 4-way
(`0°`, `90°`, `180°`, `270°`) OCR word-confidence vote whenever OSD is
unavailable or initial confidence falls below 70.

Extraction is always complete: a bad page is recorded, never dropped. Pass
`strict=True` to raise `PDFCorruptError` on a bad page instead.

### Options

`PDFReader` and `PDFWriter` take the same keyword-only options:

```python
reader = PDFReader(
    "report.pdf",
    password=None,                  # an encrypted PDF's password
    workers=1,                      # page-extraction workers (processes when > 1 or sandbox=True)
    ocr="auto",                     # "auto", "always" (every page to OCR), or "never"
    dpi=200,                        # page images (render_pages_as_images, bundles)
    vision_dpi=300,                 # pages read by OCR; at least dpi
    max_render_pixels=16_000_000,   # caps every render: a huge page gets a lower DPI
    min_image_dimension=40,         # smaller embedded images aren't figures
    strict=False,                   # raise on a bad page instead of recording it
    sandbox=False,                  # True isolates every page/render in a 2 GiB python -I worker
    orientation_fallback=False,     # True votes 0/90/180/270° when OSD fails or OCR conf < 70
)
```

Options are checked at construction: an out-of-range value raises `ValueError`
and a wrong type raises `TypeError`. `min_image_dimension` drops embedded
images narrower or shorter than that many pixels (rule lines, separators, icon
slivers); it must be at least 1.

### Execution and worker isolation

By default (`sandbox=False`, `workers=1`), `PDFReader` and `PDFWriter` run on
an in-process fast path with zero cold Python subprocess spawns on digital
PDFs, serializing native PDFium calls under a re-entrant lock. Multi-page and
multi-document parallelism scale out cleanly:

- **Across documents (`extract_corpus`):** `extract_corpus(inputs, out_dir, max_workers=8)`
  maintains a pool of warm child processes with per-document timeouts, LPT
  scheduling, and incremental bundle reuse.
- **Within a document (`workers > 1`) or strict process sandboxing (`sandbox=True`):**
  pages and renders execute in isolated `python -I` worker processes capped at
  2 GiB (`RLIMIT_AS`) that never import your script (no `if __name__ == "__main__":`
  guard required). A page exceeding its memory or time bound becomes an error
  page while remaining pages continue in a fresh worker. Inside distributed
  runtimes (**Apache Spark**, **Apache Beam**, **Apache Flink**, or **Celery**),
  per-document worker pools automatically clamp to 1 process.

### Schema-driven extraction (`rationalize`)

Install the `[rationalize]` extra (`pydantic` and `httpx`) alongside `[pdf]`:

```bash
uv pip install "datagrunt[pdf,rationalize]"
```

`rationalize()` extracts structured Pydantic records from one or more PDF
targets (`PDFDocument`, `CorpusManifest`, PDF path, bundle directory, corpus
directory, or a sequence of up to `10,000` targets). Hidden elements
(`hidden_reasons` non-empty) are excluded before prompting, bidi/tag/control
characters are escaped in the wire envelope, consecutive digital sections
coalesce adaptively up to `max_unit_chars`, `vision` and `image_only` pages
route as multimodal PNGs (`multimodal=True`), and model responses are validated,
compared across model families, grounded against cited elements, and folded
across units by entity key (`fold_across_units=True`):

```python
from decimal import Decimal
from typing import Annotated
from pydantic import BaseModel
from datagrunt import Informational, Key, extract_corpus, rationalize
from datagrunt.rationalization import AnthropicProvider, GeminiProvider, GoogleCloudAuth


class PartMention(BaseModel):
    # alnum_upper folds "04-60008-27" and "046000827" as one entity while still
    # verifying each model's raw printed identifier against cited elements:
    part_number: Annotated[str, Key("part", normalizer="alnum_upper")]
    category: str
    notes: Annotated[str | None, Informational] = None   # non-disputing free text
    specs: Annotated[list[str], Informational] = []      # united across models & units
    unit_price: Decimal | None = None


corpus = extract_corpus("pdfs/", "bundles/", max_workers=4, render_pages="vision_only")
auth = GoogleCloudAuth()

result = rationalize(
    corpus,
    schema=PartMention,
    prompt="Extract every referenced engineering part and its specifications.",
    models=(
        GeminiProvider.google_cloud("gemini-2.5-pro", project="my-gcp-proj", auth=auth),
        AnthropicProvider.google_cloud(
            "claude-opus-4-6",
            project="my-gcp-proj",
            auth=auth,
            schema_mode="prompt_fallback",  # auto-latches prompt mode on org-policy/grammar 400s
        ),
    ),
    min_families=2,          # require agreement across >= 2 distinct model families
    cache_dir=".dg-cache",   # atomic sharded on-disk response cache
)

strict_df = result.to_dataframe()                            # strict consensus records
union_df = result.to_dataframe(include_grounded_union=True)  # consensus + grounded single-family (_status, _doc_id)
review = result.needs_review                                 # disputed, ungrounded, or cross-page-conflict items
```

- **Schema markers (`Key` & `Informational`):** `Key` marks identity fields (`Annotated[str, Key]`, `Key("part")`, or `Key("part", normalizer="alnum_upper")`), including nested `model_list` child keys. `Informational` marks non-disputing fields whose wording variance never discards an agreed record — optional scalars prefer populated values over `None`, and `Informational` `list[scalar]` fields unite distinct normalized items in first-seen order across models and units.
- **Cross-family consensus & result tiers (`min_families=2`):**
  - `result.records`: strict clean tier (`status in ("consensus", "single_model")`, with `must_agree=(...)` enforcing unanimity on critical paths).
  - `result.grounded_records` / `result.grounded_items` (or `to_dataframe(..., include_grounded_union=True)`, `to_dicts`, `to_arrow_table`): grounded-union tier (`"consensus"`, `"single_model"`, and element-grounded `"missing_in_some_models"` items with zero `grounding_reasons`, `disputed_fields`, or `notes`), preserving single-family catches alongside `_status` and `_doc_id` attribution.
  - `result.needs_review`: `"field_disagreement"` (retaining non-conflicting multi-unit folded records), `"ungrounded_value"`, or `"cross_page_conflict"` items, each carrying `GroundedRecord.sources` (`doc_id`, `page_number`, `element_id`, `bbox`).
- **3-witness quote and key grounding:** supporting models must cite non-header elements (`source_element_ids`) and provide `grounding_quotes` verified against cited elements (exact substring on digital elements; token containment ≥ 0.85 on vision/OCR elements, out-of-prompt OCR witnesses on multimodal scans, and cross-family peer-quote corroboration on textless `ocr="never"` scans) containing all root and child `Key` values.
- **Providers & offline cloud batches:** model strings require an explicit prefix (`openai:`, `anthropic:`, `gemini:`, `ollama:`, or `openai-compatible:<model>@<url>`), or pass `GeminiProvider`, `AnthropicProvider` (`schema_mode="native" | "prompt" | "prompt_fallback"`), or `OpenAICompatibleProvider` (with pooled HTTP keep-alive and `GoogleCloudAuth` for Vertex AI). For asynchronous cloud batch APIs, build uncached requests with `plan = prepare_batch(...)`, export via `plan.to_vertex_jsonl(...)` / `plan.to_anthropic_jsonl(...)`, and assemble the full `RationalizedResult` with `ingest_batch(plan, responses)`.

## Engine Comparison

| Feature | Polars | DuckDB | PyArrow |
|---------|--------|--------|---------|
| **Best for** | DataFrame operations | SQL queries & analytics | Arrow ecosystem integration |
| **Performance** | Fast in-memory processing | Excellent for large datasets | Optimized columnar operations |
| **Default for** | CSVReader | CSVWriter | - |
| **Export Quality** | Good | Excellent (especially JSON) | Native Parquet support |

_The engines above apply to CSV processing. Whichever you pick, results are consistent: leading `#` comment lines, leading blank lines, logical record counts (quoted embedded newlines count as one record), and column-name normalization — including collision handling like `Col A,col_a` → `col_a, col_a_1` — behave identically across all three. Mid-file lines starting with `#` are kept as data on all three engines. PDF parsing uses the **PDFium** engine (permissively licensed) — see [PDF parsing](#pdf-parsing)._

## Primary Classes

### Readers
- **`CSVReader`**: Read and process CSV files with intelligent delimiter detection
- **`ExcelReader`**: Read Excel workbooks sheet by sheet into Polars DataFrames, dicts, PyArrow tables, or SQL query results
- **`ParquetReader`**: Read Parquet files into Polars DataFrames, dicts, PyArrow tables, or SQL query results
- **`PDFReader`**: Read PDF files into a typed document (headings, paragraphs, key-value pairs, tables, lists, figures) as Polars DataFrames, PyArrow tables, dicts, Markdown, or SQL query results

### Writers
- **`CSVWriter`**: Export CSV data to multiple formats (CSV, Excel, JSON, Parquet)
- **`ExcelWriter`**: Export a sheet (or all sheets) of an Excel workbook to CSV, Excel, JSON, JSONL, or Parquet
- **`ParquetWriter`**: Export Parquet files to multiple formats (CSV, Excel, JSON, JSONL, Parquet)
- **`PDFWriter`**: Write a PDF's document to JSON, JSONL, Markdown, Parquet, or CSV, extract its figures, render whole pages to PNG files, or write a checksummed bundle

### Bundles & Corpora
- **`write_bundle` / `load_bundle`**: Write and verify checksummed single-PDF bundles (`render_pages=True | False | "vision_only"`)
- **`extract_corpus` / `load_corpus`**: Parallel multi-PDF corpus extraction with LPT scheduling, SHA-256 incremental reuse, and `CorpusManifest` verification

### Rationalization
- **`rationalize`**: Adaptive multimodal extraction, 3-witness quote/key grounding, cross-family consensus, and cross-unit folding over PDFs, bundles, or `CorpusManifest`s
- **`Key` & `Informational`**: `Annotated` schema markers for entity identity (`Key("part", normalizer="alnum_upper")`) and non-disputing descriptive/list fields (`Informational`)
- **`prepare_batch` & `ingest_batch`**: Offline Vertex AI and Anthropic Message Batches JSONL generation (`BatchPlan`) and response ingestion
- **`RationalizedResult` & `GroundedRecord`**: Strict (`records`), grounded-union (`grounded_records`, `grounded_items`, `include_grounded_union=True`), and review (`needs_review`) tiers with `ElementSource` bounding-box provenance, telemetry, and Polars/Arrow/dict exports

## Full Documentation

For complete documentation, detailed examples, and advanced usage patterns, see:
📖 **[Complete Documentation](https://www.datagrunt.io/docs)**

## License

This project is licensed under the [MIT License](https://opensource.org/license/mit)

## Acknowledgements

A HUGE thank you to the open source community and the creators of [DuckDB](https://duckdb.org), [Polars](https://pola.rs), and [PyArrow](https://arrow.apache.org/docs/python/) for their fantastic libraries that power Datagrunt.

## Source Repository

[https://github.com/pmgraham/datagrunt](https://github.com/pmgraham/datagrunt)
