# PDF Parsing in datagrunt — Design

**Date:** 2026-05-30
**Status:** Approved (pending spec review)
**Branch:** `feature/parse-pdf-files`

## Summary

Add a self-contained PDF parsing capability to datagrunt that converts PDF
files into a unified JSON structure (text blocks, tables, images, and OCR text
for scanned pages) plus extracted image files. The capability mirrors the
existing CSV architecture (`csv_api` + `core/csv_io`) exactly and shares only
the `FileProperties` base. It does **not** touch or depend on any CSV code path.

The parsing logic is ported from the pure-Python core of
`https://github.com/pmgraham/pdf-parser-python` (PyMuPDF + pdfplumber +
Tesseract). All deployment-specific code from that repo — Google Cloud Storage
upload, BigQuery, the Dataflow flex template, and the token-based JSON splitting
— is intentionally dropped.

## Guiding Principles

- **Isolation:** PDF logic lives in its own `pdf_api` and `core/pdf_io`
  packages. The only shared/modified file is `core/file_io/fileproperties.py`,
  which gains a `pdf` extension and an `is_pdf` check with no behavior change
  for existing types.
- **Convention parity:** Structure, naming, factory/engine pattern, public API
  shape, and test layout follow the CSV implementation 1:1.
- **Optional dependency:** PDF dependencies install via a `datagrunt[pdf]`
  extra so CSV-only users are unaffected.

## Dependencies

Ported pure-Python stack (no GCP / Beam):

- `PyMuPDF>=1.27.0`
- `pdfplumber>=0.11.0`
- `pytesseract>=0.3.10`
- `Pillow>=10.0.0`

These go in a **new optional extra** in `pyproject.toml`:

```toml
[project.optional-dependencies]
pdf = [
    "PyMuPDF>=1.27.0",
    "pdfplumber>=0.11.0",
    "pytesseract>=0.3.10",
    "Pillow>=10.0.0",
]
```

OCR additionally requires the **`tesseract` system binary** (not
pip-installable). This is documented in the README. OCR-dependent tests are
guarded with `skipif` when the binary is unavailable.

### Lazy imports (required by the optional extra)

Because `datagrunt/__init__.py` exports `PDFReader`/`PDFWriter` at the package
level, the heavy third-party imports (`pymupdf`/`fitz`, `pdfplumber`,
`pytesseract`, `PIL`) **must not** be imported at module top-level. They are
imported lazily inside the extractor/engine functions that use them. This keeps
`import datagrunt` working on a base install (no `[pdf]`), preserving CSV
functionality. Invoking a PDF parse method without the extra installed raises a
clear error, e.g.:

```
ImportError: PDF parsing requires extra dependencies. Install with: pip install datagrunt[pdf]
```

## Package Structure

Mirrors `csv_api` / `core/csv_io`:

```
src/datagrunt/
  pdf_api/
    __init__.py          # exports PDFReader, PDFWriter
    pdfreader.py         # class PDFReader(PDFComponents)
    pdfwriter.py         # class PDFWriter(PDFComponents)
  core/pdf_io/
    __init__.py
    extractors.py        # ported pure functions (1:1 with source tools/):
                         #   analyze_page, extract_text_blocks, extract_tables,
                         #   extract_images, ocr_page
    pdfcomponents.py     # PDFComponents(FileProperties); per-page parse + unified
                         #   element assembly (parse_single_page_python logic)
    engines.py           # PDFEngineProperties, PDFBaseReaderEngine(ABC),
                         #   PDFReaderPyMuPDFEngine, PDFBaseWriterEngine(ABC),
                         #   PDFWriterPyMuPDFEngine
    factories.py         # PDFEngineFactory  (engine="pymupdf")
```

### Source → datagrunt mapping

| Source (`pdf-parser-python`)                | datagrunt destination |
|---------------------------------------------|-----------------------|
| `tools/page_analyzer.py:analyze_page`       | `core/pdf_io/extractors.py` |
| `tools/text_extraction.py:extract_text_blocks` | `core/pdf_io/extractors.py` |
| `tools/table_extraction.py:extract_tables`  | `core/pdf_io/extractors.py` |
| `tools/image_extraction.py:extract_images`  | `core/pdf_io/extractors.py` |
| `tools/ocr.py:ocr_page`                     | `core/pdf_io/extractors.py` |
| `run_pure_python_parsing.py:parse_single_page_python` | `core/pdf_io/pdfcomponents.py` (per-page assembly) |
| `run_pure_python_parsing.py:run_pure_python_pipeline` (page loop + `ThreadPoolExecutor`) | `core/pdf_io/engines.py` reader engine |
| `tools/gcs_utils.py`, GCS upload helpers, `estimate_tokens_and_split`, Dataflow template, BigQuery | **Dropped** |

## `FileProperties` Additions (only shared touch-point)

In `core/file_io/fileproperties.py`:

- `FileExtensions.pdf_extensions` → `["pdf"]`
- `FileProperties.is_pdf` → cached property: `extension_string.lower() in pdf_extensions`

PDF already classifies as `is_unstructured` under the existing logic (not in
`standard_extensions` or `semi_structured_extensions`); this is unchanged. No
existing type's behavior changes.

## Public API

### `PDFReader(filepath, engine="pymupdf", workers=4)`

Subclasses `PDFComponents`. `engine` selects the parsing backend via
`PDFEngineFactory` (currently only `"pymupdf"`). `workers` controls per-page
concurrency (`ThreadPoolExecutor`), default 4.

| Method | Returns |
|--------|---------|
| `get_sample()` | Parse the first page; return its elements. |
| `to_dicts()` | The full unified `{"document": {... "pages": [...]}}` structure (primary output). |
| `to_dataframe()` | Polars DataFrame, one row per extracted element (flattened). |
| `to_arrow_table()` | PyArrow Table, one row per extracted element (flattened). |

`to_dataframe`/`to_arrow_table` follow the CSV reader pattern. They flatten the
element list across pages; nested fields (`position`, `metadata`) are kept as
struct/JSON columns. This may be revisited later but follows the established
pattern for now.

### `PDFWriter(filepath, engine="pymupdf", workers=4)`

Subclasses `PDFComponents`.

| Method | Behavior |
|--------|----------|
| `write_json(export_filename=None, ...)` | Write the unified JSON to disk. |
| `write_json_newline_delimited(export_filename=None, ...)` | One page/element per line. |
| `extract_images(output_dir=None)` | Write embedded image files to disk; return their paths. JSON `metadata.file_path` references the local image paths. |

### Output path conventions (identical to the CSV writer)

The CSV writers resolve output paths with
`DuckDBQueries.set_export_filename(default_filename, export_filename)`, which
returns `export_filename` when provided (truthy) and the default otherwise; the
defaults live on the `CSVEngineProperties` dataclass
(`json_export_filename = "output.json"`, `json_newline_export_filename =
"output.jsonl"`, etc.). The argument is named `export_filename` and defaults to
`None`.

The PDF writer mirrors this exactly:

- A `PDFEngineProperties` dataclass holds the defaults:
  - `json_export_filename = "output.json"`
  - `json_newline_export_filename = "output.jsonl"`
  - `images_export_dir = "output_images"`
- A `set_export_filename(default, export_filename=None)` helper with semantics
  identical to the CSV one is added in `core/pdf_io` (a module-level function or
  a method on `PDFEngineProperties`). It is **not** reused from `DuckDBQueries`,
  so the PDF feature carries no DuckDB dependency.
- `write_json` / `write_json_newline_delimited` take `export_filename=None`
  and fall back to the dataclass defaults via that helper — same call shape and
  behavior as `CSVWriter.write_json`.
- `extract_images(output_dir=None)` falls back to `images_export_dir`, creates
  the directory if needed (`mkdir(parents=True, exist_ok=True)`), writes each
  embedded image as `{filepath.stem}_page{n}_img{i}.{ext}` (globally unique,
  from the source's image-naming scheme), and returns the list of written
  paths. The corresponding element `metadata.file_path` is set to the local
  image path.

## Output Schema (preserved verbatim from source)

```json
{
  "document": {
    "source": "<path>",
    "total_pages": 0,
    "processing_id": "proc_py_<ts>",
    "pipeline_type": "pure_python_local_v1",
    "errors": null,
    "pages": [
      {
        "page_number": 1,
        "width": 0.0,
        "height": 0.0,
        "classification": "text_only | scanned | mixed",
        "elements": [
          {
            "id": "elem_01_001",
            "type": "header | subheader | body_text | caption | table | image",
            "content": "<text | 2D array | null>",
            "page": 1,
            "position": {"x": 0, "y": 0, "w": 0, "h": 0},
            "confidence": 1.0,
            "metadata": { }
          }
        ]
      }
    ]
  }
}
```

Cloud-specific fields (GCS URIs, token-split part names) are not produced.
`processing_id` is generated from a runtime timestamp inside the engine
(`proc_py_<unix_ts>`), matching the source.

## Error Handling

- Per-page `try/except`; failures collected into `document.errors` (source
  behavior), successful pages still returned.
- `FileNotFoundError` raised on a missing file (matches CSV engines/factory).
- `is_empty` (0-byte) guard returns an empty result object.
- **Never** call `is_blank` — it opens files in text mode and would fail on
  binary PDFs. PDF classes rely on `is_empty` only.
- Invalid `engine` value raises `ValueError` (matches `CSVEngineFactory`).

## Concurrency

Per-page parsing runs on a `ThreadPoolExecutor(max_workers=workers)` inside the
reader engine, porting `run_pure_python_pipeline`'s page loop minus all GCS
interaction. Pages are reassembled in page-number order; per-page errors are
captured without aborting the run.

## Testing (mirrors existing layout, TDD)

```
tests/pdf_api_tests/
  test_pdfreader.py
  test_pdfwriter.py
tests/core_tests/pdf_io_tests/
  __init__.py
  test_extractors.py
  test_engines.py
  test_pdfcomponents.py
tests/core_tests/file_io_tests/test_fileproperties.py   # add is_pdf assertions
```

- Add a small generated PDF fixture to `tests/conftest.py` containing native
  text and one embedded image (created with PyMuPDF at fixture build time).
- OCR / scanned-page tests guarded by `skipif` on the `tesseract` binary.
- Implementation follows TDD: red → green → refactor per unit.

## Wiring

- `src/datagrunt/__init__.py`: import and add `PDFReader`, `PDFWriter` to
  `__all__`; update module docstring to mention PDF support.
- `src/datagrunt/core/__init__.py`: export the new `core/pdf_io` classes.
- `pyproject.toml`: add the `[project.optional-dependencies] pdf` extra and add
  `"pdf"` to `keywords`.

## Out of Scope

- Google Cloud Storage / BigQuery integration.
- Apache Beam / Dataflow templating.
- Token-based JSON splitting for downstream LLM ingestion.
- An LLM/Gemini parsing engine (the factory leaves room for one later).
```
