# PDF Parsing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a self-contained PDF→JSON+images parsing capability to datagrunt (`pdf_api` + `core/pdf_io`) that mirrors the CSV architecture and shares only `FileProperties`, without touching any CSV code path.

**Architecture:** Five pure extractor functions (PyMuPDF/pdfplumber/Tesseract) ported verbatim from `github.com/pmgraham/pdf-parser-python` live in `core/pdf_io/extractors.py`. `pdfcomponents.py` assembles per-page elements into a unified document dict and flattens elements for tabular output. `engines.py` provides a PyMuPDF reader engine (threaded per-page parsing) and writer engine (JSON + image files), selected via `PDFEngineFactory`. Public `PDFReader`/`PDFWriter` classes in `pdf_api` subclass `PDFComponents`. Heavy third-party imports are lazy so a base install (no `[pdf]` extra) can still `import datagrunt` for CSV use.

**Tech Stack:** Python 3.10+, PyMuPDF (`pymupdf`), pdfplumber, pytesseract (+ `tesseract` system binary), Pillow, Polars, PyArrow, pytest.

**Spec:** `docs/superpowers/specs/2026-05-30-pdf-parsing-design.md`

**Tooling:** This project uses **`uv`** for all Python package and environment management. The env is a `uv venv` at `.venv` (Python 3.12) with `uv pip install -e ".[pdf,dev]"` already applied. Run every Python/test/lint command through uv: `uv run pytest ...`, `uv run ruff ...`, `uv run python -c "..."`. Never invoke bare `pip`/`python`/`pytest`. A harmless `VIRTUAL_ENV ... does not match` warning may print — ignore it; uv uses `.venv`.

---

## File Structure

**Create:**
- `src/datagrunt/core/pdf_io/__init__.py` — package exports
- `src/datagrunt/core/pdf_io/extractors.py` — 5 ported pure extractor functions + lazy-import helper
- `src/datagrunt/core/pdf_io/pdfcomponents.py` — `PDFComponents`, `parse_page`, `parse_document`, `flatten_document_elements`
- `src/datagrunt/core/pdf_io/engines.py` — `PDFEngineProperties`, `set_export_filename`, base ABCs, `PDFReaderPyMuPDFEngine`, `PDFWriterPyMuPDFEngine`
- `src/datagrunt/core/pdf_io/factories.py` — `PDFEngineFactory`
- `src/datagrunt/pdf_api/__init__.py` — exports `PDFReader`, `PDFWriter`
- `src/datagrunt/pdf_api/pdfreader.py` — `PDFReader`
- `src/datagrunt/pdf_api/pdfwriter.py` — `PDFWriter`
- `tests/pdf_api_tests/__init__.py`
- `tests/pdf_api_tests/test_pdfreader.py`
- `tests/pdf_api_tests/test_pdfwriter.py`
- `tests/core_tests/pdf_io_tests/__init__.py`
- `tests/core_tests/pdf_io_tests/test_extractors.py`
- `tests/core_tests/pdf_io_tests/test_pdfcomponents.py`
- `tests/core_tests/pdf_io_tests/test_engines.py`
- `tests/core_tests/pdf_io_tests/test_factories.py`

**Modify:**
- `pyproject.toml` — add `[pdf]` optional extra + `"pdf"` keyword
- `src/datagrunt/core/file_io/fileproperties.py` — add `pdf_extensions` + `is_pdf`
- `src/datagrunt/core/__init__.py` — export new `core/pdf_io` symbols
- `src/datagrunt/__init__.py` — export `PDFReader`, `PDFWriter`
- `tests/conftest.py` — add PDF fixtures
- `tests/core_tests/file_io_tests/test_fileproperties.py` — add `is_pdf` assertions
- `README.md` — document PDF feature + tesseract requirement

---

## Task 1: Add the `[pdf]` optional dependency extra

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add the `pdf` extra and keyword**

In `pyproject.toml`, add `"pdf"` to the `keywords` list, and add a new `pdf` entry under `[project.optional-dependencies]` (which currently only has `dev`):

```toml
[project.optional-dependencies]
pdf = [
    "PyMuPDF>=1.27.0",
    "pdfplumber>=0.11.0",
    "pytesseract>=0.3.10",
    "Pillow>=10.0.0",
]
dev = [
    "pytest>=8.3.4",
    "pytest-cov>=6.0.0",
    "ruff>=0.5.5",
    "bumpver>=2024.1130",
    "fastexcel>=0.13.0",
]
```

Update the keywords line to:

```toml
keywords = ["csv", "data", "duckdb", "polars", "pyarrow", "xlsx", "delimiter", "ai", "gemini", "pdf"]
```

- [ ] **Step 2: Install the extra and dev deps into the environment**

Run: `uv pip install -e ".[pdf,dev]"`
Expected: installs `pymupdf`, `pdfplumber`, `pytesseract`, `Pillow` with no errors.

- [ ] **Step 3: Verify the tesseract system binary (for OCR tests)**

Run: `which tesseract || echo "TESSERACT MISSING"`
Expected: a path (e.g. `/opt/homebrew/bin/tesseract`). If `TESSERACT MISSING`, OCR tests will `skip` (not fail). To enable OCR locally on macOS: `brew install tesseract`.

- [ ] **Step 4: Verify base import still works**

Run: `uv run python -c "import datagrunt; from datagrunt import CSVReader; print('ok')"`
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml
git commit -m "build: add optional [pdf] dependency extra"
```

---

## Task 2: Add PDF support to `FileProperties`

**Files:**
- Modify: `src/datagrunt/core/file_io/fileproperties.py`
- Test: `tests/core_tests/file_io_tests/test_fileproperties.py`

- [ ] **Step 1: Write the failing test**

Add this test method to the `TestFileProperties` class in `tests/core_tests/file_io_tests/test_fileproperties.py`:

```python
    def test_is_pdf(self, tmp_path):
        """Test PDF detection and classification."""
        pdf_path = tmp_path / "doc.pdf"
        pdf_path.write_bytes(b"%PDF-1.4 dummy")

        pdf_file = FileProperties(pdf_path)
        assert pdf_file.is_pdf
        assert pdf_file.extension_string == "pdf"
        assert pdf_file.is_unstructured
        assert not pdf_file.is_csv
        assert not pdf_file.is_structured
        assert not pdf_file.is_semi_structured
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/file_io_tests/test_fileproperties.py::TestFileProperties::test_is_pdf -v`
Expected: FAIL with `AttributeError: 'FileProperties' object has no attribute 'is_pdf'`

- [ ] **Step 3: Add `pdf_extensions` to `FileExtensions`**

In `src/datagrunt/core/file_io/fileproperties.py`, add this method to the `FileExtensions` class (place it right after `tsv_extensions`):

```python
    @cached_property
    def pdf_extensions(self):
        """Define PDF extensions."""
        return ["pdf"]
```

- [ ] **Step 4: Add `is_pdf` to `FileProperties`**

In the `FileProperties` class, add this property right after `is_csv`:

```python
    @cached_property
    def is_pdf(self):
        """Check if the file is a PDF file."""
        return self.extension_string.lower() in self._ext.pdf_extensions
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/file_io_tests/test_fileproperties.py -v`
Expected: PASS (all tests, including existing ones)

- [ ] **Step 6: Commit**

```bash
git add src/datagrunt/core/file_io/fileproperties.py tests/core_tests/file_io_tests/test_fileproperties.py
git commit -m "feat: add is_pdf detection to FileProperties"
```

---

## Task 3: Add PDF test fixtures

**Files:**
- Modify: `tests/conftest.py`
- Create: `tests/core_tests/pdf_io_tests/__init__.py`
- Create: `tests/pdf_api_tests/__init__.py`

- [ ] **Step 1: Create the test package init files**

Create empty `tests/core_tests/pdf_io_tests/__init__.py` and `tests/pdf_api_tests/__init__.py`:

```bash
mkdir -p tests/core_tests/pdf_io_tests tests/pdf_api_tests
touch tests/core_tests/pdf_io_tests/__init__.py tests/pdf_api_tests/__init__.py
```

- [ ] **Step 2: Add PDF fixtures to `tests/conftest.py`**

Append the following to `tests/conftest.py`. `sample_pdf` builds a one-page native-text PDF containing a header, body text, and one embedded image (large enough to survive the 40px filter in extraction). `empty_pdf` is a 0-byte file. `tesseract_available` gates OCR tests.

```python
import shutil


@pytest.fixture
def sample_pdf(tmp_path):
    """Create a one-page PDF with native text and an embedded image."""
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)  # US Letter
    page.insert_text((72, 72), "Quarterly Report", fontsize=24)  # header (large)
    # Several body lines at 11pt so the median font size is small and the 24pt
    # title is correctly classified as a header (header threshold = median*1.6).
    page.insert_text((72, 120), "This is body text for testing.", fontsize=11)
    page.insert_text((72, 140), "Body line two for the report.", fontsize=11)
    page.insert_text((72, 160), "Body line three with details.", fontsize=11)
    page.insert_text((72, 180), "Body line four wraps up the text.", fontsize=11)

    # Build a 100x100 red PNG and embed it on the page.
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 100, 100))
    pix.set_rect(pix.irect, (255, 0, 0))
    img_bytes = pix.tobytes("png")
    page.insert_image(pymupdf.Rect(72, 200, 172, 300), stream=img_bytes)

    pdf_path = tmp_path / "report.pdf"
    doc.save(str(pdf_path))
    doc.close()
    return str(pdf_path)


@pytest.fixture
def empty_pdf(tmp_path):
    """Create a 0-byte PDF file."""
    pdf_path = tmp_path / "empty.pdf"
    pdf_path.touch()
    return str(pdf_path)


@pytest.fixture
def tesseract_available():
    """Return True if the tesseract system binary is on PATH."""
    return shutil.which("tesseract") is not None
```

- [ ] **Step 3: Verify the fixture builds a real PDF**

Run: `uv run pytest tests/core_tests/file_io_tests/test_fileproperties.py -v` (sanity check that conftest still imports cleanly).
Expected: PASS. Then verify the fixture works:

Run: `uv run python -c "import pymupdf; d=pymupdf.open(); p=d.new_page(); print(d.page_count)"`
Expected: `1`

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py tests/core_tests/pdf_io_tests/__init__.py tests/pdf_api_tests/__init__.py
git commit -m "test: add PDF fixtures and pdf_io/pdf_api test packages"
```

---

## Task 4: Port `analyze_page` extractor

**Files:**
- Create: `src/datagrunt/core/pdf_io/extractors.py`
- Create: `src/datagrunt/core/pdf_io/__init__.py` (minimal, expanded in Task 13)
- Test: `tests/core_tests/pdf_io_tests/test_extractors.py`

- [ ] **Step 1: Create a minimal package init**

Create `src/datagrunt/core/pdf_io/__init__.py`:

```python
"""Initializes the pdf_io module of the datagrunt package."""
```

- [ ] **Step 2: Write the failing test**

Create `tests/core_tests/pdf_io_tests/test_extractors.py`:

```python
"""Tests for the pure PDF extractor functions."""

import pytest

from datagrunt.core.pdf_io import extractors


class TestAnalyzePage:
    """Test suite for analyze_page."""

    def test_native_text_page(self, sample_pdf):
        result = extractors.analyze_page(sample_pdf, 0)
        assert result["status"] == "success"
        assert result["page_number"] == 0
        assert result["total_pages"] == 1
        assert result["width"] == pytest.approx(612, abs=1)
        assert result["height"] == pytest.approx(792, abs=1)
        assert result["has_text_layer"] is True
        assert result["is_scanned"] is False
        assert result["image_count"] >= 1

    def test_out_of_range_page(self, sample_pdf):
        result = extractors.analyze_page(sample_pdf, 99)
        assert result["status"] == "error"
        assert "out of range" in result["message"]
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestAnalyzePage -v`
Expected: FAIL with `ModuleNotFoundError`/`AttributeError: module ... has no attribute 'analyze_page'`

- [ ] **Step 4: Create `extractors.py` with the lazy-import helper and `analyze_page`**

Create `src/datagrunt/core/pdf_io/extractors.py`:

```python
"""Pure-Python PDF extractor functions (PyMuPDF / pdfplumber / Tesseract).

Ported from github.com/pmgraham/pdf-parser-python. Third-party imports are
performed lazily inside each function so that ``import datagrunt`` works on a
base install without the optional ``[pdf]`` extra.
"""

PDF_EXTRA_HINT = (
    "PDF parsing requires extra dependencies. "
    "Install with: pip install datagrunt[pdf]"
)


def _import_pymupdf():
    """Import pymupdf lazily with a helpful error if the extra is missing."""
    try:
        import pymupdf
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return pymupdf


def analyze_page(pdf_path: str, page_number: int) -> dict:
    """Analyze a single PDF page and return metadata about its content.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number to analyze.

    Returns:
        Dict with page metadata including dimensions, text/image presence,
        and whether the page appears to be scanned.
    """
    pymupdf = _import_pymupdf()
    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    page_count = doc.page_count
    if page_number < 0 or page_number >= page_count:
        doc.close()
        return {
            "status": "error",
            "message": f"Page {page_number} out of range (0-{page_count - 1})",
        }

    try:
        page = doc[page_number]
        text = page.get_text("text").strip()
        blocks = page.get_text("dict")["blocks"]
        images = page.get_images()

        text_blocks = [b for b in blocks if b.get("type") == 0]
        image_blocks = [b for b in blocks if b.get("type") == 1]

        has_text_layer = len(text) > 0
        is_scanned = not has_text_layer and len(images) > 0

        drawings = page.get_drawings()
        has_lines = any(
            item[0] in ("l", "re") for d in drawings for item in d.get("items", [])
        )

        result = {
            "status": "success",
            "page_number": page_number,
            "total_pages": page_count,
            "width": page.rect.width,
            "height": page.rect.height,
            "rotation": page.rotation,
            "has_text_layer": has_text_layer,
            "is_scanned": is_scanned,
            "text_block_count": len(text_blocks),
            "image_count": len(images),
            "image_block_count": len(image_blocks),
            "has_line_drawings": has_lines,
            "text_length": len(text),
        }
    finally:
        doc.close()

    return result
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestAnalyzePage -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/datagrunt/core/pdf_io/__init__.py src/datagrunt/core/pdf_io/extractors.py tests/core_tests/pdf_io_tests/test_extractors.py
git commit -m "feat: add analyze_page PDF extractor"
```

---

## Task 5: Port `extract_text_blocks` extractor

**Files:**
- Modify: `src/datagrunt/core/pdf_io/extractors.py`
- Test: `tests/core_tests/pdf_io_tests/test_extractors.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/core_tests/pdf_io_tests/test_extractors.py`:

```python
class TestExtractTextBlocks:
    """Test suite for extract_text_blocks."""

    def test_extracts_header_and_body(self, sample_pdf):
        result = extractors.extract_text_blocks(sample_pdf, 0)
        assert result["status"] == "success"
        texts = [b["text"] for b in result["blocks"]]
        assert any("Quarterly Report" in t for t in texts)
        assert any("body text" in t for t in texts)

        # The 24pt title should classify as a header; 11pt line as body_text.
        classes = {b["text"]: b["classification"] for b in result["blocks"]}
        header_text = next(t for t in texts if "Quarterly Report" in t)
        assert classes[header_text] == "header"

        # Each block carries position + font metadata.
        block = result["blocks"][0]
        assert set(block["bbox"]) == {"x", "y", "w", "h"}
        assert "font" in block and "font_size" in block
        assert "reading_order" in block
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestExtractTextBlocks -v`
Expected: FAIL with `AttributeError: ... 'extract_text_blocks'`

- [ ] **Step 3: Add `_classify_block` and `extract_text_blocks` to `extractors.py`**

Append to `src/datagrunt/core/pdf_io/extractors.py`:

```python
def _classify_block(font_size: float, is_bold: bool, all_sizes: list) -> str:
    """Classify a text block based on font size relative to the page."""
    if not all_sizes:
        return "body_text"
    median_size = sorted(all_sizes)[len(all_sizes) // 2]

    if font_size >= median_size * 1.6:
        return "header"
    elif font_size >= median_size * 1.2:
        return "subheader"
    elif font_size < median_size * 0.85:
        return "caption"
    return "body_text"


def extract_text_blocks(pdf_path: str, page_number: int) -> dict:
    """Extract text blocks from a PDF page with font and position metadata.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.

    Returns:
        Dict with status and list of text blocks, each containing text,
        bounding box, font info, and classification.
    """
    pymupdf = _import_pymupdf()
    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= doc.page_count:
        doc.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    try:
        page = doc[page_number]
        data = page.get_text("dict", sort=True)
        raw_blocks = [b for b in data["blocks"] if b.get("type") == 0]

        all_sizes = []
        for block in raw_blocks:
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    if span["text"].strip():
                        all_sizes.append(span["size"])

        blocks = []
        for order, block in enumerate(raw_blocks):
            texts = []
            fonts = []
            sizes = []
            bold_flags = []

            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text = span["text"]
                    if text.strip():
                        texts.append(text)
                        fonts.append(span["font"])
                        sizes.append(span["size"])
                        bold_flags.append(bool(span["flags"] & 16))

            full_text = " ".join(texts).strip()
            if not full_text:
                continue

            dominant_font = max(set(fonts), key=fonts.count) if fonts else "unknown"
            dominant_size = max(set(sizes), key=sizes.count) if sizes else 0
            is_bold = any(bold_flags)
            is_italic = False
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    if span["text"].strip() and (span["flags"] & 2):
                        is_italic = True
                        break

            bbox = block["bbox"]
            classification = _classify_block(dominant_size, is_bold, all_sizes)

            blocks.append({
                "text": full_text,
                "bbox": {
                    "x": round(bbox[0], 2),
                    "y": round(bbox[1], 2),
                    "w": round(bbox[2] - bbox[0], 2),
                    "h": round(bbox[3] - bbox[1], 2),
                },
                "font": dominant_font,
                "font_size": round(dominant_size, 1),
                "is_bold": is_bold,
                "is_italic": is_italic,
                "classification": classification,
                "reading_order": order,
            })
    finally:
        doc.close()

    return {"status": "success", "blocks": blocks}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestExtractTextBlocks -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/core/pdf_io/extractors.py tests/core_tests/pdf_io_tests/test_extractors.py
git commit -m "feat: add extract_text_blocks PDF extractor"
```

---

## Task 6: Port `extract_tables` extractor

**Files:**
- Modify: `src/datagrunt/core/pdf_io/extractors.py`
- Test: `tests/core_tests/pdf_io_tests/test_extractors.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/core_tests/pdf_io_tests/test_extractors.py`:

```python
class TestExtractTables:
    """Test suite for extract_tables."""

    def test_no_tables_on_plain_page(self, sample_pdf):
        # The sample page has no ruled table; should succeed with empty list.
        result = extractors.extract_tables(sample_pdf, 0)
        assert result["status"] == "success"
        assert result["tables"] == []

    def test_out_of_range_page(self, sample_pdf):
        result = extractors.extract_tables(sample_pdf, 99)
        assert result["status"] == "error"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestExtractTables -v`
Expected: FAIL with `AttributeError: ... 'extract_tables'`

- [ ] **Step 3: Add `extract_tables` to `extractors.py`**

Append to `src/datagrunt/core/pdf_io/extractors.py`. Note the lazy `pdfplumber` import:

```python
def _import_pdfplumber():
    """Import pdfplumber lazily with a helpful error if the extra is missing."""
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return pdfplumber


def extract_tables(pdf_path: str, page_number: int) -> dict:
    """Extract tables from a PDF page with position and structure metadata.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.

    Returns:
        Dict with status and list of tables, each containing a 2D data array,
        bounding box, row/column counts, and header detection.
    """
    pdfplumber = _import_pdfplumber()
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= len(pdf.pages):
        pdf.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    try:
        page = pdf.pages[page_number]
        found_tables = page.find_tables()

        tables = []
        for table in found_tables:
            data = table.extract()
            if not data:
                continue

            bbox = table.bbox
            num_rows = len(data)
            num_cols = max(len(row) for row in data) if data else 0

            has_header = (
                num_rows > 1
                and all(cell is not None and cell.strip() for cell in data[0])
            )

            tables.append({
                "data": data,
                "bbox": {
                    "x": round(bbox[0], 2),
                    "y": round(bbox[1], 2),
                    "w": round(bbox[2] - bbox[0], 2),
                    "h": round(bbox[3] - bbox[1], 2),
                },
                "rows": num_rows,
                "columns": num_cols,
                "has_header_row": has_header,
            })
    finally:
        pdf.close()

    return {"status": "success", "tables": tables}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestExtractTables -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/core/pdf_io/extractors.py tests/core_tests/pdf_io_tests/test_extractors.py
git commit -m "feat: add extract_tables PDF extractor"
```

---

## Task 7: Port `extract_images` extractor (with optional disk output)

**Files:**
- Modify: `src/datagrunt/core/pdf_io/extractors.py`
- Test: `tests/core_tests/pdf_io_tests/test_extractors.py`

> **Note:** This differs from the source in one intentional way: `output_dir` is optional. When `None` (the reader's default), image metadata (dimensions, format, bbox) is still returned but no file is written and `file_path` is `None`. When provided (the writer), files are written and `file_path` is set. `name_prefix` makes filenames globally unique across documents.

- [ ] **Step 1: Write the failing test**

Append to `tests/core_tests/pdf_io_tests/test_extractors.py`:

```python
class TestExtractImages:
    """Test suite for extract_images."""

    def test_metadata_only_when_no_output_dir(self, sample_pdf):
        result = extractors.extract_images(sample_pdf, 0)
        assert result["status"] == "success"
        assert len(result["images"]) >= 1
        img = result["images"][0]
        assert img["file_path"] is None
        assert img["width_px"] >= 40 and img["height_px"] >= 40
        assert "format" in img
        assert set(img["bbox"]) == {"x", "y", "w", "h"}

    def test_writes_files_when_output_dir_given(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        result = extractors.extract_images(
            sample_pdf, 0, output_dir=str(out), name_prefix="report"
        )
        assert result["status"] == "success"
        img = result["images"][0]
        assert img["file_path"] is not None
        import os
        assert os.path.isfile(img["file_path"])
        assert os.path.basename(img["file_path"]).startswith("report_page0_img")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestExtractImages -v`
Expected: FAIL with `AttributeError: ... 'extract_images'`

- [ ] **Step 3: Add `extract_images` to `extractors.py`**

Append to `src/datagrunt/core/pdf_io/extractors.py`:

```python
def extract_images(
    pdf_path: str,
    page_number: int,
    output_dir: str = None,
    name_prefix: str = "page",
) -> dict:
    """Extract embedded images from a PDF page.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.
        output_dir: Directory to write images to. When ``None``, no files are
            written and each image's ``file_path`` is ``None`` (metadata only).
        name_prefix: Filename prefix used to keep image names unique across
            documents: ``{name_prefix}_page{page_number}_img{idx}.{ext}``.

    Returns:
        Dict with status and list of image metadata (file path, pixel
        dimensions, format, and position on the page).
    """
    import os

    pymupdf = _import_pymupdf()
    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= doc.page_count:
        doc.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    try:
        page = doc[page_number]
        image_list = page.get_images()

        blocks = page.get_text("dict")["blocks"]
        image_blocks = [b for b in blocks if b.get("type") == 1]

        images = []
        for idx, img_info in enumerate(image_list):
            xref = img_info[0]
            try:
                base_image = doc.extract_image(xref)
            except Exception:
                continue

            ext = base_image.get("ext", "png")
            width = base_image.get("width", 0)
            height = base_image.get("height", 0)
            image_bytes = base_image.get("image", b"")

            if not image_bytes:
                continue

            # Skip tiny layout artifacts, borders, and spacer pixels.
            if width < 40 or height < 40:
                continue

            file_path = None
            if output_dir:
                filename = f"{name_prefix}_page{page_number}_img{idx}.{ext}"
                file_path = os.path.join(output_dir, filename)
                with open(file_path, "wb") as f:
                    f.write(image_bytes)

            bbox = {"x": 0, "y": 0, "w": 0, "h": 0}
            if idx < len(image_blocks):
                b = image_blocks[idx]["bbox"]
                bbox = {
                    "x": round(b[0], 2),
                    "y": round(b[1], 2),
                    "w": round(b[2] - b[0], 2),
                    "h": round(b[3] - b[1], 2),
                }

            images.append({
                "file_path": file_path,
                "bbox": bbox,
                "width_px": width,
                "height_px": height,
                "format": ext,
            })
    finally:
        doc.close()

    return {"status": "success", "images": images}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestExtractImages -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/core/pdf_io/extractors.py tests/core_tests/pdf_io_tests/test_extractors.py
git commit -m "feat: add extract_images PDF extractor with optional disk output"
```

---

## Task 8: Port `ocr_page` extractor

**Files:**
- Modify: `src/datagrunt/core/pdf_io/extractors.py`
- Test: `tests/core_tests/pdf_io_tests/test_extractors.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/core_tests/pdf_io_tests/test_extractors.py`:

```python
class TestOcrPage:
    """Test suite for ocr_page (requires the tesseract system binary)."""

    def test_ocr_returns_blocks(self, sample_pdf, tesseract_available):
        if not tesseract_available:
            pytest.skip("tesseract binary not installed")
        result = extractors.ocr_page(sample_pdf, 0, dpi=150)
        assert result["status"] == "success"
        assert result["ocr_engine"] == "tesseract"
        # Each block carries text, confidence (0-100), bbox, and word_count.
        if result["blocks"]:
            block = result["blocks"][0]
            assert set(block["bbox"]) == {"x", "y", "w", "h"}
            assert "confidence" in block
            assert "word_count" in block

    def test_out_of_range_page(self, sample_pdf, tesseract_available):
        if not tesseract_available:
            pytest.skip("tesseract binary not installed")
        result = extractors.ocr_page(sample_pdf, 99)
        assert result["status"] == "error"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestOcrPage -v`
Expected: FAIL with `AttributeError: ... 'ocr_page'` (or SKIP if tesseract missing — in that case temporarily confirm the attribute error by checking `uv run python -c "from datagrunt.core.pdf_io import extractors; extractors.ocr_page"` raises `AttributeError`).

- [ ] **Step 3: Add `ocr_page` to `extractors.py`**

Append to `src/datagrunt/core/pdf_io/extractors.py`:

```python
def _import_ocr_deps():
    """Import OCR deps (PIL, pytesseract) lazily with a helpful error."""
    try:
        from PIL import Image
        import pytesseract
        from pytesseract import Output
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise ImportError(PDF_EXTRA_HINT) from exc
    return Image, pytesseract, Output


def ocr_page(pdf_path: str, page_number: int, dpi: int = 300) -> dict:
    """OCR a PDF page by rendering it to an image and running Tesseract.

    Args:
        pdf_path: Path to the PDF file.
        page_number: Zero-indexed page number.
        dpi: Resolution for rendering the page. Higher = better OCR but slower.

    Returns:
        Dict with status and list of text blocks with confidence scores and
        bounding boxes (in PDF points).
    """
    pymupdf = _import_pymupdf()
    Image, pytesseract, Output = _import_ocr_deps()

    try:
        doc = pymupdf.open(pdf_path)
    except Exception as e:
        return {"status": "error", "message": f"Failed to open PDF: {e}"}

    if page_number < 0 or page_number >= doc.page_count:
        doc.close()
        return {"status": "error", "message": f"Page {page_number} out of range"}

    try:
        page = doc[page_number]
        pix = page.get_pixmap(dpi=dpi)
        mode = "RGBA" if pix.alpha else "RGB"
        img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
    finally:
        doc.close()

    try:
        data = pytesseract.image_to_data(img, output_type=Output.DICT)
    except Exception as e:
        return {"status": "error", "message": f"Tesseract OCR failed: {e}"}

    scale = 72.0 / dpi

    lines = {}
    n = len(data["text"])
    for i in range(n):
        conf = int(data["conf"][i])
        text = data["text"][i].strip()
        if conf < 0 or not text:
            continue

        key = (data["block_num"][i], data["par_num"][i], data["line_num"][i])
        word = {
            "text": text,
            "confidence": conf,
            "left": data["left"][i],
            "top": data["top"][i],
            "width": data["width"][i],
            "height": data["height"][i],
        }
        lines.setdefault(key, []).append(word)

    blocks = []
    for key, words in lines.items():
        line_text = " ".join(w["text"] for w in words)
        avg_conf = sum(w["confidence"] for w in words) / len(words)

        x0 = min(w["left"] for w in words)
        y0 = min(w["top"] for w in words)
        x1 = max(w["left"] + w["width"] for w in words)
        y1 = max(w["top"] + w["height"] for w in words)

        blocks.append({
            "text": line_text,
            "bbox": {
                "x": round(x0 * scale, 2),
                "y": round(y0 * scale, 2),
                "w": round((x1 - x0) * scale, 2),
                "h": round((y1 - y0) * scale, 2),
            },
            "confidence": round(avg_conf, 1),
            "word_count": len(words),
            "per_word_confidence": [w["confidence"] for w in words],
        })

    return {
        "status": "success",
        "blocks": blocks,
        "ocr_engine": "tesseract",
        "dpi": dpi,
    }
```

- [ ] **Step 4: Run test to verify it passes (or skips)**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_extractors.py::TestOcrPage -v`
Expected: PASS if tesseract installed, otherwise SKIP.

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/core/pdf_io/extractors.py tests/core_tests/pdf_io_tests/test_extractors.py
git commit -m "feat: add ocr_page PDF extractor"
```

---

## Task 9: Build `pdfcomponents.py` (page assembly + flatten + PDFComponents)

**Files:**
- Create: `src/datagrunt/core/pdf_io/pdfcomponents.py`
- Test: `tests/core_tests/pdf_io_tests/test_pdfcomponents.py`

This holds the per-page element assembly (ported from `parse_single_page_python`), a `parse_document` orchestrator (single-threaded here; the threaded variant lives in the reader engine in Task 10), a `flatten_document_elements` helper for tabular output, and the `PDFComponents(FileProperties)` base.

- [ ] **Step 1: Write the failing test**

Create `tests/core_tests/pdf_io_tests/test_pdfcomponents.py`:

```python
"""Tests for PDF component assembly."""

import pytest

from datagrunt.core.pdf_io import pdfcomponents


class TestParsePage:
    """Test suite for parse_page."""

    def test_assembles_elements(self, sample_pdf):
        page = pdfcomponents.parse_page(sample_pdf, 0)
        assert page["page_number"] == 1
        assert page["classification"] in {"text_only", "scanned", "mixed"}
        types = {e["type"] for e in page["elements"]}
        assert "header" in types
        assert "image" in types
        # Every element has the unified schema keys.
        for e in page["elements"]:
            assert set(e) >= {"id", "type", "content", "page", "position",
                              "confidence", "metadata"}
            assert e["id"].startswith("elem_01_")


class TestParseDocument:
    """Test suite for parse_document."""

    def test_combined_structure(self, sample_pdf):
        doc = pdfcomponents.parse_document(sample_pdf, total_pages=1)
        assert "document" in doc
        d = doc["document"]
        assert d["total_pages"] == 1
        assert d["pipeline_type"] == "pure_python_local_v1"
        assert d["processing_id"].startswith("proc_py_")
        assert len(d["pages"]) == 1


class TestFlatten:
    """Test suite for flatten_document_elements."""

    def test_flattens_to_records(self, sample_pdf):
        doc = pdfcomponents.parse_document(sample_pdf, total_pages=1)
        records = pdfcomponents.flatten_document_elements(doc)
        assert len(records) >= 2
        rec = records[0]
        # Scalar position columns + JSON-encoded complex fields.
        assert {"id", "type", "page", "x", "y", "w", "h", "confidence",
                "content", "metadata"} <= set(rec)
        assert isinstance(rec["x"], float)
        assert isinstance(rec["metadata"], str)  # JSON-encoded


class TestPDFComponents:
    """Test suite for the PDFComponents base class."""

    def test_total_pages(self, sample_pdf):
        comp = pdfcomponents.PDFComponents(sample_pdf)
        assert comp.is_pdf
        assert comp.total_pages == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_pdfcomponents.py -v`
Expected: FAIL with `ModuleNotFoundError: ... pdfcomponents`

- [ ] **Step 3: Create `pdfcomponents.py`**

Create `src/datagrunt/core/pdf_io/pdfcomponents.py`:

```python
"""PDF component assembly: page parsing, document combination, flattening."""

# standard library
import json
import time
from functools import cached_property
from pathlib import Path

# local libraries
from datagrunt.core.file_io import FileProperties
from datagrunt.core.pdf_io import extractors

PIPELINE_TYPE = "pure_python_local_v1"

# Dynamic DPI scaling thresholds for scanned (OCR) pages.
LARGE_FORMAT_DIMENSION = 1500
LARGE_FORMAT_DPI = 75
STANDARD_DPI = 150


def parse_page(pdf_path: str, page_index: int, image_output_dir: str = None) -> dict:
    """Parse a single PDF page into the unified element schema.

    Ported from ``parse_single_page_python`` (pure Python, no LLM). Raises
    ``ValueError`` if the page analysis itself fails.

    Args:
        pdf_path: Path to the PDF file.
        page_index: Zero-indexed page number.
        image_output_dir: If provided, embedded images are written here and
            their ``metadata.file_path`` is set; otherwise images are metadata
            only.

    Returns:
        A page dict with page_number, width, height, classification, elements.
    """
    analysis = extractors.analyze_page(pdf_path, page_index)
    if analysis.get("status") != "success":
        raise ValueError(f"Page analysis failed: {analysis.get('message')}")

    width = analysis["width"]
    height = analysis["height"]
    is_scanned = analysis["is_scanned"]
    has_text_layer = analysis["has_text_layer"]
    image_count = analysis["image_count"]
    has_lines = analysis["has_line_drawings"]

    elements = []
    counter = {"n": 1}

    def gen_elem_id():
        elem_id = f"elem_{page_index + 1:02d}_{counter['n']:03d}"
        counter["n"] += 1
        return elem_id

    name_prefix = Path(pdf_path).stem

    # 1. Text layer or OCR.
    if has_text_layer:
        text_result = extractors.extract_text_blocks(pdf_path, page_index)
        if text_result.get("status") == "success":
            for block in text_result.get("blocks", []):
                elements.append({
                    "id": gen_elem_id(),
                    "type": block["classification"],
                    "content": block["text"],
                    "page": page_index + 1,
                    "position": {
                        "x": block["bbox"]["x"],
                        "y": block["bbox"]["y"],
                        "w": block["bbox"]["w"],
                        "h": block["bbox"]["h"],
                    },
                    "confidence": 1.0,
                    "metadata": {
                        "font": block["font"],
                        "font_size": block["font_size"],
                        "is_bold": block["is_bold"],
                        "is_italic": block["is_italic"],
                        "reading_order": block["reading_order"],
                    },
                })
    elif is_scanned:
        is_large_format = width > LARGE_FORMAT_DIMENSION or height > LARGE_FORMAT_DIMENSION
        page_dpi = LARGE_FORMAT_DPI if is_large_format else STANDARD_DPI
        ocr_result = extractors.ocr_page(pdf_path, page_index, dpi=page_dpi)
        if ocr_result.get("status") == "success":
            for block in ocr_result.get("blocks", []):
                elements.append({
                    "id": gen_elem_id(),
                    "type": "body_text",
                    "content": block["text"],
                    "page": page_index + 1,
                    "position": {
                        "x": block["bbox"]["x"],
                        "y": block["bbox"]["y"],
                        "w": block["bbox"]["w"],
                        "h": block["bbox"]["h"],
                    },
                    "confidence": block["confidence"] / 100.0,
                    "metadata": {
                        "ocr_engine": "tesseract",
                        "word_count": block["word_count"],
                    },
                })

    # 2. Tables.
    if has_lines or not has_text_layer:
        table_result = extractors.extract_tables(pdf_path, page_index)
        if table_result.get("status") == "success":
            for table in table_result.get("tables", []):
                elements.append({
                    "id": gen_elem_id(),
                    "type": "table",
                    "content": table["data"],
                    "page": page_index + 1,
                    "position": {
                        "x": table["bbox"]["x"],
                        "y": table["bbox"]["y"],
                        "w": table["bbox"]["w"],
                        "h": table["bbox"]["h"],
                    },
                    "confidence": 1.0,
                    "metadata": {
                        "rows": table["rows"],
                        "columns": table["columns"],
                        "has_header_row": table["has_header_row"],
                    },
                })

    # 3. Images.
    if image_count > 0:
        image_result = extractors.extract_images(
            pdf_path, page_index, output_dir=image_output_dir, name_prefix=name_prefix
        )
        if image_result.get("status") == "success":
            for img in image_result.get("images", []):
                elements.append({
                    "id": gen_elem_id(),
                    "type": "image",
                    "content": None,
                    "page": page_index + 1,
                    "position": {
                        "x": img["bbox"]["x"],
                        "y": img["bbox"]["y"],
                        "w": img["bbox"]["w"],
                        "h": img["bbox"]["h"],
                    },
                    "confidence": 1.0,
                    "metadata": {
                        "file_path": img["file_path"],
                        "format": img["format"],
                        "width_px": img["width_px"],
                        "height_px": img["height_px"],
                    },
                })

    classification = "mixed"
    if is_scanned:
        classification = "scanned"
    elif has_text_layer and len(elements) == 0:
        classification = "text_only"

    return {
        "page_number": page_index + 1,
        "width": float(width),
        "height": float(height),
        "classification": classification,
        "elements": elements,
    }


def combine_pages(source: str, total_pages: int, pages: list, errors: list) -> dict:
    """Wrap parsed pages in the unified document envelope."""
    return {
        "document": {
            "source": str(source),
            "total_pages": total_pages,
            "processing_id": f"proc_py_{int(time.time())}",
            "pipeline_type": PIPELINE_TYPE,
            "errors": [e for e in errors] if errors else None,
            "pages": pages,
        }
    }


def parse_document(pdf_path: str, total_pages: int, image_output_dir: str = None) -> dict:
    """Parse all pages sequentially and combine into the document envelope.

    The reader engine (Task 10) provides a threaded variant; this sequential
    version is used directly by tests and as a fallback.
    """
    pages = []
    errors = []
    for idx in range(total_pages):
        try:
            pages.append(parse_page(pdf_path, idx, image_output_dir))
        except Exception as e:  # noqa: BLE001 - per-page isolation
            errors.append(str(e))
    return combine_pages(pdf_path, total_pages, pages, errors)


def flatten_document_elements(document: dict) -> list:
    """Flatten a parsed document into one record per element.

    Scalar columns (id, type, page, x, y, w, h, confidence) plus JSON-encoded
    ``content`` and ``metadata`` so the result is safe to load into a columnar
    frame regardless of mixed content types (text vs. 2D table arrays).
    """
    records = []
    pages = document.get("document", {}).get("pages", [])
    for page in pages:
        for elem in page.get("elements", []):
            pos = elem.get("position", {})
            content = elem.get("content")
            content_str = content if isinstance(content, str) else (
                json.dumps(content) if content is not None else None
            )
            records.append({
                "id": elem.get("id"),
                "type": elem.get("type"),
                "page": elem.get("page"),
                "x": float(pos.get("x", 0.0)),
                "y": float(pos.get("y", 0.0)),
                "w": float(pos.get("w", 0.0)),
                "h": float(pos.get("h", 0.0)),
                "confidence": float(elem.get("confidence", 0.0)),
                "content": content_str,
                "metadata": json.dumps(elem.get("metadata") or {}),
            })
    return records


class PDFComponents(FileProperties):
    """A class that combines PDF components into a single interface."""

    def __init__(self, filepath):
        """Initialize the PDFComponents object.

        Args:
            filepath (str or Path): Path to the PDF file.
        """
        super().__init__(filepath)  # Parent class handles Path conversion

    @cached_property
    def total_pages(self):
        """Return the total number of pages in the PDF."""
        pymupdf = extractors._import_pymupdf()
        doc = pymupdf.open(self.filepath)
        try:
            return doc.page_count
        finally:
            doc.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_pdfcomponents.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/core/pdf_io/pdfcomponents.py tests/core_tests/pdf_io_tests/test_pdfcomponents.py
git commit -m "feat: add PDF page assembly, document combination, and flattening"
```

---

## Task 10: Build engines — properties, helpers, base ABCs, reader engine

**Files:**
- Create: `src/datagrunt/core/pdf_io/engines.py`
- Test: `tests/core_tests/pdf_io_tests/test_engines.py`

- [ ] **Step 1: Write the failing test**

Create `tests/core_tests/pdf_io_tests/test_engines.py`:

```python
"""Tests for PDF engines."""

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.core.pdf_io.engines import (
    PDFEngineProperties,
    PDFReaderPyMuPDFEngine,
    set_export_filename,
)


class TestSetExportFilename:
    """Test suite for the output path resolver."""

    def test_returns_default_when_none(self):
        assert set_export_filename("output.json", None) == "output.json"

    def test_returns_override_when_given(self):
        assert set_export_filename("output.json", "custom.json") == "custom.json"


class TestPDFEngineProperties:
    """Test suite for the engine properties dataclass."""

    def test_defaults(self):
        props = PDFEngineProperties(filepath="x.pdf")
        assert props.json_export_filename == "output.json"
        assert props.json_newline_export_filename == "output.jsonl"
        assert props.images_export_dir == "output_images"
        assert "pymupdf" in props.valid_engines


class TestPDFReaderEngine:
    """Test suite for the PyMuPDF reader engine."""

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PDFReaderPyMuPDFEngine("nope.pdf")

    def test_to_dicts(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        doc = engine.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(doc["document"]["pages"]) == 1

    def test_get_sample(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        page = engine.get_sample()
        assert page["page_number"] == 1

    def test_to_dataframe(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        df = engine.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 2
        assert {"id", "type", "page", "x", "content", "metadata"} <= set(df.columns)

    def test_to_arrow_table(self, sample_pdf):
        engine = PDFReaderPyMuPDFEngine(sample_pdf)
        table = engine.to_arrow_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows >= 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_engines.py -v`
Expected: FAIL with `ModuleNotFoundError: ... engines`

- [ ] **Step 3: Create `engines.py` with properties, resolver, base ABCs, and reader engine**

Create `src/datagrunt/core/pdf_io/engines.py`:

```python
"""Module to create engines for PDF processing."""

# standard library
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core.pdf_io import extractors, pdfcomponents


def set_export_filename(default_filename, export_filename=None):
    """Return the export filename if provided, otherwise the default.

    Mirrors the CSV writer's path-resolution semantics without coupling PDF to
    DuckDB.
    """
    return export_filename if export_filename else default_filename


@dataclass
class PDFEngineProperties:
    """Base properties for PDF operations."""

    filepath: Path
    default_workers: int = 4
    json_export_filename: str = "output.json"
    json_newline_export_filename: str = "output.jsonl"
    images_export_dir: str = "output_images"
    valid_engines: tuple = ("pymupdf",)
    value_error_message: str = (
        "Engine '{engine}' is not 'pymupdf'. Pass 'pymupdf' as a valid engine param."
    )


class PDFBaseReaderEngine(ABC):
    """Abstract base class defining the interface for PDF reader engines."""

    def __init__(self, filepath, workers: int = 4):
        """Initialize the PDF reader engine.

        Args:
            filepath (str or Path): Path to the PDF file.
            workers (int): Number of concurrent per-page workers.
        """
        self.filepath = Path(filepath)
        self.workers = workers
        if not self.filepath.exists():
            raise FileNotFoundError

    @abstractmethod
    def get_sample(self) -> dict:
        """Return the parsed first page."""
        pass

    @abstractmethod
    def to_dicts(self, image_output_dir: Optional[str] = None) -> dict:
        """Return the unified parsed document dict."""
        pass

    @abstractmethod
    def to_dataframe(self) -> pl.DataFrame:
        """Return parsed elements as a Polars DataFrame."""
        pass

    @abstractmethod
    def to_arrow_table(self) -> pa.Table:
        """Return parsed elements as a PyArrow table."""
        pass


class PDFReaderPyMuPDFEngine(PDFBaseReaderEngine):
    """Read and parse PDF files using PyMuPDF / pdfplumber / Tesseract."""

    def _total_pages(self) -> int:
        pymupdf = extractors._import_pymupdf()
        doc = pymupdf.open(self.filepath)
        try:
            return doc.page_count
        finally:
            doc.close()

    def to_dicts(self, image_output_dir: Optional[str] = None) -> dict:
        """Parse all pages concurrently into the unified document dict."""
        total_pages = self._total_pages()
        page_results = {}
        errors = []

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(
                    pdfcomponents.parse_page, str(self.filepath), idx, image_output_dir
                ): idx
                for idx in range(total_pages)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    page = future.result()
                    page_results[page["page_number"]] = page
                except Exception as e:  # noqa: BLE001 - per-page isolation
                    errors.append(f"Page {idx + 1}: {e}")

        ordered = [page_results[p] for p in sorted(page_results.keys())]
        return pdfcomponents.combine_pages(self.filepath, total_pages, ordered, errors)

    def get_sample(self) -> dict:
        """Parse and return the first page only."""
        return pdfcomponents.parse_page(str(self.filepath), 0)

    def to_dataframe(self) -> pl.DataFrame:
        """Flatten parsed elements into a Polars DataFrame (one row/element)."""
        records = pdfcomponents.flatten_document_elements(self.to_dicts())
        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    def to_arrow_table(self) -> pa.Table:
        """Flatten parsed elements into a PyArrow table (one row/element)."""
        records = pdfcomponents.flatten_document_elements(self.to_dicts())
        if not records:
            return pa.Table.from_pydict({})
        return pa.Table.from_pylist(records)
```

> Note: `PDFEngineProperties` needs no `dataclasses.field` — all defaults are immutable scalars/tuples. The dataclass declares `filepath` first (required) followed by fields with defaults, which is valid ordering.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_engines.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/core/pdf_io/engines.py tests/core_tests/pdf_io_tests/test_engines.py
git commit -m "feat: add PDF engine properties, path resolver, and PyMuPDF reader engine"
```

---

## Task 11: Add the PyMuPDF writer engine

**Files:**
- Modify: `src/datagrunt/core/pdf_io/engines.py`
- Test: `tests/core_tests/pdf_io_tests/test_engines.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/core_tests/pdf_io_tests/test_engines.py`:

```python
import json
import os

from datagrunt.core.pdf_io.engines import PDFWriterPyMuPDFEngine


class TestPDFWriterEngine:
    """Test suite for the PyMuPDF writer engine."""

    def test_write_json_default_name(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json()
        assert os.path.basename(path) == "output.json"
        with open(path) as f:
            data = json.load(f)
        assert data["document"]["total_pages"] == 1

    def test_write_json_custom_name(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json(export_filename="custom.json")
        assert os.path.basename(path) == "custom.json"
        assert os.path.isfile(path)

    def test_write_json_newline_delimited(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        path = engine.write_json_newline_delimited()
        assert os.path.basename(path) == "output.jsonl"
        with open(path) as f:
            lines = [line for line in f if line.strip()]
        assert len(lines) >= 1
        json.loads(lines[0])  # each line is valid JSON

    def test_extract_images(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        engine = PDFWriterPyMuPDFEngine(sample_pdf)
        paths = engine.extract_images(output_dir=str(out))
        assert len(paths) >= 1
        assert all(os.path.isfile(p) for p in paths)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_engines.py::TestPDFWriterEngine -v`
Expected: FAIL with `ImportError: cannot import name 'PDFWriterPyMuPDFEngine'`

- [ ] **Step 3: Add the writer base ABC and PyMuPDF writer engine**

Append to `src/datagrunt/core/pdf_io/engines.py`:

```python
# standard library (writer)
import json


class PDFBaseWriterEngine(ABC):
    """Abstract base class defining the interface for PDF writer engines."""

    def __init__(self, filepath, workers: int = 4):
        """Initialize the PDF writer engine.

        Args:
            filepath (str or Path): Path to the PDF file.
            workers (int): Number of concurrent per-page workers.
        """
        self.filepath = Path(filepath)
        self.workers = workers
        self.properties = PDFEngineProperties(filepath=self.filepath)
        if not self.filepath.exists():
            raise FileNotFoundError

    @abstractmethod
    def write_json(self, export_filename=None, image_output_dir=None):
        """Write the unified document JSON to disk."""
        pass

    @abstractmethod
    def write_json_newline_delimited(self, export_filename=None, image_output_dir=None):
        """Write one element per line as JSON Lines."""
        pass

    @abstractmethod
    def extract_images(self, output_dir=None):
        """Write embedded images to disk; return their paths."""
        pass


class PDFWriterPyMuPDFEngine(PDFBaseWriterEngine):
    """Write parsed PDF output (JSON + image files) using PyMuPDF."""

    def _reader(self):
        return PDFReaderPyMuPDFEngine(self.filepath, workers=self.workers)

    def write_json(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write the unified document JSON.

        Args:
            export_filename (optional, str): Output path; defaults to output.json.
            image_output_dir (optional, str): If provided, embedded images are
                written here and referenced in the JSON; otherwise image
                ``file_path`` values are null.
        """
        filename = set_export_filename(
            self.properties.json_export_filename, export_filename
        )
        document = self._reader().to_dicts(image_output_dir=image_output_dir)
        with open(filename, "w") as f:
            json.dump(document, f, indent=2)
        return filename

    def write_json_newline_delimited(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write one flattened element per line (JSONL)."""
        filename = set_export_filename(
            self.properties.json_newline_export_filename, export_filename
        )
        document = self._reader().to_dicts(image_output_dir=image_output_dir)
        records = pdfcomponents.flatten_document_elements(document)
        with open(filename, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return filename

    def extract_images(self, output_dir=None):
        """Parse the PDF, write embedded images to disk, return their paths."""
        directory = output_dir if output_dir else self.properties.images_export_dir
        document = self._reader().to_dicts(image_output_dir=directory)
        paths = []
        for page in document.get("document", {}).get("pages", []):
            for elem in page.get("elements", []):
                if elem.get("type") == "image":
                    fp = (elem.get("metadata") or {}).get("file_path")
                    if fp:
                        paths.append(fp)
        return paths
```

- [ ] **Step 4: Move the writer `import json` to the top of the file**

The appended block adds `import json` mid-file. Delete that mid-file `import json` line and add `import json` to the standard-library import group at the top of `engines.py` (right above `from abc import ABC, abstractmethod`). Final top imports:

```python
# standard library
import json
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_engines.py -v`
Expected: PASS (all engine tests)

- [ ] **Step 6: Commit**

```bash
git add src/datagrunt/core/pdf_io/engines.py tests/core_tests/pdf_io_tests/test_engines.py
git commit -m "feat: add PyMuPDF PDF writer engine (JSON + image files)"
```

---

## Task 12: Add the `PDFEngineFactory`

**Files:**
- Create: `src/datagrunt/core/pdf_io/factories.py`
- Test: `tests/core_tests/pdf_io_tests/test_factories.py`

- [ ] **Step 1: Write the failing test**

Create `tests/core_tests/pdf_io_tests/test_factories.py`:

```python
"""Tests for the PDF engine factory."""

import pytest

from datagrunt.core.pdf_io.engines import (
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
)
from datagrunt.core.pdf_io.factories import PDFEngineFactory


class TestPDFEngineFactory:
    """Test suite for PDFEngineFactory."""

    def test_create_reader(self, sample_pdf):
        factory = PDFEngineFactory(sample_pdf, "pymupdf")
        assert isinstance(factory.create_reader(), PDFReaderPyMuPDFEngine)

    def test_create_writer(self, sample_pdf):
        factory = PDFEngineFactory(sample_pdf, "pymupdf")
        assert isinstance(factory.create_writer(), PDFWriterPyMuPDFEngine)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            PDFEngineFactory("nope.pdf", "pymupdf")

    def test_invalid_engine_raises(self, sample_pdf):
        with pytest.raises(ValueError):
            PDFEngineFactory(sample_pdf, "ghostscript")

    def test_engine_normalized(self, sample_pdf):
        factory = PDFEngineFactory(sample_pdf, "Py Mu PDF")
        assert factory.engine == "pymupdf"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_factories.py -v`
Expected: FAIL with `ModuleNotFoundError: ... factories`

- [ ] **Step 3: Create `factories.py`**

Create `src/datagrunt/core/pdf_io/factories.py`:

```python
"""Factory module for creating PDF engine instances."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core.pdf_io.engines import (
    PDFEngineProperties,
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
)


class PDFEngineFactory:
    """Factory class for creating PDF reader and writer engine instances."""

    READER_ENGINES = {
        "pymupdf": PDFReaderPyMuPDFEngine,
    }

    WRITER_ENGINES = {
        "pymupdf": PDFWriterPyMuPDFEngine,
    }

    def __init__(self, filepath, engine, workers: int = 4):
        """Initialize the PDF Engine Factory class.

        Args:
            filepath (str or Path): Path to the PDF file.
            engine (str): Engine type to create.
            workers (int): Number of concurrent per-page workers.
        """
        self.filepath = Path(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers
        if not self.filepath.exists():
            raise FileNotFoundError
        if self.engine not in PDFEngineProperties.valid_engines:
            raise ValueError(
                PDFEngineProperties.value_error_message.format(engine=self.engine)
            )

    def create_reader(self):
        """Create a PDF reader engine instance."""
        engine_class = self.READER_ENGINES.get(self.engine)
        if engine_class:
            return engine_class(self.filepath, workers=self.workers)
        raise ValueError(f"Unsupported reader engine: {self.engine}")

    def create_writer(self):
        """Create a PDF writer engine instance."""
        engine_class = self.WRITER_ENGINES.get(self.engine)
        if engine_class:
            return engine_class(self.filepath, workers=self.workers)
        raise ValueError(f"Unsupported writer engine: {self.engine}")
```

> Note: `PDFEngineProperties.valid_engines` is a class-level default on the dataclass (`("pymupdf",)`), so it is accessible without instantiation, matching `CSVEngineProperties.valid_engines` usage in `CSVEngineFactory`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core_tests/pdf_io_tests/test_factories.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/core/pdf_io/factories.py tests/core_tests/pdf_io_tests/test_factories.py
git commit -m "feat: add PDFEngineFactory"
```

---

## Task 13: Wire up `core/pdf_io/__init__.py` and `core/__init__.py`

**Files:**
- Modify: `src/datagrunt/core/pdf_io/__init__.py`
- Modify: `src/datagrunt/core/__init__.py`

- [ ] **Step 1: Expand `core/pdf_io/__init__.py`**

Replace the contents of `src/datagrunt/core/pdf_io/__init__.py` with:

```python
"""Initializes the pdf_io module of the datagrunt package."""

from datagrunt.core.pdf_io.engines import (
    PDFBaseReaderEngine,
    PDFBaseWriterEngine,
    PDFEngineProperties,
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
    set_export_filename,
)
from datagrunt.core.pdf_io.factories import PDFEngineFactory
from datagrunt.core.pdf_io.pdfcomponents import (
    PDFComponents,
    combine_pages,
    flatten_document_elements,
    parse_document,
    parse_page,
)

__all__ = [
    "PDFComponents",
    "parse_page",
    "parse_document",
    "combine_pages",
    "flatten_document_elements",
    "PDFEngineProperties",
    "PDFBaseReaderEngine",
    "PDFBaseWriterEngine",
    "PDFReaderPyMuPDFEngine",
    "PDFWriterPyMuPDFEngine",
    "set_export_filename",
    "PDFEngineFactory",
]
```

> Import ordering note: `pdf_io/__init__.py` importing `engines` then `factories` then `pdfcomponents` is safe — `engines` imports `pdfcomponents` and `extractors` (no circular dependency back into `pdf_io/__init__`), and none of these import `extractors`' heavy deps at module load.

- [ ] **Step 2: Add PDF exports to `core/__init__.py`**

In `src/datagrunt/core/__init__.py`, add this import block after the existing `from datagrunt.core.file_io import FileProperties` line:

```python
from datagrunt.core.pdf_io import (
    PDFComponents,
    PDFEngineFactory,
    PDFEngineProperties,
    PDFReaderPyMuPDFEngine,
    PDFWriterPyMuPDFEngine,
    flatten_document_elements,
    parse_document,
    parse_page,
    set_export_filename,
)
```

Then add these entries to the `__all__` list (append a new `# PDF IO` group before the closing `]`):

```python
    # PDF IO
    "PDFComponents",
    "PDFEngineFactory",
    "PDFEngineProperties",
    "PDFReaderPyMuPDFEngine",
    "PDFWriterPyMuPDFEngine",
    "parse_page",
    "parse_document",
    "flatten_document_elements",
    "set_export_filename",
```

- [ ] **Step 3: Verify imports resolve and base import is clean**

Run: `uv run python -c "from datagrunt.core import PDFEngineFactory, PDFComponents; print('ok')"`
Expected: `ok`

Run: `uv run python -c "import datagrunt; print('base ok')"`
Expected: `base ok`

- [ ] **Step 4: Commit**

```bash
git add src/datagrunt/core/pdf_io/__init__.py src/datagrunt/core/__init__.py
git commit -m "feat: export pdf_io symbols from datagrunt.core"
```

---

## Task 14: Build the public `PDFReader`

**Files:**
- Create: `src/datagrunt/pdf_api/pdfreader.py`
- Test: `tests/pdf_api_tests/test_pdfreader.py`

- [ ] **Step 1: Write the failing test**

Create `tests/pdf_api_tests/test_pdfreader.py`:

```python
"""Tests for the public PDFReader API."""

import polars as pl
import pyarrow as pa
import pytest

from datagrunt.pdf_api.pdfreader import PDFReader


class TestPDFReader:
    """Test suite for PDFReader."""

    def test_init_normalizes_engine(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="Py Mu PDF")
        assert reader.engine == "pymupdf"
        assert reader.is_pdf

    def test_to_dicts(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        doc = reader.to_dicts()
        assert doc["document"]["total_pages"] == 1
        assert len(doc["document"]["pages"]) == 1

    def test_get_sample(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        page = reader.get_sample()
        assert page["page_number"] == 1

    def test_to_dataframe(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        df = reader.to_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert df.height >= 2

    def test_to_arrow_table(self, sample_pdf):
        reader = PDFReader(sample_pdf)
        assert isinstance(reader.to_arrow_table(), pa.Table)

    def test_empty_pdf_returns_empty_objects(self, empty_pdf):
        reader = PDFReader(empty_pdf)
        assert reader.to_dicts() == {}
        assert reader.to_dataframe().is_empty()

    def test_invalid_engine_raises(self, sample_pdf):
        reader = PDFReader(sample_pdf, engine="ghostscript")
        with pytest.raises(ValueError):
            reader.to_dicts()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/pdf_api_tests/test_pdfreader.py -v`
Expected: FAIL with `ModuleNotFoundError: ... pdf_api.pdfreader`

- [ ] **Step 3: Create `pdfreader.py`**

Create `src/datagrunt/pdf_api/pdfreader.py`:

```python
"""Module for reading PDF files and converting to in-memory Python objects."""

# standard library
from pathlib import Path

# third party libraries
import polars as pl
import pyarrow as pa

# local libraries
from datagrunt.core import PDFComponents, PDFEngineFactory


class PDFReader(PDFComponents):
    """Class to unify the interface for reading and parsing PDF files."""

    def __init__(self, filepath, engine="pymupdf", workers=4):
        """Initialize the PDF Reader class.

        Args:
            filepath (str or Path): Path to the PDF file to read.
            engine (str, default 'pymupdf'): Parsing engine to instantiate.
            workers (int, default 4): Number of concurrent per-page workers.
        """
        filepath = Path(filepath)
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers

    def _return_empty_file_object(self, object):
        """Return an empty object of the specified type."""
        return object

    def _create_reader(self):
        """Create a reader engine instance."""
        return PDFEngineFactory(self.filepath, self.engine, self.workers).create_reader()

    def get_sample(self):
        """Parse and return the first page of the PDF."""
        if self.is_empty:
            return self._return_empty_file_object({})
        return self._create_reader().get_sample()

    def to_dicts(self, image_output_dir=None):
        """Parse the PDF into the unified document dict.

        Args:
            image_output_dir (optional, str): If provided, embedded images are
                written here and referenced in the result; otherwise image
                ``file_path`` values are null.

        Returns:
            dict: ``{"document": {... "pages": [...]}}``.
        """
        if self.is_empty:
            return self._return_empty_file_object({})
        return self._create_reader().to_dicts(image_output_dir=image_output_dir)

    def to_dataframe(self):
        """Parse the PDF and flatten elements into a Polars DataFrame.

        Returns:
            A Polars DataFrame with one row per extracted element.
        """
        if self.is_empty:
            return self._return_empty_file_object(pl.DataFrame())
        return self._create_reader().to_dataframe()

    def to_arrow_table(self):
        """Parse the PDF and flatten elements into a PyArrow table.

        Returns:
            A PyArrow table with one row per extracted element.
        """
        if self.is_empty:
            return self._return_empty_file_object(pa.Table.from_pydict({}))
        return self._create_reader().to_arrow_table()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/pdf_api_tests/test_pdfreader.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/pdf_api/pdfreader.py tests/pdf_api_tests/test_pdfreader.py
git commit -m "feat: add public PDFReader API"
```

---

## Task 15: Build the public `PDFWriter`

**Files:**
- Create: `src/datagrunt/pdf_api/pdfwriter.py`
- Test: `tests/pdf_api_tests/test_pdfwriter.py`

- [ ] **Step 1: Write the failing test**

Create `tests/pdf_api_tests/test_pdfwriter.py`:

```python
"""Tests for the public PDFWriter API."""

import json
import os

from datagrunt.pdf_api.pdfwriter import PDFWriter


class TestPDFWriter:
    """Test suite for PDFWriter."""

    def test_write_json_default(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_json()
        assert os.path.basename(path) == "output.json"
        with open(path) as f:
            data = json.load(f)
        assert data["document"]["total_pages"] == 1

    def test_write_json_custom_name_and_images(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        img_dir = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        path = writer.write_json(
            export_filename="report.json", image_output_dir=str(img_dir)
        )
        assert os.path.basename(path) == "report.json"
        with open(path) as f:
            data = json.load(f)
        img_paths = [
            e["metadata"]["file_path"]
            for page in data["document"]["pages"]
            for e in page["elements"]
            if e["type"] == "image"
        ]
        assert img_paths and all(os.path.isfile(p) for p in img_paths)

    def test_write_json_newline_delimited(self, sample_pdf, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        writer = PDFWriter(sample_pdf)
        path = writer.write_json_newline_delimited()
        assert os.path.basename(path) == "output.jsonl"
        with open(path) as f:
            lines = [line for line in f if line.strip()]
        assert len(lines) >= 1

    def test_extract_images(self, sample_pdf, tmp_path):
        out = tmp_path / "imgs"
        writer = PDFWriter(sample_pdf)
        paths = writer.extract_images(output_dir=str(out))
        assert paths and all(os.path.isfile(p) for p in paths)

    def test_engine_normalized(self, sample_pdf):
        writer = PDFWriter(sample_pdf, engine="Py Mu PDF")
        assert writer.engine == "pymupdf"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/pdf_api_tests/test_pdfwriter.py -v`
Expected: FAIL with `ModuleNotFoundError: ... pdf_api.pdfwriter`

- [ ] **Step 3: Create `pdfwriter.py`**

Create `src/datagrunt/pdf_api/pdfwriter.py`:

```python
"""Module for writing parsed PDF output (JSON + image files)."""

# standard library
from pathlib import Path

# local libraries
from datagrunt.core import PDFComponents, PDFEngineFactory


class PDFWriter(PDFComponents):
    """Class to unify the interface for writing parsed PDF output."""

    def __init__(self, filepath, engine="pymupdf", workers=4):
        """Initialize the PDF Writer class.

        Args:
            filepath (str or Path): Path to the PDF file to parse.
            engine (str, default 'pymupdf'): Parsing engine to instantiate.
            workers (int, default 4): Number of concurrent per-page workers.
        """
        filepath = Path(filepath)
        super().__init__(filepath)
        self.engine = engine.lower().replace(" ", "")
        self.workers = workers

    def _create_writer(self):
        """Create a writer engine instance."""
        return PDFEngineFactory(self.filepath, self.engine, self.workers).create_writer()

    def write_json(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write the unified document JSON to disk.

        Args:
            export_filename (optional, str): Output path; defaults to output.json.
            image_output_dir (optional, str): If provided, embedded images are
                written there and referenced in the JSON.

        Returns:
            str: The path of the written JSON file.
        """
        return self._create_writer().write_json(export_filename, image_output_dir)

    def write_json_newline_delimited(self, export_filename=None, image_output_dir=None):
        """Parse the PDF and write one flattened element per line (JSONL).

        Args:
            export_filename (optional, str): Output path; defaults to output.jsonl.
            image_output_dir (optional, str): If provided, embedded images are
                written there.

        Returns:
            str: The path of the written JSONL file.
        """
        return self._create_writer().write_json_newline_delimited(
            export_filename, image_output_dir
        )

    def extract_images(self, output_dir=None):
        """Parse the PDF and write embedded image files to disk.

        Args:
            output_dir (optional, str): Output directory; defaults to
                output_images.

        Returns:
            list: Paths of the written image files.
        """
        return self._create_writer().extract_images(output_dir)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/pdf_api_tests/test_pdfwriter.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datagrunt/pdf_api/pdfwriter.py tests/pdf_api_tests/test_pdfwriter.py
git commit -m "feat: add public PDFWriter API"
```

---

## Task 16: Wire up `pdf_api/__init__.py` and top-level package exports

**Files:**
- Create: `src/datagrunt/pdf_api/__init__.py`
- Modify: `src/datagrunt/__init__.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/pdf_api_tests/test_pdfreader.py`:

```python
class TestTopLevelExports:
    """Verify PDF classes are importable from the package root."""

    def test_top_level_imports(self):
        from datagrunt import PDFReader, PDFWriter
        assert PDFReader.__name__ == "PDFReader"
        assert PDFWriter.__name__ == "PDFWriter"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/pdf_api_tests/test_pdfreader.py::TestTopLevelExports -v`
Expected: FAIL with `ImportError: cannot import name 'PDFReader' from 'datagrunt'`

- [ ] **Step 3: Create `pdf_api/__init__.py`**

Create `src/datagrunt/pdf_api/__init__.py`:

```python
# Import key classes that should be available at the package level
from datagrunt.pdf_api.pdfreader import PDFReader
from datagrunt.pdf_api.pdfwriter import PDFWriter

__all__ = ["PDFReader", "PDFWriter"]
```

- [ ] **Step 4: Update the top-level `datagrunt/__init__.py`**

In `src/datagrunt/__init__.py`, update the import line and `__all__`. Change:

```python
from datagrunt.csv_api import CSVReader, CSVSchemaReportAIGenerated, CSVWriter

__all__ = ["CSVReader", "CSVWriter", "CSVSchemaReportAIGenerated"]
```

to:

```python
from datagrunt.csv_api import CSVReader, CSVSchemaReportAIGenerated, CSVWriter
from datagrunt.pdf_api import PDFReader, PDFWriter

__all__ = [
    "CSVReader",
    "CSVWriter",
    "CSVSchemaReportAIGenerated",
    "PDFReader",
    "PDFWriter",
]
```

Also update the module docstring's opening sentence to mention PDF:

Change `A Python library designed to simplify the way you work with CSV files.` to `A Python library designed to simplify the way you work with CSV and PDF files.`

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/pdf_api_tests/test_pdfreader.py::TestTopLevelExports -v`
Expected: PASS

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest`
Expected: PASS (CSV + PDF; OCR tests SKIP if tesseract absent). Coverage report prints.

- [ ] **Step 7: Commit**

```bash
git add src/datagrunt/pdf_api/__init__.py src/datagrunt/__init__.py tests/pdf_api_tests/test_pdfreader.py
git commit -m "feat: export PDFReader and PDFWriter at package root"
```

---

## Task 17: Lint and document

**Files:**
- Modify: `README.md`
- (Lint-only) all new files

- [ ] **Step 1: Run ruff and fix any issues**

Run: `uv run ruff check src/datagrunt/pdf_api src/datagrunt/core/pdf_io tests/pdf_api_tests tests/core_tests/pdf_io_tests`
Expected: no errors. If imports are flagged unused or import order is off, run `uv run ruff check --fix` and re-run.

Run: `uv run ruff format src/datagrunt/pdf_api src/datagrunt/core/pdf_io`
Expected: files formatted to the project's 120-char line length.

- [ ] **Step 2: Add a PDF section to `README.md`**

Add a section to `README.md` (after the CSV usage section) documenting the new capability. Include the install command, the tesseract requirement, and a usage snippet:

````markdown
## PDF parsing

PDF support is an optional extra:

```bash
pip install datagrunt[pdf]
```

OCR of scanned pages additionally requires the **Tesseract** system binary
(e.g. `brew install tesseract` on macOS, `apt-get install tesseract-ocr` on
Debian/Ubuntu). Native-text PDFs, tables, and embedded images work without it.

```python
from datagrunt import PDFReader, PDFWriter

# Parse a PDF into the unified document structure.
reader = PDFReader("report.pdf")
document = reader.to_dicts()           # {"document": {"pages": [...]}}
df = reader.to_dataframe()             # one row per extracted element

# Write JSON and extract embedded images to disk.
writer = PDFWriter("report.pdf")
writer.write_json("report.json", image_output_dir="report_images")
writer.extract_images(output_dir="report_images")
```
````

- [ ] **Step 3: Run the full suite once more**

Run: `uv run pytest`
Expected: PASS (OCR SKIP allowed).

- [ ] **Step 4: Commit**

```bash
git add README.md src/datagrunt/pdf_api src/datagrunt/core/pdf_io
git commit -m "docs: document PDF parsing feature; apply ruff formatting"
```

---

## Done

All tasks complete. Final verification:

- [ ] `uv run pytest` passes (PDF + CSV; OCR skipped only if tesseract absent)
- [ ] `uv run python -c "import datagrunt"` works on a base install (no `[pdf]`)
- [ ] `uv run ruff check src tests` is clean
- [ ] `PDFReader`/`PDFWriter` importable from `datagrunt` root
- [ ] No CSV code path was modified (only `FileProperties` gained `is_pdf`)
