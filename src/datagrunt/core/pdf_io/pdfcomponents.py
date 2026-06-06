"""PDF component assembly: page parsing, document combination, flattening."""

# standard library
import hashlib
import json
import os
import time
from functools import cached_property
from pathlib import Path

# local libraries
from datagrunt.core.file_io import FileProperties
from datagrunt.core.pdf_io.extraction import PdfPlumberTableExtractor
from datagrunt.core.pdf_io.extraction.pdfium_document import PdfiumDocument
from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend

PIPELINE_TYPE = "pure_python_local_v1"

# Dynamic DPI scaling thresholds for scanned (OCR) pages.
LARGE_FORMAT_DIMENSION = 1500
LARGE_FORMAT_DPI = 75
STANDARD_DPI = 150


def parse_page(pdf_path, page_index, image_output_dir=None, backend=None, table_extractor=None):
    """Parse a single PDF page into the unified element schema.

    Args:
        pdf_path: Path to the PDF file.
        page_index: Zero-indexed page number.
        image_output_dir: If set, images are written there and referenced.
        backend (ExtractionBackend, optional): Defaults to PyMuPDFBackend.
        table_extractor (PdfPlumberTableExtractor, optional): Shared table source.

    Returns:
        A page dict: page_number, width, height, classification, elements.
    """
    backend = backend or PyMuPDFBackend(pdf_path)
    table_extractor = table_extractor or PdfPlumberTableExtractor(pdf_path)

    analysis = backend.analyze_page(page_index)
    elements = []
    counter = {"n": 1}

    def gen_elem_id():
        elem_id = f"elem_{page_index + 1:02d}_{counter['n']:03d}"
        counter["n"] += 1
        return elem_id

    if analysis.has_text_layer:
        for block in backend.extract_text_blocks(page_index):
            elements.append(_text_element(block, gen_elem_id(), page_index))
    elif analysis.is_scanned:
        is_large = analysis.width > LARGE_FORMAT_DIMENSION or analysis.height > LARGE_FORMAT_DIMENSION
        dpi = LARGE_FORMAT_DPI if is_large else STANDARD_DPI
        for block in backend.ocr_page(page_index, dpi=dpi):
            elements.append(_ocr_element(block, gen_elem_id(), page_index))

    if analysis.has_line_drawings or not analysis.has_text_layer:
        for table in table_extractor.extract(page_index):
            elements.append(_table_element(table, gen_elem_id(), page_index))

    if analysis.image_count > 0:
        for img in backend.extract_images(page_index, output_dir=image_output_dir, name_prefix=Path(pdf_path).stem):
            elements.append(_image_element(img, gen_elem_id(), page_index))

    classification = "mixed"
    if analysis.is_scanned:
        classification = "scanned"
    elif analysis.has_text_layer and len(elements) == 0:
        classification = "text_only"

    return {
        "page_number": page_index + 1,
        "width": float(analysis.width),
        "height": float(analysis.height),
        "classification": classification,
        "elements": elements,
    }


def _text_element(block, elem_id, page_index):
    """Serialize a TextBlock to the unified text element dict."""
    return {
        "id": elem_id, "type": block.classification, "content": block.text, "page": page_index + 1,
        "position": block.bbox.to_dict(), "confidence": 1.0,
        "metadata": {"font": block.font, "font_size": block.font_size, "is_bold": block.is_bold,
                     "is_italic": block.is_italic, "reading_order": block.reading_order},
    }


def _ocr_element(block, elem_id, page_index):
    """Serialize an OcrBlock to the unified text element dict."""
    return {
        "id": elem_id, "type": "body_text", "content": block.text, "page": page_index + 1,
        "position": block.bbox.to_dict(), "confidence": block.confidence / 100.0,
        "metadata": {"ocr_engine": "tesseract", "word_count": block.word_count},
    }


def _table_element(table, elem_id, page_index):
    """Serialize a TableBlock to the unified table element dict."""
    return {
        "id": elem_id, "type": "table", "content": table.data, "page": page_index + 1,
        "position": table.bbox.to_dict(), "confidence": 1.0,
        "metadata": {"rows": table.rows, "columns": table.columns, "has_header_row": table.has_header_row},
    }


def _image_element(img, elem_id, page_index):
    """Serialize an ImageBlock to the unified image element dict."""
    return {
        "id": elem_id, "type": "image", "content": None, "page": page_index + 1,
        "position": img.bbox.to_dict(), "confidence": 1.0,
        "metadata": {"file_path": img.file_path, "format": img.fmt,
                     "width_px": img.width_px, "height_px": img.height_px},
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


def parse_document(pdf_path, total_pages, image_output_dir=None, backend=None, table_extractor=None):
    """Parse all pages sequentially and combine into the document envelope."""
    pages, errors = [], []
    for idx in range(total_pages):
        try:
            pages.append(parse_page(pdf_path, idx, image_output_dir, backend=backend, table_extractor=table_extractor))
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
            content_str = (
                content if isinstance(content, str) else (json.dumps(content) if content is not None else None)
            )
            records.append(
                {
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
                }
            )
    return records


def dedupe_document_images(document: dict) -> int:
    """Remove byte-duplicate extracted image files, repointing references.

    Walks the document's image elements, hashes each on-disk file referenced by
    ``metadata.file_path``, and for any content already seen, repoints the
    element at the first file and deletes the redundant copy from disk.
    Elements with no ``file_path`` (metadata-only reads) or whose file is
    missing are skipped.

    Args:
        document: A parsed document dict (mutated in place).

    Returns:
        The number of duplicate image files removed from disk.
    """
    seen = {}  # md5 digest -> first file_path that produced it
    removed = 0
    pages = document.get("document", {}).get("pages", [])
    for page in pages:
        for elem in page.get("elements", []):
            if elem.get("type") != "image":
                continue
            meta = elem.get("metadata") or {}
            path = meta.get("file_path")
            if not path or not os.path.isfile(path):
                continue
            with open(path, "rb") as f:
                digest = hashlib.md5(f.read()).hexdigest()
            first = seen.get(digest)
            if first is None:
                seen[digest] = path
                continue
            if first == path:
                # Same file already referenced; repoint is a no-op, never delete.
                continue
            meta["file_path"] = first
            try:
                os.remove(path)
            except OSError:
                pass
            else:
                removed += 1
    return removed


def drop_layout_tables(document: dict, min_rows: int = 2, min_cols: int = 2) -> int:
    """Drop table elements that look like layout boxes rather than real tables.

    Line-based table detection fires on decorative boxes and single rule lines
    in graphically dense PDFs, producing 1xN or Nx1 "tables". This optional
    post-filter removes any ``table`` element whose ``metadata.rows`` is below
    ``min_rows`` or ``metadata.columns`` is below ``min_cols``. Non-table
    elements are never touched.

    Args:
        document: A parsed document dict (mutated in place).
        min_rows: Minimum rows for a table to be kept (default 2).
        min_cols: Minimum columns for a table to be kept (default 2).

    Returns:
        The number of table elements dropped.
    """
    removed = 0
    pages = document.get("document", {}).get("pages", [])
    for page in pages:
        elements = page.get("elements", [])
        kept = []
        for elem in elements:
            if elem.get("type") == "table":
                meta = elem.get("metadata") or {}
                if meta.get("rows", 0) < min_rows or meta.get("columns", 0) < min_cols:
                    removed += 1
                    continue
            kept.append(elem)
        page["elements"] = kept
    return removed


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
        with PdfiumDocument(self.filepath) as d:
            return len(d)
