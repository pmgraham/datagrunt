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
                elements.append(
                    {
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
                    }
                )
    elif is_scanned:
        is_large_format = width > LARGE_FORMAT_DIMENSION or height > LARGE_FORMAT_DIMENSION
        page_dpi = LARGE_FORMAT_DPI if is_large_format else STANDARD_DPI
        ocr_result = extractors.ocr_page(pdf_path, page_index, dpi=page_dpi)
        if ocr_result.get("status") == "success":
            for block in ocr_result.get("blocks", []):
                elements.append(
                    {
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
                    }
                )

    # 2. Tables.
    if has_lines or not has_text_layer:
        table_result = extractors.extract_tables(pdf_path, page_index)
        if table_result.get("status") == "success":
            for table in table_result.get("tables", []):
                elements.append(
                    {
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
                    }
                )

    # 3. Images.
    if image_count > 0:
        image_result = extractors.extract_images(
            pdf_path, page_index, output_dir=image_output_dir, name_prefix=name_prefix
        )
        if image_result.get("status") == "success":
            for img in image_result.get("images", []):
                elements.append(
                    {
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
                    }
                )

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
