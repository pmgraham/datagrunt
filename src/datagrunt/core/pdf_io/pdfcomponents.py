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


class DocumentAssembler:
    """Assemble a unified-schema document from an extraction backend + tables.

    Holds the extraction backend (defaults to ``PyMuPDFBackend``) and the shared
    ``PdfPlumberTableExtractor``, and turns per-page extraction results into the
    unified element dicts.
    """

    def __init__(self, filepath, backend=None, table_extractor=None):
        """Initialize the assembler.

        Args:
            filepath (str or Path): Path to the PDF file.
            backend (ExtractionBackend, optional): Defaults to PyMuPDFBackend.
            table_extractor (PdfPlumberTableExtractor, optional): Shared table source.
        """
        self.filepath = Path(filepath)
        self.backend = backend or PyMuPDFBackend(filepath)
        self.table_extractor = table_extractor or PdfPlumberTableExtractor(filepath)

    def parse_page(self, page_index, image_output_dir=None) -> dict:
        """Parse a single page into the unified element schema."""
        analysis = self.backend.analyze_page(page_index)
        elements = []
        counter = {"n": 1}

        def gen_elem_id():
            elem_id = f"elem_{page_index + 1:02d}_{counter['n']:03d}"
            counter["n"] += 1
            return elem_id

        if analysis.has_text_layer:
            for block in self.backend.extract_text_blocks(page_index):
                elements.append(self._text_element(block, gen_elem_id(), page_index))
        elif analysis.is_scanned:
            is_large = analysis.width > LARGE_FORMAT_DIMENSION or analysis.height > LARGE_FORMAT_DIMENSION
            dpi = LARGE_FORMAT_DPI if is_large else STANDARD_DPI
            for block in self.backend.ocr_page(page_index, dpi=dpi):
                elements.append(self._ocr_element(block, gen_elem_id(), page_index))

        if analysis.has_line_drawings or not analysis.has_text_layer:
            for table in self.table_extractor.extract(page_index):
                elements.append(self._table_element(table, gen_elem_id(), page_index))

        if analysis.image_count > 0:
            for img in self.backend.extract_images(
                page_index, output_dir=image_output_dir, name_prefix=self.filepath.stem
            ):
                elements.append(self._image_element(img, gen_elem_id(), page_index))

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

    def parse_document(self, total_pages, image_output_dir=None) -> dict:
        """Parse all pages sequentially and combine into the document envelope."""
        pages, errors = [], []
        for idx in range(total_pages):
            try:
                pages.append(self.parse_page(idx, image_output_dir))
            except Exception as e:  # noqa: BLE001 - per-page isolation
                errors.append(str(e))
        return self.combine(total_pages, pages, errors)

    def combine(self, total_pages, pages, errors) -> dict:
        """Wrap parsed pages in the unified document envelope."""
        return {
            "document": {
                "source": str(self.filepath),
                "total_pages": total_pages,
                "processing_id": f"proc_py_{int(time.time())}",
                "pipeline_type": PIPELINE_TYPE,
                "errors": [e for e in errors] if errors else None,
                "pages": pages,
            }
        }

    @staticmethod
    def _text_element(block, elem_id, page_index) -> dict:
        """Serialize a TextBlock to the unified text element dict."""
        return {
            "id": elem_id, "type": block.classification, "content": block.text, "page": page_index + 1,
            "position": block.bbox.to_dict(), "confidence": 1.0,
            "metadata": {"font": block.font, "font_size": block.font_size, "is_bold": block.is_bold,
                         "is_italic": block.is_italic, "reading_order": block.reading_order},
        }

    @staticmethod
    def _ocr_element(block, elem_id, page_index) -> dict:
        """Serialize an OcrBlock to the unified text element dict."""
        return {
            "id": elem_id, "type": "body_text", "content": block.text, "page": page_index + 1,
            "position": block.bbox.to_dict(), "confidence": block.confidence / 100.0,
            "metadata": {"ocr_engine": "tesseract", "word_count": block.word_count},
        }

    @staticmethod
    def _table_element(table, elem_id, page_index) -> dict:
        """Serialize a TableBlock to the unified table element dict."""
        return {
            "id": elem_id, "type": "table", "content": table.data, "page": page_index + 1,
            "position": table.bbox.to_dict(), "confidence": 1.0,
            "metadata": {"rows": table.rows, "columns": table.columns, "has_header_row": table.has_header_row},
        }

    @staticmethod
    def _image_element(img, elem_id, page_index) -> dict:
        """Serialize an ImageBlock to the unified image element dict."""
        return {
            "id": elem_id, "type": "image", "content": None, "page": page_index + 1,
            "position": img.bbox.to_dict(), "confidence": 1.0,
            "metadata": {"file_path": img.file_path, "format": img.fmt,
                         "width_px": img.width_px, "height_px": img.height_px},
        }


class ParsedDocument:
    """A parsed unified-schema document with element-level operations."""

    def __init__(self, document: dict):
        """Wrap a parsed document dict (operations mutate it in place).

        Args:
            document (dict): A ``{"document": {...}}`` parsed document.
        """
        self.document = document

    def to_dict(self) -> dict:
        """Return the underlying document dict."""
        return self.document

    def flatten(self) -> list:
        """Flatten the document into one scalar record per element."""
        records = []
        for page in self.document.get("document", {}).get("pages", []):
            for elem in page.get("elements", []):
                pos = elem.get("position", {})
                content = elem.get("content")
                content_str = (
                    content if isinstance(content, str) else (json.dumps(content) if content is not None else None)
                )
                records.append(
                    {
                        "id": elem.get("id"), "type": elem.get("type"), "page": elem.get("page"),
                        "x": float(pos.get("x", 0.0)), "y": float(pos.get("y", 0.0)),
                        "w": float(pos.get("w", 0.0)), "h": float(pos.get("h", 0.0)),
                        "confidence": float(elem.get("confidence", 0.0)),
                        "content": content_str, "metadata": json.dumps(elem.get("metadata") or {}),
                    }
                )
        return records

    def dedupe_images(self) -> int:
        """Collapse byte-identical extracted image files; return count removed."""
        seen, removed = {}, 0
        for page in self.document.get("document", {}).get("pages", []):
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
                    continue
                meta["file_path"] = first
                try:
                    os.remove(path)
                except OSError:
                    pass
                else:
                    removed += 1
        return removed

    def drop_layout_tables(self, min_rows: int = 2, min_cols: int = 2) -> int:
        """Drop table elements below ``min_rows`` x ``min_cols`` (layout boxes)."""
        removed = 0
        for page in self.document.get("document", {}).get("pages", []):
            kept = []
            for elem in page.get("elements", []):
                if elem.get("type") == "table":
                    meta = elem.get("metadata") or {}
                    if meta.get("rows", 0) < min_rows or meta.get("columns", 0) < min_cols:
                        removed += 1
                        continue
                kept.append(elem)
            page["elements"] = kept
        return removed


def parse_page(pdf_path, page_index, image_output_dir=None, backend=None, table_extractor=None):
    """Deprecated shim: use DocumentAssembler. Parse one page to the unified schema."""
    return DocumentAssembler(pdf_path, backend, table_extractor).parse_page(page_index, image_output_dir)


def parse_document(pdf_path, total_pages, image_output_dir=None, backend=None, table_extractor=None):
    """Deprecated shim: use DocumentAssembler.parse_document."""
    return DocumentAssembler(pdf_path, backend, table_extractor).parse_document(total_pages, image_output_dir)


def combine_pages(source, total_pages, pages, errors) -> dict:
    """Deprecated shim: use DocumentAssembler.combine."""
    return DocumentAssembler(source).combine(total_pages, pages, errors)


def flatten_document_elements(document: dict) -> list:
    """Deprecated shim: use ParsedDocument.flatten."""
    return ParsedDocument(document).flatten()


def dedupe_document_images(document: dict) -> int:
    """Deprecated shim: use ParsedDocument.dedupe_images."""
    return ParsedDocument(document).dedupe_images()


def drop_layout_tables(document: dict, min_rows: int = 2, min_cols: int = 2) -> int:
    """Deprecated shim: use ParsedDocument.drop_layout_tables."""
    return ParsedDocument(document).drop_layout_tables(min_rows, min_cols)


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
