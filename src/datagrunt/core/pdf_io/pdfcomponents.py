"""PDF component assembly: page parsing, document combination, flattening."""

# standard library
import json
import os
import time
from functools import cached_property
from pathlib import Path

# local libraries
from datagrunt.core.file_io import FileProperties
from datagrunt.core.pdf_io.extraction import PdfPlumberTableExtractor
from datagrunt.core.pdf_io.extraction.config import _PDFExtractionConfig
from datagrunt.core.pdf_io.extraction.image_dedupe import dedupe_image_files
from datagrunt.core.pdf_io.extraction.layout_sorter import ElementAdapter, PageLayoutSorter
from datagrunt.core.pdf_io.extraction.markdown_escape import (
    escape_leading_markdown,
    escape_markdown_link_target,
    escape_markdown_link_text,
)
from datagrunt.core.pdf_io.extraction.ocr import dpi_for_page
from datagrunt.core.pdf_io.extraction.pdfium_native import PdfiumNativeReader
from datagrunt.core.pdf_io.extraction.pymupdf_backend import PyMuPDFBackend
from datagrunt.core.pdf_io.extraction.shapes import TableBlock

PIPELINE_TYPE = "pure_python_local_v1"


class DocumentAssembler:
    """Assemble a unified-schema document from an extraction backend + tables.

    Holds the extraction backend (defaults to ``PyMuPDFBackend``) and the shared
    ``PdfPlumberTableExtractor``, and turns per-page extraction results into the
    unified element dicts.
    """

    def __init__(self, filepath, backend=None, table_extractor=None, extraction_config=None):
        """Initialize the assembler.

        Args:
            filepath (str or Path): Path to the PDF file.
            backend (ExtractionBackend, optional): Defaults to PyMuPDFBackend.
            table_extractor (PdfPlumberTableExtractor, optional): Shared table source.
            extraction_config (_PDFExtractionConfig, optional): Tunables passed to
                the default backend; defaults to ``_PDFExtractionConfig()``.
        """
        self.filepath = Path(filepath)
        self._extraction_config = extraction_config or _PDFExtractionConfig()
        self.backend = backend or PyMuPDFBackend(filepath, extraction_config=self._extraction_config)
        self.table_extractor = table_extractor or PdfPlumberTableExtractor(filepath)

    def parse_page(self, page_index, image_output_dir=None) -> dict:
        """Parse a single page into the unified element schema."""
        with self.backend, self.table_extractor:
            analysis, text_blocks, images = self.backend.extract_page(
                page_index, output_dir=image_output_dir, name_prefix=self.filepath.stem
            )
            elements = []
            counter = {"n": 1}

            def gen_elem_id():
                elem_id = f"elem_{page_index + 1:02d}_{counter['n']:03d}"
                counter["n"] += 1
                return elem_id

            tables: list[TableBlock] = []
            if analysis.has_line_drawings or not analysis.has_text_layer:
                tables = self.table_extractor.extract(page_index)

            def is_inside_table(bbox) -> bool:
                tol = 3.0
                for t in tables:
                    tb = t.bbox
                    if (
                        bbox.x >= tb.x - tol
                        and bbox.y >= tb.y - tol
                        and (bbox.x + bbox.w) <= (tb.x + tb.w) + tol
                        and (bbox.y + bbox.h) <= (tb.y + tb.h) + tol
                    ):
                        return True
                return False

            warnings = []
            if analysis.has_text_layer:
                for block in text_blocks:
                    if not is_inside_table(block.bbox):
                        elements.append(self._text_element(block, gen_elem_id(), page_index))
            elif analysis.is_scanned:
                try:
                    ocr_blocks = self.backend.ocr_page(
                        page_index, dpi=dpi_for_page(analysis.width, analysis.height, self._extraction_config)
                    )
                except Exception as exc:  # noqa: BLE001 - soft per-category failure
                    # OCR failed (e.g. missing tesseract). Keep the page with its
                    # already-extracted images/tables rather than dropping it, and
                    # record a page-level warning. See base.py soft-failure contract.
                    ocr_blocks = []
                    warnings.append(f"OCR failed: {exc}")
                for block in ocr_blocks:
                    if not is_inside_table(block.bbox):
                        elements.append(self._ocr_element(block, gen_elem_id(), page_index))

            for table in tables:
                elements.append(self._table_element(table, gen_elem_id(), page_index))

            for img in images:
                elements.append(self._image_element(img, gen_elem_id(), page_index))

            classification = "mixed"
            if analysis.is_scanned:
                classification = "scanned"
            elif analysis.has_text_layer and analysis.image_count == 0 and not tables:
                classification = "text_only"

            page_dict = {
                "page_number": page_index + 1,
                "width": float(analysis.width),
                "height": float(analysis.height),
                "classification": classification,
                "elements": PageLayoutSorter(ElementAdapter()).sort(elements),
            }
            if warnings:
                page_dict["warnings"] = warnings
            return page_dict

    def parse_document(self, total_pages, image_output_dir=None) -> dict:
        """Parse all pages sequentially and combine into the document envelope."""
        pages, errors = [], []
        with self.backend, self.table_extractor:
            for idx in range(total_pages):
                try:
                    pages.append(self.parse_page(idx, image_output_dir))
                except Exception as e:  # noqa: BLE001 - per-page isolation
                    errors.append(f"Page {idx + 1}: {e}")
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
            "id": elem_id,
            "type": block.classification,
            "content": block.text,
            "page": page_index + 1,
            "position": block.bbox.to_dict(),
            "confidence": 1.0,
            "metadata": {
                "font": block.font,
                "font_size": block.font_size,
                "is_bold": block.is_bold,
                "is_italic": block.is_italic,
                "reading_order": block.reading_order,
            },
        }

    @staticmethod
    def _ocr_element(block, elem_id, page_index) -> dict:
        """Serialize an OcrBlock to the unified text element dict."""
        return {
            "id": elem_id,
            "type": "body_text",
            "content": block.text,
            "page": page_index + 1,
            "position": block.bbox.to_dict(),
            "confidence": block.confidence / 100.0,
            "metadata": {"ocr_engine": "tesseract", "word_count": block.word_count},
        }

    @staticmethod
    def _table_element(table, elem_id, page_index) -> dict:
        """Serialize a TableBlock to the unified table element dict."""
        return {
            "id": elem_id,
            "type": "table",
            "content": table.data,
            "page": page_index + 1,
            "position": table.bbox.to_dict(),
            "confidence": 1.0,
            "metadata": {"rows": table.rows, "columns": table.columns, "has_header_row": table.has_header_row},
        }

    @staticmethod
    def _image_element(img, elem_id, page_index) -> dict:
        """Serialize an ImageBlock to the unified image element dict."""
        return {
            "id": elem_id,
            "type": "image",
            "content": None,
            "page": page_index + 1,
            "position": img.bbox.to_dict(),
            "confidence": 1.0,
            "metadata": {
                "file_path": img.file_path,
                "format": img.fmt,
                "width_px": img.width_px,
                "height_px": img.height_px,
            },
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

    def dedupe_images(self, image_output_dir: str | None = None) -> int:
        """Collapse byte-identical extracted image files; return count removed.

        Args:
            image_output_dir: Directory holding the images written by this run.
                Only files resolving inside it are eligible for deletion (see
                issue #101); when ``None`` no file is removed from disk.
        """
        images = [
            el
            for page in self.document.get("document", {}).get("pages", [])
            for el in page.get("elements", [])
            if el.get("type") == "image"
        ]
        return dedupe_image_files(
            images,
            lambda el: (el.get("metadata") or {}).get("file_path"),
            lambda el, p: el.setdefault("metadata", {}).__setitem__("file_path", p),
            allowed_dir=image_output_dir,
        )

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

    def to_markdown(self, export_filename=None) -> str:
        """Render the unified structured elements of the document into Markdown."""
        blocks = []
        heading_map = {"header": "# ", "subheader": "## "}
        for page in self.document.get("document", {}).get("pages", []):
            elements = page.get("elements", [])
            for el in elements:
                etype = el.get("type")
                content = el.get("content")
                meta = el.get("metadata") or {}
                if etype in heading_map:
                    # Keep the intentional heading marker, but escape the
                    # element's own content so it cannot inject a second one.
                    blocks.append(f"{heading_map[etype]}{escape_leading_markdown(content)}")
                elif etype == "caption":
                    blocks.append(f"*{escape_leading_markdown(content)}*")
                elif etype == "table":
                    blocks.append(self._render_table(content, meta.get("has_header_row", False)))
                elif etype == "image":
                    path = meta.get("file_path") or ""
                    if export_filename and path:
                        try:
                            md_dir = Path(export_filename).parent.resolve()
                            abs_img_path = Path(path).resolve()
                            path = os.path.relpath(abs_img_path, md_dir)
                        except (OSError, ValueError):
                            # resolve()/relpath can fail (unresolvable path, or a
                            # cross-drive relpath on Windows); keep the original path.
                            pass
                    alt = escape_markdown_link_text(Path(path).name or "image")
                    blocks.append(f"![{alt}]({escape_markdown_link_target(path)})")
                elif isinstance(content, str) and content.strip():
                    blocks.append(escape_leading_markdown(content))
        return "\n\n".join(blocks) + "\n"

    @staticmethod
    def _render_table(content: list, has_header: bool) -> str:
        if not content:
            return ""

        def cell_str(val):
            text = "" if val is None else str(val)
            return text.replace("\n", " ").replace("|", "\\|").strip()

        rows = [[cell_str(c) for c in row] for row in content]
        width = max(len(r) for r in rows)
        rows = [r + [""] * (width - len(r)) for r in rows]
        if has_header:
            header, body = rows[0], rows[1:]
        else:
            header, body = [""] * width, rows
        lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(["---"] * width) + " |"]
        lines += ["| " + " | ".join(r) + " |" for r in body]
        return "\n".join(lines)


def document_is_structured(document: dict) -> bool:
    """True if the document uses the unified ``elements`` schema (vs native).

    The unified schema (pymupdf, or pdfium in structured mode) carries an
    ``elements`` list per page; the lean native pdfium schema does not. A single
    detector keeps every schema-dependent dispatch (flatten, dedupe, image
    collection) consistent.
    """
    return any("elements" in pg for pg in document.get("document", {}).get("pages", []))


def flatten_document(document: dict) -> list:
    """Flatten a parsed document into one record per element, dispatching on schema."""
    if document_is_structured(document):
        return ParsedDocument(document).flatten()
    return PdfiumNativeReader.flatten(document)


def dedupe_document_images(document: dict, image_output_dir: str) -> None:
    """Collapse byte-identical image files in a parsed document, dispatching on schema."""
    if document_is_structured(document):
        ParsedDocument(document).dedupe_images(image_output_dir=image_output_dir)
    else:
        PdfiumNativeReader.dedupe_images(document, image_output_dir=image_output_dir)


def write_document_json(document: dict, filename: str) -> str:
    """Serialize *document* to a JSON file at *filename* and return *filename*.

    The file is written with ``indent=2`` to match the historical engine output
    exactly. This is the single source of truth for JSON serialization — both
    the engine base class and the ``PDFWriter`` ``_parsed_dict`` path delegate
    here so the byte output is identical regardless of call site.

    Args:
        document (dict): A parsed document dict (unified or native schema).
        filename (str): Destination file path.

    Returns:
        str: The same *filename* that was written.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(document, f, indent=2)
    return filename


def write_document_jsonl(document: dict, filename: str) -> str:
    """Flatten *document* and write one JSON record per line to *filename*.

    Dispatches via ``flatten_document`` so both the unified and native pdfium
    schemas produce the correct flat records. Byte output is identical to the
    historical per-engine implementation (``json.dumps(record) + "\\n"`` per
    line, no trailing blank line).

    Args:
        document (dict): A parsed document dict (unified or native schema).
        filename (str): Destination file path.

    Returns:
        str: The same *filename* that was written.
    """
    records = flatten_document(document)
    with open(filename, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    return filename


def write_document_markdown(document: dict, filename: str) -> str:
    """Render *document* to Markdown and write it to *filename*.

    Dispatches on schema via ``document_is_structured``:

    - **Structured** (unified element schema from pymupdf or pdfium structured
      mode) → ``ParsedDocument.to_markdown``, which resolves image paths
      relative to *filename*'s directory.
    - **Native** (lean pdfium schema without ``elements`` per page) →
      ``PdfiumNativeReader.to_markdown``, which concatenates the page-level
      ``text`` fields.

    This is the single source of truth for Markdown serialization.

    Args:
        document (dict): A parsed document dict (unified or native schema).
        filename (str): Destination file path.

    Returns:
        str: The same *filename* that was written.
    """
    if document_is_structured(document):
        text = ParsedDocument(document).to_markdown(export_filename=filename)
    else:
        text = PdfiumNativeReader.to_markdown(document)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)
    return filename


def collect_image_paths(document: dict) -> list:
    """Return unique image file paths in document order, dispatching on schema."""
    structured = document_is_structured(document)
    paths = []
    seen = set()
    for page in document.get("document", {}).get("pages", []):
        if structured:
            candidates = [
                (el.get("metadata") or {}).get("file_path")
                for el in page.get("elements", [])
                if el.get("type") == "image"
            ]
        else:
            candidates = [img.get("file") for img in page.get("images", [])]
        for fp in candidates:
            if fp and fp not in seen:
                seen.add(fp)
                paths.append(fp)
    return paths


class PDFComponents(FileProperties):
    """A class that combines PDF components into a single interface."""

    # Guardrail for untrusted ``.json`` document paths (full file is loaded into memory).
    MAX_JSON_DOCUMENT_BYTES = 50 * 1024 * 1024

    def __init__(self, filepath):
        """Initialize the PDFComponents object.

        Args:
            filepath (str, Path, or dict): Path to the PDF/JSON file, or parsed document dict.
        """
        self._parsed_dict = None
        if isinstance(filepath, dict):
            # In-memory parsed document: not a file on disk, so there is always
            # content (never empty/blank).
            self._parsed_dict = filepath
            self._apply_virtual_file_properties(
                filepath=Path("in_memory.json"), size_in_bytes=0, is_empty=False, is_blank=False
            )
        else:
            filepath_path = Path(filepath)
            if filepath_path.suffix.lower() == ".json":
                size_in_bytes = filepath_path.stat().st_size
                if size_in_bytes > self.MAX_JSON_DOCUMENT_BYTES:
                    raise ValueError(
                        f"JSON document exceeds maximum size of "
                        f"{self.MAX_JSON_DOCUMENT_BYTES} bytes ({size_in_bytes} bytes)"
                    )
                with open(filepath_path, "r", encoding="utf-8") as f:
                    self._parsed_dict = json.load(f)
                # Untrusted JSON: a top-level non-object (list/number/string/null)
                # would otherwise raise a bare AttributeError deep in .get(...)
                # at total_pages / flatten paths. Fail fast with a clear message.
                if not isinstance(self._parsed_dict, dict):
                    raise ValueError(
                        f"Expected a JSON object for the parsed document, got {type(self._parsed_dict).__name__}"
                    )
                self._apply_virtual_file_properties(
                    filepath=filepath_path,
                    size_in_bytes=size_in_bytes,
                    is_empty=size_in_bytes == 0,
                    is_blank=size_in_bytes <= 100,
                )
            else:
                super().__init__(filepath_path)

    def _apply_virtual_file_properties(self, *, filepath, size_in_bytes, is_empty, is_blank):
        """Populate the full ``FileProperties`` surface for non-PDF inputs.

        A parsed-document dict or a ``.json`` document file is not a PDF backed
        by the helper objects ``FileProperties.__init__`` builds (``_stats`` /
        ``_ext`` / ``_empty`` / ``_blank``), so the inherited ``cached_property``
        flags cannot delegate. Set every public ``FileProperties`` attribute
        here - in one place shared by both virtual entry points - so the two
        cannot diverge and a newly added ``FileProperties`` attribute has a
        single mirror site (guarded by the attribute-parity test). The flag
        values describe a parsed document, not the real ``.json``/dict extension.

        Args:
            filepath (Path): The (possibly synthetic) source path.
            size_in_bytes (int): Source size; the kB/MB/GB/TB tiers derive from it.
            is_empty (bool): Whether the source has no content.
            is_blank (bool): Whether the source is effectively blank.
        """
        self.filepath = Path(filepath)
        self.filename = self.filepath.name
        self.extension = self.filepath.suffix
        self.extension_string = self.extension.replace(".", "")

        self.size_in_bytes = size_in_bytes
        self.size_in_kb = round(size_in_bytes / 1000.0, 5)
        self.size_in_mb = round(self.size_in_kb / 1000.0, 5)
        self.size_in_gb = round(self.size_in_mb / 1000.0, 5)
        self.size_in_tb = round(self.size_in_gb / 1000.0, 5)

        self.is_empty = is_empty
        self.is_blank = is_blank

        # A parsed document is treated as structured, semi-structured, standard
        # content and never as any concrete file format or a large/tabular file.
        self.is_structured = True
        self.is_semi_structured = True
        self.is_unstructured = False
        self.is_standard = True
        self.is_proprietary = False
        self.is_csv = False
        self.is_pdf = False
        self.is_excel = False
        self.is_apache = False
        self.is_parquet = False
        self.is_large = False
        self.is_tabular = False
        self.is_tsv = False

    @cached_property
    def total_pages(self):
        """Return the total number of pages in the PDF/JSON."""
        if self._parsed_dict is not None:
            pages = self._parsed_dict.get("document", {}).get("pages", [])
            return len(pages)
        # Count pages with the pymupdf backend rather than pdfium: pdfium cannot
        # load zero-page PDFs that pymupdf handles fine, and pymupdf reports the
        # same count for valid PDFs, so the pdfium engine path stays correct
        # while page counting no longer depends on pdfium (see issue #95).
        return PyMuPDFBackend(self.filepath).page_count()
